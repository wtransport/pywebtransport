"""WebTransport connection."""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self, cast

from pywebtransport._controller.controller import EndpointController
from pywebtransport._controller.events import (
    ProtocolEvent,
    UserCloseConnection,
    UserCloseConnectionGracefully,
    UserCreateSession,
    UserCreateSessionOptimistic,
    UserDiagnoseConnection,
)
from pywebtransport.config import ClientConfig, ServerConfig
from pywebtransport.constants import ErrorCodes
from pywebtransport.events import EventEmitter
from pywebtransport.exceptions import ConnectionError, SessionError, StreamError, TimeoutError
from pywebtransport.session import WebTransportSession
from pywebtransport.stream import (
    WebTransportBidirectionalStream,
    WebTransportReceiveStream,
    WebTransportSendStream,
    WebTransportStream,
)
from pywebtransport.types import (
    Address,
    ConnectionHandle,
    ConnectionState,
    ErrorCode,
    EventType,
    Headers,
    SessionId,
    StreamDirection,
    StreamId,
)

__all__: list[str] = ["ConnectionDiagnostics", "WebTransportConnection"]

_logger = logging.getLogger(name=__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionDiagnostics:
    """Encapsulate connection diagnostic data."""

    close_code: ErrorCode | None
    close_reason: str | None
    closed_at: float | None
    connected_at: float | None
    connection_handle: ConnectionHandle
    handshake_completed: bool
    is_client: bool
    local_goaway_sent: bool
    peer_goaway_received: bool
    peer_initial_max_data: int
    peer_initial_max_streams_bidi: int
    peer_initial_max_streams_uni: int
    peer_max_datagram_frame_size: int | None
    peer_settings_received: bool
    state: ConnectionState


class WebTransportConnection:
    """Manage the high-level WebTransport connection."""

    __slots__ = (
        "__weakref__",
        "_handle",
        "_controller",
        "_config",
        "_is_client",
        "_incoming_sessions",
        "_is_closed",
        "_session_handles",
        "_stream_handles",
        "events",
    )

    def __init__(
        self,
        *,
        handle: ConnectionHandle,
        controller: EndpointController,
        config: ClientConfig | ServerConfig,
        is_client: bool,
    ) -> None:
        """Initialize the instance."""
        self._handle = handle
        self._controller = controller
        self._config = config
        self._is_client = is_client

        self._incoming_sessions: asyncio.Queue[WebTransportSession | None] = asyncio.Queue()
        self._is_closed = False
        self._session_handles: dict[SessionId, WebTransportSession] = {}
        self._stream_handles: dict[StreamId, WebTransportStream] = {}
        self.events = EventEmitter()

        _logger.debug("wt_connection create connection_handle=%d", self._handle)

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        error_code: ErrorCode = ErrorCodes.H3_NO_ERROR

        if isinstance(exc_value, BaseException) and not isinstance(exc_value, asyncio.CancelledError):
            error_code = getattr(exc_value, "error_code", ErrorCodes.H3_INTERNAL_ERROR)

        await self.close(error_code=error_code)

    @property
    def config(self) -> ClientConfig | ServerConfig:
        """Return the configuration."""
        return self._config

    @property
    def handle(self) -> ConnectionHandle:
        """Return the connection handle."""
        return self._handle

    @property
    def is_client(self) -> bool:
        """Return True if the connection is client-side."""
        return self._is_client

    @property
    def is_closed(self) -> bool:
        """Return True if the connection is closed."""
        return self._is_closed

    @property
    def remote_address(self) -> Address | None:
        """Return the remote address."""
        return self._controller.get_remote_address(handle=self.handle)

    @property
    def sessions(self) -> list[WebTransportSession]:
        """Return the sessions."""
        return list(self._session_handles.values())

    async def close(self, *, error_code: ErrorCode = ErrorCodes.H3_NO_ERROR, reason: str | None = None) -> None:
        """Terminate the connection."""
        if self._is_closed:
            return

        _logger.debug("wt_connection close connection_handle=%d", self.handle)

        try:
            async with asyncio.timeout(delay=self.config.close_timeout):
                await self._execute_request(
                    event_factory=lambda request_id: UserCloseConnection(
                        request_id=request_id, error_code=error_code, reason=reason
                    )
                )
        except asyncio.TimeoutError:
            pass
        except ConnectionError as e:
            if "channel closed" not in str(e):
                _logger.warning("wt_connection close failed connection_handle=%d err=%s", self.handle, e)
        except Exception as e:
            _logger.warning("wt_connection close failed connection_handle=%d err=%s", self.handle, e)

    async def close_gracefully(self) -> None:
        """Terminate the connection gracefully."""
        if self._is_closed:
            return

        _logger.debug("wt_connection drain connection_handle=%d", self.handle)

        try:
            async with asyncio.timeout(delay=self.config.close_timeout):
                await self._execute_request(
                    event_factory=lambda request_id: UserCloseConnectionGracefully(request_id=request_id)
                )

                for session in self.sessions:
                    if not session.is_closed:
                        await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except asyncio.TimeoutError:
            _logger.warning("wt_connection drain failed connection_handle=%d", self.handle)
        except ConnectionError as e:
            if "channel closed" not in str(e):
                _logger.warning("wt_connection drain failed connection_handle=%d err=%s", self.handle, e)
        except Exception as e:
            _logger.warning("wt_connection drain failed connection_handle=%d err=%s", self.handle, e)
        finally:
            await self.close()

    async def create_session(
        self,
        *,
        authority: str,
        path: str,
        headers: Headers | None = None,
        wt_available_protocols: list[str] | None = None,
    ) -> WebTransportSession:
        """Instantiate a session."""
        return await self._create_session(
            event_factory=lambda request_id: UserCreateSession(
                request_id=request_id,
                authority=authority,
                path=path,
                headers=headers if headers is not None else {},
                wt_available_protocols=wt_available_protocols,
            )
        )

    async def create_session_optimistic(
        self,
        *,
        authority: str,
        path: str,
        headers: Headers | None = None,
        wt_available_protocols: list[str] | None = None,
    ) -> WebTransportSession:
        """Initiate a session optimistically."""
        return await self._create_session(
            event_factory=lambda request_id: UserCreateSessionOptimistic(
                request_id=request_id,
                authority=authority,
                path=path,
                headers=headers if headers is not None else {},
                wt_available_protocols=wt_available_protocols,
            )
        )

    async def diagnose(self) -> ConnectionDiagnostics:
        """Diagnose the connection."""
        if self._is_closed:
            raise ConnectionError(
                message=f"wt_connection resolve failed connection_handle={self.handle}", connection_handle=self.handle
            )

        try:
            diagnostic_data: dict[str, Any] = await self._execute_request(
                event_factory=lambda request_id: UserDiagnoseConnection(request_id=request_id)
            )
        except (ConnectionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise ConnectionError.from_cause(
                message=f"wt_connection resolve failed connection_handle={self.handle}",
                cause=e,
                connection_handle=self.handle,
            ) from e

        return ConnectionDiagnostics(**diagnostic_data)

    async def incoming_sessions(self) -> AsyncGenerator[WebTransportSession, None]:
        """Yield incoming sessions until the connection is closed."""
        while True:
            session = await self._incoming_sessions.get()
            if session is None:
                self._incoming_sessions.put_nowait(None)
                return
            yield session

    async def _create_session(self, *, event_factory: Callable[[int], ProtocolEvent]) -> WebTransportSession:
        if not self.is_client:
            raise ConnectionError(message=f"wt_connection validate failed actual={self.is_client} expected=true")

        if self._is_closed:
            raise ConnectionError(
                message=f"wt_connection resolve failed connection_handle={self.handle}", connection_handle=self.handle
            )

        try:
            return cast(
                WebTransportSession,
                await self._execute_request(event_factory=event_factory, resolver=self._resolve_session_handle),
            )
        except (ConnectionError, asyncio.CancelledError):
            raise
        except asyncio.TimeoutError:
            raise TimeoutError(message=f"wt_session create failed connection_handle={self.handle}") from None
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session create failed connection_handle={self.handle}", cause=e
            ) from e

    async def _dispatch_final_event(
        self, *, emitter: EventEmitter, event_type: EventType, data: dict[str, Any]
    ) -> None:
        await emitter.emit(event_type=event_type, data=data)
        await emitter.close()

    async def _execute_request(
        self, *, event_factory: Callable[[int], ProtocolEvent], resolver: Callable[[Any], Any] | None = None
    ) -> Any:
        def dispatch(request_id: int) -> None:
            self._controller.send_protocol_event(
                handle=self.handle, request_id=request_id, event=event_factory(request_id)
            )

        return await self._controller.execute_request(dispatcher=dispatch, resolver=resolver)

    def _handle_closed(self) -> None:
        self._is_closed = True
        for session in self._session_handles.values():
            session._handle_closed(error_code=None)
        for stream in self._stream_handles.values():
            stream._handle_closed()
        self._session_handles.clear()
        self._stream_handles.clear()
        self._incoming_sessions.put_nowait(None)
        _logger.info("wt_connection close connection_handle=%d", self.handle)

    def _handle_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        try:
            if "connection" not in data:
                data["connection"] = weakref.proxy(self)

            if "connection_handle" in data:
                data["connection_handle"] = self.handle

            if event_type == EventType.CONNECTION_CLOSED:
                self._handle_closed()

            if event_type in (EventType.SESSION_PENDING, EventType.SESSION_READY, EventType.SESSION_REQUEST_RECEIVED):
                self._handle_session_event(event_type=event_type, data=data)

            if event_type in (
                EventType.SESSION_CLOSED,
                EventType.SESSION_DATA_BLOCKED,
                EventType.SESSION_DRAINING,
                EventType.SESSION_MAX_DATA_UPDATED,
                EventType.SESSION_MAX_STREAMS_BIDI_UPDATED,
                EventType.SESSION_MAX_STREAMS_UNI_UPDATED,
                EventType.SESSION_PENDING,
                EventType.SESSION_READY,
                EventType.SESSION_STREAMS_BLOCKED,
            ):
                self._route_session_event(event_type=event_type, data=data)
            elif event_type == EventType.STREAM_OPENED:
                self._handle_stream_event(event_type=event_type, data=data)
            elif event_type in (
                EventType.STREAM_CLOSED,
                EventType.STREAM_RESET_RECEIVED,
                EventType.STREAM_STOP_SENDING_RECEIVED,
            ):
                self._route_stream_event(event_type=event_type, data=data)

            if event_type == EventType.CONNECTION_CLOSED:
                asyncio.create_task(
                    coro=self._dispatch_final_event(emitter=self.events, event_type=event_type, data=data)
                )
            else:
                self.events.emit_nowait(event_type=event_type, data=data)

        except Exception as e:
            raise ConnectionError.from_cause(
                message=f"wt_connection receive failed connection_handle={self.handle}",
                cause=e,
                connection_handle=self.handle,
            ) from e

    def _handle_session_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        session_id = data.get("session_id")
        if session_id is None:
            return

        create_handle = (not self.is_client and event_type == EventType.SESSION_REQUEST_RECEIVED) or (
            self.is_client and event_type in (EventType.SESSION_PENDING, EventType.SESSION_READY)
        )

        if create_handle and session_id not in self._session_handles:
            path = data.get("path")
            headers = data.get("headers")
            wt_available_protocols = data.get("wt_available_protocols")
            wt_protocol = data.get("wt_protocol")

            if path is not None and headers is not None:
                session = WebTransportSession(
                    session_id=session_id,
                    connection=self,
                    path=path,
                    headers=headers,
                    wt_available_protocols=wt_available_protocols,
                    wt_protocol=wt_protocol,
                )
                self._session_handles[session_id] = session
                data["session"] = session

                if event_type == EventType.SESSION_REQUEST_RECEIVED:
                    self._incoming_sessions.put_nowait(session)
            else:
                _logger.warning("wt_session validate failed session_id=%d", session_id)

    def _handle_stream_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        stream_id = data.get("stream_id")
        if stream_id is None:
            return

        session_id = data.get("session_id")
        direction = data.get("direction")
        is_peer_initiated = data.get("is_peer_initiated", False)

        if session_id is not None and direction is not None and stream_id not in self._stream_handles:
            session = self._session_handles.get(session_id)
            if session is not None:
                handle_class: type[WebTransportStream]
                match direction:
                    case StreamDirection.BIDIRECTIONAL:
                        handle_class = WebTransportBidirectionalStream
                    case StreamDirection.SEND_ONLY:
                        handle_class = WebTransportSendStream
                    case StreamDirection.RECEIVE_ONLY:
                        handle_class = WebTransportReceiveStream
                    case _:
                        _logger.warning("wt_stream validate invalid actual=%s stream_id=%d", direction, stream_id)
                        return

                new_stream = handle_class(stream_id=stream_id, session=session, is_peer_initiated=is_peer_initiated)
                self._stream_handles[stream_id] = new_stream
                data["stream"] = new_stream

                session._handle_stream_opened(stream=new_stream)
                session.events.emit_nowait(event_type=event_type, data=data)
            else:
                _logger.warning("wt_session resolve failed session_id=%d stream_id=%d", session_id, stream_id)

    def _resolve_session_handle(self, session_id: SessionId) -> WebTransportSession:
        session_handle = self._session_handles.get(session_id)
        if session_handle is None:
            raise SessionError(message=f"wt_session resolve failed session_id={session_id}", session_id=session_id)
        return session_handle

    def _resolve_stream_handle(self, stream_id: StreamId) -> WebTransportStream:
        stream_handle = self._stream_handles.get(stream_id)
        if stream_handle is None:
            raise StreamError(message=f"wt_stream resolve failed stream_id={stream_id}", stream_id=stream_id)
        return stream_handle

    def _route_session_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        session_id = data.get("session_id")
        if session_id is None:
            return

        session = self._session_handles.get(session_id)
        if session is not None:
            data["session"] = session

            if event_type == EventType.SESSION_READY:
                session._handle_ready(wt_protocol=data.get("wt_protocol"))

            if event_type == EventType.SESSION_CLOSED:
                self._session_handles.pop(session_id, None)
                session._handle_closed(error_code=data.get("error_code"))
                asyncio.create_task(
                    coro=self._dispatch_final_event(emitter=session.events, event_type=event_type, data=data)
                )
            else:
                session.events.emit_nowait(event_type=event_type, data=data)
        else:
            _logger.debug("wt_session resolve failed event=%s session_id=%d", event_type, session_id)

    def _route_stream_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        stream_id = data.get("stream_id")
        if stream_id is None:
            return

        stream = self._stream_handles.get(stream_id)
        if stream is not None:
            data["stream"] = stream

            if event_type == EventType.STREAM_CLOSED:
                self._stream_handles.pop(stream_id, None)
                stream._handle_closed()
                asyncio.create_task(
                    coro=self._dispatch_final_event(emitter=stream.events, event_type=event_type, data=data)
                )
            else:
                stream.events.emit_nowait(event_type=event_type, data=data)
        else:
            _logger.debug("wt_stream resolve failed event=%s stream_id=%d", event_type, stream_id)
