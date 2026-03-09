"""Core WebTransport connection object representing a QUIC connection."""

from __future__ import annotations

import asyncio
import uuid
import weakref
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from pywebtransport._controller.controller import EndpointController
from pywebtransport._protocol.events import (
    ConnectionClose,
    UserConnectionGracefulClose,
    UserCreateSession,
    UserGetConnectionDiagnostics,
)
from pywebtransport.config import ClientConfig, ServerConfig
from pywebtransport.constants import ErrorCodes
from pywebtransport.events import EventEmitter
from pywebtransport.exceptions import ConnectionError, SessionError, TimeoutError
from pywebtransport.session import WebTransportSession
from pywebtransport.stream import WebTransportReceiveStream, WebTransportSendStream, WebTransportStream
from pywebtransport.types import (
    Address,
    ConnectionId,
    ConnectionState,
    EventType,
    Headers,
    SessionId,
    StreamDirection,
    StreamId,
)
from pywebtransport.utils import get_logger

__all__: list[str] = ["ConnectionDiagnostics", "WebTransportConnection"]

type _StreamHandle = WebTransportStream | WebTransportReceiveStream | WebTransportSendStream

_logger = get_logger(name=__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionDiagnostics:
    """Encapsulate connection diagnostic data."""

    connection_id: ConnectionId
    is_client: bool
    state: ConnectionState
    max_datagram_size: int
    remote_max_datagram_frame_size: int | None
    handshake_complete: bool
    peer_settings_received: bool
    local_goaway_sent: bool
    session_count: int
    stream_count: int
    pending_request_count: int
    early_event_count: int
    connected_at: float | None
    closed_at: float | None
    active_session_handles: int
    active_stream_handles: int


class WebTransportConnection:
    """Manage the high-level WebTransport connection over the shared multiplexing driver."""

    __slots__ = (
        "_config",
        "_controller",
        "_handle",
        "_is_client",
        "_connection_id",
        "_cached_state",
        "events",
        "_session_handles",
        "_stream_handles",
        "__weakref__",
    )

    def __init__(
        self,
        *,
        config: ClientConfig | ServerConfig,
        controller: EndpointController,
        handle: int,
        is_client: bool,
    ) -> None:
        """Initialize the instance."""
        self._config = config
        self._controller = controller
        self._handle = handle
        self._is_client = is_client

        self._connection_id: ConnectionId = str(uuid.uuid4())
        self._cached_state = ConnectionState.IDLE
        self.events = EventEmitter(
            max_queue_size=self._config.max_event_queue_size,
            max_listeners=self._config.max_event_listeners,
            max_history=self._config.max_event_history_size,
        )
        self._session_handles: dict[SessionId, WebTransportSession] = {}
        self._stream_handles: dict[StreamId, _StreamHandle] = {}

        self._controller.register_connection(handle=self._handle, callback=self._notify_owner)
        _logger.debug("WebTransportConnection %s initialized with handle %d.", self.connection_id, self._handle)

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        await self.close()

    @property
    def config(self) -> ClientConfig | ServerConfig:
        """Return the configuration associated with this connection."""
        return self._config

    @property
    def connection_id(self) -> ConnectionId:
        """Return the unique identifier for this connection."""
        return self._connection_id

    @property
    def is_client(self) -> bool:
        """Return True if this is a client-side connection."""
        return self._is_client

    @property
    def is_closed(self) -> bool:
        """Return True if the connection is closed."""
        return self.state == ConnectionState.CLOSED

    @property
    def is_closing(self) -> bool:
        """Return True if the connection is closing."""
        return self.state == ConnectionState.CLOSING

    @property
    def is_connected(self) -> bool:
        """Return True if the connection is established."""
        return self.state == ConnectionState.CONNECTED

    @property
    def local_addresses(self) -> list[Address]:
        """Return the local addresses of the connection."""
        return self._controller.get_local_addresses()

    @property
    def remote_address(self) -> Address | None:
        """Return the remote address of the connection."""
        if self.is_closed:
            return None
        return self._controller.get_remote_address(handle=self._handle)

    @property
    def state(self) -> ConnectionState:
        """Return the current state of the connection."""
        return self._cached_state

    @classmethod
    def accept(cls, *, controller: EndpointController, handle: int, config: ServerConfig) -> WebTransportConnection:
        """Instantiate a connection wrapper for an accepted server connection."""
        connection = cls(config=config, controller=controller, handle=handle, is_client=False)
        return connection

    async def close(self, *, error_code: int = ErrorCodes.NO_ERROR, reason: str = "Closed by application") -> None:
        """Terminate the WebTransport connection."""
        if self.state == ConnectionState.CLOSED:
            return

        _logger.info("Closing connection %s...", self.connection_id)

        try:
            request_id, future = self._controller._pending_manager.create_request()
            event = ConnectionClose(request_id=request_id, error_code=error_code, reason=reason)
            self._controller.send_user_event(handle=self._handle, event=event)

            try:
                async with asyncio.timeout(delay=self._config.close_timeout):
                    await future
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            except ConnectionError as e:
                if "Connection closed" in str(e):
                    _logger.debug("Connection closed while waiting for close confirmation: %s", e)
                else:
                    _logger.warning("Connection error during close: %s", e)
            except Exception as e:
                _logger.warning("Error during close event processing: %s", e)
        finally:
            _logger.info("Connection %s close process finished.", self.connection_id)

    async def create_session(self, *, path: str, headers: Headers | None = None) -> WebTransportSession:
        """Initiate a new WebTransport session."""
        if not self.is_client:
            raise ConnectionError("Sessions can only be created by the client.")

        request_id, future = self._controller._pending_manager.create_request()
        event = UserCreateSession(request_id=request_id, path=path, headers=headers if headers is not None else {})
        self._controller.send_user_event(handle=self._handle, event=event)

        try:
            session_id: SessionId = await future
        except ConnectionError:
            raise
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError as e:
            raise TimeoutError(f"Session creation timed out: {e}") from e
        except Exception as e:
            raise SessionError(f"Session creation failed: {e}") from e

        session_handle = self._session_handles.get(session_id)
        if session_handle is None:
            _logger.error("Internal error: Session handle %s missing after successful creation effect.", session_id)
            raise SessionError(f"Internal error creating session handle for {session_id}")

        return session_handle

    async def diagnostics(self) -> ConnectionDiagnostics:
        """Retrieve diagnostic information about the connection."""
        request_id, future = self._controller._pending_manager.create_request()
        event = UserGetConnectionDiagnostics(request_id=request_id)
        self._controller.send_user_event(handle=self._handle, event=event)

        diag_data: dict[str, Any] = await future
        diag_data["active_session_handles"] = len(self._session_handles)
        diag_data["active_stream_handles"] = len(self._stream_handles)
        return ConnectionDiagnostics(**diag_data)

    def get_all_sessions(self) -> list[WebTransportSession]:
        """Retrieve a list of all active session handles."""
        return list(self._session_handles.values())

    async def graceful_shutdown(self) -> None:
        """Initiate a graceful shutdown of the connection."""
        _logger.info("Initiating graceful shutdown for connection %s...", self.connection_id)

        request_id, future = self._controller._pending_manager.create_request()
        event = UserConnectionGracefulClose(request_id=request_id)
        self._controller.send_user_event(handle=self._handle, event=event)

        try:
            async with asyncio.timeout(delay=self._config.close_timeout):
                await future
        except asyncio.TimeoutError:
            _logger.warning("Timeout waiting for graceful shutdown GOAWAY confirmation.")
        except Exception as e:
            _logger.warning("Error during graceful shutdown: %s", e)

        await self.close(reason="Graceful shutdown complete")

    def _handle_session_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        """Process internal session-related events and manage handles."""
        session_id = data.get("session_id")
        if session_id is None:
            return

        create_handle = (not self.is_client and event_type == EventType.SESSION_REQUEST) or (
            self.is_client and event_type == EventType.SESSION_READY
        )

        if create_handle and session_id not in self._session_handles:
            path = data.get("path")
            headers = data.get("headers")

            if path is not None and headers is not None:
                session = WebTransportSession(connection=self, session_id=session_id, path=path, headers=headers)
                self._session_handles[session_id] = session
                _logger.debug("Created session handle for %s", session_id)
                data["session"] = session
            else:
                _logger.error("Missing metadata for session handle creation %s", session_id)

    def _handle_stream_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        """Process internal stream-related events and manage handles."""
        stream_id = data.get("stream_id")
        if stream_id is None:
            return

        if event_type == EventType.STREAM_OPENED:
            session_id = data.get("session_id")
            direction = data.get("direction")
            is_remote = data.get("is_remote", False)

            if session_id is not None and direction is not None and stream_id not in self._stream_handles:
                session = self._session_handles.get(session_id)
                if session is not None:
                    handle_class: type[_StreamHandle]
                    match direction:
                        case StreamDirection.BIDIRECTIONAL:
                            handle_class = WebTransportStream
                        case StreamDirection.SEND_ONLY:
                            handle_class = WebTransportSendStream
                        case StreamDirection.RECEIVE_ONLY:
                            handle_class = WebTransportReceiveStream
                        case _:
                            _logger.error("Unknown stream direction: %s", direction)
                            return

                    new_stream = handle_class(session=session, stream_id=stream_id, is_remote=is_remote)
                    self._stream_handles[stream_id] = new_stream
                    data["stream"] = new_stream

                    session.events.emit_nowait(event_type=event_type, data=data)
                else:
                    _logger.warning("Session %s not found for stream %d", session_id, stream_id)

        elif event_type == EventType.STREAM_CLOSED:
            stream = self._stream_handles.pop(stream_id, None)
            if stream is not None:
                data["stream"] = stream
                stream.events.emit_nowait(event_type=event_type, data=data)
                asyncio.create_task(coro=stream.events.close())

        elif event_type in (EventType.STOP_SENDING_RECEIVED, EventType.STREAM_RESET_RECEIVED):
            stream = self._stream_handles.get(stream_id)
            if stream is not None:
                data["stream"] = stream
                stream.events.emit_nowait(event_type=event_type, data=data)
            else:
                _logger.debug("Received %s for unknown or closed stream %d", event_type, stream_id)

    def _notify_owner(self, event_type: EventType, data: dict[str, Any]) -> None:
        """Process status events received from the low-level driver."""
        try:
            if "connection" not in data:
                data["connection"] = weakref.proxy(self)

            if "connection_id" in data:
                data["connection_id"] = self._connection_id

            if event_type == EventType.CONNECTION_ESTABLISHED:
                self._cached_state = ConnectionState.CONNECTED
            elif event_type == EventType.CONNECTION_CLOSED:
                self._cached_state = ConnectionState.CLOSED
                self._controller.unregister_connection(handle=self._handle)
                self._session_handles.clear()
                self._stream_handles.clear()

            if event_type in (EventType.SESSION_REQUEST, EventType.SESSION_READY):
                self._handle_session_event(event_type=event_type, data=data)

            if event_type in (
                EventType.SESSION_READY,
                EventType.SESSION_CLOSED,
                EventType.SESSION_DRAINING,
                EventType.SESSION_MAX_DATA_UPDATED,
                EventType.SESSION_MAX_STREAMS_BIDI_UPDATED,
                EventType.SESSION_MAX_STREAMS_UNI_UPDATED,
                EventType.SESSION_DATA_BLOCKED,
                EventType.SESSION_STREAMS_BLOCKED,
                EventType.DATAGRAM_RECEIVED,
            ):
                self._route_session_event(event_type=event_type, data=data)
            elif event_type in (
                EventType.STREAM_OPENED,
                EventType.STREAM_CLOSED,
                EventType.STOP_SENDING_RECEIVED,
                EventType.STREAM_RESET_RECEIVED,
            ):
                self._handle_stream_event(event_type=event_type, data=data)

            self.events.emit_nowait(event_type=event_type, data=data)

        except Exception as e:
            _logger.error("Error during owner notification callback: %s", e, exc_info=True)

    def _route_session_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        """Dispatch events to the appropriate session handle."""
        session_id = data.get("session_id")
        if session_id is None:
            return

        session = self._session_handles.get(session_id)
        if session is not None:
            data["session"] = session
            session.events.emit_nowait(event_type=event_type, data=data)

            if event_type == EventType.SESSION_CLOSED:
                self._session_handles.pop(session_id, None)
                asyncio.create_task(coro=session.events.close())

    def __repr__(self) -> str:
        """Return the string representation."""
        return f"<WebTransportConnection id={self.connection_id} state={self.state} client={self.is_client}>"
