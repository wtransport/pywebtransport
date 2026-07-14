"""High-level abstraction for a WebTransport session."""

from __future__ import annotations

import asyncio
import http
import logging
import weakref
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, cast

from pywebtransport._protocol.events import (
    UserAcceptSession,
    UserCloseSession,
    UserCreateStream,
    UserExportKeyingMaterial,
    UserGetSessionDiagnostics,
    UserRejectSession,
    UserSendDatagram,
)
from pywebtransport.constants import ErrorCodes
from pywebtransport.events import Event, EventEmitter
from pywebtransport.exceptions import (
    ConnectionError,
    DatagramError,
    SessionClosedError,
    SessionError,
    StreamError,
    TimeoutError,
)
from pywebtransport.stream import WebTransportReceiveStream, WebTransportSendStream, WebTransportStream
from pywebtransport.types import Address, Buffer, EventType, Headers, SessionId, SessionState, StreamId

if TYPE_CHECKING:
    from pywebtransport.connection import WebTransportConnection

__all__: list[str] = ["SessionDiagnostics", "WebTransportSession"]

_logger = logging.getLogger(name=__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class SessionDiagnostics:
    """Encapsulate session diagnostic data."""

    active_streams: list[StreamId]
    blocked_streams: list[StreamId]
    close_code: int | None
    close_reason: str | None
    closed_at: float | None
    created_at: float
    datagram_bytes_received: int
    datagram_bytes_sent: int
    datagrams_received: int
    datagrams_sent: int
    flow_control_negotiated: bool
    headers: Headers
    is_client: bool
    local_data_consumed: int
    local_data_received: int
    local_data_sent: int
    local_max_data: int
    local_max_streams_bidi: int
    local_max_streams_uni: int
    local_streams_bidi_opened: int
    local_streams_uni_opened: int
    path: str
    peer_max_data: int
    peer_max_streams_bidi: int
    peer_max_streams_uni: int
    peer_streams_bidi_closed: int
    peer_streams_bidi_opened: int
    peer_streams_uni_closed: int
    peer_streams_uni_opened: int
    pending_bidi_stream_requests: list[int]
    pending_uni_stream_requests: list[int]
    ready_at: float | None
    session_id: SessionId
    state: SessionState
    wt_available_protocols: list[str] | None
    wt_protocol: str | None


class WebTransportSession:
    """Manage the high-level WebTransport session."""

    __slots__ = (
        "__weakref__",
        "_cached_state",
        "_connection",
        "_headers",
        "_incoming_bidi_streams",
        "_incoming_uni_streams",
        "_path",
        "_session_id",
        "_wt_available_protocols",
        "_wt_protocol",
        "events",
    )

    def __init__(
        self,
        *,
        connection: WebTransportConnection,
        session_id: SessionId,
        path: str,
        headers: Headers,
        wt_available_protocols: list[str] | None = None,
        wt_protocol: str | None = None,
    ) -> None:
        """Initialize the instance."""
        self._connection = weakref.ref(connection)
        self._session_id = session_id
        self._path = path
        self._headers = headers
        self._wt_available_protocols = wt_available_protocols
        self._wt_protocol = wt_protocol

        self._cached_state = SessionState.CONNECTING
        self._incoming_bidi_streams: asyncio.Queue[WebTransportStream | None] = asyncio.Queue()
        self._incoming_uni_streams: asyncio.Queue[WebTransportReceiveStream | None] = asyncio.Queue()

        self.events = EventEmitter(
            max_listeners=connection.config.max_event_listeners,
            max_history=connection.config.event_history_capacity,
            max_queue_size=connection.config.event_queue_capacity,
        )

        self.events.on(event_type=EventType.SESSION_READY, handler=self._on_session_ready)
        self.events.on(event_type=EventType.STREAM_OPENED, handler=self._enqueue_stream)
        self.events.on(event_type=EventType.SESSION_CLOSED, handler=self._on_session_closed)

        _logger.debug("wt_session create session_id=%d", self._session_id)

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        await self.close()

    @property
    def headers(self) -> Headers:
        """Return the initial request headers for this session."""
        return self._headers.copy()

    @property
    def is_closed(self) -> bool:
        """Return True if the session is closed."""
        return self._cached_state == SessionState.CLOSED

    @property
    def path(self) -> str:
        """Return the request path associated with this session."""
        return self._path

    @property
    def remote_address(self) -> Address | None:
        """Return the remote address of the peer."""
        connection = self._connection()
        if connection is not None:
            return connection.remote_address
        return None

    @property
    def session_id(self) -> SessionId:
        """Return the unique identifier for this session."""
        return self._session_id

    @property
    def state(self) -> SessionState:
        """Return the current state of the session."""
        return self._cached_state

    @property
    def wt_available_protocols(self) -> list[str] | None:
        """Return the wt_available_protocols requested by the client."""
        return self._wt_available_protocols

    @property
    def wt_protocol(self) -> str | None:
        """Return the negotiated wt_protocol for this session."""
        return self._wt_protocol

    @wt_protocol.setter
    def wt_protocol(self, value: str | None) -> None:
        """Set the wt_protocol for this session before accepting."""
        self._wt_protocol = value

    async def accept(self) -> None:
        """Accept the incoming WebTransport session request."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection.execute_request(
                event_factory=lambda request_id: UserAcceptSession(
                    request_id=request_id, session_id=self.session_id, wt_protocol=self.wt_protocol
                )
            )
        except (ConnectionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session open failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

        self._cached_state = SessionState.CONNECTED

    async def accept_bidirectional_stream(self) -> WebTransportStream:
        """Accept the next incoming bidirectional stream."""
        stream = await self._incoming_bidi_streams.get()
        if stream is None:
            raise SessionClosedError()
        return stream

    async def accept_unidirectional_stream(self) -> WebTransportReceiveStream:
        """Accept the next incoming unidirectional stream."""
        stream = await self._incoming_uni_streams.get()
        if stream is None:
            raise SessionClosedError()
        return stream

    async def close(self, *, error_code: int = ErrorCodes.APP_NO_ERROR, reason: str | None = None) -> None:
        """Terminate the WebTransport session."""
        if self._cached_state == SessionState.CLOSED:
            return

        _logger.debug("wt_session close session_id=%d err=%s", self.session_id, error_code)

        connection = self._connection()
        if connection is None:
            return

        try:
            await connection.execute_request(
                event_factory=lambda request_id: UserCloseSession(
                    request_id=request_id, session_id=self.session_id, error_code=error_code, reason=reason
                )
            )
        except asyncio.CancelledError:
            pass
        except (ConnectionError, SessionError) as e:
            if "channel closed" not in str(e):
                _logger.warning("wt_session close failed session_id=%d err=%s", self.session_id, e)
        except Exception as e:
            _logger.warning("wt_session close failed session_id=%d err=%s", self.session_id, e)

    async def create_bidirectional_stream(self) -> WebTransportStream:
        """Instantiate a new bidirectional WebTransport stream."""
        stream = await self._create_stream_internal(is_unidirectional=False)
        if not isinstance(stream, WebTransportStream):
            raise StreamError(
                message=(
                    f"wt_stream validate invalid actual={type(stream).__name__} expected=bidirectional "
                    f"stream_id={stream.stream_id}"
                ),
                stream_id=stream.stream_id,
            )
        return stream

    async def create_unidirectional_stream(self) -> WebTransportSendStream:
        """Instantiate a new unidirectional WebTransport stream."""
        stream = await self._create_stream_internal(is_unidirectional=True)
        if not isinstance(stream, WebTransportSendStream) or isinstance(stream, WebTransportStream):
            raise StreamError(
                message=(
                    f"wt_stream validate invalid actual={type(stream).__name__} expected=send_only "
                    f"stream_id={stream.stream_id}"
                ),
                stream_id=stream.stream_id,
            )
        return stream

    async def diagnostics(self) -> SessionDiagnostics:
        """Retrieve diagnostic information about the session."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            diag_data: dict[str, Any] = await connection.execute_request(
                event_factory=lambda request_id: UserGetSessionDiagnostics(
                    request_id=request_id, session_id=self.session_id
                )
            )
        except (ConnectionError, SessionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session resolve failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

        diag_data["wt_available_protocols"] = self._wt_available_protocols
        return SessionDiagnostics(**diag_data)

    async def ensure_ready(self) -> None:
        """Wait until the session transitions out of the connecting state."""
        if self._cached_state == SessionState.CONNECTING:
            event = await self.events.wait_for(event_type=[EventType.SESSION_READY, EventType.SESSION_CLOSED])
            if event.type == EventType.SESSION_CLOSED:
                error_code = event.data.get("error_code") if isinstance(event.data, dict) else None
                raise SessionError(
                    message=f"wt_session open failed session_id={self.session_id}",
                    session_id=self.session_id,
                    error_code=error_code,
                )
        elif self._cached_state != SessionState.CONNECTED:
            raise SessionError(
                message=f"wt_session open failed session_id={self.session_id}", session_id=self.session_id
            )

    async def export_keying_material(self, *, label: str, context: Buffer, length: int) -> bytes:
        """Export TLS keying material for this session."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            return cast(
                bytes,
                await connection.execute_request(
                    event_factory=lambda request_id: UserExportKeyingMaterial(
                        request_id=request_id, session_id=self.session_id, label=label, context=context, length=length
                    )
                ),
            )
        except (ConnectionError, SessionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session resolve failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

    async def incoming_bidirectional_streams(self) -> AsyncGenerator[WebTransportStream, None]:
        """Yield incoming bidirectional streams until the session is closed."""
        try:
            while True:
                yield await self.accept_bidirectional_stream()
        except SessionClosedError:
            pass

    async def incoming_unidirectional_streams(self) -> AsyncGenerator[WebTransportReceiveStream, None]:
        """Yield incoming unidirectional streams until the session is closed."""
        try:
            while True:
                yield await self.accept_unidirectional_stream()
        except SessionClosedError:
            pass

    async def reject(self, *, status_code: int = http.HTTPStatus.FORBIDDEN) -> None:
        """Reject the incoming WebTransport session request."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection.execute_request(
                event_factory=lambda request_id: UserRejectSession(
                    request_id=request_id, session_id=self.session_id, status_code=status_code
                )
            )
        except asyncio.CancelledError:
            pass
        except ConnectionError as e:
            if "channel closed" not in str(e):
                _logger.warning("wt_session reject failed session_id=%d err=%s", self.session_id, e)
        except Exception as e:
            _logger.warning("wt_session reject failed session_id=%d err=%s", self.session_id, e)

        self._cached_state = SessionState.CLOSED
        self._incoming_bidi_streams.put_nowait(None)
        self._incoming_uni_streams.put_nowait(None)
        _logger.debug("wt_session reject session_id=%d err=%s", self.session_id, status_code)

    async def send_datagram(self, *, data: Buffer) -> None:
        """Transmit an unreliable datagram."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection.execute_request(
                event_factory=lambda request_id: UserSendDatagram(
                    request_id=request_id, session_id=self.session_id, data=data
                )
            )
        except (ConnectionError, DatagramError, SessionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session send failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

    async def _create_stream_internal(self, *, is_unidirectional: bool) -> WebTransportStream | WebTransportSendStream:
        """Execute internal logic for stream creation with timeout."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            timeout = connection.config.stream_creation_timeout
            async with asyncio.timeout(delay=timeout):
                stream_id = cast(
                    StreamId,
                    await connection.execute_request(
                        event_factory=lambda request_id: UserCreateStream(
                            request_id=request_id, session_id=self.session_id, is_unidirectional=is_unidirectional
                        )
                    ),
                )
        except (ConnectionError, SessionError, asyncio.CancelledError):
            raise
        except asyncio.TimeoutError:
            raise TimeoutError(message=f"wt_stream create failed session_id={self.session_id}") from None
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream create failed session_id={self.session_id}", cause=e
            ) from e

        stream_handle = connection._stream_handles.get(stream_id)
        if stream_handle is None:
            raise StreamError(message=f"wt_stream resolve failed stream_id={stream_id}", stream_id=stream_id)

        if not isinstance(stream_handle, (WebTransportStream, WebTransportSendStream)):
            raise StreamError(
                message=f"wt_stream validate invalid actual={type(stream_handle).__name__} stream_id={stream_id}",
                stream_id=stream_id,
            )

        return stream_handle

    def _enqueue_stream(self, event: Event) -> None:
        """Route incoming streams to the appropriate internal queues."""
        if not isinstance(event.data, dict):
            return

        stream = event.data.get("stream")
        if stream is None or not stream.is_remote:
            return

        if isinstance(stream, WebTransportStream):
            self._incoming_bidi_streams.put_nowait(stream)
        elif isinstance(stream, WebTransportReceiveStream):
            self._incoming_uni_streams.put_nowait(stream)

    def _on_session_closed(self, event: Event) -> None:
        """Handle the session closed event."""
        self._cached_state = SessionState.CLOSED
        self._incoming_bidi_streams.put_nowait(None)
        self._incoming_uni_streams.put_nowait(None)
        _logger.info("wt_session close session_id=%d", self.session_id)

    def _on_session_ready(self, event: Event) -> None:
        """Handle the session ready event."""
        self._cached_state = SessionState.CONNECTED
        if isinstance(event.data, dict):
            new_wt_protocol = event.data.get("wt_protocol")
            if new_wt_protocol is not None:
                self._wt_protocol = new_wt_protocol
        _logger.info("wt_session open session_id=%d", self._session_id)

    def __repr__(self) -> str:
        """Return the string representation."""
        return f"<{self.__class__.__name__} id={self.session_id} state={self.state}>"
