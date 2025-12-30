"""High-level abstraction for a WebTransport session."""

from __future__ import annotations

import asyncio
import weakref
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from pywebtransport._protocol.events import (
    UserCloseSession,
    UserCreateStream,
    UserGetSessionDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserSendDatagram,
)
from pywebtransport.constants import ErrorCodes
from pywebtransport.events import Event, EventEmitter
from pywebtransport.exceptions import ConnectionError, SessionError, StreamError, TimeoutError
from pywebtransport.stream import WebTransportSendStream, WebTransportStream
from pywebtransport.types import Address, Buffer, EventType, Headers, SessionId, SessionState, StreamId
from pywebtransport.utils import get_logger

if TYPE_CHECKING:
    from pywebtransport.connection import WebTransportConnection
    from pywebtransport.stream import StreamType


__all__: list[str] = ["SessionDiagnostics", "WebTransportSession"]

logger = get_logger(name=__name__)


@dataclass(kw_only=True)
class SessionDiagnostics:
    """A snapshot of session diagnostics."""

    session_id: SessionId
    state: SessionState
    path: str
    headers: Headers
    created_at: float
    local_max_data: int
    local_data_sent: int
    peer_max_data: int
    peer_data_sent: int
    local_max_streams_bidi: int
    local_streams_bidi_opened: int
    peer_max_streams_bidi: int
    peer_streams_bidi_opened: int
    local_max_streams_uni: int
    local_streams_uni_opened: int
    peer_max_streams_uni: int
    peer_streams_uni_opened: int
    pending_bidi_stream_requests: list[Any]
    pending_uni_stream_requests: list[Any]
    datagrams_sent: int
    datagram_bytes_sent: int
    datagrams_received: int
    datagram_bytes_received: int
    active_streams: list[StreamId]
    blocked_streams: list[StreamId]
    close_code: int | None
    close_reason: str | None
    closed_at: float | None
    ready_at: float | None


class WebTransportSession:
    """A high-level handle for a WebTransport session."""

    def __init__(
        self, *, connection: WebTransportConnection, session_id: SessionId, path: str, headers: Headers
    ) -> None:
        """Initialize the WebTransportSession handle."""
        self._connection = weakref.ref(connection)
        self._session_id = session_id
        self._path = path
        self._headers = headers
        self._cached_state = SessionState.CONNECTING

        config = connection.config
        self.events = EventEmitter(
            max_queue_size=config.max_event_queue_size,
            max_listeners=config.max_event_listeners,
            max_history=config.max_event_history_size,
        )

        self.events.on(event_type=EventType.SESSION_READY, handler=self._on_session_ready)
        self.events.on(event_type=EventType.SESSION_CLOSED, handler=self._on_session_closed)

        logger.debug("WebTransportSession handle created for session %s", self._session_id)

    @property
    def headers(self) -> Headers:
        """Get the initial request headers for this session."""
        return self._headers.copy()

    @property
    def is_closed(self) -> bool:
        """Return True if the session is closed."""
        return self._cached_state == SessionState.CLOSED

    @property
    def path(self) -> str:
        """Get the request path associated with this session."""
        return self._path

    @property
    def remote_address(self) -> Address | None:
        """Get the remote address of the peer."""
        connection = self._connection()
        if connection is not None:
            return connection.remote_address
        return None

    @property
    def session_id(self) -> SessionId:
        """Get the unique identifier for this session."""
        return self._session_id

    @property
    def state(self) -> SessionState:
        """Get the current state of the session."""
        return self._cached_state

    async def __aenter__(self) -> Self:
        """Enter async context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit async context, closing the session."""
        await self.close()

    async def close(self, *, error_code: int = ErrorCodes.NO_ERROR, reason: str | None = None) -> None:
        """Close the WebTransport session."""
        if self._cached_state == SessionState.CLOSED:
            return

        logger.info("Closing session %s: code=%#x reason='%s'", self.session_id, error_code, reason or "")
        connection = self._connection()
        if connection is None:
            return

        request_id, future = connection._protocol.create_request()
        event = UserCloseSession(
            request_id=request_id, session_id=self.session_id, error_code=error_code, reason=reason
        )
        connection._protocol.send_event(event=event)

        try:
            await future
        except (ConnectionError, SessionError) as e:
            logger.warning("Error initiating session close for %s: %s", self.session_id, e, exc_info=True)

    async def create_bidirectional_stream(self) -> WebTransportStream:
        """Create a new bidirectional WebTransport stream."""
        stream = await self._create_stream_internal(is_unidirectional=False)
        if not isinstance(stream, WebTransportStream):
            raise StreamError(f"Internal error: Expected bidirectional stream, got {type(stream).__name__}")
        return stream

    async def create_unidirectional_stream(self) -> WebTransportSendStream:
        """Create a new unidirectional (send-only) WebTransport stream."""
        stream = await self._create_stream_internal(is_unidirectional=True)
        if not isinstance(stream, WebTransportSendStream) or isinstance(stream, WebTransportStream):
            raise StreamError(f"Internal error: Expected unidirectional send stream, got {type(stream).__name__}")
        return stream

    async def diagnostics(self) -> SessionDiagnostics:
        """Get diagnostic information about the session."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserGetSessionDiagnostics(request_id=request_id, session_id=self.session_id)

        try:
            connection._protocol.send_event(event=event)
            diag_data: dict[str, Any] = await future
            return SessionDiagnostics(**diag_data)
        except ConnectionError as e:
            raise SessionError(f"Connection is closed, cannot get diagnostics: {e}") from e

    async def grant_data_credit(self, *, max_data: int) -> None:
        """Manually grant data flow control credit to the peer."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserGrantDataCredit(request_id=request_id, session_id=self.session_id, max_data=max_data)
        connection._protocol.send_event(event=event)
        await future

    async def grant_streams_credit(self, *, max_streams: int, is_unidirectional: bool) -> None:
        """Manually grant stream flow control credit to the peer."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserGrantStreamsCredit(
            request_id=request_id,
            session_id=self.session_id,
            max_streams=max_streams,
            is_unidirectional=is_unidirectional,
        )
        connection._protocol.send_event(event=event)
        await future

    async def send_datagram(self, *, data: Buffer | list[Buffer]) -> None:
        """Send an unreliable datagram."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserSendDatagram(request_id=request_id, session_id=self.session_id, data=data)
        connection._protocol.send_event(event=event)
        await future

    def _add_stream_handle(self, *, stream: StreamType, event_data: dict[str, Any]) -> None:
        """Register an incoming stream and re-emit the STREAM_OPENED event."""
        logger.debug("Session %s re-emitting STREAM_OPENED for stream %s", self.session_id, stream.stream_id)

        event_payload = event_data.copy()
        event_payload["stream"] = stream

        self.events.emit_nowait(event_type=EventType.STREAM_OPENED, data=event_payload)

    async def _create_stream_internal(self, *, is_unidirectional: bool) -> WebTransportStream | WebTransportSendStream:
        """Internal logic for creating a stream with timeout handling."""
        connection = self._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserCreateStream(request_id=request_id, session_id=self.session_id, is_unidirectional=is_unidirectional)
        connection._protocol.send_event(event=event)

        try:
            timeout = connection.config.stream_creation_timeout
            async with asyncio.timeout(delay=timeout):
                stream_id: StreamId = await future
        except asyncio.TimeoutError:
            logger.warning("Timeout creating stream on session %s", self.session_id)
            raise TimeoutError(f"Session {self.session_id} timed out creating stream after {timeout}s") from None
        except Exception:
            raise

        stream_handle = connection._stream_handles.get(stream_id)
        if stream_handle is None:
            logger.error("Internal error: Stream handle %d missing after creation", stream_id)
            raise StreamError(f"Internal error creating stream handle for {stream_id}")

        if not isinstance(stream_handle, (WebTransportStream, WebTransportSendStream)):
            raise StreamError(f"Invalid stream handle type for {stream_id}")

        return stream_handle

    def _on_session_closed(self, event: Event) -> None:
        """Handle session closed event to update cached state."""
        self._cached_state = SessionState.CLOSED

    def _on_session_ready(self, event: Event) -> None:
        """Handle session ready event to update cached state."""
        self._cached_state = SessionState.CONNECTED

    def __repr__(self) -> str:
        """Provide a developer-friendly representation."""
        return f"<WebTransportSession id={self.session_id} state={self._cached_state}>"
