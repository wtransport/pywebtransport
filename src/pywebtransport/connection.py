"""Core WebTransport connection object representing a QUIC connection."""

from __future__ import annotations

import asyncio
import uuid
import weakref
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from pywebtransport._adapter.client import create_quic_endpoint
from pywebtransport._protocol.events import (
    ConnectionClose,
    UserConnectionGracefulClose,
    UserCreateSession,
    UserGetConnectionDiagnostics,
)
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

if TYPE_CHECKING:
    from pywebtransport._adapter.client import WebTransportClientProtocol
    from pywebtransport._adapter.server import WebTransportServerProtocol
    from pywebtransport.config import ClientConfig, ServerConfig

    type AdapterProtocol = WebTransportServerProtocol | WebTransportClientProtocol

__all__: list[str] = ["ConnectionDiagnostics", "WebTransportConnection"]

logger = get_logger(name=__name__)

type StreamHandle = WebTransportStream | WebTransportReceiveStream | WebTransportSendStream


@dataclass(kw_only=True)
class ConnectionDiagnostics:
    """A snapshot of connection diagnostics."""

    connection_id: ConnectionId
    state: ConnectionState
    is_client: bool
    connected_at: float | None
    closed_at: float | None
    max_datagram_size: int
    remote_max_datagram_frame_size: int
    session_count: int
    stream_count: int
    active_session_handles: int
    active_stream_handles: int


class WebTransportConnection:
    """A high-level handle for a WebTransport connection over QUIC."""

    def __init__(
        self,
        *,
        config: ClientConfig | ServerConfig,
        protocol: AdapterProtocol,
        transport: asyncio.DatagramTransport,
        is_client: bool,
    ) -> None:
        """Initialize the WebTransport connection."""
        self._config = config
        self._protocol = protocol
        self._transport = transport
        self._is_client = is_client
        self._connection_id: ConnectionId = str(uuid.uuid4())
        self.events = EventEmitter(
            max_queue_size=config.max_event_queue_size,
            max_listeners=config.max_event_listeners,
            max_history=config.max_event_history_size,
        )
        self._cached_state = ConnectionState.IDLE

        self._protocol.set_status_callback(callback=self._notify_owner)

        self._session_handles: dict[SessionId, WebTransportSession] = {}
        self._stream_handles: dict[StreamId, StreamHandle] = {}

        logger.debug("WebTransportConnection %s initialized.", self.connection_id)

    @classmethod
    def accept(
        cls, *, transport: asyncio.DatagramTransport, protocol: AdapterProtocol, config: ServerConfig
    ) -> WebTransportConnection:
        """Static factory to wrap an accepted server connection."""
        connection = cls(config=config, protocol=protocol, transport=transport, is_client=False)
        return connection

    @classmethod
    async def connect(
        cls, *, host: str, port: int, config: ClientConfig, loop: asyncio.AbstractEventLoop | None = None
    ) -> WebTransportConnection:
        """Static factory to establish a client connection."""
        loop = loop or asyncio.get_running_loop()
        transport, protocol = await create_quic_endpoint(host=host, port=port, config=config, loop=loop)

        connection = cls(config=config, protocol=protocol, transport=transport, is_client=True)
        return connection

    @property
    def config(self) -> ClientConfig | ServerConfig:
        """Get the configuration associated with this connection."""
        return self._config

    @property
    def connection_id(self) -> ConnectionId:
        """Get the unique identifier for this connection."""
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
    def local_address(self) -> Address | None:
        """Get the local address of the connection."""
        addr = self._transport.get_extra_info("sockname")
        if isinstance(addr, tuple) and len(addr) >= 2:
            return (addr[0], addr[1])
        return None

    @property
    def remote_address(self) -> Address | None:
        """Get the remote address of the connection."""
        addr = self._transport.get_extra_info("peername")
        if isinstance(addr, tuple) and len(addr) >= 2:
            return (addr[0], addr[1])
        return None

    @property
    def state(self) -> ConnectionState:
        """Get the current state of the connection."""
        return self._cached_state

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the async context manager."""
        await self.close()

    async def close(self, *, error_code: int = ErrorCodes.NO_ERROR, reason: str = "Closed by application") -> None:
        """Immediately close the WebTransport connection."""
        if self._cached_state == ConnectionState.CLOSED:
            return

        logger.info("Closing connection %s...", self.connection_id)

        try:
            request_id, future = self._protocol.create_request()
            event = ConnectionClose(request_id=request_id, error_code=error_code, reason=reason)
            self._protocol.send_event(event=event)

            try:
                async with asyncio.timeout(delay=5.0):
                    await future
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            except ConnectionError as e:
                if "Connection closed" in str(e):
                    logger.debug("Connection closed while waiting for close confirmation: %s", e)
                else:
                    logger.warning("Connection error during close: %s", e)
            except Exception as e:
                logger.warning("Error during close event processing: %s", e)

        finally:
            if self.is_client:
                if not self._transport.is_closing():
                    logger.debug("Closing underlying transport for connection %s", self.connection_id)
                    self._transport.close()

            self._session_handles.clear()
            self._stream_handles.clear()
            self._cached_state = ConnectionState.CLOSED
            logger.info("Connection %s close process finished.", self.connection_id)

    async def graceful_shutdown(self) -> None:
        """Gracefully shut down the connection."""
        logger.info("Initiating graceful shutdown for connection %s...", self.connection_id)

        request_id, future = self._protocol.create_request()
        event = UserConnectionGracefulClose(request_id=request_id)
        self._protocol.send_event(event=event)

        try:
            async with asyncio.timeout(delay=5.0):
                await future
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for graceful shutdown GOAWAY confirmation.")
        except Exception as e:
            logger.warning("Error during graceful shutdown: %s", e)

        await self.close(reason="Graceful shutdown complete")

    async def create_session(self, *, path: str, headers: Headers | None = None) -> WebTransportSession:
        """Create a new WebTransport session."""
        if not self.is_client:
            raise ConnectionError("Sessions can only be created by the client.")

        request_id, future = self._protocol.create_request()
        event = UserCreateSession(request_id=request_id, path=path, headers=headers if headers is not None else {})
        self._protocol.send_event(event=event)

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
            logger.error("Internal error: Session handle %s missing after successful creation effect.", session_id)
            raise SessionError(f"Internal error creating session handle for {session_id}")

        return session_handle

    async def diagnostics(self) -> ConnectionDiagnostics:
        """Get diagnostic information about the connection."""
        request_id, future = self._protocol.create_request()
        event = UserGetConnectionDiagnostics(request_id=request_id)
        self._protocol.send_event(event=event)

        diag_data: dict[str, Any] = await future
        diag_data["active_session_handles"] = len(self._session_handles)
        diag_data["active_stream_handles"] = len(self._stream_handles)
        return ConnectionDiagnostics(**diag_data)

    def get_all_sessions(self) -> list[WebTransportSession]:
        """Get a list of all active session handles."""
        return list(self._session_handles.values())

    def _notify_owner(self, event_type: EventType, data: dict[str, Any]) -> None:
        """Handle high-level status events from the adapter."""
        try:
            if "connection" not in data:
                data["connection"] = weakref.proxy(self)

            if "connection_id" in data:
                data["connection_id"] = self._connection_id

            if event_type == EventType.CONNECTION_ESTABLISHED:
                self._cached_state = ConnectionState.CONNECTED
            elif event_type == EventType.CONNECTION_CLOSED:
                self._cached_state = ConnectionState.CLOSED

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
            elif event_type in (EventType.STREAM_OPENED, EventType.STREAM_CLOSED):
                self._handle_stream_event(event_type=event_type, data=data)

            self.events.emit_nowait(event_type=event_type, data=data)

        except Exception as e:
            logger.error("Error during owner notification callback: %s", e, exc_info=True)

    def _handle_session_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        """Create or update session handles based on events."""
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
                logger.debug("Created session handle for %s", session_id)
                data["session"] = session
            else:
                logger.error("Missing metadata for session handle creation %s", session_id)

    def _route_session_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        """Route events to existing session handles."""
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

    def _handle_stream_event(self, *, event_type: EventType, data: dict[str, Any]) -> None:
        """Manage stream handles."""
        stream_id = data.get("stream_id")
        if stream_id is None:
            return

        if event_type == EventType.STREAM_OPENED:
            session_id = data.get("session_id")
            direction = data.get("direction")

            if session_id is not None and direction is not None and stream_id not in self._stream_handles:
                session = self._session_handles.get(session_id)
                if session is not None:
                    handle_class: type[StreamHandle]
                    match direction:
                        case StreamDirection.BIDIRECTIONAL:
                            handle_class = WebTransportStream
                        case StreamDirection.SEND_ONLY:
                            handle_class = WebTransportSendStream
                        case StreamDirection.RECEIVE_ONLY:
                            handle_class = WebTransportReceiveStream
                        case _:
                            logger.error("Unknown stream direction: %s", direction)
                            return

                    new_stream = handle_class(session=session, stream_id=stream_id)
                    self._stream_handles[stream_id] = new_stream
                    data["stream"] = new_stream

                    session.events.emit_nowait(event_type=event_type, data=data)
                else:
                    logger.warning("Session %s not found for stream %d", session_id, stream_id)

        elif event_type == EventType.STREAM_CLOSED:
            stream = self._stream_handles.pop(stream_id, None)
            if stream is not None:
                data["stream"] = stream
                stream.events.emit_nowait(event_type=event_type, data=data)
                asyncio.create_task(coro=stream.events.close())

    def __repr__(self) -> str:
        """Provide a developer-friendly representation."""
        return f"<WebTransportConnection id={self.connection_id} state={self._cached_state} client={self.is_client}>"
