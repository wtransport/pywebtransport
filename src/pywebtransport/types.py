"""Core data types and interface protocols for the library."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

__all__: list[str] = [
    "Address",
    "AsyncContextManager",
    "AsyncGenerator",
    "AsyncIterator",
    "Buffer",
    "ConnectionId",
    "ConnectionState",
    "Data",
    "ErrorCode",
    "EventData",
    "EventType",
    "Future",
    "Headers",
    "Priority",
    "RequestId",
    "SSLContext",
    "Serializer",
    "SessionId",
    "SessionProtocol",
    "SessionState",
    "StreamDirection",
    "StreamId",
    "StreamState",
    "Timeout",
    "Timestamp",
    "URL",
    "URLParts",
    "WebTransportProtocol",
    "Weight",
]

type Address = tuple[str, int]
type Buffer = bytes | bytearray | memoryview
type ConnectionId = str
type Data = bytes | bytearray | memoryview | str
type ErrorCode = int
type EventData = Any
type Future[T] = asyncio.Future[T]
type Headers = dict[str | bytes, str | bytes] | list[tuple[str | bytes, str | bytes]]
type Priority = int
type RequestId = int
type SessionId = int
type SSLContext = ssl.SSLContext
type StreamId = int
type Timeout = float | None
type Timestamp = float
type URL = str
type URLParts = tuple[str, int, str]
type Weight = int


class ConnectionState(StrEnum):
    """Enumeration of connection states."""

    IDLE = "idle"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    CLOSING = "closing"
    DRAINING = "draining"
    CLOSED = "closed"
    FAILED = "failed"


class EventType(StrEnum):
    """Enumeration of system event types."""

    CAPSULE_RECEIVED = "capsule_received"
    CONNECTION_CLOSED = "connection_closed"
    CONNECTION_ESTABLISHED = "connection_established"
    CONNECTION_FAILED = "connection_failed"
    CONNECTION_LOST = "connection_lost"
    DATAGRAM_ERROR = "datagram_error"
    DATAGRAM_RECEIVED = "datagram_received"
    DATAGRAM_SENT = "datagram_sent"
    PROTOCOL_ERROR = "protocol_error"
    SESSION_CLOSED = "session_closed"
    SESSION_DATA_BLOCKED = "session_data_blocked"
    SESSION_DRAINING = "session_draining"
    SESSION_MAX_DATA_UPDATED = "session_max_data_updated"
    SESSION_MAX_STREAMS_BIDI_UPDATED = "session_max_streams_bidi_updated"
    SESSION_MAX_STREAMS_UNI_UPDATED = "session_max_streams_uni_updated"
    SESSION_READY = "session_ready"
    SESSION_REQUEST = "session_request"
    SESSION_STREAMS_BLOCKED = "session_streams_blocked"
    SETTINGS_RECEIVED = "settings_received"
    STOP_SENDING_RECEIVED = "stop_sending_received"
    STREAM_CLOSED = "stream_closed"
    STREAM_DATA_RECEIVED = "stream_data_received"
    STREAM_ERROR = "stream_error"
    STREAM_OPENED = "stream_opened"
    STREAM_RESET_RECEIVED = "stream_reset_received"
    TIMEOUT_ERROR = "timeout_error"


@runtime_checkable
class Serializer(Protocol):
    """Define the interface for serializing and deserializing structured data."""

    __slots__ = ()

    def deserialize(self, *, data: Buffer, obj_type: Any = None) -> Any:
        """Deserialize the buffer into an object."""
        ...

    def serialize(self, *, obj: Any) -> bytes:
        """Serialize the object into bytes."""
        ...


@runtime_checkable
class SessionProtocol(Protocol):
    """Define the essential interface of a WebTransport session."""

    __slots__ = ()

    @property
    def headers(self) -> Headers:
        """Return the session headers."""
        ...

    @property
    def path(self) -> str:
        """Return the session path."""
        ...

    @property
    def remote_address(self) -> Address | None:
        """Return the remote address of the peer."""
        ...

    @property
    def session_id(self) -> SessionId:
        """Return the session ID."""
        ...

    @property
    def state(self) -> SessionState:
        """Return the current session state."""
        ...

    async def close(self, *, error_code: int = 0, reason: str | None = None) -> None:
        """Terminate the session."""
        ...


class SessionState(StrEnum):
    """Enumeration of WebTransport session states."""

    CONNECTING = "connecting"
    CONNECTED = "connected"
    CLOSING = "closing"
    DRAINING = "draining"
    CLOSED = "closed"


class StreamDirection(StrEnum):
    """Enumeration of stream directions."""

    BIDIRECTIONAL = "bidirectional"
    SEND_ONLY = "send_only"
    RECEIVE_ONLY = "receive_only"


class StreamState(StrEnum):
    """Enumeration of WebTransport stream states."""

    OPEN = "open"
    HALF_CLOSED_LOCAL = "half_closed_local"
    HALF_CLOSED_REMOTE = "half_closed_remote"
    RESET_SENT = "reset_sent"
    RESET_RECEIVED = "reset_received"
    CLOSED = "closed"


@runtime_checkable
class WebTransportProtocol(Protocol):
    """Define the interface for the underlying WebTransport layer."""

    __slots__ = ()

    def connection_lost(self, exc: Exception | None) -> None:
        """Handle connection loss."""
        ...

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Handle connection establishment."""
        ...

    def datagram_received(self, data: Buffer, addr: Address) -> None:
        """Handle incoming datagrams."""
        ...

    def error_received(self, exc: Exception) -> None:
        """Handle incoming transport errors."""
        ...
