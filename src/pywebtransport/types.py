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
    "ConnectionHandle",
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
type ConnectionHandle = int
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

    CLOSED = "closed"
    CLOSING = "closing"
    CONNECTED = "connected"
    CONNECTING = "connecting"
    IDLE = "idle"


class EventType(StrEnum):
    """Enumeration of system event types."""

    CONNECTION_CLOSED = "connection_closed"
    CONNECTION_ESTABLISHED = "connection_established"
    DATAGRAM_RECEIVED = "datagram_received"
    SESSION_CLOSED = "session_closed"
    SESSION_DATA_BLOCKED = "session_data_blocked"
    SESSION_DRAINING = "session_draining"
    SESSION_MAX_DATA_UPDATED = "session_max_data_updated"
    SESSION_MAX_STREAMS_BIDI_UPDATED = "session_max_streams_bidi_updated"
    SESSION_MAX_STREAMS_UNI_UPDATED = "session_max_streams_uni_updated"
    SESSION_READY = "session_ready"
    SESSION_REQUEST = "session_request"
    SESSION_STREAMS_BLOCKED = "session_streams_blocked"
    STOP_SENDING_RECEIVED = "stop_sending_received"
    STREAM_CLOSED = "stream_closed"
    STREAM_OPENED = "stream_opened"
    STREAM_RESET_RECEIVED = "stream_reset_received"


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

    @property
    def wt_available_protocols(self) -> list[str] | None:
        """Return the requested wt_available_protocols."""
        ...

    @property
    def wt_protocol(self) -> str | None:
        """Return the negotiated wt_protocol."""
        ...

    @wt_protocol.setter
    def wt_protocol(self, value: str | None) -> None:
        """Set the negotiated wt_protocol."""
        ...

    async def close(self, *, error_code: int = 0, reason: str | None = None) -> None:
        """Terminate the session."""
        ...


class SessionState(StrEnum):
    """Enumeration of WebTransport session states."""

    CLOSED = "closed"
    CLOSING = "closing"
    CONNECTED = "connected"
    CONNECTING = "connecting"
    DRAINING = "draining"


class StreamDirection(StrEnum):
    """Enumeration of stream directions."""

    BIDIRECTIONAL = "bidirectional"
    RECEIVE_ONLY = "receive_only"
    SEND_ONLY = "send_only"


class StreamState(StrEnum):
    """Enumeration of WebTransport stream states."""

    CLOSED = "closed"
    HALF_CLOSED_LOCAL = "half_closed_local"
    HALF_CLOSED_REMOTE = "half_closed_remote"
    OPEN = "open"
    RESET_RECEIVED = "reset_received"
    RESET_SENT = "reset_sent"


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
