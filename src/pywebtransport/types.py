"""Common types."""

from __future__ import annotations

from enum import StrEnum

__all__: list[str] = [
    "Address",
    "Buffer",
    "ConnectionHandle",
    "ConnectionState",
    "ErrorCode",
    "EventType",
    "Headers",
    "RequestId",
    "SessionId",
    "SessionState",
    "StreamDirection",
    "StreamId",
    "StreamReceiveState",
    "StreamSendState",
]

type Address = tuple[str, int]
type Buffer = bytes | bytearray | memoryview
type ConnectionHandle = int
type ErrorCode = int
type Headers = dict[str | bytes, str | bytes] | list[tuple[str | bytes, str | bytes]]
type RequestId = int
type SessionId = int
type StreamId = int


class ConnectionState(StrEnum):
    """Enumeration of connection states."""

    CLOSED = "closed"
    CLOSING = "closing"
    CONNECTED = "connected"
    CONNECTING = "connecting"
    IDLE = "idle"


class EventType(StrEnum):
    """Enumeration of event types."""

    CONNECTION_CLOSED = "connection_closed"
    CONNECTION_ESTABLISHED = "connection_established"
    SESSION_CLOSED = "session_closed"
    SESSION_DATA_BLOCKED = "session_data_blocked"
    SESSION_DRAINING = "session_draining"
    SESSION_MAX_DATA_UPDATED = "session_max_data_updated"
    SESSION_MAX_STREAMS_BIDI_UPDATED = "session_max_streams_bidi_updated"
    SESSION_MAX_STREAMS_UNI_UPDATED = "session_max_streams_uni_updated"
    SESSION_PENDING = "session_pending"
    SESSION_READY = "session_ready"
    SESSION_REQUEST_RECEIVED = "session_request_received"
    SESSION_STREAMS_BLOCKED = "session_streams_blocked"
    STREAM_CLOSED = "stream_closed"
    STREAM_OPENED = "stream_opened"
    STREAM_RESET_RECEIVED = "stream_reset_received"
    STREAM_STOP_SENDING_RECEIVED = "stream_stop_sending_received"


class SessionState(StrEnum):
    """Enumeration of session states."""

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


class StreamReceiveState(StrEnum):
    """Enumeration of stream receive states."""

    DATA_READ = "data_read"
    DATA_RECEIVED = "data_received"
    RECEIVE = "receive"
    RESET_READ = "reset_read"


class StreamSendState(StrEnum):
    """Enumeration of stream send states."""

    DATA_RECEIVED = "data_received"
    DATA_SENT = "data_sent"
    READY = "ready"
    RESET_SENT = "reset_sent"
