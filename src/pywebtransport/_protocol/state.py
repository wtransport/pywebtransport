"""Internal state dataclasses for the protocol engine."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from pywebtransport._protocol.events import ProtocolEvent
from pywebtransport.types import (
    Buffer,
    ConnectionState,
    Headers,
    RequestId,
    SessionId,
    SessionState,
    StreamDirection,
    StreamId,
    StreamState,
)

__all__: list[str] = []


@dataclass(kw_only=True, slots=True)
class StreamStateData:
    """Represent the complete state of a single WebTransport stream."""

    stream_id: StreamId
    session_id: SessionId
    direction: StreamDirection
    state: StreamState
    created_at: float

    bytes_sent: int = 0
    bytes_received: int = 0

    read_buffer: deque[Buffer] = field(default_factory=deque)
    read_buffer_size: int = 0

    pending_read_requests: deque[tuple[RequestId, int]] = field(default_factory=deque)
    write_buffer: deque[tuple[Buffer, RequestId, bool]] = field(default_factory=deque)
    write_buffer_size: int = 0

    close_code: int | None = None
    close_reason: str | None = None
    closed_at: float | None = None


@dataclass(kw_only=True, slots=True)
class SessionInitData:
    """Temporary storage for session configuration during creation."""

    path: str
    headers: Headers
    created_at: float


@dataclass(kw_only=True, slots=True)
class SessionStateData:
    """Represent the complete state of a single WebTransport session."""

    session_id: SessionId
    state: SessionState
    path: str
    headers: Headers
    created_at: float

    local_max_data: int
    local_data_sent: int = 0
    peer_max_data: int
    peer_data_sent: int = 0

    local_max_streams_bidi: int
    local_streams_bidi_opened: int = 0
    peer_max_streams_bidi: int
    peer_streams_bidi_opened: int = 0

    local_max_streams_uni: int
    local_streams_uni_opened: int = 0
    peer_max_streams_uni: int
    peer_streams_uni_opened: int = 0

    pending_bidi_stream_requests: deque[RequestId] = field(default_factory=deque)
    pending_uni_stream_requests: deque[RequestId] = field(default_factory=deque)

    datagrams_sent: int = 0
    datagram_bytes_sent: int = 0
    datagrams_received: int = 0
    datagram_bytes_received: int = 0

    active_streams: set[StreamId] = field(default_factory=set)
    blocked_streams: set[StreamId] = field(default_factory=set)

    close_code: int | None = None
    close_reason: str | None = None
    closed_at: float | None = None
    ready_at: float | None = None


@dataclass(kw_only=True, slots=True)
class ProtocolState:
    """Represent the single source of truth for an entire connection."""

    is_client: bool
    connection_state: ConnectionState
    max_datagram_size: int
    remote_max_datagram_frame_size: int = 0

    handshake_complete: bool = False
    peer_settings_received: bool = False
    local_goaway_sent: bool = False

    sessions: dict[SessionId, SessionStateData] = field(default_factory=dict)
    streams: dict[StreamId, StreamStateData] = field(default_factory=dict)

    pending_requests: dict[StreamId, RequestId] = field(default_factory=dict)
    pending_session_configs: dict[RequestId, SessionInitData] = field(default_factory=dict)

    early_event_buffer: dict[StreamId, list[tuple[float, ProtocolEvent]]] = field(default_factory=dict)
    early_event_count: int = 0

    peer_initial_max_data: int = 0
    peer_initial_max_streams_bidi: int = 0
    peer_initial_max_streams_uni: int = 0

    connected_at: float | None = None
    closed_at: float | None = None
