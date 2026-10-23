"""FFI event definitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pywebtransport.types import (
    Address,
    Buffer,
    ConnectionHandle,
    ErrorCode,
    EventType,
    Headers,
    RequestId,
    SessionId,
    StreamDirection,
    StreamId,
)

__all__: list[str] = []

type Effect = EmitConnectionEvent | EmitSessionEvent | EmitStreamEvent | NotifyRequestDone | NotifyRequestFailed
type RuntimeEvent = CommandFailed | ConnectionEffects | ConnectionSpawned | ReactorShutdown


@dataclass(frozen=True, kw_only=True, slots=True)
class CommandFailed:

    request_id: RequestId
    exception: Exception


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionEffects:

    handle: ConnectionHandle
    effects: list[tuple[int, Any]]


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionSpawned:

    handle: ConnectionHandle
    remote_address: Address
    effects: list[tuple[int, Any]]


@dataclass(frozen=True, kw_only=True, slots=True)
class EmitConnectionEvent:

    connection_handle: ConnectionHandle
    event_type: EventType
    error_code: ErrorCode | None
    reason: str | None


@dataclass(frozen=True, kw_only=True, slots=True)
class EmitSessionEvent:

    session_id: SessionId
    event_type: EventType
    path: str | None
    headers: Headers | None
    wt_available_protocols: list[str] | None
    wt_protocol: str | None
    data: bytes | None
    is_unidirectional: bool | None
    max_data: int | None
    max_streams: int | None
    ready_at: float | None
    status_code: int | None
    error_code: ErrorCode | None
    reason: str | None


@dataclass(frozen=True, kw_only=True, slots=True)
class EmitStreamEvent:

    stream_id: StreamId
    event_type: EventType
    session_id: SessionId | None
    direction: StreamDirection | None
    is_peer_initiated: bool | None
    error_code: ErrorCode | None


@dataclass(frozen=True, kw_only=True, slots=True)
class NotifyRequestDone:

    request_id: RequestId
    result: Any


@dataclass(frozen=True, kw_only=True, slots=True)
class NotifyRequestFailed:

    request_id: RequestId
    exception: Exception


@dataclass(frozen=True, kw_only=True, slots=True)
class ProtocolEvent:

    request_id: RequestId


@dataclass(frozen=True, kw_only=True, slots=True)
class ReactorShutdown:
    pass


@dataclass(frozen=True, kw_only=True, slots=True)
class UserAcceptSession(ProtocolEvent):

    session_id: SessionId
    headers: Headers
    wt_protocol: str | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCloseConnection(ProtocolEvent):

    error_code: ErrorCode
    reason: str | None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCloseConnectionGracefully(ProtocolEvent):
    pass


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCloseSession(ProtocolEvent):

    session_id: SessionId
    error_code: ErrorCode
    reason: str | None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCreateSession(ProtocolEvent):

    authority: str
    path: str
    headers: Headers
    wt_available_protocols: list[str] | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCreateSessionOptimistic(ProtocolEvent):

    authority: str
    path: str
    headers: Headers
    wt_available_protocols: list[str] | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCreateStream(ProtocolEvent):

    session_id: SessionId
    is_unidirectional: bool


@dataclass(frozen=True, kw_only=True, slots=True)
class UserDiagnoseConnection(ProtocolEvent):
    pass


@dataclass(frozen=True, kw_only=True, slots=True)
class UserDiagnoseSession(ProtocolEvent):

    session_id: SessionId


@dataclass(frozen=True, kw_only=True, slots=True)
class UserDiagnoseStream(ProtocolEvent):

    stream_id: StreamId


@dataclass(frozen=True, kw_only=True, slots=True)
class UserExportKeyingMaterial(ProtocolEvent):

    session_id: SessionId
    label: str
    context: Buffer
    length: int


@dataclass(frozen=True, kw_only=True, slots=True)
class UserReadDatagram(ProtocolEvent):

    session_id: SessionId


@dataclass(frozen=True, kw_only=True, slots=True)
class UserReadStream(ProtocolEvent):

    stream_id: StreamId
    max_size: int | None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserRejectSession(ProtocolEvent):

    session_id: SessionId
    headers: Headers
    status_code: int


@dataclass(frozen=True, kw_only=True, slots=True)
class UserResetStream(ProtocolEvent):

    stream_id: StreamId
    error_code: ErrorCode


@dataclass(frozen=True, kw_only=True, slots=True)
class UserSendDatagram(ProtocolEvent):

    session_id: SessionId
    data: Buffer


@dataclass(frozen=True, kw_only=True, slots=True)
class UserStopSending(ProtocolEvent):

    stream_id: StreamId
    error_code: ErrorCode


@dataclass(frozen=True, kw_only=True, slots=True)
class UserWriteStream(ProtocolEvent):

    stream_id: StreamId
    data: Buffer
    end_stream: bool
