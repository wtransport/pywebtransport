"""Internal events, commands, and effects for the protocol engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pywebtransport.types import Buffer, ErrorCode, Headers, RequestId, SessionId, StreamId

__all__: list[str] = []


@dataclass(frozen=True, kw_only=True, slots=True)
class ProtocolEvent:
    """Base class for all events processed by the _WebTransportEngine."""


@dataclass(frozen=True, kw_only=True, slots=True)
class UserEvent[T](ProtocolEvent):
    """Base class for commands originating from the user-facing API."""

    request_id: RequestId


@dataclass(frozen=True, kw_only=True, slots=True)
class UserAcceptSession(UserEvent[None]):
    """User command to accept a pending session."""

    session_id: SessionId
    wt_protocol: str | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCloseConnection(UserEvent[None]):
    """User command to close the entire connection."""

    error_code: ErrorCode
    reason: str | None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCloseConnectionGracefully(UserEvent[None]):
    """User command to gracefully close the connection."""


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCloseSession(UserEvent[None]):
    """User command to close an active session."""

    session_id: SessionId
    error_code: ErrorCode
    reason: str | None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCreateSession(UserEvent[SessionId]):
    """User command to create a new WebTransport session."""

    authority: str
    path: str
    headers: Headers
    wt_available_protocols: list[str] | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCreateSessionOptimistic(UserEvent[SessionId]):
    """User command to optimistically create a new WebTransport session."""

    authority: str
    path: str
    headers: Headers
    wt_available_protocols: list[str] | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserCreateStream(UserEvent[StreamId]):
    """User command to create a new stream."""

    session_id: SessionId
    is_unidirectional: bool


@dataclass(frozen=True, kw_only=True, slots=True)
class UserExportKeyingMaterial(UserEvent[bytes]):
    """User command to export TLS keying material for a session."""

    session_id: SessionId
    label: str
    context: Buffer
    length: int


@dataclass(frozen=True, kw_only=True, slots=True)
class UserGetConnectionDiagnostics(UserEvent[dict[str, Any]]):
    """User command to get connection diagnostics."""


@dataclass(frozen=True, kw_only=True, slots=True)
class UserGetSessionDiagnostics(UserEvent[dict[str, Any]]):
    """User command to get session diagnostics."""

    session_id: SessionId


@dataclass(frozen=True, kw_only=True, slots=True)
class UserGetStreamDiagnostics(UserEvent[dict[str, Any]]):
    """User command to get stream diagnostics."""

    stream_id: StreamId


@dataclass(frozen=True, kw_only=True, slots=True)
class UserReadStream(UserEvent[bytes]):
    """User command to read data from a stream."""

    stream_id: StreamId
    max_bytes: int | None


@dataclass(frozen=True, kw_only=True, slots=True)
class UserRejectSession(UserEvent[None]):
    """User command to reject a pending session."""

    session_id: SessionId
    status_code: int


@dataclass(frozen=True, kw_only=True, slots=True)
class UserResetStream(UserEvent[None]):
    """User command to reset the sending side of a stream."""

    stream_id: StreamId
    error_code: ErrorCode


@dataclass(frozen=True, kw_only=True, slots=True)
class UserSendDatagram(UserEvent[None]):
    """User command to send a datagram."""

    session_id: SessionId
    data: Buffer


@dataclass(frozen=True, kw_only=True, slots=True)
class UserSendStreamData(UserEvent[None]):
    """User command to send data on a stream."""

    stream_id: StreamId
    data: Buffer
    end_stream: bool


@dataclass(frozen=True, kw_only=True, slots=True)
class UserStopSending(UserEvent[None]):
    """User command to stop the receiving side of a stream."""

    stream_id: StreamId
    error_code: ErrorCode
