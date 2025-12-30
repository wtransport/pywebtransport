"""Internal events, commands, and effects for the protocol engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pywebtransport.types import Buffer, ErrorCode, EventType, Headers, RequestId, SessionId, StreamId

__all__: list[str] = []


@dataclass(kw_only=True, frozen=True, slots=True)
class ProtocolEvent:
    """Base class for all events processed by the _WebTransportEngine."""


@dataclass(kw_only=True, frozen=True, slots=True)
class InternalBindH3Session(ProtocolEvent):
    """Internal command to bind a created H3 session to the state."""

    request_id: RequestId
    stream_id: StreamId


@dataclass(kw_only=True, frozen=True, slots=True)
class InternalBindQuicStream(ProtocolEvent):
    """Internal command to bind a created QUIC stream to the state."""

    request_id: RequestId
    stream_id: StreamId
    session_id: SessionId
    is_unidirectional: bool


@dataclass(kw_only=True, frozen=True, slots=True)
class InternalCleanupEarlyEvents(ProtocolEvent):
    """Internal command signaling the engine to clean up the early event buffer."""


@dataclass(kw_only=True, frozen=True, slots=True)
class InternalCleanupResources(ProtocolEvent):
    """Internal command signaling the engine to garbage collect closed resources."""


@dataclass(kw_only=True, frozen=True, slots=True)
class InternalFailH3Session(ProtocolEvent):
    """Internal command to handle a failed H3 session creation attempt."""

    request_id: RequestId
    exception: Exception


@dataclass(kw_only=True, frozen=True, slots=True)
class InternalFailQuicStream(ProtocolEvent):
    """Internal command to handle a failed QUIC stream creation attempt."""

    request_id: RequestId
    session_id: SessionId
    is_unidirectional: bool
    exception: Exception


@dataclass(kw_only=True, frozen=True, slots=True)
class InternalReturnStreamData(ProtocolEvent):
    """Internal command to return unconsumed data to a stream buffer."""

    stream_id: StreamId
    data: Buffer


@dataclass(kw_only=True, frozen=True, slots=True)
class TransportConnectionTerminated(ProtocolEvent):
    """Event indicating the underlying QUIC connection was terminated."""

    error_code: ErrorCode
    reason_phrase: str


@dataclass(kw_only=True, frozen=True, slots=True)
class TransportDatagramFrameReceived(ProtocolEvent):
    """Event for a raw datagram frame received from QUIC."""

    data: Buffer


@dataclass(kw_only=True, frozen=True, slots=True)
class TransportHandshakeCompleted(ProtocolEvent):
    """Event signaling QUIC handshake completion is processed."""


@dataclass(kw_only=True, frozen=True, slots=True)
class TransportQuicParametersReceived(ProtocolEvent):
    """Event signaling peer's QUIC transport parameters are received."""

    remote_max_datagram_frame_size: int


@dataclass(kw_only=True, frozen=True, slots=True)
class TransportQuicTimerFired(ProtocolEvent):
    """Event signaling the transport timer has fired."""


@dataclass(kw_only=True, frozen=True, slots=True)
class TransportStreamDataReceived(ProtocolEvent):
    """Event for raw stream data received from QUIC."""

    data: Buffer
    end_stream: bool
    stream_id: StreamId


@dataclass(kw_only=True, frozen=True, slots=True)
class TransportStreamReset(ProtocolEvent):
    """Event for a stream reset received from QUIC."""

    error_code: ErrorCode
    stream_id: StreamId


@dataclass(kw_only=True, frozen=True, slots=True)
class H3Event(ProtocolEvent):
    """Base class for all H3 protocol engine semantic events."""


@dataclass(kw_only=True, frozen=True, slots=True)
class CapsuleReceived(H3Event):
    """Represent an HTTP Capsule received on a stream."""

    capsule_data: Buffer
    capsule_type: int
    stream_id: StreamId


@dataclass(kw_only=True, frozen=True, slots=True)
class ConnectStreamClosed(H3Event):
    """H3 event signaling the CONNECT stream was cleanly closed."""

    stream_id: StreamId


@dataclass(kw_only=True, frozen=True, slots=True)
class DatagramReceived(H3Event):
    """Represent a WebTransport datagram received."""

    data: Buffer
    stream_id: StreamId


@dataclass(kw_only=True, frozen=True, slots=True)
class GoawayReceived(H3Event):
    """Represent an H3 GOAWAY frame received on the control stream."""


@dataclass(kw_only=True, frozen=True, slots=True)
class HeadersReceived(H3Event):
    """Represent a HEADERS frame received on a stream."""

    headers: Headers
    stream_id: StreamId
    stream_ended: bool


@dataclass(kw_only=True, frozen=True, slots=True)
class SettingsReceived(H3Event):
    """Represent an H3 SETTINGS frame received and parsed."""

    settings: dict[int, int]


@dataclass(kw_only=True, frozen=True, slots=True)
class WebTransportStreamDataReceived(H3Event):
    """Represent semantic data received on an established WebTransport stream."""

    data: Buffer
    session_id: SessionId
    stream_id: StreamId
    stream_ended: bool


@dataclass(kw_only=True, frozen=True, slots=True)
class UserEvent[T](ProtocolEvent):
    """Base class for commands originating from the user-facing API."""

    request_id: RequestId


@dataclass(kw_only=True, frozen=True, slots=True)
class ConnectionClose(UserEvent[None]):
    """User or internal command to close the entire connection."""

    error_code: ErrorCode
    reason: str | None


@dataclass(kw_only=True, frozen=True, slots=True)
class UserAcceptSession(UserEvent[None]):
    """User command to accept a pending session."""

    session_id: SessionId


@dataclass(kw_only=True, frozen=True, slots=True)
class UserCloseSession(UserEvent[None]):
    """User command to close an active session."""

    session_id: SessionId
    error_code: ErrorCode
    reason: str | None


@dataclass(kw_only=True, frozen=True, slots=True)
class UserConnectionGracefulClose(UserEvent[None]):
    """User command to gracefully close the connection."""


@dataclass(kw_only=True, frozen=True, slots=True)
class UserCreateSession(UserEvent[SessionId]):
    """User command to create a new WebTransport session."""

    path: str
    headers: Headers


@dataclass(kw_only=True, frozen=True, slots=True)
class UserCreateStream(UserEvent[StreamId]):
    """User command to create a new stream."""

    session_id: SessionId
    is_unidirectional: bool


@dataclass(kw_only=True, frozen=True, slots=True)
class UserGetConnectionDiagnostics(UserEvent[dict[str, Any]]):
    """User command to get connection diagnostics."""


@dataclass(kw_only=True, frozen=True, slots=True)
class UserGetSessionDiagnostics(UserEvent[dict[str, Any]]):
    """User command to get session diagnostics."""

    session_id: SessionId


@dataclass(kw_only=True, frozen=True, slots=True)
class UserGetStreamDiagnostics(UserEvent[dict[str, Any]]):
    """User command to get stream diagnostics."""

    stream_id: StreamId


@dataclass(kw_only=True, frozen=True, slots=True)
class UserGrantDataCredit(UserEvent[None]):
    """User command to manually grant data credit."""

    session_id: SessionId
    max_data: int


@dataclass(kw_only=True, frozen=True, slots=True)
class UserGrantStreamsCredit(UserEvent[None]):
    """User command to manually grant stream credit."""

    session_id: SessionId
    max_streams: int
    is_unidirectional: bool


@dataclass(kw_only=True, frozen=True, slots=True)
class UserRejectSession(UserEvent[None]):
    """User command to reject a pending session."""

    session_id: SessionId
    status_code: int


@dataclass(kw_only=True, frozen=True, slots=True)
class UserResetStream(UserEvent[None]):
    """User command to reset the sending side of a stream."""

    stream_id: StreamId
    error_code: ErrorCode


@dataclass(kw_only=True, frozen=True, slots=True)
class UserSendDatagram(UserEvent[None]):
    """User command to send a datagram."""

    session_id: SessionId
    data: Buffer | list[Buffer]


@dataclass(kw_only=True, frozen=True, slots=True)
class UserSendStreamData(UserEvent[None]):
    """User command to send data on a stream."""

    stream_id: StreamId
    data: Buffer
    end_stream: bool


@dataclass(kw_only=True, frozen=True, slots=True)
class UserStopStream(UserEvent[None]):
    """User command to stop the receiving side of a stream."""

    stream_id: StreamId
    error_code: ErrorCode


@dataclass(kw_only=True, frozen=True, slots=True)
class UserStreamRead(UserEvent[bytes]):
    """User command to read data from a stream."""

    stream_id: StreamId
    max_bytes: int


@dataclass(kw_only=True, frozen=True, slots=True)
class Effect:
    """Base class for all side effects returned by the state machine."""


@dataclass(kw_only=True, frozen=True, slots=True)
class CleanupH3Stream(Effect):
    """Effect instructing Engine to cleanup H3-level stream state."""

    stream_id: StreamId


@dataclass(kw_only=True, frozen=True, slots=True)
class CloseQuicConnection(Effect):
    """Effect to close the entire QUIC connection."""

    error_code: ErrorCode
    reason: str | None


@dataclass(kw_only=True, frozen=True, slots=True)
class CreateH3Session(Effect):
    """Effect instructing Engine to initiate H3 session creation."""

    request_id: RequestId
    path: str
    headers: Headers


@dataclass(kw_only=True, frozen=True, slots=True)
class CreateQuicStream(Effect):
    """Effect instructing Adapter to create a new QUIC stream."""

    request_id: RequestId
    session_id: SessionId
    is_unidirectional: bool


@dataclass(kw_only=True, frozen=True, slots=True)
class EmitConnectionEvent(Effect):
    """Effect to emit an event on the WebTransportConnection."""

    event_type: EventType
    data: dict[str, Any]


@dataclass(kw_only=True, frozen=True, slots=True)
class EmitSessionEvent(Effect):
    """Effect to emit an event on the WebTransportSession."""

    session_id: SessionId
    event_type: EventType
    data: dict[str, Any]


@dataclass(kw_only=True, frozen=True, slots=True)
class EmitStreamEvent(Effect):
    """Effect to emit an event on the WebTransportStream."""

    stream_id: StreamId
    event_type: EventType
    data: dict[str, Any]


@dataclass(kw_only=True, frozen=True, slots=True)
class LogH3Frame(Effect):
    """Effect instructing Adapter to log an H3-level frame."""

    category: str
    event: str
    data: dict[str, Any]


@dataclass(kw_only=True, frozen=True, slots=True)
class NotifyRequestDone(Effect):
    """Effect to notify that a user request has completed successfully."""

    request_id: RequestId
    result: Any


@dataclass(kw_only=True, frozen=True, slots=True)
class NotifyRequestFailed(Effect):
    """Effect to notify that a user request has failed."""

    request_id: RequestId
    exception: Exception


@dataclass(kw_only=True, frozen=True, slots=True)
class ProcessProtocolEvent(Effect):
    """Effect instructing the Adapter to re-process a protocol event."""

    event: ProtocolEvent


@dataclass(kw_only=True, frozen=True, slots=True)
class RescheduleQuicTimer(Effect):
    """Effect instructing the Adapter to schedule the next QUIC timer."""


@dataclass(kw_only=True, frozen=True, slots=True)
class ResetQuicStream(Effect):
    """Effect to reset the sending side of a QUIC stream."""

    stream_id: StreamId
    error_code: ErrorCode


@dataclass(kw_only=True, frozen=True, slots=True)
class SendH3Capsule(Effect):
    """Effect instructing Engine to encode and send an H3 Capsule."""

    stream_id: StreamId
    capsule_type: int
    capsule_data: Buffer
    end_stream: bool = False


@dataclass(kw_only=True, frozen=True, slots=True)
class SendH3Datagram(Effect):
    """Effect instructing Engine to encode and send an H3 Datagram."""

    stream_id: StreamId
    data: Buffer | list[Buffer]


@dataclass(kw_only=True, frozen=True, slots=True)
class SendH3Goaway(Effect):
    """Effect instructing Engine to encode and send an H3 GOAWAY frame."""


@dataclass(kw_only=True, frozen=True, slots=True)
class SendH3Headers(Effect):
    """Effect instructing Engine to send simple H3 status headers."""

    stream_id: StreamId
    status: int
    end_stream: bool = True


@dataclass(kw_only=True, frozen=True, slots=True)
class SendQuicData(Effect):
    """Effect to send data on a QUIC stream."""

    stream_id: StreamId
    data: Buffer
    end_stream: bool = False


@dataclass(kw_only=True, frozen=True, slots=True)
class SendQuicDatagram(Effect):
    """Effect to send a QUIC datagram frame."""

    data: Buffer | list[Buffer]


@dataclass(kw_only=True, frozen=True, slots=True)
class StopQuicStream(Effect):
    """Effect to stop the receiving side of a QUIC stream."""

    stream_id: StreamId
    error_code: ErrorCode


@dataclass(kw_only=True, frozen=True, slots=True)
class TriggerQuicTimer(Effect):
    """Effect instructing the Adapter to handle the QUIC timer."""
