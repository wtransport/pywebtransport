//! Protocol event definitions and state machine effects.

use std::collections::HashMap;

use bytes::Bytes;

use crate::common::types::{
    ConnectionId, ErrorCode, EventType, Headers, RequestId, SessionId, StreamDirection, StreamId,
};

// Protocol state machine input events.
#[derive(Clone, Debug)]
pub(crate) enum ProtocolEvent {
    InternalBindH3Session {
        request_id: RequestId,
        stream_id: StreamId,
    },
    InternalBindQuicStream {
        request_id: RequestId,
        stream_id: StreamId,
        session_id: SessionId,
        is_unidirectional: bool,
    },
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalFailH3Session {
        request_id: RequestId,
        error_code: Option<ErrorCode>,
        reason: String,
    },
    InternalFailQuicStream {
        request_id: RequestId,
        session_id: SessionId,
        is_unidirectional: bool,
        error_code: Option<ErrorCode>,
        reason: String,
    },
    InternalReturnStreamData {
        stream_id: StreamId,
        data: Bytes,
    },
    TransportConnectionTerminated {
        error_code: ErrorCode,
        reason_phrase: String,
    },
    TransportDatagramFrameReceived {
        data: Bytes,
    },
    TransportHandshakeCompleted,
    TransportQuicParametersReceived {
        remote_max_datagram_frame_size: u64,
    },
    TransportQuicTimerFired,
    TransportStreamDataReceived {
        data: Bytes,
        end_stream: bool,
        stream_id: StreamId,
    },
    TransportStreamReset {
        error_code: ErrorCode,
        stream_id: StreamId,
    },
    CapsuleReceived {
        capsule_data: Bytes,
        capsule_type: u64,
        stream_id: StreamId,
    },
    ConnectStreamClosed {
        stream_id: StreamId,
    },
    DatagramReceived {
        data: Bytes,
        stream_id: StreamId,
    },
    GoawayReceived,
    HeadersReceived {
        headers: Headers,
        stream_id: StreamId,
        stream_ended: bool,
    },
    SettingsReceived {
        settings: HashMap<u64, u64>,
    },
    WebTransportStreamDataReceived {
        data: Bytes,
        session_id: SessionId,
        stream_id: StreamId,
        stream_ended: bool,
    },
    ConnectionClose {
        request_id: RequestId,
        error_code: ErrorCode,
        reason: Option<String>,
    },
    UserAcceptSession {
        request_id: RequestId,
        session_id: SessionId,
    },
    UserCloseSession {
        request_id: RequestId,
        session_id: SessionId,
        error_code: ErrorCode,
        reason: Option<String>,
    },
    UserConnectionGracefulClose {
        request_id: RequestId,
    },
    UserCreateSession {
        request_id: RequestId,
        path: String,
        headers: Headers,
    },
    UserCreateStream {
        request_id: RequestId,
        session_id: SessionId,
        is_unidirectional: bool,
    },
    UserGetConnectionDiagnostics {
        request_id: RequestId,
    },
    UserGetSessionDiagnostics {
        request_id: RequestId,
        session_id: SessionId,
    },
    UserGetStreamDiagnostics {
        request_id: RequestId,
        stream_id: StreamId,
    },
    UserGrantDataCredit {
        request_id: RequestId,
        session_id: SessionId,
        max_data: u64,
    },
    UserGrantStreamsCredit {
        request_id: RequestId,
        session_id: SessionId,
        max_streams: u64,
        is_unidirectional: bool,
    },
    UserRejectSession {
        request_id: RequestId,
        session_id: SessionId,
        status_code: u16,
    },
    UserResetStream {
        request_id: RequestId,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    UserSendDatagram {
        request_id: RequestId,
        session_id: SessionId,
        data: Bytes,
    },
    UserSendStreamData {
        request_id: RequestId,
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    UserStopStream {
        request_id: RequestId,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    UserStreamRead {
        request_id: RequestId,
        stream_id: StreamId,
        max_bytes: u64,
    },
}

// State machine side effects.
#[derive(Clone, Debug)]
pub(crate) enum Effect {
    CleanupH3Stream {
        stream_id: StreamId,
    },
    CloseQuicConnection {
        error_code: ErrorCode,
        reason: Option<String>,
    },
    CreateH3Session {
        request_id: RequestId,
        path: String,
        headers: Headers,
    },
    CreateQuicStream {
        request_id: RequestId,
        session_id: SessionId,
        is_unidirectional: bool,
    },
    EmitConnectionEvent {
        event_type: EventType,
        connection_id: ConnectionId,
        error_code: Option<ErrorCode>,
        reason: Option<String>,
    },
    EmitSessionEvent {
        session_id: SessionId,
        event_type: EventType,
        code: Option<ErrorCode>,
        data: Option<Bytes>,
        headers: Option<Headers>,
        is_unidirectional: Option<bool>,
        max_data: Option<u64>,
        max_streams: Option<u64>,
        path: Option<String>,
        ready_at: Option<f64>,
        reason: Option<String>,
    },
    EmitStreamEvent {
        stream_id: StreamId,
        event_type: EventType,
        direction: Option<StreamDirection>,
        session_id: Option<SessionId>,
    },
    LogH3Frame {
        category: String,
        event: String,
        data: String,
    },
    NotifyRequestDone {
        request_id: RequestId,
        result: RequestResult,
    },
    NotifyRequestFailed {
        request_id: RequestId,
        error_code: Option<ErrorCode>,
        reason: String,
    },
    ProcessProtocolEvent {
        event: Box<ProtocolEvent>,
    },
    RescheduleQuicTimer,
    ResetQuicStream {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    SendH3Capsule {
        stream_id: StreamId,
        capsule_type: u64,
        capsule_data: Bytes,
        end_stream: bool,
    },
    SendH3Datagram {
        stream_id: StreamId,
        data: Bytes,
    },
    SendH3Goaway,
    SendH3Headers {
        stream_id: StreamId,
        status: u16,
        end_stream: bool,
    },
    SendQuicData {
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    SendQuicDatagram {
        data: Bytes,
    },
    StopQuicStream {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    TriggerQuicTimer,
}

// Asynchronous request completion result.
#[derive(Clone, Debug)]
pub(crate) enum RequestResult {
    None,
    SessionId(SessionId),
    StreamId(StreamId),
    ReadData(Bytes),
    Diagnostics(String),
}

#[cfg(test)]
mod tests;
