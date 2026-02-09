//! Protocol event definitions and state machine effects.

use std::collections::HashMap;

use bytes::Bytes;

use crate::common::types::{
    ConnectionId, ErrorCode, ErrorSource, EventType, Headers, RequestId, SessionId,
    StreamDirection, StreamId,
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
        reason: String,
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
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    TransportStopSendingReceived {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    TransportStreamResetReceived {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    CapsuleReceived {
        stream_id: StreamId,
        capsule_type: u64,
        capsule_data: Bytes,
    },
    ConnectStreamClosed {
        stream_id: StreamId,
    },
    DatagramReceived {
        stream_id: StreamId,
        data: Bytes,
    },
    GoawayReceived,
    HeadersReceived {
        stream_id: StreamId,
        headers: Headers,
        stream_ended: bool,
    },
    SettingsReceived {
        settings: HashMap<u64, u64>,
    },
    WebTransportStreamDataReceived {
        session_id: SessionId,
        stream_id: StreamId,
        data: Bytes,
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
        is_unidirectional: bool,
        max_streams: u64,
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
    UserStopSending {
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
        connection_id: ConnectionId,
        event_type: EventType,
        error_code: Option<ErrorCode>,
        reason: Option<String>,
    },
    EmitSessionEvent {
        session_id: SessionId,
        event_type: EventType,
        path: Option<String>,
        headers: Option<Headers>,
        data: Option<Bytes>,
        is_unidirectional: Option<bool>,
        max_data: Option<u64>,
        max_streams: Option<u64>,
        ready_at: Option<f64>,
        error_code: Option<ErrorCode>,
        reason: Option<String>,
    },
    EmitStreamEvent {
        stream_id: StreamId,
        event_type: EventType,
        session_id: Option<SessionId>,
        direction: Option<StreamDirection>,
        is_peer_initiated: Option<bool>,
        error_code: Option<ErrorCode>,
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
        source: ErrorSource,
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
