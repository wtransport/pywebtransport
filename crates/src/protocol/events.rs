//! Protocol event definitions and state machine effects.

use bytes::Bytes;
use std::borrow::Cow;

use crate::common::types::{
    ConnectionHandle, ErrorCode, ErrorSource, EventType, Headers, RequestId, SessionId,
    StreamDirection, StreamId,
};
use crate::protocol::H3Settings;
use crate::protocol::{ConnectionDiagnostics, SessionDiagnostics, StreamDiagnostics};

// Protocol state machine input events.
#[derive(Clone, Debug)]
pub(crate) enum ProtocolEvent {
    H3CapsuleReceived {
        stream_id: StreamId,
        capsule_type: u64,
        capsule_data: Bytes,
    },
    H3ConnectStreamClosed {
        stream_id: StreamId,
    },
    H3DatagramReceived {
        stream_id: StreamId,
        data: Bytes,
    },
    H3GoawayReceived,
    H3HeadersReceived {
        stream_id: StreamId,
        headers: Headers,
        stream_ended: bool,
    },
    H3SettingsReceived {
        settings: H3Settings,
    },
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
        reason: Cow<'static, str>,
    },
    TransportConnectionTerminated {
        error_code: ErrorCode,
        reason: Cow<'static, str>,
    },
    TransportDatagramFrameReceived {
        data: Bytes,
    },
    TransportHandshakeCompleted,
    TransportQuicParametersReceived {
        peer_max_datagram_frame_size: u64,
    },
    TransportStopSendingReceived {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    TransportStreamDataReceived {
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    TransportStreamResetReceived {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    UserAcceptSession {
        request_id: RequestId,
        session_id: SessionId,
        wt_protocol: Option<String>,
    },
    UserCloseConnection {
        request_id: RequestId,
        error_code: ErrorCode,
        reason: Option<Cow<'static, str>>,
    },
    UserCloseConnectionGracefully {
        request_id: RequestId,
    },
    UserCloseSession {
        request_id: RequestId,
        session_id: SessionId,
        error_code: ErrorCode,
        reason: Option<Cow<'static, str>>,
    },
    UserCreateSession {
        request_id: RequestId,
        authority: String,
        path: String,
        headers: Headers,
        wt_available_protocols: Option<Vec<String>>,
    },
    UserCreateStream {
        request_id: RequestId,
        session_id: SessionId,
        is_unidirectional: bool,
    },
    UserExportKeyingMaterial {
        request_id: RequestId,
        session_id: SessionId,
        label: String,
        context: Bytes,
        length: u32,
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
    UserReadStream {
        request_id: RequestId,
        stream_id: StreamId,
        max_bytes: u64,
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
    WebTransportStreamDataReceived {
        session_id: SessionId,
        stream_id: StreamId,
        data: Bytes,
        stream_ended: bool,
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
        reason: Option<Cow<'static, str>>,
    },
    CreateH3Session {
        request_id: RequestId,
        authority: String,
        path: String,
        headers: Headers,
    },
    CreateQuicStream {
        request_id: RequestId,
        session_id: SessionId,
        is_unidirectional: bool,
    },
    EmitConnectionEvent {
        connection_handle: ConnectionHandle,
        event_type: EventType,
        error_code: Option<ErrorCode>,
        reason: Option<Cow<'static, str>>,
    },
    EmitSessionEvent {
        session_id: SessionId,
        event_type: EventType,
        path: Option<String>,
        headers: Option<Headers>,
        wt_available_protocols: Option<Vec<String>>,
        wt_protocol: Option<String>,
        data: Option<Bytes>,
        is_unidirectional: Option<bool>,
        max_data: Option<u64>,
        max_streams: Option<u64>,
        ready_at: Option<f64>,
        error_code: Option<ErrorCode>,
        reason: Option<Cow<'static, str>>,
    },
    EmitStreamEvent {
        stream_id: StreamId,
        event_type: EventType,
        session_id: Option<SessionId>,
        direction: Option<StreamDirection>,
        is_peer_initiated: Option<bool>,
        error_code: Option<ErrorCode>,
    },
    ExportTlsKeyingMaterial {
        request_id: RequestId,
        label: String,
        context: Bytes,
        length: u32,
    },
    NotifyRequestDone {
        request_id: RequestId,
        result: RequestResult,
    },
    NotifyRequestFailed {
        request_id: RequestId,
        source: ErrorSource,
        error_code: Option<ErrorCode>,
        reason: Cow<'static, str>,
    },
    ProcessProtocolEvent {
        event: Box<ProtocolEvent>,
    },
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
        headers: Headers,
        end_stream: bool,
    },
    SendQuicData {
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    SendQuicDatagram {
        header: Bytes,
        payload: Bytes,
    },
    StopQuicStream {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
}

// Asynchronous request completion result.
#[allow(
    variant_size_differences,
    reason = "Optimized layout: 32B zero-copy hot-path vs 8B boxed cold-path."
)]
#[derive(Clone, Debug)]
pub(crate) enum RequestResult {
    ConnectionDiagnostics(Box<ConnectionDiagnostics>),
    KeyingMaterial(Bytes),
    None,
    ReadData(Bytes),
    SessionDiagnostics(Box<SessionDiagnostics>),
    SessionId(SessionId),
    StreamDiagnostics(Box<StreamDiagnostics>),
    StreamId(StreamId),
}

#[cfg(test)]
mod tests;
