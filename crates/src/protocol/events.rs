//! Protocol event and effect definitions.

use std::borrow::Cow;

use bytes::Bytes;

use crate::common::types::{
    ConnectionHandle, ErrorCode, ErrorSource, EventType, Headers, RequestId, SessionId,
    StreamDirection, StreamId,
};
use crate::protocol::connection::ConnectionDiagnostics;
use crate::protocol::h3::H3Settings;
use crate::protocol::session::SessionDiagnostics;
use crate::protocol::stream::StreamDiagnostics;

#[derive(Clone, Debug)]
pub(crate) enum Effect {
    CloseQuicConnection {
        error_code: ErrorCode,
        reason: Option<Cow<'static, str>>,
    },
    CreateH3ConnectStream {
        request_id: RequestId,
        authority: String,
        path: String,
        headers: Headers,
    },
    CreateH3LocalStreams,
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
        status_code: Option<u16>,
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
    ReceiveQuicData {
        stream_id: StreamId,
        max_size: u64,
    },
    ResetQuicStream {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    SendH3Capsule {
        stream_id: StreamId,
        capsule_type: u64,
        capsule_value: Bytes,
        end_stream: bool,
    },
    SendH3Datagram {
        stream_id: StreamId,
        data: Bytes,
    },
    SendH3Goaway {
        identifier: u64,
    },
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

#[derive(Clone, Debug)]
pub(crate) enum ProtocolEvent {
    H3CapsuleReceived {
        stream_id: StreamId,
        capsule_type: u64,
        capsule_value: Bytes,
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
    },
    H3SettingsReceived {
        settings: H3Settings,
    },
    H3WebTransportStreamOpened {
        session_id: SessionId,
        stream_id: StreamId,
    },
    InternalBindH3ConnectStream {
        request_id: RequestId,
        stream_id: StreamId,
    },
    InternalBindH3LocalStreams {
        control_stream_id: StreamId,
        encoder_stream_id: StreamId,
        decoder_stream_id: StreamId,
    },
    InternalBindQuicStream {
        request_id: RequestId,
        stream_id: StreamId,
        session_id: SessionId,
        is_unidirectional: bool,
    },
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalCloseConnection,
    InternalCompleteQuicData {
        stream_id: StreamId,
    },
    InternalDeferH3ConnectStream {
        request_id: RequestId,
    },
    InternalDeferQuicStream {
        request_id: RequestId,
        session_id: SessionId,
        is_unidirectional: bool,
    },
    InternalFailH3ConnectStream {
        request_id: RequestId,
        error_code: Option<ErrorCode>,
        reason: Cow<'static, str>,
    },
    InternalFailH3LocalStreams,
    TransportConnectionTerminated {
        error_code: ErrorCode,
        reason: Cow<'static, str>,
    },
    TransportDatagramReceived {
        data: Bytes,
    },
    TransportHandshakeCompleted,
    TransportMaxStreamsReceived {
        is_unidirectional: bool,
    },
    TransportQuicParametersReceived {
        peer_max_datagram_frame_size: u64,
    },
    TransportResetStreamReceived {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    TransportStopSendingReceived {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    TransportStreamDataAcknowledged {
        stream_id: StreamId,
    },
    TransportStreamDataReceived {
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    TransportStreamOpened {
        stream_id: StreamId,
    },
    UserAcceptSession {
        request_id: RequestId,
        session_id: SessionId,
        headers: Headers,
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
    UserCreateSessionOptimistic {
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
    UserDiagnoseConnection {
        request_id: RequestId,
    },
    UserDiagnoseSession {
        request_id: RequestId,
        session_id: SessionId,
    },
    UserDiagnoseStream {
        request_id: RequestId,
        stream_id: StreamId,
    },
    UserExportKeyingMaterial {
        request_id: RequestId,
        session_id: SessionId,
        label: String,
        context: Bytes,
        length: u32,
    },
    UserReadDatagram {
        request_id: RequestId,
        session_id: SessionId,
    },
    UserReadStream {
        request_id: RequestId,
        stream_id: StreamId,
        max_size: u64,
    },
    UserRejectSession {
        request_id: RequestId,
        session_id: SessionId,
        headers: Headers,
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
    UserStopSending {
        request_id: RequestId,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    UserWriteStream {
        request_id: RequestId,
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
}

#[allow(
    variant_size_differences,
    reason = "Optimized layout: 32B zero-copy hot-path vs 8B boxed cold-path."
)]
#[derive(Clone, Debug)]
pub(crate) enum RequestResult {
    ConnectionDiagnostics(Box<ConnectionDiagnostics>),
    ConnectionHandle(ConnectionHandle),
    Datagram(Bytes),
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
