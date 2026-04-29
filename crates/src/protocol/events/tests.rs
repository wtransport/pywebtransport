//! Unit tests for the `crate::protocol::events` module.

use std::borrow::Cow;
use std::collections::{HashSet, VecDeque};

use bytes::Bytes;
use rstest::*;

use super::*;
use crate::common::types::{
    ConnectionState, ErrorCode, ErrorSource, EventType, Headers, RequestId, SessionId,
    SessionState, StreamDirection, StreamId, StreamState,
};
use crate::protocol::H3Settings;
use crate::protocol::{ConnectionDiagnostics, SessionDiagnostics, StreamDiagnostics};

#[fixture]
fn fixture_bytes() -> Bytes {
    Bytes::from_static(b"payload_data")
}

#[fixture]
fn fixture_error_code() -> ErrorCode {
    404
}

#[fixture]
fn fixture_error_source() -> ErrorSource {
    ErrorSource::Stream
}

#[fixture]
fn fixture_headers() -> Headers {
    vec![
        (Bytes::from("content-type"), Bytes::from("application/json")),
        (Bytes::from("user-agent"), Bytes::from("rust-client")),
    ]
}

#[fixture]
fn fixture_request_id() -> RequestId {
    100
}

#[fixture]
fn fixture_session_id() -> SessionId {
    2
}

#[fixture]
fn fixture_stream_id() -> StreamId {
    4
}

#[rstest]
fn test_effect_emit_session_event_optional_fields_none_success(fixture_session_id: SessionId) {
    let effect = Effect::EmitSessionEvent {
        session_id: fixture_session_id,
        event_type: EventType::SessionClosed,
        path: None,
        headers: None,
        wt_available_protocols: None,
        wt_protocol: None,
        data: None,
        is_unidirectional: None,
        max_data: None,
        max_streams: None,
        ready_at: None,
        error_code: None,
        reason: None,
    };

    assert!(matches!(effect, Effect::EmitSessionEvent { .. }));

    if let Effect::EmitSessionEvent {
        headers,
        wt_available_protocols,
        wt_protocol,
        error_code,
        reason,
        ..
    } = effect
    {
        assert!(headers.is_none());
        assert!(wt_available_protocols.is_none());
        assert!(wt_protocol.is_none());
        assert!(error_code.is_none());
        assert!(reason.is_none());
    }
}

#[rstest]
fn test_effect_emit_session_event_optional_fields_some_success(
    fixture_session_id: SessionId,
    fixture_headers: Headers,
    fixture_bytes: Bytes,
    fixture_error_code: ErrorCode,
) {
    let effect = Effect::EmitSessionEvent {
        session_id: fixture_session_id,
        event_type: EventType::SessionReady,
        path: Some("/test".to_owned()),
        headers: Some(fixture_headers),
        wt_available_protocols: Some(vec!["h3".to_owned()]),
        wt_protocol: Some("h3".to_owned()),
        data: Some(fixture_bytes),
        is_unidirectional: Some(true),
        max_data: Some(1024),
        max_streams: Some(10),
        ready_at: Some(1.5),
        error_code: Some(fixture_error_code),
        reason: Some(Cow::Borrowed("OK")),
    };

    assert!(matches!(effect, Effect::EmitSessionEvent { .. }));

    if let Effect::EmitSessionEvent {
        path,
        wt_available_protocols,
        wt_protocol,
        max_data,
        ready_at,
        ..
    } = effect
    {
        assert_eq!(path, Some("/test".to_owned()));
        assert_eq!(wt_available_protocols, Some(vec!["h3".to_owned()]));
        assert_eq!(wt_protocol, Some("h3".to_owned()));
        assert_eq!(max_data, Some(1024));
        assert!(ready_at.is_some());
    }
}

#[rstest]
fn test_effect_emit_stream_event_full_fields_success(
    fixture_stream_id: StreamId,
    fixture_session_id: SessionId,
    fixture_error_code: ErrorCode,
) {
    let effect = Effect::EmitStreamEvent {
        stream_id: fixture_stream_id,
        event_type: EventType::StreamOpened,
        direction: Some(StreamDirection::Bidirectional),
        session_id: Some(fixture_session_id),
        is_peer_initiated: Some(true),
        error_code: Some(fixture_error_code),
    };

    assert!(matches!(effect, Effect::EmitStreamEvent { .. }));

    if let Effect::EmitStreamEvent {
        is_peer_initiated,
        error_code,
        ..
    } = effect
    {
        assert_eq!(is_peer_initiated, Some(true));
        assert_eq!(error_code, Some(fixture_error_code));
    }
}

#[rstest]
fn test_effect_export_tls_keying_material_success(
    fixture_request_id: RequestId,
    fixture_bytes: Bytes,
) {
    let effect = Effect::ExportTlsKeyingMaterial {
        request_id: fixture_request_id,
        label: "EXPORTER-test".to_owned(),
        context: fixture_bytes,
        length: 32,
    };

    let debug_output = format!("{effect:?}");
    assert!(debug_output.contains("ExportTlsKeyingMaterial"));
    assert!(debug_output.contains("EXPORTER-test"));
}

#[rstest]
fn test_effect_notify_request_failed_structure_success(
    fixture_request_id: RequestId,
    fixture_error_source: ErrorSource,
    fixture_error_code: ErrorCode,
) {
    let effect = Effect::NotifyRequestFailed {
        request_id: fixture_request_id,
        source: fixture_error_source,
        error_code: Some(fixture_error_code),
        reason: Cow::Borrowed("Failed"),
    };

    assert!(matches!(effect, Effect::NotifyRequestFailed { .. }));

    if let Effect::NotifyRequestFailed { source, .. } = effect {
        assert_eq!(source, fixture_error_source);
    }
}

#[rstest]
fn test_effect_send_h3_headers_lifecycle_success(
    fixture_stream_id: StreamId,
    fixture_headers: Headers,
) {
    let effect = Effect::SendH3Headers {
        stream_id: fixture_stream_id,
        headers: fixture_headers.clone(),
        end_stream: true,
    };

    let cloned = effect.clone();

    assert!(matches!(
        (&effect, &cloned),
        (Effect::SendH3Headers { .. }, Effect::SendH3Headers { .. })
    ));

    if let (
        Effect::SendH3Headers {
            stream_id: s1,
            headers: h1,
            end_stream: e1,
        },
        Effect::SendH3Headers {
            stream_id: s2,
            headers: h2,
            end_stream: e2,
        },
    ) = (effect, cloned)
    {
        assert_eq!(s1, s2);
        assert_eq!(h1, h2);
        assert_eq!(e1, e2);
    }
}

#[rstest]
fn test_protocol_event_clone_integrity_success(fixture_stream_id: StreamId, fixture_bytes: Bytes) {
    let original = ProtocolEvent::H3DatagramReceived {
        stream_id: fixture_stream_id,
        data: fixture_bytes.clone(),
    };

    let cloned = original.clone();

    assert!(matches!(
        (&original, &cloned),
        (
            ProtocolEvent::H3DatagramReceived { .. },
            ProtocolEvent::H3DatagramReceived { .. }
        )
    ));

    if let (
        ProtocolEvent::H3DatagramReceived {
            stream_id: id1,
            data: d1,
        },
        ProtocolEvent::H3DatagramReceived {
            stream_id: id2,
            data: d2,
        },
    ) = (original, cloned)
    {
        assert_eq!(id1, id2);
        assert_eq!(d1, d2);
    }
}

#[rstest]
fn test_protocol_event_h3_settings_received_instantiation_success() {
    let event = ProtocolEvent::H3SettingsReceived {
        settings: H3Settings::default(),
    };

    assert!(matches!(event, ProtocolEvent::H3SettingsReceived { .. }));
}

#[rstest]
fn test_protocol_event_internal_bind_quic_stream_debug_formatting_success(
    fixture_request_id: RequestId,
    fixture_stream_id: StreamId,
    fixture_session_id: SessionId,
) {
    let event = ProtocolEvent::InternalBindQuicStream {
        request_id: fixture_request_id,
        stream_id: fixture_stream_id,
        session_id: fixture_session_id,
        is_unidirectional: true,
    };

    let debug_output = format!("{event:?}");

    assert!(debug_output.contains("InternalBindQuicStream"));
    assert!(debug_output.contains("is_unidirectional: true"));
}

#[rstest]
fn test_protocol_event_transport_connection_terminated_properties_success(
    fixture_error_code: ErrorCode,
) {
    let reason: Cow<'static, str> = Cow::Owned("Connection timeout".to_owned());
    let event = ProtocolEvent::TransportConnectionTerminated {
        error_code: fixture_error_code,
        reason: reason.clone(),
    };

    assert!(matches!(
        event,
        ProtocolEvent::TransportConnectionTerminated { .. }
    ));

    if let ProtocolEvent::TransportConnectionTerminated {
        error_code,
        reason: r,
    } = event
    {
        assert_eq!(error_code, fixture_error_code);
        assert_eq!(r, reason);
    }
}

#[rstest]
fn test_protocol_event_transport_stop_sending_received_success(
    fixture_stream_id: StreamId,
    fixture_error_code: ErrorCode,
) {
    let event = ProtocolEvent::TransportStopSendingReceived {
        stream_id: fixture_stream_id,
        error_code: fixture_error_code,
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("TransportStopSendingReceived"));
    assert!(debug_output.contains(&fixture_stream_id.to_string()));
}

#[rstest]
fn test_protocol_event_user_accept_session_success(
    fixture_request_id: RequestId,
    fixture_session_id: SessionId,
) {
    let event = ProtocolEvent::UserAcceptSession {
        request_id: fixture_request_id,
        session_id: fixture_session_id,
        wt_protocol: Some("webtransport".to_owned()),
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserAcceptSession"));
    assert!(debug_output.contains("webtransport"));
}

#[rstest]
fn test_protocol_event_user_create_session_success(
    fixture_request_id: RequestId,
    fixture_headers: Headers,
) {
    let event = ProtocolEvent::UserCreateSession {
        request_id: fixture_request_id,
        path: "/path".to_owned(),
        headers: fixture_headers,
        wt_available_protocols: Some(vec!["p1".to_owned()]),
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserCreateSession"));
    assert!(debug_output.contains("p1"));
}

#[rstest]
fn test_protocol_event_user_export_keying_material_success(
    fixture_request_id: RequestId,
    fixture_session_id: SessionId,
    fixture_bytes: Bytes,
) {
    let event = ProtocolEvent::UserExportKeyingMaterial {
        request_id: fixture_request_id,
        session_id: fixture_session_id,
        label: "EXPORTER-test".to_owned(),
        context: fixture_bytes,
        length: 32,
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserExportKeyingMaterial"));
    assert!(debug_output.contains("EXPORTER-test"));
}

#[rstest]
fn test_protocol_event_user_stop_sending_success(
    fixture_request_id: RequestId,
    fixture_stream_id: StreamId,
    fixture_error_code: ErrorCode,
) {
    let event = ProtocolEvent::UserStopSending {
        request_id: fixture_request_id,
        stream_id: fixture_stream_id,
        error_code: fixture_error_code,
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserStopSending"));
    assert!(debug_output.contains(&fixture_request_id.to_string()));
}

#[rstest]
fn test_request_result_read_data_content_success(fixture_bytes: Bytes) {
    let result = RequestResult::ReadData(fixture_bytes.clone());

    assert!(matches!(result, RequestResult::ReadData(_)));

    if let RequestResult::ReadData(data) = result {
        assert_eq!(data, fixture_bytes);
    }
}

#[rstest]
#[case::conn_diag(RequestResult::ConnectionDiagnostics(Box::new(ConnectionDiagnostics {
    connection_handle: 42,
    is_client: true,
    close_code: None,
    close_reason: None,
    closed_at: None,
    connected_at: None,
    handshake_complete: true,
    local_goaway_sent: false,
    peer_goaway_received: false,
    peer_settings_received: true,
    state: ConnectionState::Connected,
    early_event_count: 0,
    peer_initial_max_data: 0,
    peer_initial_max_streams_bidi: 0,
    peer_initial_max_streams_uni: 0,
    peer_max_datagram_frame_size: None,
    pending_request_count: 0,
    session_count: 0,
    stream_count: 0,
})))]
#[case::key_mat(RequestResult::KeyingMaterial(Bytes::from_static(b"key")))]
#[case::none(RequestResult::None)]
#[case::read_data(RequestResult::ReadData(Bytes::from_static(b"data")))]
#[case::sess_diag(RequestResult::SessionDiagnostics(Box::new(SessionDiagnostics {
    headers: vec![],
    is_client: false,
    path: "/".to_owned(),
    session_id: 1,
    wt_protocol: None,
    close_code: None,
    close_reason: None,
    closed_at: None,
    created_at: 0.0,
    flow_control_negotiated: true,
    ready_at: None,
    state: SessionState::Connected,
    active_streams: HashSet::new(),
    blocked_streams: HashSet::new(),
    pending_bidi_stream_requests: VecDeque::new(),
    pending_uni_stream_requests: VecDeque::new(),
    datagram_bytes_received: 0,
    datagram_bytes_sent: 0,
    datagrams_received: 0,
    datagrams_sent: 0,
    local_data_consumed: 0,
    local_data_received: 0,
    local_data_sent: 0,
    local_max_data: 0,
    local_max_streams_bidi: 0,
    local_max_streams_uni: 0,
    local_streams_bidi_opened: 0,
    local_streams_uni_opened: 0,
    peer_max_data: 0,
    peer_max_streams_bidi: 0,
    peer_max_streams_uni: 0,
    peer_streams_bidi_closed: 0,
    peer_streams_bidi_opened: 0,
    peer_streams_uni_closed: 0,
    peer_streams_uni_opened: 0,
})))]
#[case::session(RequestResult::SessionId(1))]
#[case::stream_diag(RequestResult::StreamDiagnostics(Box::new(StreamDiagnostics {
    direction: StreamDirection::Bidirectional,
    is_peer_initiated: false,
    session_id: 1,
    stream_id: 1,
    close_code: None,
    close_reason: None,
    closed_at: None,
    created_at: 0.0,
    state: StreamState::Open,
    bytes_received: 0,
    bytes_sent: 0,
    read_buffer_size: 0,
    write_buffer_size: 0,
})))]
#[case::stream(RequestResult::StreamId(2))]
fn test_request_result_variants_instantiation_success(#[case] result: RequestResult) {
    let debug_str = format!("{result:?}");

    assert!(!debug_str.is_empty());
}
