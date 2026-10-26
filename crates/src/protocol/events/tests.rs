//! Unit tests for the `crate::protocol::events` module.

use std::borrow::Cow;

use bytes::Bytes;
use rstest::{fixture, rstest};

use super::*;
use crate::common::types::{
    ConnectionState, ErrorCode, ErrorSource, EventType, Headers, RequestId, SessionId,
    SessionState, StreamDirection, StreamId, StreamReceiveState, StreamSendState,
};
use crate::protocol::connection::ConnectionDiagnostics;
use crate::protocol::h3::H3Settings;
use crate::protocol::session::SessionDiagnostics;
use crate::protocol::stream::StreamDiagnostics;

#[fixture]
fn fixture_data() -> Bytes {
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
fn fixture_header() -> Bytes {
    Bytes::from_static(b"header_data")
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
fn test_effect_create_h3_connect_stream_success(
    fixture_request_id: RequestId,
    fixture_headers: Headers,
) {
    let effect = Effect::CreateH3ConnectStream {
        request_id: fixture_request_id,
        authority: "example.com".to_owned(),
        path: "/".to_owned(),
        headers: fixture_headers,
    };

    let debug_output = format!("{effect:?}");
    assert!(debug_output.contains("CreateH3ConnectStream"));
    assert!(debug_output.contains("example.com"));
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
        status_code: None,
        error_code: None,
        reason: None,
    };

    assert!(matches!(effect, Effect::EmitSessionEvent { .. }));

    if let Effect::EmitSessionEvent {
        headers,
        wt_available_protocols,
        wt_protocol,
        status_code,
        error_code,
        reason,
        ..
    } = effect
    {
        assert!(headers.is_none());
        assert!(wt_available_protocols.is_none());
        assert!(wt_protocol.is_none());
        assert!(status_code.is_none());
        assert!(error_code.is_none());
        assert!(reason.is_none());
    }
}

#[rstest]
fn test_effect_emit_session_event_optional_fields_some_success(
    fixture_session_id: SessionId,
    fixture_headers: Headers,
    fixture_data: Bytes,
    fixture_error_code: ErrorCode,
) {
    let effect = Effect::EmitSessionEvent {
        session_id: fixture_session_id,
        event_type: EventType::SessionReady,
        path: Some("/test".to_owned()),
        headers: Some(fixture_headers),
        wt_available_protocols: Some(vec!["h3".to_owned()]),
        wt_protocol: Some("h3".to_owned()),
        data: Some(fixture_data),
        is_unidirectional: Some(true),
        max_data: Some(4 * 1024 * 1024),
        max_streams: Some(10),
        ready_at: Some(1.5),
        status_code: Some(403),
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
        assert_eq!(max_data, Some(4 * 1024 * 1024));
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
    fixture_data: Bytes,
) {
    let effect = Effect::ExportTlsKeyingMaterial {
        request_id: fixture_request_id,
        label: "EXPORTER-test".to_owned(),
        context: fixture_data,
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
        reason: Cow::Borrowed("failed"),
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
            stream_id: original_stream_id,
            headers: original_headers,
            end_stream: original_end_stream,
        },
        Effect::SendH3Headers {
            stream_id: cloned_stream_id,
            headers: cloned_headers,
            end_stream: cloned_end_stream,
        },
    ) = (effect, cloned)
    {
        assert_eq!(original_stream_id, cloned_stream_id);
        assert_eq!(original_headers, cloned_headers);
        assert_eq!(original_end_stream, cloned_end_stream);
    }
}

#[rstest]
fn test_effect_send_quic_datagram_structure_success(fixture_header: Bytes, fixture_data: Bytes) {
    let effect = Effect::SendQuicDatagram {
        header: fixture_header.clone(),
        payload: fixture_data.clone(),
    };

    assert!(matches!(effect, Effect::SendQuicDatagram { .. }));

    if let Effect::SendQuicDatagram { header, payload } = effect {
        assert_eq!(header, fixture_header);
        assert_eq!(payload, fixture_data);
    }
}

#[rstest]
fn test_protocol_event_clone_integrity_success(fixture_session_id: SessionId, fixture_data: Bytes) {
    let original = ProtocolEvent::H3DatagramReceived {
        session_id: fixture_session_id,
        data: fixture_data.clone(),
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
            session_id: original_session_id,
            data: original_data,
        },
        ProtocolEvent::H3DatagramReceived {
            session_id: cloned_session_id,
            data: cloned_data,
        },
    ) = (original, cloned)
    {
        assert_eq!(original_session_id, cloned_session_id);
        assert_eq!(original_data, cloned_data);
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
    let reason: Cow<'static, str> = Cow::Owned("connection timeout".to_owned());
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
        reason: event_reason,
    } = event
    {
        assert_eq!(error_code, fixture_error_code);
        assert_eq!(event_reason, reason);
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
    fixture_headers: Headers,
) {
    let event = ProtocolEvent::UserAcceptSession {
        request_id: fixture_request_id,
        session_id: fixture_session_id,
        headers: fixture_headers,
        wt_protocol: Some("webtransport".to_owned()),
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserAcceptSession"));
    assert!(debug_output.contains("user-agent"));
    assert!(debug_output.contains("webtransport"));
}

#[rstest]
fn test_protocol_event_user_create_session_optimistic_success(
    fixture_request_id: RequestId,
    fixture_headers: Headers,
) {
    let event = ProtocolEvent::UserCreateSessionOptimistic {
        request_id: fixture_request_id,
        authority: "localhost".to_owned(),
        path: "/path".to_owned(),
        headers: fixture_headers,
        wt_available_protocols: Some(vec!["p1".to_owned()]),
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserCreateSessionOptimistic"));
    assert!(debug_output.contains("localhost"));
    assert!(debug_output.contains("p1"));
}

#[rstest]
fn test_protocol_event_user_create_session_success(
    fixture_request_id: RequestId,
    fixture_headers: Headers,
) {
    let event = ProtocolEvent::UserCreateSession {
        request_id: fixture_request_id,
        authority: "localhost".to_owned(),
        path: "/path".to_owned(),
        headers: fixture_headers,
        wt_available_protocols: Some(vec!["p1".to_owned()]),
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserCreateSession"));
    assert!(debug_output.contains("localhost"));
    assert!(debug_output.contains("p1"));
}

#[rstest]
fn test_protocol_event_user_export_keying_material_success(
    fixture_request_id: RequestId,
    fixture_session_id: SessionId,
    fixture_data: Bytes,
) {
    let event = ProtocolEvent::UserExportKeyingMaterial {
        request_id: fixture_request_id,
        session_id: fixture_session_id,
        label: "EXPORTER-test".to_owned(),
        context: fixture_data,
        length: 32,
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserExportKeyingMaterial"));
    assert!(debug_output.contains("EXPORTER-test"));
}

#[rstest]
fn test_protocol_event_user_reject_session_success(
    fixture_request_id: RequestId,
    fixture_session_id: SessionId,
    fixture_headers: Headers,
) {
    let event = ProtocolEvent::UserRejectSession {
        request_id: fixture_request_id,
        session_id: fixture_session_id,
        headers: fixture_headers,
        status_code: 403,
    };

    let debug_output = format!("{event:?}");
    assert!(debug_output.contains("UserRejectSession"));
    assert!(debug_output.contains("user-agent"));
    assert!(debug_output.contains("403"));
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
fn test_request_result_read_data_content_success(fixture_data: Bytes) {
    let result = RequestResult::ReadData(fixture_data.clone());

    assert!(matches!(result, RequestResult::ReadData(_)));

    if let RequestResult::ReadData(data) = result {
        assert_eq!(data, fixture_data);
    }
}

#[rstest]
#[case::connection_diagnostics(RequestResult::ConnectionDiagnostics(Box::new(ConnectionDiagnostics {
    close_code: None,
    close_reason: None,
    closed_at: None,
    connected_at: None,
    connection_handle: 42,
    handshake_completed: true,
    is_client: true,
    local_goaway_sent: false,
    peer_goaway_received: false,
    peer_initial_max_data: 0,
    peer_initial_max_streams_bidi: 0,
    peer_initial_max_streams_uni: 0,
    peer_max_datagram_frame_size: None,
    peer_settings_received: true,
    state: ConnectionState::Connected,
})))]
#[case::connection_handle(RequestResult::ConnectionHandle(42))]
#[case::datagram(RequestResult::Datagram(Bytes::from_static(b"datagram")))]
#[case::keying_material(RequestResult::KeyingMaterial(Bytes::from_static(b"key")))]
#[case::none(RequestResult::None)]
#[case::read_data(RequestResult::ReadData(Bytes::from_static(b"data")))]
#[case::session_diagnostics(RequestResult::SessionDiagnostics(Box::new(SessionDiagnostics {
    close_code: None,
    close_reason: None,
    closed_at: None,
    created_at: 0.0,
    data_consumed: 0,
    data_received: 0,
    data_sent: 0,
    flow_control_negotiated: true,
    headers: vec![],
    is_client: false,
    local_max_data: 0,
    local_max_streams_bidi: 0,
    local_max_streams_uni: 0,
    local_streams_bidi_opened: 0,
    local_streams_uni_opened: 0,
    path: "/".to_owned(),
    peer_max_data: 0,
    peer_max_streams_bidi: 0,
    peer_max_streams_uni: 0,
    peer_streams_bidi_closed: 0,
    peer_streams_bidi_opened: 0,
    peer_streams_uni_closed: 0,
    peer_streams_uni_opened: 0,
    ready_at: None,
    session_id: 1,
    state: SessionState::Connected,
    wt_available_protocols: None,
    wt_protocol: None,
})))]
#[case::session_id(RequestResult::SessionId(1))]
#[case::stream_diagnostics(RequestResult::StreamDiagnostics(Box::new(StreamDiagnostics {
    created_at: 0.0,
    direction: StreamDirection::Bidirectional,
    is_peer_initiated: false,
    receive_error_code: None,
    receive_state: Some(StreamReceiveState::Receive),
    send_error_code: None,
    send_state: Some(StreamSendState::Ready),
    session_id: 1,
    stream_id: 1,
})))]
#[case::stream_id(RequestResult::StreamId(2))]
fn test_request_result_variants_instantiation_success(#[case] result: RequestResult) {
    let debug_output = format!("{result:?}");

    assert!(!debug_output.is_empty());
}
