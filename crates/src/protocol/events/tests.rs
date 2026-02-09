//! Unit tests for the `crate::protocol::events` module.

use std::collections::HashMap;

use bytes::Bytes;
use rstest::*;

use super::*;
use crate::common::types::{
    ErrorCode, ErrorSource, EventType, Headers, RequestId, SessionId, StreamDirection, StreamId,
};

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
        error_code,
        reason,
        ..
    } = effect
    {
        assert!(headers.is_none());
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
        data: Some(fixture_bytes),
        is_unidirectional: Some(true),
        max_data: Some(1024),
        max_streams: Some(10),
        ready_at: Some(1.5),
        error_code: Some(fixture_error_code),
        reason: Some("OK".to_owned()),
    };

    assert!(matches!(effect, Effect::EmitSessionEvent { .. }));

    if let Effect::EmitSessionEvent {
        path,
        max_data,
        ready_at,
        ..
    } = effect
    {
        assert_eq!(path, Some("/test".to_owned()));
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
fn test_effect_log_h3_frame_structure_success() {
    let effect = Effect::LogH3Frame {
        category: "TRANSPORT".to_owned(),
        event: "PACKET_SENT".to_owned(),
        data: "payload=0x00".to_owned(),
    };

    assert!(matches!(effect, Effect::LogH3Frame { .. }));

    if let Effect::LogH3Frame { category, .. } = effect {
        assert_eq!(category, "TRANSPORT");
    }
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
        reason: "Failed".to_owned(),
    };

    assert!(matches!(effect, Effect::NotifyRequestFailed { .. }));

    if let Effect::NotifyRequestFailed { source, .. } = effect {
        assert_eq!(source, fixture_error_source);
    }
}

#[rstest]
fn test_effect_send_h3_headers_lifecycle_success(fixture_stream_id: StreamId) {
    let effect = Effect::SendH3Headers {
        stream_id: fixture_stream_id,
        status: 200,
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
            status: st1,
            end_stream: e1,
        },
        Effect::SendH3Headers {
            stream_id: s2,
            status: st2,
            end_stream: e2,
        },
    ) = (effect, cloned)
    {
        assert_eq!(s1, s2);
        assert_eq!(st1, st2);
        assert_eq!(e1, e2);
    }
}

#[rstest]
fn test_protocol_event_clone_integrity_success(fixture_stream_id: StreamId, fixture_bytes: Bytes) {
    let original = ProtocolEvent::InternalReturnStreamData {
        stream_id: fixture_stream_id,
        data: fixture_bytes.clone(),
    };

    let cloned = original.clone();

    assert!(matches!(
        (&original, &cloned),
        (
            ProtocolEvent::InternalReturnStreamData { .. },
            ProtocolEvent::InternalReturnStreamData { .. }
        )
    ));

    if let (
        ProtocolEvent::InternalReturnStreamData {
            stream_id: id1,
            data: d1,
        },
        ProtocolEvent::InternalReturnStreamData {
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
fn test_protocol_event_settings_received_map_handling_success() {
    let mut settings = HashMap::new();
    settings.insert(1, 100);
    settings.insert(2, 200);

    let event = ProtocolEvent::SettingsReceived {
        settings: settings.clone(),
    };

    assert!(matches!(event, ProtocolEvent::SettingsReceived { .. }));

    if let ProtocolEvent::SettingsReceived { settings: map } = event {
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&1), Some(&100));
    }
}

#[rstest]
fn test_protocol_event_transport_connection_terminated_properties_success(
    fixture_error_code: ErrorCode,
) {
    let reason = "Connection timeout".to_owned();

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
#[case::diagnostics(RequestResult::Diagnostics("info".to_owned()))]
#[case::none(RequestResult::None)]
#[case::read_data(RequestResult::ReadData(Bytes::from_static(b"data")))]
#[case::session(RequestResult::SessionId(1))]
#[case::stream(RequestResult::StreamId(2))]
fn test_request_result_variants_instantiation_success(#[case] result: RequestResult) {
    let debug_str = format!("{result:?}");

    assert!(!debug_str.is_empty());
}
