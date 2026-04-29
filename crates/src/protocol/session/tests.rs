//! Unit tests for the `crate::protocol::session` module.

use bytes::{BufMut, Bytes, BytesMut};
use rstest::*;

use super::*;
use crate::common::constants::{
    ERR_H3_DATAGRAM_ERROR, ERR_H3_FRAME_UNEXPECTED, ERR_H3_GENERAL_PROTOCOL_ERROR,
    ERR_LIB_INTERNAL_ERROR, ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR,
    ERR_WT_FLOW_CONTROL_ERROR, WT_CAPSULE_TYPE_CLOSE_SESSION, WT_CAPSULE_TYPE_DATA_BLOCKED,
    WT_CAPSULE_TYPE_DRAIN_SESSION, WT_CAPSULE_TYPE_MAX_DATA, WT_CAPSULE_TYPE_MAX_STREAM_DATA,
    WT_CAPSULE_TYPE_MAX_STREAMS_BIDI, WT_CAPSULE_TYPE_MAX_STREAMS_UNI,
    WT_CAPSULE_TYPE_STREAM_DATA_BLOCKED, WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
    WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI, WT_PROTOCOL, WT_STREAMS_LIMIT,
};
use crate::common::types::{ErrorSource, EventType, SessionState, StreamDirection};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::utils::write_varint;

#[fixture]
fn fixture_client_session(fixture_headers: Headers) -> Session {
    Session::new(
        100,
        "/test".to_owned(),
        fixture_headers,
        None,
        SessionState::Connecting,
        true,
        SessionParams {
            flow_control_negotiated: true,
            flow_control_window: 1024,
            flow_control_window_auto_scale_enabled: true,
            initial_max_data: 10_000,
            initial_max_streams_bidi: 5,
            initial_max_streams_uni: 5,
            max_stream_read_buffer_size: 1024,
            max_stream_write_buffer_size: 1024,
            peer_max_data: 10_000,
            peer_max_streams_bidi: 5,
            peer_max_streams_uni: 5,
        },
        0.0,
    )
}

#[fixture]
fn fixture_headers() -> Headers {
    vec![]
}

#[fixture]
fn fixture_server_session(fixture_headers: Headers) -> Session {
    Session::new(
        100,
        "/test".to_owned(),
        fixture_headers,
        None,
        SessionState::Connecting,
        false,
        SessionParams {
            flow_control_negotiated: true,
            flow_control_window: 1024,
            flow_control_window_auto_scale_enabled: true,
            initial_max_data: 10_000,
            initial_max_streams_bidi: 5,
            initial_max_streams_uni: 5,
            max_stream_read_buffer_size: 1024,
            max_stream_write_buffer_size: 1024,
            peer_max_data: 10_000,
            peer_max_streams_bidi: 5,
            peer_max_streams_uni: 5,
        },
        0.0,
    )
}

#[rstest]
fn test_accept_session_client_failure(mut fixture_client_session: Session) {
    let effects = fixture_client_session.accept(500, None, 1.0);

    assert_eq!(fixture_client_session.state, SessionState::Connecting);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_session validate failed");
    }
}

#[rstest]
fn test_accept_session_server_success(mut fixture_server_session: Session) {
    let effects = fixture_server_session.accept(500, None, 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Connected);
    assert!(fixture_server_session.ready_at.is_some());
    assert!(fixture_server_session.wt_protocol.is_none());

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Headers { .. },
            Effect::EmitSessionEvent {
                event_type: EventType::SessionReady,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_accept_session_with_invalid_wt_protocol_failure(mut fixture_server_session: Session) {
    let effects = fixture_server_session.accept(500, Some("bad\nproto".to_owned()), 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Connecting);
    assert!(fixture_server_session.ready_at.is_none());

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: None,
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_protocol encode failed");
    }
}

#[rstest]
fn test_accept_session_with_wt_protocol_success(mut fixture_server_session: Session) {
    let effects = fixture_server_session.accept(500, Some("h3".to_owned()), 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Connected);
    assert_eq!(fixture_server_session.wt_protocol, Some("h3".to_owned()));

    let has_protocol_header = effects.iter().any(|e| {
        if let Effect::SendH3Headers { headers, .. } = e {
            headers.iter().any(|(k, v)| {
                k.as_ref() == WT_PROTOCOL && String::from_utf8_lossy(v).contains("\"h3\"")
            })
        } else {
            false
        }
    });

    assert!(has_protocol_header);
}

#[rstest]
fn test_accept_session_wrong_state_failure(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;

    let effects = fixture_server_session.accept(500, None, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_session validate failed");
    }
}

#[rstest]
fn test_bind_stream_success(mut fixture_server_session: Session) {
    let stream_id = 4;
    let effects = fixture_server_session.bind_stream(stream_id, 500, false, 1.0);

    assert!(fixture_server_session.streams.contains_key(&stream_id));
    assert!(fixture_server_session.active_streams.contains(&stream_id));

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestDone { .. },
            Effect::EmitStreamEvent {
                event_type: EventType::StreamOpened,
                is_peer_initiated: Some(true),
                ..
            }
        ]
    ));
}

#[rstest]
fn test_close_already_closed_session(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closed;
    let effects = fixture_server_session.close(500, 0, None, 1.0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_close_closing_session(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closing;
    let effects = fixture_server_session.close(500, 0, None, 1.0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_close_session_resets_streams(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, 1, false, 1.0);
    fixture_server_session.bind_stream(8, 2, false, 1.0);

    let effects = fixture_server_session.close(500, 0, None, 1.0);

    let resets = effects
        .iter()
        .filter(|e| matches!(e, Effect::ResetQuicStream { .. }))
        .count();
    assert_eq!(resets, 2);
    assert!(fixture_server_session.active_streams.is_empty());
}

#[rstest]
fn test_close_session_resets_streams_by_direction(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(1, 1, false, 1.0);
    fixture_server_session.recv_stream_data(2, Bytes::from_static(b"data"), false, 1.0);

    let effects = fixture_server_session.close(500, 0, None, 1.0);

    let resets = effects
        .iter()
        .filter(|e| matches!(e, Effect::ResetQuicStream { .. }))
        .count();
    let stops = effects
        .iter()
        .filter(|e| matches!(e, Effect::StopQuicStream { .. }))
        .count();

    assert_eq!(resets, 1);
    assert_eq!(stops, 2);
    assert!(fixture_server_session.active_streams.is_empty());
}

#[rstest]
fn test_close_session_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let error_code = 0;

    let effects = fixture_server_session.close(500, error_code, None, 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Closing);
    assert_eq!(fixture_server_session.close_code, Some(0));

    let has_clean_close = effects.iter().any(|e| {
        matches!(
            e,
            Effect::SendQuicData {
                end_stream: true,
                ..
            }
        )
    });
    assert!(has_clean_close);
}

#[rstest]
fn test_close_session_with_reason_capsule(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;

    let effects = fixture_server_session.close(500, 404, Some("Not Found".into()), 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Closing);

    let has_close_capsule = effects.iter().any(|e| {
        matches!(
            e,
            Effect::SendH3Capsule {
                capsule_type: WT_CAPSULE_TYPE_CLOSE_SESSION,
                ..
            }
        )
    });
    assert!(has_close_capsule);
}

#[rstest]
fn test_create_stream_limit_reached_client_blocking(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connected;
    fixture_client_session.local_streams_bidi_opened = 5;

    let effects = fixture_client_session.create_stream(500, false);

    assert_eq!(fixture_client_session.pending_bidi_stream_requests.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
            ..
        }]
    ));
}

#[rstest]
fn test_create_stream_limit_reached_server_blocking(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.local_streams_bidi_opened = 5;

    let effects = fixture_server_session.create_stream(500, false);

    assert_eq!(fixture_server_session.pending_bidi_stream_requests.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
            ..
        }]
    ));
}

#[rstest]
fn test_create_stream_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;

    let effects = fixture_server_session.create_stream(500, false);

    assert_eq!(fixture_server_session.local_streams_bidi_opened, 1);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateQuicStream {
            session_id: 100,
            ..
        }]
    ));
}

#[rstest]
fn test_create_stream_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connecting;
    let effects = fixture_server_session.create_stream(500, false);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_create_uni_stream_limit_reached_client_blocking(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connected;
    fixture_client_session.local_streams_uni_opened = 5;

    let effects = fixture_client_session.create_stream(500, true);

    assert_eq!(fixture_client_session.pending_uni_stream_requests.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI,
            ..
        }]
    ));
}

#[rstest]
fn test_create_uni_stream_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let effects = fixture_server_session.create_stream(500, true);

    assert_eq!(fixture_server_session.local_streams_uni_opened, 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateQuicStream {
            is_unidirectional: true,
            ..
        }]
    ));
}

#[rstest]
fn test_diagnostics_snapshot(fixture_server_session: Session) {
    let effects = fixture_server_session.diagnose(500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::SessionDiagnostics(_),
            ..
        }]
    ));

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::SessionDiagnostics(diag),
            ..
        },
    ] = effects.as_slice()
    {
        assert_eq!(diag.session_id, 100);
        assert_eq!(diag.state, SessionState::Connecting);
        assert_eq!(diag.local_data_received, 0);
        assert_eq!(diag.wt_protocol, None);
        assert!(!diag.is_client);
        assert!(diag.flow_control_negotiated);
    }
}

#[rstest]
fn test_drain_session_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let effects = fixture_server_session.drain();

    assert_eq!(fixture_server_session.state, SessionState::Draining);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Capsule {
                capsule_type: WT_CAPSULE_TYPE_DRAIN_SESSION,
                ..
            },
            Effect::EmitSessionEvent {
                event_type: EventType::SessionDraining,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_drain_session_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connecting;
    let effects = fixture_server_session.drain();

    assert_eq!(fixture_server_session.state, SessionState::Connecting);
    assert!(effects.is_empty());
}

#[rstest]
fn test_export_keying_material_success(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connected;
    let context = b"test_context";
    let effects =
        fixture_client_session.export_keying_material(500, "EXPORTER-test".to_owned(), context, 32);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ExportTlsKeyingMaterial {
            request_id: 500,
            length: 32,
            ..
        }]
    ));

    if let [
        Effect::ExportTlsKeyingMaterial {
            context: exported_ctx,
            ..
        },
    ] = effects.as_slice()
    {
        assert_eq!(exported_ctx.get(8..), Some(context.as_slice()));
    }
}

#[rstest]
fn test_export_keying_material_wrong_state(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connecting;
    let effects =
        fixture_client_session.export_keying_material(500, "label".to_owned(), b"ctx", 32);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_fail_stream_decrements_counts(mut fixture_server_session: Session) {
    fixture_server_session.local_streams_bidi_opened = 1;
    fixture_server_session.fail_stream(500, false, Some(1), "Fail".into());
    assert_eq!(fixture_server_session.local_streams_bidi_opened, 0);
}

#[rstest]
fn test_flush_blocked_writes_on_max_data_update(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, 500, false, 1.0);
    fixture_server_session.local_data_sent = 10_000;
    fixture_server_session.send_stream_data(4, 500, Bytes::from_static(b"pending"), false);
    assert!(fixture_server_session.blocked_streams.contains(&4));

    let new_max = 11_000;
    let mut buf = BytesMut::new();
    assert_eq!(
        write_varint(&mut buf, new_max).map_err(|e| e.to_string()),
        Ok(())
    );

    let effects = fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_MAX_DATA, &buf.freeze(), 1.0);

    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::SendQuicData { .. }))
    );
    assert!(!fixture_server_session.blocked_streams.contains(&4));
}

#[rstest]
fn test_grant_data_credit_ignore_lower_value(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let lower_credit = 9999;
    let effects = fixture_server_session.grant_data_credit(500, lower_credit);

    assert_eq!(fixture_server_session.local_max_data, 10_000);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_grant_data_credit_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let new_credit = 20_000;
    let effects = fixture_server_session.grant_data_credit(500, new_credit);

    assert_eq!(fixture_server_session.local_max_data, new_credit);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Capsule {
                capsule_type: WT_CAPSULE_TYPE_MAX_DATA,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_grant_data_credit_varint_error(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let effects = fixture_server_session.grant_data_credit(500, u64::MAX);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_INTERNAL_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_grant_data_credit_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closed;
    let effects = fixture_server_session.grant_data_credit(500, 10_010);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_grant_streams_credit_ignore_lower(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let lower = 4;
    let effects = fixture_server_session.grant_streams_credit(500, false, lower);

    assert_eq!(fixture_server_session.local_max_streams_bidi, 5);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_grant_streams_credit_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let new_credit = 100;
    let effects = fixture_server_session.grant_streams_credit(500, false, new_credit);

    assert_eq!(fixture_server_session.local_max_streams_bidi, new_credit);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Capsule {
                capsule_type: WT_CAPSULE_TYPE_MAX_STREAMS_BIDI,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_grant_streams_credit_uni_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let new_credit = 100;
    let effects = fixture_server_session.grant_streams_credit(500, true, new_credit);

    assert_eq!(fixture_server_session.local_max_streams_uni, new_credit);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Capsule {
                capsule_type: WT_CAPSULE_TYPE_MAX_STREAMS_UNI,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_grant_streams_credit_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closing;
    let effects = fixture_server_session.grant_streams_credit(500, false, 100);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_is_closed_predicate(mut fixture_server_session: Session) {
    assert!(!fixture_server_session.is_closed());
    fixture_server_session.state = SessionState::Closed;
    assert!(fixture_server_session.is_closed());
}

#[rstest]
fn test_prune_closed_streams(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, 500, false, 1.0);
    fixture_server_session.reset_stream(4, 500, 0, 1.0);
    fixture_server_session.recv_stream_reset(4, 0, 1.0);

    let effects = fixture_server_session.prune_closed_streams();

    assert!(!fixture_server_session.streams.contains_key(&4));
    assert!(matches!(
        effects.as_slice(),
        [Effect::CleanupH3Stream { stream_id: 4 }]
    ));
}

#[rstest]
fn test_recv_capsule_close_session(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let mut buf = BytesMut::new();
    assert_eq!(
        write_varint(&mut buf, 404).map_err(|e| e.to_string()),
        Ok(())
    );
    buf.put_slice(b"Reason");

    let effects =
        fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_CLOSE_SESSION, &buf.freeze(), 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Closed);
    assert_eq!(fixture_server_session.close_code, Some(404));
    assert_eq!(
        fixture_server_session.close_reason,
        Some("Reason".to_owned())
    );

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitSessionEvent {
                event_type: EventType::SessionClosed,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_data_blocked_no_replenish(mut fixture_server_session: Session) {
    fixture_server_session.local_max_data = 100_000;
    fixture_server_session.local_data_consumed = 0;

    let effects =
        fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_DATA_BLOCKED, &Bytes::new(), 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitSessionEvent {
            event_type: EventType::SessionDataBlocked,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_capsule_data_blocked_replenishes_credit(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.local_max_data = 100;
    fixture_server_session.local_data_consumed = 90;

    let effects =
        fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_DATA_BLOCKED, &Bytes::new(), 1.0);

    assert!(fixture_server_session.local_max_data > 100);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_CAPSULE_TYPE_MAX_DATA,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_capsule_drain_session(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let effects =
        fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_DRAIN_SESSION, &Bytes::new(), 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Draining);
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitSessionEvent {
            event_type: EventType::SessionDraining,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_capsule_flow_control_negotiation_fallback(mut fixture_server_session: Session) {
    fixture_server_session.flow_control_negotiated = false;
    let data = Bytes::from(vec![0x14]);
    let effects = fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_MAX_STREAMS_BIDI, &data, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_capsule_forbidden_type_error(mut fixture_server_session: Session) {
    let effects =
        fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_MAX_STREAM_DATA, &Bytes::new(), 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_H3_FRAME_UNEXPECTED,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_malformed_error(mut fixture_server_session: Session) {
    let data = Bytes::from(vec![0xFF]);
    let effects = fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_MAX_DATA, &data, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_H3_GENERAL_PROTOCOL_ERROR,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_max_data_decreased_error(mut fixture_server_session: Session) {
    fixture_server_session.peer_max_data = 20_000;
    let mut buf = BytesMut::new();
    assert_eq!(
        write_varint(&mut buf, 10_000).map_err(|e| e.to_string()),
        Ok(())
    );

    let effects = fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_MAX_DATA, &buf.freeze(), 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_WT_FLOW_CONTROL_ERROR,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_max_data_update_success(mut fixture_server_session: Session) {
    let new_max = 50_000u64;
    let data = Bytes::from(vec![0x80, 0x00, 0xC3, 0x50]);

    let effects = fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_MAX_DATA, &data, 1.0);

    assert_eq!(fixture_server_session.peer_max_data, new_max);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitSessionEvent {
                event_type: EventType::SessionMaxDataUpdated,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_max_streams_limit_exceeded_error(mut fixture_server_session: Session) {
    let huge_limit = WT_STREAMS_LIMIT + 1;
    let mut buf = BytesMut::new();
    assert_eq!(
        write_varint(&mut buf, huge_limit).map_err(|e| e.to_string()),
        Ok(())
    );

    let effects =
        fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_MAX_STREAMS_UNI, &buf.freeze(), 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_H3_DATAGRAM_ERROR,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_max_streams_uni_malformed(mut fixture_server_session: Session) {
    let data = Bytes::from(vec![0xFF]);
    let effects = fixture_server_session.recv_capsule(WT_CAPSULE_TYPE_MAX_STREAMS_UNI, &data, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_H3_GENERAL_PROTOCOL_ERROR,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_max_streams_uni_update_unblocks_client(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connected;
    fixture_client_session.local_streams_uni_opened = 5;
    fixture_client_session
        .pending_uni_stream_requests
        .push_back(500);

    let data = Bytes::from(vec![0x14]);

    let effects = fixture_client_session.recv_capsule(WT_CAPSULE_TYPE_MAX_STREAMS_UNI, &data, 1.0);

    assert_eq!(fixture_client_session.peer_max_streams_uni, 20);
    assert!(
        fixture_client_session
            .pending_uni_stream_requests
            .is_empty()
    );

    let has_create = effects.iter().any(|e| {
        matches!(
            e,
            Effect::CreateQuicStream {
                is_unidirectional: true,
                ..
            }
        )
    });
    assert!(has_create);
}

#[rstest]
fn test_recv_capsule_max_streams_update_unblocks_client(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connected;
    fixture_client_session.local_streams_bidi_opened = 5;
    fixture_client_session
        .pending_bidi_stream_requests
        .push_back(500);

    let data = Bytes::from(vec![0x14]);

    let effects = fixture_client_session.recv_capsule(WT_CAPSULE_TYPE_MAX_STREAMS_BIDI, &data, 1.0);

    assert_eq!(fixture_client_session.peer_max_streams_bidi, 20);
    assert_eq!(fixture_client_session.local_streams_bidi_opened, 6);
    assert!(
        fixture_client_session
            .pending_bidi_stream_requests
            .is_empty()
    );

    let has_create = effects
        .iter()
        .any(|e| matches!(e, Effect::CreateQuicStream { .. }));
    assert!(has_create);
}

#[rstest]
fn test_recv_capsule_stream_data_blocked_error(mut fixture_server_session: Session) {
    let effects = fixture_server_session.recv_capsule(
        WT_CAPSULE_TYPE_STREAM_DATA_BLOCKED,
        &Bytes::new(),
        1.0,
    );

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_H3_FRAME_UNEXPECTED,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_streams_blocked_no_replenish(mut fixture_server_session: Session) {
    fixture_server_session.local_max_streams_bidi = 1000;
    fixture_server_session.peer_streams_bidi_closed = 0;

    let effects = fixture_server_session.recv_capsule(
        WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
        &Bytes::new(),
        1.0,
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitSessionEvent {
            event_type: EventType::SessionStreamsBlocked,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_capsule_streams_blocked_replenishes_credit(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.local_max_streams_uni = 10;
    fixture_server_session.peer_streams_uni_closed = 8;
    fixture_server_session.initial_max_streams_uni = 10;

    let effects = fixture_server_session.recv_capsule(
        WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI,
        &Bytes::new(),
        1.0,
    );

    assert!(fixture_server_session.local_max_streams_uni > 10);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_CAPSULE_TYPE_MAX_STREAMS_UNI,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_connect_close(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let effects = fixture_server_session.recv_connect_close(1.0);

    assert_eq!(fixture_server_session.state, SessionState::Closed);
    assert_eq!(fixture_server_session.close_code, Some(0));
    assert!(matches!(
        effects.last(),
        Some(Effect::EmitSessionEvent {
            event_type: EventType::SessionClosed,
            ..
        })
    ));
}

#[rstest]
fn test_recv_connect_close_idempotent(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closed;
    let effects = fixture_server_session.recv_connect_close(1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_connect_close_when_closing(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closing;
    fixture_server_session.closed_at = Some(0.5);

    let effects = fixture_server_session.recv_connect_close(1.0);

    assert_eq!(fixture_server_session.state, SessionState::Closed);
    assert_eq!(fixture_server_session.closed_at, Some(0.5));
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_datagram_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let data = Bytes::from_static(b"recv");
    let effects = fixture_server_session.recv_datagram(data);

    assert_eq!(fixture_server_session.datagrams_received, 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitSessionEvent {
            event_type: EventType::DatagramReceived,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_datagram_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closed;
    let effects = fixture_server_session.recv_datagram(Bytes::from_static(b"recv"));
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stop_sending_delegates(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, 500, false, 1.0);
    let effects = fixture_server_session.recv_stop_sending(4, 0);
    assert!(matches!(
        effects.first(),
        Some(Effect::EmitStreamEvent {
            event_type: EventType::StopSendingReceived,
            ..
        })
    ));
}

#[rstest]
fn test_recv_stream_data_closed_session(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closed;
    let effects = fixture_server_session.recv_stream_data(4, Bytes::new(), false, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stream_data_exceeds_local_max_data(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, 500, false, 1.0);

    let huge_len = usize::try_from(10_000).unwrap_or_default() + 1;
    let huge_data = Bytes::from(vec![0; huge_len]);
    let effects = fixture_server_session.recv_stream_data(4, huge_data, false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_WT_FLOW_CONTROL_ERROR,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_stream_data_implicit_open_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let stream_id = 4;
    let data = Bytes::from_static(b"hello");

    let effects = fixture_server_session.recv_stream_data(stream_id, data, false, 1.0);

    assert!(fixture_server_session.streams.contains_key(&stream_id));
    assert_eq!(fixture_server_session.peer_streams_bidi_opened, 1);
    assert_eq!(fixture_server_session.local_data_received, 5);

    let has_opened = effects.iter().any(|e| {
        matches!(
            e,
            Effect::EmitStreamEvent {
                event_type: EventType::StreamOpened,
                ..
            }
        )
    });
    assert!(has_opened);
}

#[rstest]
fn test_recv_stream_data_implicit_open_uni_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let stream_id = 2;
    let data = Bytes::from_static(b"uni data");

    let effects = fixture_server_session.recv_stream_data(stream_id, data, false, 1.0);

    assert!(fixture_server_session.streams.contains_key(&stream_id));
    assert_eq!(fixture_server_session.peer_streams_uni_opened, 1);
    assert_eq!(fixture_server_session.local_data_received, 8);

    let has_opened = effects.iter().any(|e| {
        matches!(
            e,
            Effect::EmitStreamEvent {
                event_type: EventType::StreamOpened,
                direction: Some(StreamDirection::ReceiveOnly),
                ..
            }
        )
    });
    assert!(has_opened);
}

#[rstest]
fn test_recv_stream_data_limit_exceeded_bidi_aborts(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.peer_streams_bidi_opened = 5;
    let stream_id = 400;

    let effects = fixture_server_session.recv_stream_data(stream_id, Bytes::new(), false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_WT_FLOW_CONTROL_ERROR,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_stream_data_limit_exceeded_uni_aborts(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.peer_streams_uni_opened = 5;
    let stream_id = 402;

    let effects = fixture_server_session.recv_stream_data(stream_id, Bytes::new(), false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_WT_FLOW_CONTROL_ERROR,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_stream_data_send_only_stream_server(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let stream_id = 3;
    let effects = fixture_server_session.recv_stream_data(stream_id, Bytes::new(), false, 1.0);
    assert!(effects.is_empty());
    assert!(!fixture_server_session.streams.contains_key(&stream_id));
}

#[rstest]
fn test_recv_stream_data_unknown_stream_client(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connected;
    let effects = fixture_client_session.recv_stream_data(2, Bytes::new(), false, 1.0);
    assert!(effects.is_empty());
    assert!(!fixture_client_session.streams.contains_key(&2));
}

#[rstest]
fn test_recv_stream_reset(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, 500, false, 1.0);
    let effects = fixture_server_session.recv_stream_reset(4, 0, 1.0);
    assert!(matches!(
        effects.first(),
        Some(Effect::EmitStreamEvent {
            event_type: EventType::StreamResetReceived,
            ..
        })
    ));
}

#[rstest]
fn test_recv_stream_reset_unknown_stream(mut fixture_server_session: Session) {
    let effects = fixture_server_session.recv_stream_reset(99, 0, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_reject_session_client_failure(mut fixture_client_session: Session) {
    let effects = fixture_client_session.reject(500, 403, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_session validate failed");
    }
}

#[rstest]
fn test_reject_session_server_success(mut fixture_server_session: Session) {
    let status_code = 403;
    let effects = fixture_server_session.reject(500, status_code, 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Closed);
    assert_eq!(fixture_server_session.close_code, None);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Headers { .. },
            Effect::EmitSessionEvent {
                event_type: EventType::SessionClosed,
                error_code: Some(403),
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_reject_session_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let effects = fixture_server_session.reject(500, 403, 1.0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_session validate failed");
    }
}

#[rstest]
fn test_reset_stream_not_found(mut fixture_server_session: Session) {
    let effects = fixture_server_session.reset_stream(99, 500, 0, 1.0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_reset_stream_user_command(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, 500, false, 1.0);
    let effects = fixture_server_session.reset_stream(4, 500, 0, 1.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_send_datagram_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let data = Bytes::from_static(b"dgram");
    let effects = fixture_server_session.send_datagram(500, data, 1500);

    assert_eq!(fixture_server_session.datagrams_sent, 1);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Datagram { .. },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_send_datagram_too_large_failure(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let data = Bytes::from(vec![0u8; 2000]);
    let effects = fixture_server_session.send_datagram(500, data, 1500);

    assert_eq!(fixture_server_session.datagrams_sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Datagram,
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_datagram validate exceeded");
    }
}

#[rstest]
fn test_send_datagram_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connecting;
    let data = Bytes::from_static(b"dgram");
    let effects = fixture_server_session.send_datagram(500, data, 1500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_session validate failed");
    }
}

#[rstest]
fn test_send_stream_data_blocked_by_session_window(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, 500, false, 1.0);

    fixture_server_session.local_data_sent = 10_000;

    let data = Bytes::from_static(b"blocked");
    let effects = fixture_server_session.send_stream_data(4, 500, data, false);

    assert_eq!(fixture_server_session.local_data_sent, 10_000);
    assert!(fixture_server_session.blocked_streams.contains(&4));

    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::SendH3Capsule {
            capsule_type: WT_CAPSULE_TYPE_DATA_BLOCKED,
            ..
        }
    )));
}

#[rstest]
fn test_send_stream_data_fin_cleanup(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, 500, false, 1.0);

    let effects = fixture_server_session.send_stream_data(4, 500, Bytes::new(), true);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_send_stream_data_not_found(mut fixture_server_session: Session) {
    let effects = fixture_server_session.send_stream_data(99, 500, Bytes::new(), false);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_send_stream_data_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, 500, false, 1.0);

    let data = Bytes::from_static(b"payload");
    let effects = fixture_server_session.send_stream_data(4, 500, data, false);

    assert_eq!(fixture_server_session.local_data_sent, 7);
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::SendQuicData { .. }))
    );
}

#[rstest]
fn test_session_initialization_success(fixture_server_session: Session) {
    let session = fixture_server_session;

    assert_eq!(session.id, 100);
    assert_eq!(session.state, SessionState::Connecting);
    assert_eq!(session.local_max_data, 10_000);
    assert!(session.active_streams.is_empty());
    assert_eq!(session.wt_protocol, None);
}

#[rstest]
fn test_stop_stream_not_found(mut fixture_server_session: Session) {
    let effects = fixture_server_session.stop_stream(99, 500, 0, 1.0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_stop_stream_user_command(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, 500, false, 1.0);
    let effects = fixture_server_session.stop_stream(4, 500, 0, 1.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_stream_diagnostics_not_found(fixture_server_session: Session) {
    let effects = fixture_server_session.stream_diagnostics(99, 500);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            ..
        }]
    ));
}

#[rstest]
fn test_stream_diagnostics_success(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, 500, false, 1.0);
    let effects = fixture_server_session.stream_diagnostics(4, 500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::StreamDiagnostics(_),
            ..
        }]
    ));
}

#[rstest]
fn test_stream_read_fin_cleanup(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, 500, false, 1.0);
    fixture_server_session.reset_stream(4, 500, 0, 1.0);
    fixture_server_session.recv_stream_reset(4, 0, 1.0);

    fixture_server_session.stream_read(4, 500, 1024);

    assert!(!fixture_server_session.active_streams.contains(&4));
}

#[rstest]
fn test_stream_read_not_found(mut fixture_server_session: Session) {
    let effects = fixture_server_session.stream_read(99, 500, 1024);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            ..
        }]
    ));
}

#[rstest]
fn test_stream_read_success(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, 500, false, 1.0);

    fixture_server_session.recv_stream_data(4, Bytes::from_static(b"data"), false, 1.0);

    let effects = fixture_server_session.stream_read(4, 500, 1024);
    assert!(!effects.is_empty());

    if let [Effect::NotifyRequestDone { result, .. }, ..] = effects.as_slice() {
        assert!(matches!(result, RequestResult::ReadData(_)));
    }
}
