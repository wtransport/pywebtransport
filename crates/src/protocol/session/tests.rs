//! Unit tests for the `crate::protocol::session` module.

use bytes::{BufMut, Bytes, BytesMut};
use rstest::*;

use super::*;
use crate::common::constants::{
    CLOSE_WEBTRANSPORT_SESSION_TYPE, DRAIN_WEBTRANSPORT_SESSION_TYPE, ERR_FLOW_CONTROL_ERROR,
    ERR_H3_FRAME_UNEXPECTED, ERR_H3_GENERAL_PROTOCOL_ERROR, ERR_LIB_INTERNAL_ERROR,
    ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR, MAX_PROTOCOL_STREAMS_LIMIT,
    WT_DATA_BLOCKED_TYPE, WT_MAX_DATA_TYPE, WT_MAX_STREAM_DATA_TYPE, WT_MAX_STREAMS_BIDI_TYPE,
    WT_MAX_STREAMS_UNI_TYPE, WT_STREAM_DATA_BLOCKED_TYPE, WT_STREAMS_BLOCKED_BIDI_TYPE,
    WT_STREAMS_BLOCKED_UNI_TYPE,
};
use crate::common::types::{EventType, SessionState, StreamDirection, StreamState};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::utils::write_varint;

const INITIAL_MAX_DATA: u64 = 10_000;
const INITIAL_MAX_STREAMS: u64 = 5;
const MOCK_REQUEST_ID: RequestId = 500;
const MOCK_SESSION_ID: SessionId = 100;

#[fixture]
fn fixture_client_session(fixture_headers: Headers) -> Session {
    Session::new(
        MOCK_SESSION_ID,
        "/test".to_owned(),
        fixture_headers,
        0.0,
        INITIAL_MAX_DATA,
        INITIAL_MAX_STREAMS,
        INITIAL_MAX_STREAMS,
        INITIAL_MAX_DATA,
        INITIAL_MAX_STREAMS,
        INITIAL_MAX_STREAMS,
        1024,
        1024,
        1024,
        true,
        true,
        SessionState::Connecting,
    )
}

#[fixture]
fn fixture_headers() -> Headers {
    vec![]
}

#[fixture]
fn fixture_server_session(fixture_headers: Headers) -> Session {
    Session::new(
        MOCK_SESSION_ID,
        "/test".to_owned(),
        fixture_headers,
        0.0,
        INITIAL_MAX_DATA,
        INITIAL_MAX_STREAMS,
        INITIAL_MAX_STREAMS,
        INITIAL_MAX_DATA,
        INITIAL_MAX_STREAMS,
        INITIAL_MAX_STREAMS,
        1024,
        1024,
        1024,
        true,
        false,
        SessionState::Connecting,
    )
}

#[rstest]
fn test_accept_session_client_failure(mut fixture_client_session: Session) {
    let effects = fixture_client_session.accept(MOCK_REQUEST_ID, 1.0);

    assert_eq!(fixture_client_session.state, SessionState::Connecting);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert!(reason.contains("Client cannot accept"));
    }
}

#[rstest]
fn test_accept_session_server_success(mut fixture_server_session: Session) {
    let effects = fixture_server_session.accept(MOCK_REQUEST_ID, 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Connected);
    assert!(fixture_server_session.ready_at.is_some());

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Headers { status: 200, .. },
            Effect::EmitSessionEvent {
                event_type: EventType::SessionReady,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_accept_session_wrong_state_failure(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;

    let effects = fixture_server_session.accept(MOCK_REQUEST_ID, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_bind_stream_success(mut fixture_server_session: Session) {
    let stream_id = 4;
    let effects = fixture_server_session.bind_stream(stream_id, MOCK_REQUEST_ID, false, 1.0);

    assert!(fixture_server_session.streams.contains_key(&stream_id));
    assert!(fixture_server_session.active_streams.contains(&stream_id));

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestDone { .. },
            Effect::EmitStreamEvent {
                event_type: EventType::StreamOpened,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_close_already_closed_session(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closed;
    let effects = fixture_server_session.close(MOCK_REQUEST_ID, 0, None, 1.0);
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

    let effects = fixture_server_session.close(MOCK_REQUEST_ID, 0, None, 1.0);

    let resets = effects
        .iter()
        .filter(|e| matches!(e, Effect::ResetQuicStream { .. }))
        .count();
    assert_eq!(resets, 2);
    assert!(fixture_server_session.active_streams.is_empty());
}

#[rstest]
fn test_close_session_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let error_code = 0;
    let reason = "Clean close".to_owned();

    let effects = fixture_server_session.close(MOCK_REQUEST_ID, error_code, Some(reason), 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Closed);
    assert_eq!(fixture_server_session.close_code, Some(0));

    let has_close_capsule = effects
        .iter()
        .any(|e| matches!(e, Effect::SendH3Capsule { .. }));
    assert!(has_close_capsule);
}

#[rstest]
fn test_create_stream_limit_reached_client_blocking(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connected;
    fixture_client_session.local_streams_bidi_opened = INITIAL_MAX_STREAMS;

    let effects = fixture_client_session.create_stream(MOCK_REQUEST_ID, false);

    assert_eq!(fixture_client_session.pending_bidi_stream_requests.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_STREAMS_BLOCKED_BIDI_TYPE,
            ..
        }]
    ));
}

#[rstest]
fn test_create_stream_limit_reached_server_failure(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.local_streams_bidi_opened = INITIAL_MAX_STREAMS;

    let effects = fixture_server_session.create_stream(MOCK_REQUEST_ID, false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));
}

#[rstest]
fn test_create_stream_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;

    let effects = fixture_server_session.create_stream(MOCK_REQUEST_ID, false);

    assert_eq!(fixture_server_session.local_streams_bidi_opened, 1);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateQuicStream {
            session_id: MOCK_SESSION_ID,
            ..
        }]
    ));
}

#[rstest]
fn test_create_stream_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connecting;
    let effects = fixture_server_session.create_stream(MOCK_REQUEST_ID, false);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_create_uni_stream_limit_reached_client_blocking(mut fixture_client_session: Session) {
    fixture_client_session.state = SessionState::Connected;
    fixture_client_session.local_streams_uni_opened = INITIAL_MAX_STREAMS;

    let effects = fixture_client_session.create_stream(MOCK_REQUEST_ID, true);

    assert_eq!(fixture_client_session.pending_uni_stream_requests.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_STREAMS_BLOCKED_UNI_TYPE,
            ..
        }]
    ));
}

#[rstest]
fn test_create_uni_stream_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let effects = fixture_server_session.create_stream(MOCK_REQUEST_ID, true);

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
    let effects = fixture_server_session.diagnose(MOCK_REQUEST_ID);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::Diagnostics(_),
            ..
        }]
    ));

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::Diagnostics(json),
            ..
        },
    ] = effects.as_slice()
    {
        assert!(json.contains("\"session_id\":100"));
        assert!(json.contains("\"state\":\"connecting\""));
    }
}

#[rstest]
fn test_fail_stream_decrements_counts(mut fixture_server_session: Session) {
    fixture_server_session.local_streams_bidi_opened = 1;
    let _unused =
        fixture_server_session.fail_stream(MOCK_REQUEST_ID, false, Some(1), "Fail".to_owned());
    assert_eq!(fixture_server_session.local_streams_bidi_opened, 0);
}

#[rstest]
fn test_flush_blocked_writes_on_max_data_update(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);
    fixture_server_session.local_data_sent = INITIAL_MAX_DATA;
    fixture_server_session.send_stream_data(
        4,
        MOCK_REQUEST_ID,
        Bytes::from_static(b"pending"),
        false,
    );
    assert!(fixture_server_session.blocked_streams.contains(&4));

    let new_max = INITIAL_MAX_DATA + 1000;
    let mut buf = BytesMut::new();
    assert_eq!(
        write_varint(&mut buf, new_max).map_err(|e| e.to_string()),
        Ok(())
    );

    let effects = fixture_server_session.recv_capsule(WT_MAX_DATA_TYPE, &buf.freeze(), 1.0);

    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::SendQuicData { .. }))
    );
    assert!(!fixture_server_session.blocked_streams.contains(&4));
}

#[rstest]
fn test_grant_data_credit_ignore_lower_value(mut fixture_server_session: Session) {
    let lower_credit = INITIAL_MAX_DATA - 1;
    let effects = fixture_server_session.grant_data_credit(MOCK_REQUEST_ID, lower_credit);

    assert_eq!(fixture_server_session.local_max_data, INITIAL_MAX_DATA);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_grant_data_credit_success(mut fixture_server_session: Session) {
    let new_credit = 20_000;
    let effects = fixture_server_session.grant_data_credit(MOCK_REQUEST_ID, new_credit);

    assert_eq!(fixture_server_session.local_max_data, new_credit);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Capsule {
                capsule_type: WT_MAX_DATA_TYPE,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_grant_data_credit_varint_error(mut fixture_server_session: Session) {
    let effects = fixture_server_session.grant_data_credit(MOCK_REQUEST_ID, u64::MAX);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            error_code: Some(ERR_LIB_INTERNAL_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_grant_streams_credit_ignore_lower(mut fixture_server_session: Session) {
    let lower = INITIAL_MAX_STREAMS - 1;
    let effects = fixture_server_session.grant_streams_credit(MOCK_REQUEST_ID, lower, false);

    assert_eq!(
        fixture_server_session.local_max_streams_bidi,
        INITIAL_MAX_STREAMS
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_grant_streams_credit_success(mut fixture_server_session: Session) {
    let new_credit = 100;
    let effects = fixture_server_session.grant_streams_credit(MOCK_REQUEST_ID, new_credit, false);

    assert_eq!(fixture_server_session.local_max_streams_bidi, new_credit);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Capsule {
                capsule_type: WT_MAX_STREAMS_BIDI_TYPE,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_grant_streams_credit_uni_success(mut fixture_server_session: Session) {
    let new_credit = 100;
    let effects = fixture_server_session.grant_streams_credit(MOCK_REQUEST_ID, new_credit, true);

    assert_eq!(fixture_server_session.local_max_streams_uni, new_credit);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Capsule {
                capsule_type: WT_MAX_STREAMS_UNI_TYPE,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_grant_streams_credit_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closed;
    let effects = fixture_server_session.grant_streams_credit(MOCK_REQUEST_ID, 100, false);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_prune_closed_streams(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);
    if let Some(stream) = fixture_server_session.streams.get_mut(&4) {
        stream.state = StreamState::Closed;
    }

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
        fixture_server_session.recv_capsule(CLOSE_WEBTRANSPORT_SESSION_TYPE, &buf.freeze(), 1.0);

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

    let effects = fixture_server_session.recv_capsule(WT_DATA_BLOCKED_TYPE, &Bytes::new(), 1.0);

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
    fixture_server_session.local_max_data = 100;
    fixture_server_session.local_data_consumed = 90;

    let effects = fixture_server_session.recv_capsule(WT_DATA_BLOCKED_TYPE, &Bytes::new(), 1.0);

    assert!(fixture_server_session.local_max_data > 100);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_MAX_DATA_TYPE,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_capsule_drain_session(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let effects =
        fixture_server_session.recv_capsule(DRAIN_WEBTRANSPORT_SESSION_TYPE, &Bytes::new(), 1.0);

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
fn test_recv_capsule_forbidden_type_error(mut fixture_server_session: Session) {
    let effects = fixture_server_session.recv_capsule(WT_MAX_STREAM_DATA_TYPE, &Bytes::new(), 1.0);

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
    let effects = fixture_server_session.recv_capsule(WT_MAX_DATA_TYPE, &data, 1.0);

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

    let effects = fixture_server_session.recv_capsule(WT_MAX_DATA_TYPE, &buf.freeze(), 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_FLOW_CONTROL_ERROR,
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

    let effects = fixture_server_session.recv_capsule(WT_MAX_DATA_TYPE, &data, 1.0);

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
    let huge_limit = MAX_PROTOCOL_STREAMS_LIMIT + 1;
    let mut buf = BytesMut::new();
    assert_eq!(
        write_varint(&mut buf, huge_limit).map_err(|e| e.to_string()),
        Ok(())
    );

    let effects = fixture_server_session.recv_capsule(WT_MAX_STREAMS_UNI_TYPE, &buf.freeze(), 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_FLOW_CONTROL_ERROR,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_recv_capsule_max_streams_uni_malformed(mut fixture_server_session: Session) {
    let data = Bytes::from(vec![0xFF]);
    let effects = fixture_server_session.recv_capsule(WT_MAX_STREAMS_UNI_TYPE, &data, 1.0);

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
    fixture_client_session.local_streams_uni_opened = INITIAL_MAX_STREAMS;
    fixture_client_session
        .pending_uni_stream_requests
        .push_back(MOCK_REQUEST_ID);

    let data = Bytes::from(vec![0x14]);

    let effects = fixture_client_session.recv_capsule(WT_MAX_STREAMS_UNI_TYPE, &data, 1.0);

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
    fixture_client_session.local_streams_bidi_opened = INITIAL_MAX_STREAMS;
    fixture_client_session
        .pending_bidi_stream_requests
        .push_back(MOCK_REQUEST_ID);

    let data = Bytes::from(vec![0x14]);

    let effects = fixture_client_session.recv_capsule(WT_MAX_STREAMS_BIDI_TYPE, &data, 1.0);

    assert_eq!(fixture_client_session.peer_max_streams_bidi, 20);
    assert_eq!(
        fixture_client_session.local_streams_bidi_opened,
        INITIAL_MAX_STREAMS + 1
    );
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
    let effects =
        fixture_server_session.recv_capsule(WT_STREAM_DATA_BLOCKED_TYPE, &Bytes::new(), 1.0);

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

    let effects =
        fixture_server_session.recv_capsule(WT_STREAMS_BLOCKED_BIDI_TYPE, &Bytes::new(), 1.0);

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
    fixture_server_session.local_max_streams_uni = 10;
    fixture_server_session.peer_streams_uni_closed = 8;
    fixture_server_session.initial_max_streams_uni = 10;

    let effects =
        fixture_server_session.recv_capsule(WT_STREAMS_BLOCKED_UNI_TYPE, &Bytes::new(), 1.0);

    assert!(fixture_server_session.local_max_streams_uni > 10);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Capsule {
            capsule_type: WT_MAX_STREAMS_UNI_TYPE,
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
fn test_recv_stream_data_closed_session(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Closed;
    let effects = fixture_server_session.recv_stream_data(4, Bytes::new(), false, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stream_data_implicit_open_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let stream_id = 4;
    let data = Bytes::from_static(b"hello");

    let effects = fixture_server_session.recv_stream_data(stream_id, data, false, 1.0);

    assert!(fixture_server_session.streams.contains_key(&stream_id));
    assert_eq!(fixture_server_session.peer_streams_bidi_opened, 1);

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
fn test_recv_stream_data_limit_exceeded_ignored(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.peer_streams_bidi_opened = INITIAL_MAX_STREAMS;
    let stream_id = 400;

    let effects = fixture_server_session.recv_stream_data(stream_id, Bytes::new(), false, 1.0);

    assert!(!fixture_server_session.streams.contains_key(&stream_id));
    assert!(effects.is_empty());
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
fn test_recv_stream_data_uni_limit_exceeded_ignored(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.peer_streams_uni_opened = INITIAL_MAX_STREAMS;
    let stream_id = 402;

    let effects = fixture_server_session.recv_stream_data(stream_id, Bytes::new(), false, 1.0);

    assert!(!fixture_server_session.streams.contains_key(&stream_id));
    assert!(effects.is_empty());
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
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);
    let effects = fixture_server_session.recv_stream_reset(4, 0, 1.0);
    assert!(matches!(
        effects.last(),
        Some(Effect::EmitStreamEvent {
            event_type: EventType::StreamClosed,
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
    let effects = fixture_client_session.reject(MOCK_REQUEST_ID, 403, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert!(reason.contains("Client cannot reject"));
    }
}

#[rstest]
fn test_reject_session_server_success(mut fixture_server_session: Session) {
    let status_code = 403;
    let effects = fixture_server_session.reject(MOCK_REQUEST_ID, status_code, 1.0);

    assert_eq!(fixture_server_session.state, SessionState::Closed);
    assert_eq!(fixture_server_session.close_code, None);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Headers { status: 403, .. },
            Effect::EmitSessionEvent {
                event_type: EventType::SessionClosed,
                code: Some(403),
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_reset_stream_user_command(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);
    let effects = fixture_server_session.reset_stream(4, MOCK_REQUEST_ID, 0, 1.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_send_datagram_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    let data = Bytes::from_static(b"dgram");
    let effects = fixture_server_session.send_datagram(MOCK_REQUEST_ID, data, 1500);

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
    let effects = fixture_server_session.send_datagram(MOCK_REQUEST_ID, data, 1500);

    assert_eq!(fixture_server_session.datagrams_sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));
}

#[rstest]
fn test_send_datagram_wrong_state(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connecting;
    let data = Bytes::from_static(b"dgram");
    let effects = fixture_server_session.send_datagram(MOCK_REQUEST_ID, data, 1500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_send_stream_data_blocked_by_session_window(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);

    fixture_server_session.local_data_sent = INITIAL_MAX_DATA;

    let data = Bytes::from_static(b"blocked");
    let effects = fixture_server_session.send_stream_data(4, MOCK_REQUEST_ID, data, false);

    assert_eq!(fixture_server_session.local_data_sent, INITIAL_MAX_DATA);
    assert!(fixture_server_session.blocked_streams.contains(&4));

    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::SendH3Capsule {
            capsule_type: WT_DATA_BLOCKED_TYPE,
            ..
        }
    )));
}

#[rstest]
fn test_send_stream_data_fin_cleanup(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);

    let effects = fixture_server_session.send_stream_data(4, MOCK_REQUEST_ID, Bytes::new(), true);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_send_stream_data_not_found(mut fixture_server_session: Session) {
    let effects = fixture_server_session.send_stream_data(99, MOCK_REQUEST_ID, Bytes::new(), false);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_send_stream_data_success(mut fixture_server_session: Session) {
    fixture_server_session.state = SessionState::Connected;
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);

    let data = Bytes::from_static(b"payload");
    let effects = fixture_server_session.send_stream_data(4, MOCK_REQUEST_ID, data, false);

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

    assert_eq!(session.id, MOCK_SESSION_ID);
    assert_eq!(session.state, SessionState::Connecting);
    assert_eq!(session.local_max_data, INITIAL_MAX_DATA);
    assert!(session.active_streams.is_empty());
}

#[rstest]
fn test_stop_stream_user_command(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);
    let effects = fixture_server_session.stop_stream(4, MOCK_REQUEST_ID, 0, 1.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_stream_diagnostics_not_found(fixture_server_session: Session) {
    let effects = fixture_server_session.stream_diagnostics(99, MOCK_REQUEST_ID);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));
}

#[rstest]
fn test_stream_diagnostics_success(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);
    let effects = fixture_server_session.stream_diagnostics(4, MOCK_REQUEST_ID);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::Diagnostics(_),
            ..
        }]
    ));
}

#[rstest]
fn test_stream_read_fin_cleanup(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);
    if let Some(stream) = fixture_server_session.streams.get_mut(&4) {
        stream.state = StreamState::Closed;
    }
    let _effects = fixture_server_session.stream_read(4, MOCK_REQUEST_ID, 1024);

    assert!(!fixture_server_session.active_streams.contains(&4));
}

#[rstest]
fn test_stream_read_not_found(mut fixture_server_session: Session) {
    let effects = fixture_server_session.stream_read(99, MOCK_REQUEST_ID, 1024);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));
}

#[rstest]
fn test_stream_read_success(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);

    let _unused = fixture_server_session.unread_stream(4, Bytes::from_static(b"data"));

    let effects = fixture_server_session.stream_read(4, MOCK_REQUEST_ID, 1024);
    assert!(!effects.is_empty());

    if let [Effect::NotifyRequestDone { result, .. }] = effects.as_slice() {
        assert!(matches!(result, RequestResult::ReadData(_)));
    }
}

#[rstest]
fn test_unread_stream(mut fixture_server_session: Session) {
    fixture_server_session.bind_stream(4, MOCK_REQUEST_ID, false, 1.0);
    let _unused = fixture_server_session.unread_stream(4, Bytes::from_static(b"returned"));

    let effects = fixture_server_session.stream_read(4, MOCK_REQUEST_ID, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::ReadData(_),
            ..
        }]
    ));

    if let [Effect::NotifyRequestDone { result, .. }] = effects.as_slice() {
        if let RequestResult::ReadData(data) = result {
            assert_eq!(data, &Bytes::from_static(b"returned"));
        } else {
            assert!(matches!(result, RequestResult::ReadData(_)));
        }
    }
}

#[rstest]
fn test_unread_stream_not_found(mut fixture_server_session: Session) {
    let effects = fixture_server_session.unread_stream(99, Bytes::from_static(b"data"));
    assert!(effects.is_empty());
}
