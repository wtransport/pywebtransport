//! Unit tests for the `crate::protocol::connection` module.

use bytes::Bytes;
use rstest::*;

use super::*;
use crate::common::constants::{
    ERR_H3_REQUEST_REJECTED, ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_INTERNAL_ERROR,
    ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR, ERR_WT_BUFFERED_STREAM_REJECTED,
    WT_CAPSULE_TYPE_DRAIN_SESSION,
};
use crate::common::types::{ConnectionState, ErrorSource, EventType, Headers, StreamId};
use crate::protocol::H3Settings;
use crate::protocol::events::{Effect, RequestResult};

#[fixture]
fn fixture_client_connection() -> Connection {
    Connection::new(
        42,
        true,
        ConnectionParams {
            early_event_ttl: 5.0,
            flow_control_window: 1024 * 1024,
            flow_control_window_auto_scale_enabled: true,
            initial_max_data: 10000,
            initial_max_streams_bidi: 10,
            initial_max_streams_uni: 10,
            max_session_pending_events: 10,
            max_sessions: 10,
            max_stream_read_buffer_size: 1024,
            max_stream_write_buffer_size: 1024,
            max_total_pending_events: 100,
        },
    )
}

#[fixture]
fn fixture_headers() -> Headers {
    vec![
        (
            Bytes::from_static(b":method"),
            Bytes::from_static(b"CONNECT"),
        ),
        (
            Bytes::from_static(b":protocol"),
            Bytes::from_static(b"webtransport-h3"),
        ),
        (Bytes::from_static(b":scheme"), Bytes::from_static(b"https")),
        (Bytes::from_static(b":path"), Bytes::from_static(b"/wt")),
        (
            Bytes::from_static(b":authority"),
            Bytes::from_static(b"example.com"),
        ),
    ]
}

#[fixture]
fn fixture_server_connection() -> Connection {
    Connection::new(
        42,
        false,
        ConnectionParams {
            early_event_ttl: 5.0,
            flow_control_window: 1024 * 1024,
            flow_control_window_auto_scale_enabled: true,
            initial_max_data: 10000,
            initial_max_streams_bidi: 10,
            initial_max_streams_uni: 10,
            max_session_pending_events: 10,
            max_sessions: 10,
            max_stream_read_buffer_size: 1024,
            max_stream_write_buffer_size: 1024,
            max_total_pending_events: 100,
        },
    )
}

#[rstest]
fn test_accept_session_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.accept_session(0, 100, None, 2.0);

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::EmitSessionEvent {
            event_type: EventType::SessionReady,
            ..
        }
    )));
}

#[rstest]
fn test_accept_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.accept_session(999, 100, None, 1.0);

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
fn test_accept_session_with_wt_protocol(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.accept_session(0, 100, Some("h3".into()), 2.0);

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::EmitSessionEvent {
            event_type: EventType::SessionReady,
            wt_protocol: Some(sub),
            ..
        } if sub == "h3"
    )));
}

#[rstest]
fn test_accessors(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    assert!(!fixture_server_connection.is_client());
    assert!(fixture_server_connection.is_pre_connected());
    assert!(!fixture_server_connection.is_connected());
    assert_eq!(
        fixture_server_connection.peer_max_datagram_frame_size(),
        None
    );

    fixture_server_connection.recv_transport_parameters(1500);

    assert_eq!(
        fixture_server_connection.peer_max_datagram_frame_size(),
        Some(1500)
    );

    fixture_server_connection.state = ConnectionState::Connected;

    assert!(!fixture_server_connection.is_pre_connected());
    assert!(fixture_server_connection.is_connected());

    assert!(!fixture_server_connection.is_session_stream(0));

    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    assert!(fixture_server_connection.is_session_stream(0));
}

#[rstest]
fn test_bind_session(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.bind_session(4, 100);

    assert!(effects.is_empty());
    assert_eq!(
        fixture_server_connection.pending_requests.get(&4),
        Some(&100)
    );
}

#[rstest]
fn test_bind_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.bind_stream(999, 4, 100, false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_bind_stream_updates_map(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.5);

    let effects = fixture_server_connection.bind_stream(0, 4, 101, false, 2.0);

    assert_eq!(fixture_server_connection.stream_map.get(&4), Some(&0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestDone { .. },
            Effect::EmitStreamEvent { .. }
        ]
    ));
}

#[rstest]
fn test_client_create_session_limit_reached(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.peer_initial_max_data = 100;
    fixture_client_connection.max_sessions = 1;
    fixture_client_connection.create_session(100, "/".into(), fixture_headers.clone(), None, 1.0);

    let effects =
        fixture_client_connection.create_session(101, "/".into(), fixture_headers, None, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Connection,
            error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_client_create_session_no_flow_control_limit_reached(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.peer_initial_max_data = 0;
    fixture_client_connection.peer_initial_max_streams_bidi = 0;
    fixture_client_connection.peer_initial_max_streams_uni = 0;
    fixture_client_connection.create_session(100, "/".into(), fixture_headers.clone(), None, 1.0);

    let effects =
        fixture_client_connection.create_session(101, "/".into(), fixture_headers, None, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Connection,
            error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_client_create_session_peer_goaway_rejects(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.peer_goaway_received = true;

    let effects =
        fixture_client_connection.create_session(100, "/".into(), fixture_headers, None, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Connection,
            error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_client_create_session_success(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;

    let effects =
        fixture_client_connection.create_session(100, "/".into(), fixture_headers, None, 1.0);

    assert!(
        fixture_client_connection
            .pending_session_configs
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateH3Session { .. }]
    ));
}

#[rstest]
fn test_client_create_session_with_invalid_wt_available_protocols(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    let wt_available_protocols = Some(vec!["bad\nproto".into()]);

    let effects = fixture_client_connection.create_session(
        100,
        "/".into(),
        fixture_headers,
        wt_available_protocols,
        1.0,
    );

    assert!(
        !fixture_client_connection
            .pending_session_configs
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Connection,
            error_code: None,
            ..
        }]
    ));
}

#[rstest]
fn test_client_create_session_with_wt_available_protocols(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    let wt_available_protocols = Some(vec!["h3".into()]);

    let effects = fixture_client_connection.create_session(
        100,
        "/".into(),
        fixture_headers,
        wt_available_protocols,
        1.0,
    );

    assert!(
        fixture_client_connection
            .pending_session_configs
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateH3Session { .. }]
    ));
}

#[rstest]
fn test_client_create_session_wrong_state(mut fixture_client_connection: Connection) {
    let effects = fixture_client_connection.create_session(100, "/".into(), vec![], None, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Connection,
            error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_client_recv_headers_completes_session(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.pending_requests.insert(0, 100);
    fixture_client_connection.create_session(100, "/".into(), fixture_headers, None, 1.0);
    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];

    let effects = fixture_client_connection.recv_headers(0, response_headers, false, 2.0);

    assert!(fixture_client_connection.sessions.contains_key(&0));
    assert!(
        !fixture_client_connection
            .pending_session_configs
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitSessionEvent {
                event_type: EventType::SessionReady,
                ..
            },
            Effect::NotifyRequestDone { .. },
            ..
        ]
    ));
}

#[rstest]
fn test_client_recv_headers_missing_config(mut fixture_client_connection: Connection) {
    fixture_client_connection.pending_requests.insert(0, 100);

    let effects = fixture_client_connection.recv_headers(0, vec![], false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Unspecified,
            error_code: Some(ERR_LIB_INTERNAL_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_client_recv_headers_missing_config_stream_ended(mut fixture_client_connection: Connection) {
    fixture_client_connection.pending_requests.insert(0, 100);

    let effects = fixture_client_connection.recv_headers(0, vec![], true, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Unspecified,
            error_code: Some(ERR_LIB_INTERNAL_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_client_recv_headers_rejects_non_200(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.pending_requests.insert(0, 100);
    fixture_client_connection.create_session(100, "/".into(), fixture_headers, None, 1.0);
    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"404"))];

    let effects = fixture_client_connection.recv_headers(0, response_headers, false, 2.0);

    assert!(!fixture_client_connection.sessions.contains_key(&0));
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            error_code: Some(ERR_H3_REQUEST_REJECTED),
            ..
        }]
    ));
}

#[rstest]
fn test_client_recv_headers_unknown_request(mut fixture_client_connection: Connection) {
    let effects = fixture_client_connection.recv_headers(0, vec![], false, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_client_recv_headers_unknown_request_stream_ended(
    mut fixture_client_connection: Connection,
) {
    let effects = fixture_client_connection.recv_headers(0, vec![], true, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_close_connection_lifecycle(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.close(100, 42, Some("Bye".into()), 1.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Closing);
    assert_eq!(fixture_server_connection.close_code, Some(42));
    assert_eq!(fixture_server_connection.close_reason, Some("Bye".into()));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::CloseQuicConnection { .. },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_close_idempotent_when_closing_or_closed(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Closing;

    let effects1 = fixture_server_connection.close(100, 0, None, 1.0);

    assert!(matches!(
        effects1.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));

    fixture_server_connection.state = ConnectionState::Closed;

    let effects2 = fixture_server_connection.close(100, 0, None, 1.0);

    assert!(matches!(
        effects2.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_close_session_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.close_session(0, 100, 0, None, 2.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_close_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.close_session(999, 100, 0, None, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_connection_initialization(fixture_server_connection: Connection) {
    let conn = fixture_server_connection;

    assert_eq!(conn.handle, 42);
    assert!(!conn.is_client());
    assert_eq!(conn.state, ConnectionState::Idle);
    assert!(conn.sessions.is_empty());
}

#[rstest]
fn test_create_session_server_failure(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.create_session(100, "/".into(), vec![], None, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Connection,
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_connection validate failed");
    }
}

#[rstest]
fn test_create_stream_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.peer_initial_max_streams_bidi = 100;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.5);

    let effects = fixture_server_connection.create_stream(0, 101, false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateQuicStream { .. }]
    ));
}

#[rstest]
fn test_create_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.create_stream(999, 100, false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_diagnose(fixture_server_connection: Connection) {
    let effects = fixture_server_connection.diagnose(100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::ConnectionDiagnostics(_),
            ..
        }]
    ));
}

#[rstest]
fn test_early_buffer_global_limit(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    for sid in 0..10 {
        for _ in 0..10 {
            fixture_server_connection.recv_datagram(sid, Bytes::from_static(b"d"), 1.0);
        }
    }
    assert_eq!(fixture_server_connection.early_event_count, 100);

    let effects =
        fixture_server_connection.recv_stream_data(100, 4, Bytes::from_static(b"d"), false, 2.0);

    assert_eq!(fixture_server_connection.early_event_count, 100);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_export_keying_material_delegates(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.pending_requests.insert(0, 100);
    fixture_client_connection.create_session(100, "/".into(), fixture_headers, None, 1.0);
    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];
    fixture_client_connection.recv_headers(0, response_headers, false, 2.0);

    let effects =
        fixture_client_connection.export_keying_material(0, 101, "label".into(), &[1, 2, 3], 32);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ExportTlsKeyingMaterial { .. }]
    ));
}

#[rstest]
fn test_export_keying_material_not_found(fixture_server_connection: Connection) {
    let effects =
        fixture_server_connection.export_keying_material(999, 100, "label".into(), &[1, 2, 3], 32);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_fail_session_cleans_pending(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.create_session(100, "/".into(), vec![], None, 1.0);

    let effects = fixture_client_connection.fail_session(100, None, "Error".into());

    assert!(
        !fixture_client_connection
            .pending_session_configs
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_fail_stream_delegates(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.5);

    let effects = fixture_server_connection.fail_stream(0, 101, false, None, "Reason".into());

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            ..
        }]
    ));
}

#[rstest]
fn test_fail_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.fail_stream(999, 100, false, None, "".into());

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            ..
        }]
    ));
}

#[rstest]
fn test_graceful_close(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.graceful_close(100);

    assert!(fixture_server_connection.local_goaway_sent);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Goaway, Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_grant_credits_delegate(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.5);

    let effects_data = fixture_server_connection.grant_data_credit(0, 101, 99999);
    let effects_streams = fixture_server_connection.grant_streams_credit(0, 102, false, 100);

    assert!(!effects_data.is_empty());
    assert!(!effects_streams.is_empty());
}

#[rstest]
fn test_grant_credits_not_found(mut fixture_server_connection: Connection) {
    let e1 = fixture_server_connection.grant_data_credit(999, 100, 100);
    let e2 = fixture_server_connection.grant_streams_credit(999, 100, false, 10);

    assert!(matches!(
        e1.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
    assert!(matches!(
        e2.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_handshake_completed_client_from_idle(mut fixture_client_connection: Connection) {
    assert_eq!(fixture_client_connection.state, ConnectionState::Idle);
    fixture_client_connection.peer_settings_received = true;

    let effects = fixture_client_connection.handshake_completed(1.0);

    assert_eq!(fixture_client_connection.state, ConnectionState::Connected);
    assert!(fixture_client_connection.handshake_complete);
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionEstablished,
            ..
        }]
    ));
}

#[rstest]
fn test_handshake_completed_server_from_idle(mut fixture_server_connection: Connection) {
    assert_eq!(fixture_server_connection.state, ConnectionState::Idle);

    let effects = fixture_server_connection.handshake_completed(1.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Connected);
    assert!(fixture_server_connection.handshake_complete);
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionEstablished,
            ..
        }]
    ));
}

#[rstest]
fn test_handshake_completed_wrong_state(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Closed;

    let effects = fixture_server_connection.handshake_completed(1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_has_flow_control_with_bidi_streams() {
    let mut conn = fixture_server_connection();
    conn.initial_max_streams_bidi = 100;
    conn.peer_initial_max_streams_bidi = 100;

    assert!(conn.has_flow_control());
}

#[rstest]
fn test_has_flow_control_with_data() {
    let mut conn = fixture_server_connection();
    conn.initial_max_data = 100;
    conn.peer_initial_max_data = 100;

    assert!(conn.has_flow_control());
}

#[rstest]
fn test_has_flow_control_with_uni_streams() {
    let mut conn = fixture_server_connection();
    conn.initial_max_streams_uni = 100;
    conn.peer_initial_max_streams_uni = 100;

    assert!(conn.has_flow_control());
}

#[rstest]
fn test_prune_early_events_deduplicates_child_resets(mut fixture_server_connection: Connection) {
    let child_id: StreamId = 4;
    fixture_server_connection.recv_stream_data(
        0,
        child_id,
        Bytes::from_static(b"chunk1"),
        false,
        1.0,
    );
    fixture_server_connection.recv_stream_data(
        0,
        child_id,
        Bytes::from_static(b"chunk2"),
        false,
        2.0,
    );

    let effects = fixture_server_connection.prune_early_events(10.0);

    let resets = effects
        .iter()
        .filter(|e| matches!(e, Effect::ResetQuicStream { stream_id: 4, .. }))
        .count();
    let stops = effects
        .iter()
        .filter(|e| matches!(e, Effect::StopQuicStream { stream_id: 4, .. }))
        .count();
    assert_eq!(resets, 1);
    assert_eq!(stops, 1);
    assert_eq!(fixture_server_connection.early_event_count, 0);
}

#[rstest]
fn test_prune_early_events_timeout(mut fixture_server_connection: Connection) {
    let stream_id: StreamId = 0;
    let data = Bytes::from_static(b"data");
    fixture_server_connection.recv_stream_data(0, stream_id, data, false, 1.0);

    let effects = fixture_server_connection.prune_early_events(10.0);

    assert_eq!(fixture_server_connection.early_event_count, 0);
    assert!(fixture_server_connection.early_event_buffer.is_empty());
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::ResetQuicStream {
            error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            ..
        }
    )));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::StopQuicStream {
            error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            ..
        }
    )));
}

#[rstest]
fn test_prune_early_events_timeout_unidirectional(mut fixture_server_connection: Connection) {
    let stream_id: StreamId = 2;
    let data = Bytes::from_static(b"data");
    fixture_server_connection.recv_stream_data(0, stream_id, data, false, 1.0);

    let effects = fixture_server_connection.prune_early_events(10.0);

    assert_eq!(fixture_server_connection.early_event_count, 0);
    assert!(fixture_server_connection.early_event_buffer.is_empty());
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::StopQuicStream { stream_id: 2, .. }))
    );
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::ResetQuicStream { stream_id: 2, .. }))
    );
}

#[rstest]
fn test_prune_resources_removes_closed_sessions(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.5);
    fixture_server_connection.close_session(0, 101, 0, None, 2.0);
    fixture_server_connection.recv_connect_close(0, 2.5);

    let effects = fixture_server_connection.prune_resources();

    assert!(!fixture_server_connection.sessions.contains_key(&0));
    assert!(matches!(
        effects.as_slice(),
        [Effect::CleanupH3Stream { .. }, ..]
    ));
}

#[rstest]
fn test_recv_capsule_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.recv_capsule(0, 0, &[], 2.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_capsule_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_capsule(999, 0, &[], 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_connect_close(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.recv_connect_close(0, 2.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_recv_connect_close_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_connect_close(999, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_datagram_early_buffer_full(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    for _ in 0..10 {
        fixture_server_connection.recv_datagram(0, Bytes::from_static(b"d"), 1.0);
    }

    let effects = fixture_server_connection.recv_datagram(0, Bytes::from_static(b"drop"), 2.0);

    assert!(effects.is_empty());
    assert_eq!(fixture_server_connection.early_event_count, 10);
}

#[rstest]
fn test_recv_goaway_drains_sessions(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.5);

    let effects = fixture_server_connection.recv_goaway();

    assert!(fixture_server_connection.peer_goaway_received);
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::SendH3Capsule {
            capsule_type: WT_CAPSULE_TYPE_DRAIN_SESSION,
            ..
        }
    )));
}

#[rstest]
fn test_recv_settings_parses_values(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connecting;
    fixture_client_connection.handshake_complete = true;
    let settings = H3Settings {
        wt_initial_max_data: Some(1024),
        wt_initial_max_streams_bidi: Some(10),
        wt_initial_max_streams_uni: Some(5),
        ..Default::default()
    };

    fixture_client_connection.recv_settings(&settings, 1.0);

    assert_eq!(fixture_client_connection.peer_initial_max_data, 1024);
    assert_eq!(fixture_client_connection.peer_initial_max_streams_bidi, 10);
    assert_eq!(fixture_client_connection.peer_initial_max_streams_uni, 5);
}

#[rstest]
fn test_recv_settings_triggers_ready(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connecting;
    fixture_client_connection.handshake_complete = true;
    let settings = H3Settings::default();

    let effects = fixture_client_connection.recv_settings(&settings, 1.0);

    assert_eq!(fixture_client_connection.state, ConnectionState::Connected);
    assert!(fixture_client_connection.peer_settings_received);
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionEstablished,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_stop_sending_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.0);
    fixture_server_connection.bind_stream(0, 4, 101, false, 2.0);

    let effects = fixture_server_connection.recv_stop_sending(4, 0);

    assert!(!effects.is_empty());
    assert!(matches!(
        effects.first(),
        Some(Effect::EmitStreamEvent {
            event_type: EventType::StopSendingReceived,
            ..
        })
    ));
}

#[rstest]
fn test_recv_stop_sending_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_stop_sending(4, 0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stream_data_early_buffer_full_bidi(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    for _ in 0..10 {
        fixture_server_connection.recv_stream_data(0, 4, Bytes::from_static(b"data"), false, 1.0);
    }

    let effects =
        fixture_server_connection.recv_stream_data(0, 4, Bytes::from_static(b"drop"), false, 2.0);

    assert_eq!(fixture_server_connection.early_event_count, 10);
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::ResetQuicStream { stream_id: 4, .. }))
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::StopQuicStream { stream_id: 4, .. }))
    );
}

#[rstest]
fn test_recv_stream_data_early_buffer_full_uni(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    for _ in 0..10 {
        fixture_server_connection.recv_stream_data(0, 2, Bytes::from_static(b"data"), false, 1.0);
    }

    let effects =
        fixture_server_connection.recv_stream_data(0, 2, Bytes::from_static(b"drop"), false, 2.0);

    assert_eq!(fixture_server_connection.early_event_count, 10);
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::ResetQuicStream { .. }))
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::StopQuicStream { stream_id: 2, .. }))
    );
}

#[rstest]
fn test_recv_stream_data_routes_and_buffers(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    let data = Bytes::from_static(b"early");

    let effects_early = fixture_server_connection.recv_stream_data(0, 4, data.clone(), false, 1.0);

    assert!(effects_early.is_empty());
    assert_eq!(fixture_server_connection.early_event_count, 1);
    assert!(
        fixture_server_connection
            .early_event_buffer
            .contains_key(&0)
    );

    fixture_server_connection.recv_headers(0, fixture_headers, false, 2.0);

    assert_eq!(fixture_server_connection.stream_map.get(&4), Some(&0));
}

#[rstest]
fn test_recv_stream_reset_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.5);
    fixture_server_connection.bind_stream(0, 4, 101, false, 2.0);

    let effects = fixture_server_connection.recv_stream_reset(4, 0, 3.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_recv_stream_reset_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_stream_reset(4, 0, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_transport_parameters(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_transport_parameters(2000);

    assert_eq!(
        fixture_server_connection.peer_max_datagram_frame_size(),
        Some(2000)
    );
    assert!(effects.is_empty());
}

#[rstest]
fn test_reject_session_client_failure(mut fixture_client_connection: Connection) {
    let effects = fixture_client_connection.reject_session(0, 100, 403, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "wt_session resolve failed");
    }
}

#[rstest]
fn test_reject_session_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.reject_session(0, 100, 403, 2.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_reject_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.reject_session(999, 100, 403, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_reset_stream_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.0);
    fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    let effects = fixture_server_connection.reset_stream(1, 102, 0, 3.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_reset_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.reset_stream(4, 100, 0, 1.0);

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
fn test_send_datagram_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_transport_parameters(1200);
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.5);
    let data = Bytes::from_static(b"dg");

    let effects = fixture_server_connection.send_datagram(0, 101, data);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Datagram { .. }, ..]
    ));
}

#[rstest]
fn test_send_datagram_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.send_datagram(999, 100, Bytes::from_static(b"d"));

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_send_stream_data_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.send_stream_data(4, 100, Bytes::new(), false);

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
fn test_server_recv_headers_creates_session(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitSessionEvent {
                event_type: EventType::SessionRequest,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_server_recv_headers_creates_session_stream_ended(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, true, 1.0);

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert!(effects.len() > 1);
}

#[rstest]
fn test_server_recv_headers_existing_session_ignored(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers.clone(), false, 1.0);

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_server_recv_headers_existing_session_ignored_stream_ended(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers.clone(), false, 1.0);

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, true, 1.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_server_recv_headers_local_goaway_rejects(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.local_goaway_sent = true;

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Headers { .. },
            Effect::StopQuicStream {
                error_code: ERR_H3_REQUEST_REJECTED,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_server_recv_headers_no_flow_control_limit_reached(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.initial_max_data = 0;
    fixture_server_connection.initial_max_streams_bidi = 0;
    fixture_server_connection.initial_max_streams_uni = 0;
    fixture_server_connection.peer_initial_max_data = 0;
    fixture_server_connection.recv_headers(0, fixture_headers.clone(), false, 1.0);

    let effects = fixture_server_connection.recv_headers(1, fixture_headers, false, 1.0);

    assert_eq!(fixture_server_connection.sessions.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Headers { .. }, Effect::StopQuicStream { .. }]
    ));
}

#[rstest]
fn test_server_recv_headers_rejects_invalid_protocol(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    let headers = vec![(Bytes::from_static(b":method"), Bytes::from_static(b"POST"))];

    let effects = fixture_server_connection.recv_headers(0, headers, false, 1.0);

    assert!(!fixture_server_connection.sessions.contains_key(&0));
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Headers { .. }, Effect::StopQuicStream { .. }]
    ));
}

#[rstest]
fn test_server_recv_headers_rejects_max_sessions(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.peer_initial_max_data = 100;
    fixture_server_connection.max_sessions = 1;
    fixture_server_connection.recv_headers(0, fixture_headers.clone(), false, 1.0);

    let effects = fixture_server_connection.recv_headers(1, fixture_headers, false, 1.0);

    assert_eq!(fixture_server_connection.sessions.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Headers { .. }, Effect::StopQuicStream { .. }]
    ));
}

#[rstest]
fn test_server_recv_headers_wrong_state_rejects(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Idle;

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Headers { .. }, Effect::StopQuicStream { .. }]
    ));
}

#[rstest]
fn test_session_diagnostics(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.session_diagnostics(0, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::SessionDiagnostics(_),
            ..
        }]
    ));
}

#[rstest]
fn test_session_diagnostics_not_found(fixture_server_connection: Connection) {
    let effects = fixture_server_connection.session_diagnostics(999, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_stop_stream_delegates(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.0);
    fixture_server_connection.bind_stream(0, 4, 101, false, 2.0);

    let effects = fixture_server_connection.stop_stream(4, 102, 0, 3.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_stop_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.stop_stream(4, 100, 0, 1.0);

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
fn test_stream_diagnostics(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.0);
    fixture_server_connection.bind_stream(0, 4, 101, false, 2.0);

    let effects = fixture_server_connection.stream_diagnostics(4, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::StreamDiagnostics(_),
            ..
        }]
    ));
}

#[rstest]
fn test_stream_diagnostics_not_found(fixture_server_connection: Connection) {
    let effects = fixture_server_connection.stream_diagnostics(4, 100);

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
fn test_stream_read_delegates(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(0, 100, None, 1.0);
    fixture_server_connection.bind_stream(0, 4, 101, false, 2.0);

    let effects = fixture_server_connection.stream_read(4, 100, 1024);

    assert!(effects.is_empty());
}

#[rstest]
fn test_stream_read_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.stream_read(4, 100, 1024);

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
fn test_terminated_cleans_up(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.terminated(0, "Reset".into(), 2.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Closed);
    assert!(matches!(
        effects.last(),
        Some(Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionClosed,
            ..
        })
    ));
}

#[rstest]
fn test_terminated_idempotent(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Closed;

    let effects = fixture_server_connection.terminated(0, "".into(), 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_terminated_when_closing_preserves_reason(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Closing;
    fixture_server_connection.close_code = Some(999);
    fixture_server_connection.close_reason = Some("App Close".into());
    fixture_server_connection.closed_at = Some(1.0);

    let effects = fixture_server_connection.terminated(0, "Network Error".into(), 2.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Closed);
    assert_eq!(fixture_server_connection.close_code, Some(999));
    assert_eq!(
        fixture_server_connection.close_reason,
        Some("App Close".into())
    );
    assert_eq!(fixture_server_connection.closed_at, Some(1.0));
    assert!(matches!(
        effects.last(),
        Some(Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionClosed,
            error_code: Some(0),
            ..
        })
    ));
}
