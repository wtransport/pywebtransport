//! Unit tests for the `crate::protocol::connection` module.

use std::collections::HashMap;

use bytes::Bytes;
use rstest::*;

use super::*;
use crate::common::constants::{
    DRAIN_WEBTRANSPORT_SESSION_TYPE, ERR_H3_REQUEST_REJECTED, ERR_LIB_CONNECTION_STATE_ERROR,
    ERR_LIB_INTERNAL_ERROR, ERR_LIB_SESSION_STATE_ERROR, ERR_WT_BUFFERED_STREAM_REJECTED,
    SETTINGS_WT_INITIAL_MAX_DATA, SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI,
    SETTINGS_WT_INITIAL_MAX_STREAMS_UNI,
};
use crate::common::types::{ConnectionState, ErrorSource, EventType, Headers, SessionId, StreamId};
use crate::protocol::events::{Effect, RequestResult};

#[fixture]
fn fixture_client_connection() -> Connection {
    Connection::new(
        MOCK_CONN_ID.to_owned(),
        true,
        1200,
        1024 * 1024,
        10,
        10000,
        10,
        10,
        1024,
        1024,
        true,
        10,
        100,
        5.0,
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
        MOCK_CONN_ID.to_owned(),
        false,
        1200,
        1024 * 1024,
        10,
        10000,
        10,
        10,
        1024,
        1024,
        true,
        10,
        100,
        5.0,
    )
}

const MOCK_CONN_ID: &str = "conn-123";
const MOCK_REQUEST_ID: RequestId = 100;
const MOCK_SESSION_ID: SessionId = 0;
const MOCK_STREAM_ID: StreamId = 4;

#[rstest]
fn test_accept_session_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);

    let effects =
        fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 2.0);

    assert!(
        fixture_server_connection
            .sessions
            .contains_key(&MOCK_SESSION_ID)
    );
    if let Some(session) = fixture_server_connection.sessions.get(&MOCK_SESSION_ID) {
        assert_eq!(session.state, SessionState::Connected);
    }

    assert!(!effects.is_empty());
}

#[rstest]
fn test_accept_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.accept_session(999, MOCK_REQUEST_ID, None, 1.0);
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
fn test_accept_session_with_subprotocol(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.accept_session(
        MOCK_SESSION_ID,
        MOCK_REQUEST_ID,
        Some("h3".to_owned()),
        2.0,
    );

    assert!(
        fixture_server_connection
            .sessions
            .contains_key(&MOCK_SESSION_ID)
    );
    if let Some(session) = fixture_server_connection.sessions.get(&MOCK_SESSION_ID) {
        assert_eq!(session.state, SessionState::Connected);
        assert_eq!(session.subprotocol, Some("h3".to_owned()));
    }

    assert!(!effects.is_empty());
}

#[rstest]
fn test_bind_session(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.bind_session(MOCK_STREAM_ID, MOCK_REQUEST_ID);
    assert!(effects.is_empty());
    assert_eq!(
        fixture_server_connection
            .pending_requests
            .get(&MOCK_STREAM_ID),
        Some(&MOCK_REQUEST_ID)
    );
}

#[rstest]
fn test_bind_stream_not_found(mut fixture_server_connection: Connection) {
    let effects =
        fixture_server_connection.bind_stream(999, MOCK_STREAM_ID, MOCK_REQUEST_ID, false, 1.0);
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
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.5);

    let effects = fixture_server_connection.bind_stream(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID + 1,
        false,
        2.0,
    );

    assert_eq!(
        fixture_server_connection.stream_map.get(&MOCK_STREAM_ID),
        Some(&MOCK_SESSION_ID)
    );
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
    fixture_client_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        fixture_headers.clone(),
        None,
        1.0,
    );

    let effects = fixture_client_connection.create_session(
        MOCK_REQUEST_ID + 1,
        "/".to_owned(),
        fixture_headers,
        None,
        1.0,
    );

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
    fixture_client_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        fixture_headers.clone(),
        None,
        1.0,
    );

    let effects = fixture_client_connection.create_session(
        MOCK_REQUEST_ID + 1,
        "/".to_owned(),
        fixture_headers,
        None,
        1.0,
    );

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
    let effects = fixture_client_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        fixture_headers,
        None,
        1.0,
    );

    assert!(
        fixture_client_connection
            .pending_session_configs
            .contains_key(&MOCK_REQUEST_ID)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateH3Session { .. }]
    ));
}

#[rstest]
fn test_client_create_session_with_subprotocols(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    let subprotocols = Some(vec!["h3".to_owned()]);
    let effects = fixture_client_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        fixture_headers,
        subprotocols,
        1.0,
    );

    assert!(
        fixture_client_connection
            .pending_session_configs
            .contains_key(&MOCK_REQUEST_ID)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateH3Session { .. }]
    ));
}

#[rstest]
fn test_client_create_session_wrong_state(mut fixture_client_connection: Connection) {
    let effects = fixture_client_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        vec![],
        None,
        1.0,
    );

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
    fixture_client_connection
        .pending_requests
        .insert(MOCK_SESSION_ID, MOCK_REQUEST_ID);
    fixture_client_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        fixture_headers,
        None,
        1.0,
    );

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];
    let effects =
        fixture_client_connection.recv_headers(MOCK_SESSION_ID, response_headers, false, 2.0);

    assert!(
        fixture_client_connection
            .sessions
            .contains_key(&MOCK_SESSION_ID)
    );
    assert!(
        !fixture_client_connection
            .pending_session_configs
            .contains_key(&MOCK_REQUEST_ID)
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
    fixture_client_connection
        .pending_requests
        .insert(MOCK_SESSION_ID, MOCK_REQUEST_ID);

    let effects = fixture_client_connection.recv_headers(MOCK_SESSION_ID, vec![], false, 1.0);

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
    fixture_client_connection
        .pending_requests
        .insert(MOCK_SESSION_ID, MOCK_REQUEST_ID);

    let effects = fixture_client_connection.recv_headers(MOCK_SESSION_ID, vec![], true, 1.0);

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
    fixture_client_connection
        .pending_requests
        .insert(MOCK_SESSION_ID, MOCK_REQUEST_ID);
    fixture_client_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        fixture_headers,
        None,
        1.0,
    );

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"404"))];
    let effects =
        fixture_client_connection.recv_headers(MOCK_SESSION_ID, response_headers, false, 2.0);

    assert!(
        !fixture_client_connection
            .sessions
            .contains_key(&MOCK_SESSION_ID)
    );
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
    let effects = fixture_client_connection.recv_headers(MOCK_SESSION_ID, vec![], false, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_client_recv_headers_unknown_request_stream_ended(
    mut fixture_client_connection: Connection,
) {
    let effects = fixture_client_connection.recv_headers(MOCK_SESSION_ID, vec![], true, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_close_connection_lifecycle(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.close(MOCK_REQUEST_ID, 0, Some("Bye".to_owned()), 1.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Closing);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::CloseQuicConnection { .. },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_close_session_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);

    let effects =
        fixture_server_connection.close_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, 0, None, 2.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_close_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.close_session(999, MOCK_REQUEST_ID, 0, None, 1.0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));
}

#[rstest]
fn test_connection_initialization(fixture_server_connection: Connection) {
    let conn = fixture_server_connection;
    assert_eq!(conn.id, MOCK_CONN_ID);
    assert!(!conn.is_client);
    assert_eq!(conn.state, ConnectionState::Idle);
    assert!(conn.sessions.is_empty());
}

#[rstest]
fn test_create_session_server_failure(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        vec![],
        None,
        1.0,
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Connection,
            reason,
            ..
        }] if reason.contains("Server cannot create")
    ));
}

#[rstest]
fn test_create_stream_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.peer_initial_max_streams_bidi = 100;

    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.5);

    let effects =
        fixture_server_connection.create_stream(MOCK_SESSION_ID, MOCK_REQUEST_ID + 1, false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateQuicStream { .. }]
    ));
}

#[rstest]
fn test_create_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.create_stream(999, MOCK_REQUEST_ID, false);
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
    let effects = fixture_server_connection.diagnose(MOCK_REQUEST_ID);
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
    fixture_client_connection
        .pending_requests
        .insert(MOCK_SESSION_ID, MOCK_REQUEST_ID);
    fixture_client_connection.create_session(
        MOCK_REQUEST_ID,
        "/".to_owned(),
        fixture_headers,
        None,
        1.0,
    );

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];
    fixture_client_connection.recv_headers(MOCK_SESSION_ID, response_headers, false, 2.0);

    let effects = fixture_client_connection.export_keying_material(
        MOCK_SESSION_ID,
        MOCK_REQUEST_ID + 1,
        "label".to_owned(),
        &[1, 2, 3],
        32,
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::ExportTlsKeyingMaterial { .. }]
    ));
}

#[rstest]
fn test_export_keying_material_not_found(fixture_server_connection: Connection) {
    let effects = fixture_server_connection.export_keying_material(
        999,
        MOCK_REQUEST_ID,
        "label".to_owned(),
        &[1, 2, 3],
        32,
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
fn test_fail_session_cleans_pending(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.create_session(MOCK_REQUEST_ID, "/".to_owned(), vec![], None, 1.0);

    let effects = fixture_client_connection.fail_session(MOCK_REQUEST_ID, None, "Error".to_owned());

    assert!(
        !fixture_client_connection
            .pending_session_configs
            .contains_key(&MOCK_REQUEST_ID)
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
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.5);

    let effects = fixture_server_connection.fail_stream(
        MOCK_SESSION_ID,
        MOCK_REQUEST_ID + 1,
        false,
        None,
        "Reason".to_owned(),
    );

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
    let effects =
        fixture_server_connection.fail_stream(999, MOCK_REQUEST_ID, false, None, String::new());
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
    let effects = fixture_server_connection.graceful_close(MOCK_REQUEST_ID, 1.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Closing);
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
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.5);

    let effects_data =
        fixture_server_connection.grant_data_credit(MOCK_SESSION_ID, MOCK_REQUEST_ID + 1, 99999);
    assert!(!effects_data.is_empty());

    let effects_streams = fixture_server_connection.grant_streams_credit(
        MOCK_SESSION_ID,
        MOCK_REQUEST_ID + 2,
        false,
        100,
    );
    assert!(!effects_streams.is_empty());
}

#[rstest]
fn test_grant_credits_not_found(mut fixture_server_connection: Connection) {
    let e1 = fixture_server_connection.grant_data_credit(999, MOCK_REQUEST_ID, 100);
    assert!(matches!(
        e1.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));

    let e2 = fixture_server_connection.grant_streams_credit(999, MOCK_REQUEST_ID, false, 10);
    assert!(matches!(
        e2.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_handshake_completed_transitions_state(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connecting;
    fixture_server_connection.peer_settings_received = true;

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
        MOCK_SESSION_ID,
        child_id,
        Bytes::from_static(b"chunk1"),
        false,
        1.0,
    );
    fixture_server_connection.recv_stream_data(
        MOCK_SESSION_ID,
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
    fixture_server_connection.recv_stream_data(MOCK_SESSION_ID, stream_id, data, false, 1.0);

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
    fixture_server_connection.recv_stream_data(MOCK_SESSION_ID, stream_id, data, false, 1.0);

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
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);

    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.5);

    fixture_server_connection.close_session(MOCK_SESSION_ID, MOCK_REQUEST_ID + 1, 0, None, 2.0);

    let effects = fixture_server_connection.prune_resources();

    assert!(
        !fixture_server_connection
            .sessions
            .contains_key(&MOCK_SESSION_ID)
    );
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
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.recv_capsule(MOCK_SESSION_ID, 0, &Bytes::new(), 2.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_capsule_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_capsule(999, 0, &Bytes::new(), 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_connect_close(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    let effects = fixture_server_connection.recv_connect_close(MOCK_SESSION_ID, 2.0);
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
        fixture_server_connection.recv_datagram(MOCK_SESSION_ID, Bytes::from_static(b"d"), 1.0);
    }

    let effects =
        fixture_server_connection.recv_datagram(MOCK_SESSION_ID, Bytes::from_static(b"drop"), 2.0);

    assert!(effects.is_empty());
    assert_eq!(fixture_server_connection.early_event_count, 10);
}

#[rstest]
fn test_recv_goaway_drains_sessions(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.5);

    let effects = fixture_server_connection.recv_goaway(2.0);

    if let Some(session) = fixture_server_connection.sessions.get(&MOCK_SESSION_ID) {
        assert_eq!(session.state, SessionState::Draining);
    }

    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::SendH3Capsule {
            capsule_type: DRAIN_WEBTRANSPORT_SESSION_TYPE,
            ..
        }
    )));
}

#[rstest]
fn test_recv_settings_parses_values(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connecting;
    fixture_client_connection.handshake_complete = true;

    let mut settings = HashMap::new();
    settings.insert(SETTINGS_WT_INITIAL_MAX_DATA, 1024);
    settings.insert(SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, 10);
    settings.insert(SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, 5);

    fixture_client_connection.recv_settings(&settings, 1.0);

    assert_eq!(fixture_client_connection.peer_initial_max_data, 1024);
    assert_eq!(fixture_client_connection.peer_initial_max_streams_bidi, 10);
    assert_eq!(fixture_client_connection.peer_initial_max_streams_uni, 5);
}

#[rstest]
fn test_recv_settings_triggers_ready(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connecting;
    fixture_client_connection.handshake_complete = true;

    let settings = HashMap::new();
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
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.0);
    fixture_server_connection.bind_stream(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID + 1,
        false,
        2.0,
    );

    let effects = fixture_server_connection.recv_stop_sending(MOCK_STREAM_ID, 0);
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
    let effects = fixture_server_connection.recv_stop_sending(999, 0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stream_data_early_buffer_full_bidi(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;

    for _ in 0..10 {
        fixture_server_connection.recv_stream_data(
            MOCK_SESSION_ID,
            4,
            Bytes::from_static(b"data"),
            false,
            1.0,
        );
    }

    let effects = fixture_server_connection.recv_stream_data(
        MOCK_SESSION_ID,
        4,
        Bytes::from_static(b"drop"),
        false,
        2.0,
    );

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
        fixture_server_connection.recv_stream_data(
            MOCK_SESSION_ID,
            2,
            Bytes::from_static(b"data"),
            false,
            1.0,
        );
    }

    let effects = fixture_server_connection.recv_stream_data(
        MOCK_SESSION_ID,
        2,
        Bytes::from_static(b"drop"),
        false,
        2.0,
    );

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
    let effects_early = fixture_server_connection.recv_stream_data(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        data.clone(),
        false,
        1.0,
    );
    assert!(effects_early.is_empty());
    assert_eq!(fixture_server_connection.early_event_count, 1);
    assert!(
        fixture_server_connection
            .early_event_buffer
            .contains_key(&MOCK_SESSION_ID)
    );

    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 2.0);
    assert_eq!(
        fixture_server_connection.stream_map.get(&MOCK_STREAM_ID),
        Some(&MOCK_SESSION_ID)
    );
}

#[rstest]
fn test_recv_stream_reset_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.5);
    fixture_server_connection.bind_stream(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID + 1,
        false,
        2.0,
    );

    let effects = fixture_server_connection.recv_stream_reset(MOCK_STREAM_ID, 0, 3.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_recv_stream_reset_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_stream_reset(999, 0, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_transport_parameters(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_transport_parameters(2000);
    assert_eq!(
        fixture_server_connection.remote_max_datagram_frame_size,
        Some(2000)
    );
    assert!(effects.is_empty());
}

#[rstest]
fn test_reject_session_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    let effects =
        fixture_server_connection.reject_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, 403, 2.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_reject_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.reject_session(999, MOCK_REQUEST_ID, 403, 1.0);
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
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.0);
    fixture_server_connection.bind_stream(MOCK_SESSION_ID, 1, MOCK_REQUEST_ID + 1, false, 2.0);

    let effects =
        fixture_server_connection.reset_stream(MOCK_SESSION_ID, 1, MOCK_REQUEST_ID + 2, 0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_reset_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.reset_stream(999, MOCK_STREAM_ID, MOCK_REQUEST_ID, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
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

    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.5);

    let data = Bytes::from_static(b"dg");
    let effects =
        fixture_server_connection.send_datagram(MOCK_SESSION_ID, MOCK_REQUEST_ID + 1, data);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Datagram { .. }, ..]
    ));
}

#[rstest]
fn test_send_datagram_not_found(mut fixture_server_connection: Connection) {
    let effects =
        fixture_server_connection.send_datagram(999, MOCK_REQUEST_ID, Bytes::from_static(b"d"));
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
    let effects = fixture_server_connection.send_stream_data(
        999,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID,
        Bytes::new(),
        false,
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
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
    let effects =
        fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);

    assert!(
        fixture_server_connection
            .sessions
            .contains_key(&MOCK_SESSION_ID)
    );
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
    let effects =
        fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, true, 1.0);

    assert!(
        fixture_server_connection
            .sessions
            .contains_key(&MOCK_SESSION_ID)
    );
    assert!(effects.len() > 1);
}

#[rstest]
fn test_server_recv_headers_existing_session_ignored(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers.clone(), false, 1.0);
    let effects =
        fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_server_recv_headers_existing_session_ignored_stream_ended(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers.clone(), false, 1.0);
    let effects =
        fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, true, 1.0);
    assert!(!effects.is_empty());
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

    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers.clone(), false, 1.0);
    assert_eq!(fixture_server_connection.sessions.len(), 1);

    let effects =
        fixture_server_connection.recv_headers(MOCK_SESSION_ID + 1, fixture_headers, false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Headers { .. }, Effect::StopQuicStream { .. }]
    ));
}

#[rstest]
fn test_server_recv_headers_rejects_invalid_protocol(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    let headers = vec![(Bytes::from_static(b":method"), Bytes::from_static(b"POST"))];

    let effects = fixture_server_connection.recv_headers(MOCK_SESSION_ID, headers, false, 1.0);

    assert!(
        !fixture_server_connection
            .sessions
            .contains_key(&MOCK_SESSION_ID)
    );
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

    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers.clone(), false, 1.0);
    assert_eq!(fixture_server_connection.sessions.len(), 1);

    let effects =
        fixture_server_connection.recv_headers(MOCK_SESSION_ID + 1, fixture_headers, false, 1.0);

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
    let effects =
        fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Headers { .. }, Effect::StopQuicStream { .. }]
    ));
}

#[rstest]
fn test_session_diagnostics(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    let effects = fixture_server_connection.session_diagnostics(MOCK_SESSION_ID, MOCK_REQUEST_ID);
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
    let effects = fixture_server_connection.session_diagnostics(999, MOCK_REQUEST_ID);
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
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.0);
    fixture_server_connection.bind_stream(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID + 1,
        false,
        2.0,
    );

    let effects = fixture_server_connection.stop_stream(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID + 2,
        0,
    );
    assert!(!effects.is_empty());
}

#[rstest]
fn test_stop_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.stop_stream(999, MOCK_STREAM_ID, MOCK_REQUEST_ID, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            ..
        }]
    ));
}

#[rstest]
fn test_stream_diagnostics(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);

    fixture_server_connection.accept_session(MOCK_SESSION_ID, MOCK_REQUEST_ID, None, 1.0);
    fixture_server_connection.bind_stream(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID + 1,
        false,
        2.0,
    );

    let effects = fixture_server_connection.stream_diagnostics(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID,
    );

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
    let effects =
        fixture_server_connection.stream_diagnostics(999, MOCK_STREAM_ID, MOCK_REQUEST_ID);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            ..
        }]
    ));
}

#[rstest]
fn test_stream_read_delegates(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);
    let effects = fixture_server_connection.stream_read(
        MOCK_SESSION_ID,
        MOCK_STREAM_ID,
        MOCK_REQUEST_ID,
        1024,
    );
    assert!(!effects.is_empty());
}

#[rstest]
fn test_stream_read_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.stream_read(999, MOCK_STREAM_ID, MOCK_REQUEST_ID, 1024);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            ..
        }]
    ));
}

#[rstest]
fn test_terminated_cleans_up(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(MOCK_SESSION_ID, fixture_headers, false, 1.0);

    let effects = fixture_server_connection.terminated(0, "Reset".to_owned(), 2.0);

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
    let effects = fixture_server_connection.terminated(0, String::new(), 1.0);
    assert!(effects.is_empty());
}
