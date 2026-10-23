//! Unit tests for the `crate::protocol::connection` module.

use bytes::Bytes;
use rstest::{fixture, rstest};

use super::*;
use crate::common::constants::{
    ERR_H3_REQUEST_REJECTED, ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_INTERNAL_ERROR,
    ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR, ERR_WT_BUFFERED_STREAM_REJECTED,
    ERR_WT_SESSION_GONE, WT_AVAILABLE_PROTOCOLS, WT_CAPSULE_TYPE_DRAIN_SESSION, WT_PROTOCOL,
};
use crate::common::types::{ConnectionState, ErrorSource, EventType, Headers};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::h3::H3Settings;

fn create_session_request(
    headers: Headers,
    wt_available_protocols: Option<Vec<String>>,
    optimistic: bool,
) -> SessionRequest {
    SessionRequest {
        authority: "example.com".into(),
        created_at: 1.0,
        headers,
        optimistic,
        path: "/".into(),
        wt_available_protocols,
    }
}

#[fixture]
fn fixture_client_connection() -> Connection {
    Connection::new(
        42,
        None,
        true,
        ConnectionParameters {
            early_event_ttl: 5.0,
            initial_max_data: 4 * 1024 * 1024,
            initial_max_streams_bidi: 10,
            initial_max_streams_uni: 10,
            max_connection_pending_events: 1000,
            max_pending_capsules: 20,
            max_pending_datagrams: 100,
            max_pending_streams: 10,
            max_session_pending_events: 100,
            max_sessions: 10,
            max_write_buffer_size: 1024 * 1024,
            session_receive_window: 4 * 1024 * 1024,
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
        None,
        false,
        ConnectionParameters {
            early_event_ttl: 5.0,
            initial_max_data: 4 * 1024 * 1024,
            initial_max_streams_bidi: 10,
            initial_max_streams_uni: 10,
            max_connection_pending_events: 1000,
            max_pending_capsules: 20,
            max_pending_datagrams: 100,
            max_pending_streams: 10,
            max_session_pending_events: 100,
            max_sessions: 10,
            max_write_buffer_size: 1024 * 1024,
            session_receive_window: 4 * 1024 * 1024,
        },
    )
}

#[rstest]
fn test_accept_session_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    let effects = fixture_server_connection.accept_session(0, 100, vec![], None, 2.0);

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitSessionEvent {
            event_type: EventType::SessionReady,
            ..
        }
    )));
}

#[rstest]
fn test_accept_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.accept_session(999, 100, vec![], None, 1.0);

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
    let mut headers = fixture_headers;
    headers.push((
        Bytes::from_static(WT_AVAILABLE_PROTOCOLS),
        Bytes::from_static(b"\"h3\""),
    ));
    fixture_server_connection.recv_headers(0, headers, 1.0);

    let effects = fixture_server_connection.accept_session(0, 100, vec![], Some("h3".into()), 2.0);

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitSessionEvent {
            event_type: EventType::SessionReady,
            wt_protocol: Some(protocol),
            ..
        } if protocol == "h3"
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

    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    assert!(fixture_server_connection.is_session_stream(0));
}

#[rstest]
fn test_bind_session(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.bind_session(4, 100);

    assert!(effects.is_empty());
    assert_eq!(
        fixture_server_connection.pending_session_requests.get(&4),
        Some(&100)
    );
}

#[rstest]
fn test_bind_session_optimistic_completes_via_recv_headers(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.create_session(100, create_session_request(vec![], None, true));

    let bind_effects = fixture_client_connection.bind_session(0, 100);

    assert!(fixture_client_connection.sessions.contains_key(&0));
    assert!(matches!(
        bind_effects.as_slice(),
        [
            Effect::EmitSessionEvent {
                event_type: EventType::SessionPending,
                ..
            },
            Effect::NotifyRequestDone {
                result: RequestResult::SessionId(0),
                ..
            }
        ]
    ));

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];
    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitSessionEvent {
            event_type: EventType::SessionReady,
            ..
        }
    )));
}

#[rstest]
fn test_bind_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.bind_stream(999, 1, 100, false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_bind_stream_on_closed_session_not_mapped(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.reject_session(0, 100, vec![], 403, 1.5);

    let effects = fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    assert!(!fixture_server_connection.has_stream(1));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 1,
                error_code: ERR_WT_SESSION_GONE,
            },
            Effect::StopQuicStream {
                stream_id: 1,
                error_code: ERR_WT_SESSION_GONE,
            },
            Effect::NotifyRequestFailed {
                request_id: 101,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_bind_stream_updates_map(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

    let effects = fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    assert_eq!(fixture_server_connection.stream_map.get(&1), Some(&0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitStreamEvent { .. },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_client_create_session_limit_reached(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.peer_initial_max_data = 100;
    fixture_client_connection.max_sessions = 1;
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));

    let effects =
        fixture_client_connection.create_session(101, create_session_request(vec![], None, false));

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
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.peer_initial_max_data = 0;
    fixture_client_connection.peer_initial_max_streams_bidi = 0;
    fixture_client_connection.peer_initial_max_streams_uni = 0;
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));

    let effects =
        fixture_client_connection.create_session(101, create_session_request(vec![], None, false));

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
fn test_client_create_session_peer_goaway_rejects(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.peer_goaway_received = true;

    let effects =
        fixture_client_connection.create_session(100, create_session_request(vec![], None, false));

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
fn test_client_create_session_success(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    let headers = vec![(
        Bytes::from_static(b"user-agent"),
        Bytes::from_static(b"test"),
    )];

    let effects =
        fixture_client_connection.create_session(100, create_session_request(headers, None, false));

    assert!(
        fixture_client_connection
            .session_requests
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateH3ConnectStream { authority, headers, .. }] if authority == "example.com" && headers.len() == 1 && headers.first().is_some_and(|(key, _)| key.as_ref() == b"user-agent")
    ));
}

#[rstest]
fn test_client_create_session_with_invalid_headers_failure(
    mut fixture_client_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_client_connection.state = ConnectionState::Connected;

    let effects = fixture_client_connection
        .create_session(100, create_session_request(fixture_headers, None, false));

    assert!(
        !fixture_client_connection
            .session_requests
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

    if let [Effect::NotifyRequestFailed { reason, .. }] = effects.as_slice() {
        assert_eq!(reason, "h3_headers validate invalid");
    }
}

#[rstest]
fn test_client_create_session_with_invalid_wt_available_protocols(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    let wt_available_protocols = Some(vec!["bad\nproto".into()]);

    let effects = fixture_client_connection.create_session(
        100,
        create_session_request(vec![], wt_available_protocols, false),
    );

    assert!(
        !fixture_client_connection
            .session_requests
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
) {
    fixture_client_connection.state = ConnectionState::Connected;
    let wt_available_protocols = Some(vec!["h3".into()]);

    let effects = fixture_client_connection.create_session(
        100,
        create_session_request(vec![], wt_available_protocols, false),
    );

    assert!(
        fixture_client_connection
            .session_requests
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateH3ConnectStream { authority, headers, .. }] if authority == "example.com" && headers.len() == 1 && headers.first().is_some_and(|(key, _)| key.as_ref() == WT_AVAILABLE_PROTOCOLS)
    ));
}

#[rstest]
fn test_client_create_session_wrong_state(mut fixture_client_connection: Connection) {
    let effects =
        fixture_client_connection.create_session(100, create_session_request(vec![], None, false));

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
fn test_client_recv_headers_completes_session(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));
    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];

    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(fixture_client_connection.sessions.contains_key(&0));
    assert!(
        !fixture_client_connection
            .session_requests
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
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);

    let effects = fixture_client_connection.recv_headers(0, vec![], 1.0);

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
fn test_client_recv_headers_missing_status_rejects(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));

    let effects = fixture_client_connection.recv_headers(0, vec![], 2.0);

    assert!(!fixture_client_connection.sessions.contains_key(&0));
    assert!(
        !fixture_client_connection
            .session_requests
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestFailed {
                request_id: 100,
                error_code: Some(ERR_H3_REQUEST_REJECTED),
                ..
            },
            Effect::StopQuicStream { stream_id: 0, .. },
            Effect::ResetQuicStream { stream_id: 0, .. }
        ]
    ));
}

#[rstest]
fn test_client_recv_headers_negotiated_protocol_not_requested_fails(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);
    fixture_client_connection.create_session(
        100,
        create_session_request(vec![], Some(vec!["webtransport".into()]), false),
    );

    let response_headers = vec![
        (Bytes::from_static(b":status"), Bytes::from_static(b"200")),
        (
            Bytes::from_static(WT_PROTOCOL),
            Bytes::from_static(b"\"other-protocol\""),
        ),
    ];
    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(!fixture_client_connection.sessions.contains_key(&0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestFailed {
                source: ErrorSource::Session,
                ..
            },
            Effect::StopQuicStream { .. },
            Effect::ResetQuicStream { .. }
        ]
    ));
}

#[rstest]
fn test_client_recv_headers_non_200_aborts_existing_session(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.create_session(100, create_session_request(vec![], None, true));
    fixture_client_connection.bind_session(0, 100);
    assert!(fixture_client_connection.sessions.contains_key(&0));

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"404"))];
    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitSessionEvent {
            event_type: EventType::SessionClosed,
            ..
        }
    )));
}

#[rstest]
fn test_client_recv_headers_protocol_mismatch_aborts_existing_session(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.create_session(
        100,
        create_session_request(vec![], Some(vec!["webtransport".into()]), true),
    );
    fixture_client_connection.bind_session(0, 100);
    assert!(fixture_client_connection.sessions.contains_key(&0));

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];
    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitSessionEvent {
            event_type: EventType::SessionClosed,
            ..
        }
    )));
}

#[rstest]
fn test_client_recv_headers_protocol_mismatch_no_session_fails_request(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);
    fixture_client_connection.create_session(
        100,
        create_session_request(vec![], Some(vec!["webtransport".into()]), false),
    );

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];
    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(!fixture_client_connection.sessions.contains_key(&0));
    assert!(
        !fixture_client_connection
            .session_requests
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestFailed {
                source: ErrorSource::Session,
                ..
            },
            Effect::StopQuicStream { .. },
            Effect::ResetQuicStream { .. }
        ]
    ));
}

#[rstest]
fn test_client_recv_headers_rejects_non_200(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));
    fixture_client_connection.recv_datagram(0, Bytes::from_static(b"early"), 1.5);
    fixture_client_connection.handle_stream_opened(0, 1, 1.5);
    fixture_client_connection.handle_stream_opened(0, 3, 1.5);

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"404"))];
    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(!fixture_client_connection.sessions.contains_key(&0));
    assert_eq!(fixture_client_connection.early_event_count, 0);
    assert!(
        !fixture_client_connection
            .early_event_buffer
            .contains_key(&0)
    );
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestFailed {
                source: ErrorSource::Session,
                error_code: Some(ERR_H3_REQUEST_REJECTED),
                ..
            },
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_REJECTED,
            },
            Effect::ResetQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_REJECTED,
            },
            Effect::ResetQuicStream {
                stream_id: 1,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            },
            Effect::StopQuicStream {
                stream_id: 1,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            },
            Effect::StopQuicStream {
                stream_id: 3,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            }
        ]
    ));
}

#[rstest]
fn test_client_recv_headers_replays_early_events_for_new_session(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));
    fixture_client_connection.recv_datagram(0, Bytes::from_static(b"early"), 1.5);
    fixture_client_connection.handle_stream_opened(0, 1, 1.5);

    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];
    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(
        !fixture_client_connection
            .early_event_buffer
            .contains_key(&0)
    );
    assert_eq!(fixture_client_connection.early_event_count, 0);
    assert_eq!(fixture_client_connection.stream_map.get(&1), Some(&0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitSessionEvent {
                event_type: EventType::SessionReady,
                ..
            },
            Effect::NotifyRequestDone {
                request_id: 100,
                result: RequestResult::SessionId(0),
            },
            Effect::EmitStreamEvent {
                stream_id: 1,
                event_type: EventType::StreamOpened,
                ..
            }
        ]
    ));

    let read_effects = fixture_client_connection.read_datagram(0, 101);

    assert!(matches!(
        read_effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 101,
            result: RequestResult::Datagram(data),
        }] if data.as_ref() == b"early"
    ));
}

#[rstest]
fn test_client_recv_headers_unknown_request(mut fixture_client_connection: Connection) {
    let effects = fixture_client_connection.recv_headers(0, vec![], 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_client_recv_headers_with_negotiated_protocol_success(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);
    fixture_client_connection.create_session(
        100,
        create_session_request(vec![], Some(vec!["h3".into()]), false),
    );

    let response_headers = vec![
        (Bytes::from_static(b":status"), Bytes::from_static(b"200")),
        (
            Bytes::from_static(WT_PROTOCOL),
            Bytes::from_static(b"\"h3\""),
        ),
    ];
    let effects = fixture_client_connection.recv_headers(0, response_headers, 2.0);

    assert!(fixture_client_connection.sessions.contains_key(&0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitSessionEvent {
                event_type: EventType::SessionReady,
                wt_protocol: Some(protocol),
                ..
            },
            Effect::NotifyRequestDone {
                request_id: 100,
                result: RequestResult::SessionId(0),
            }
        ] if protocol == "h3"
    ));
}

#[rstest]
fn test_close_connection_lifecycle(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.close(Some(100), 42, Some("bye".into()), 1.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Closing);
    assert_eq!(fixture_server_connection.close_code, Some(42));
    assert_eq!(fixture_server_connection.close_reason, Some("bye".into()));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::CloseQuicConnection { .. },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_close_gracefully(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;

    let effects = fixture_server_connection.close_gracefully(100);

    assert!(fixture_server_connection.local_goaway_sent);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Goaway { identifier: 0 },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_close_gracefully_client_goaway_identifier(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;

    let effects = fixture_client_connection.close_gracefully(100);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Goaway { identifier: 0 },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_close_gracefully_drains_sessions(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

    let effects = fixture_server_connection.close_gracefully(101);

    assert!(matches!(
        effects.first(),
        Some(Effect::SendH3Goaway { identifier: 4 })
    ));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::SendH3Capsule {
            stream_id: 0,
            capsule_type: WT_CAPSULE_TYPE_DRAIN_SESSION,
            ..
        }
    )));
    assert!(matches!(
        effects.last(),
        Some(Effect::NotifyRequestDone {
            request_id: 101,
            ..
        })
    ));
}

#[rstest]
fn test_close_gracefully_goaway_already_sent(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.local_goaway_sent = true;

    let effects = fixture_server_connection.close_gracefully(100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 100,
            ..
        }]
    ));
}

#[rstest]
fn test_close_gracefully_when_not_connected(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.close_gracefully(100);

    assert!(!fixture_server_connection.local_goaway_sent);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 100,
            ..
        }]
    ));
}

#[rstest]
fn test_close_idempotent_when_closing_or_closed(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Closing;

    let effects1 = fixture_server_connection.close(Some(100), 0, None, 1.0);

    assert!(matches!(
        effects1.as_slice(),
        [Effect::NotifyRequestDone { .. }]
    ));

    fixture_server_connection.state = ConnectionState::Closed;

    let effects2 = fixture_server_connection.close(Some(100), 0, None, 1.0);

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
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

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
fn test_complete_stream_write_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);
    fixture_server_connection.write_stream(4, 101, Bytes::from_static(b"data"), false);

    let effects = fixture_server_connection.complete_stream_write(4);

    assert!(fixture_server_connection.has_stream(4));
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 101,
            result: RequestResult::None,
        }]
    ));
}

#[rstest]
fn test_complete_stream_write_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.complete_stream_write(4);

    assert!(effects.is_empty());
}

#[rstest]
fn test_complete_stream_write_removes_stale_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);
    fixture_server_connection.close_session(0, 101, 0, None, 2.5);
    assert!(fixture_server_connection.has_stream(4));

    let effects = fixture_server_connection.complete_stream_write(4);

    assert!(effects.is_empty());
    assert!(!fixture_server_connection.has_stream(4));
}

#[rstest]
fn test_connection_initialization(fixture_server_connection: Connection) {
    let connection = fixture_server_connection;

    assert_eq!(connection.handle, 42);
    assert!(!connection.is_client());
    assert_eq!(connection.state, ConnectionState::Idle);
    assert!(connection.sessions.is_empty());
}

#[rstest]
fn test_create_session_server_failure(mut fixture_server_connection: Connection) {
    let effects =
        fixture_server_connection.create_session(100, create_session_request(vec![], None, false));

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
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

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
fn test_defer_stream_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.defer_stream(0, 200, false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 200,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
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
fn test_diagnose_session(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    let effects = fixture_server_connection.diagnose_session(0, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::SessionDiagnostics(_),
            ..
        }]
    ));
}

#[rstest]
fn test_diagnose_session_not_found(fixture_server_connection: Connection) {
    let effects = fixture_server_connection.diagnose_session(999, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_diagnose_stream(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.0);
    fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    let effects = fixture_server_connection.diagnose_stream(1, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::StreamDiagnostics(_),
            ..
        }]
    ));
}

#[rstest]
fn test_diagnose_stream_not_found(fixture_server_connection: Connection) {
    let effects = fixture_server_connection.diagnose_stream(4, 100);

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
fn test_early_buffer_global_limit(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    for stream_id in 0..10 {
        for _ in 0..100 {
            fixture_server_connection.recv_datagram(stream_id, Bytes::from_static(b"d"), 1.0);
        }
    }
    assert_eq!(fixture_server_connection.early_event_count, 1000);

    let effects = fixture_server_connection.handle_stream_opened(100, 4, 2.0);

    assert_eq!(fixture_server_connection.early_event_count, 1000);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 4,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            },
            Effect::StopQuicStream {
                stream_id: 4,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            }
        ]
    ));
}

#[rstest]
fn test_export_keying_material_delegates(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection
        .pending_session_requests
        .insert(0, 100);
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));
    let response_headers = vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))];
    fixture_client_connection.recv_headers(0, response_headers, 2.0);

    let effects = fixture_client_connection.export_keying_material(0, 101, "label", &[1, 2, 3], 32);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ExportTlsKeyingMaterial { .. }]
    ));
}

#[rstest]
fn test_export_keying_material_not_found(fixture_server_connection: Connection) {
    let effects =
        fixture_server_connection.export_keying_material(999, 100, "label", &[1, 2, 3], 32);

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
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));

    let effects = fixture_client_connection.fail_session(100, None, "error".into());

    assert!(
        !fixture_client_connection
            .session_requests
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
fn test_handle_handshake_completed_answers_connect_request(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.request_id = Some(7);
    fixture_client_connection.peer_settings_received = true;
    let handle = fixture_client_connection.handle;

    let effects = fixture_client_connection.handle_handshake_completed(1.0);

    assert!(fixture_client_connection.request_id.is_none());
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::CreateH3LocalStreams,
            Effect::EmitConnectionEvent {
                event_type: EventType::ConnectionEstablished,
                ..
            },
            Effect::NotifyRequestDone {
                request_id: 7,
                result: RequestResult::ConnectionHandle(result_handle),
            }
        ] if *result_handle == handle
    ));
}

#[rstest]
fn test_handle_handshake_completed_client_from_idle(mut fixture_client_connection: Connection) {
    assert_eq!(fixture_client_connection.state, ConnectionState::Idle);
    fixture_client_connection.peer_settings_received = true;

    let effects = fixture_client_connection.handle_handshake_completed(1.0);

    assert_eq!(fixture_client_connection.state, ConnectionState::Connected);
    assert!(fixture_client_connection.handshake_completed);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::CreateH3LocalStreams,
            Effect::EmitConnectionEvent {
                event_type: EventType::ConnectionEstablished,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_handle_handshake_completed_client_waits_for_settings(
    mut fixture_client_connection: Connection,
) {
    assert_eq!(fixture_client_connection.state, ConnectionState::Idle);

    let effects = fixture_client_connection.handle_handshake_completed(1.0);

    assert_eq!(fixture_client_connection.state, ConnectionState::Connecting);
    assert!(!fixture_client_connection.is_connected());
    assert!(matches!(effects.as_slice(), [Effect::CreateH3LocalStreams]));
}

#[rstest]
fn test_handle_handshake_completed_server_from_idle(mut fixture_server_connection: Connection) {
    assert_eq!(fixture_server_connection.state, ConnectionState::Idle);

    let effects = fixture_server_connection.handle_handshake_completed(1.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Connected);
    assert!(fixture_server_connection.handshake_completed);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::CreateH3LocalStreams,
            Effect::EmitConnectionEvent {
                event_type: EventType::ConnectionEstablished,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_handle_handshake_completed_wrong_state(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Closed;

    let effects = fixture_server_connection.handle_handshake_completed(1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_handle_stream_data_acknowledged_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.handle_stream_data_acknowledged(3);

    assert!(effects.is_empty());
}

#[rstest]
fn test_handle_stream_data_acknowledged_removes_closed_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.bind_stream(0, 3, 101, true, 2.0);
    fixture_server_connection.write_stream(3, 102, Bytes::new(), true);
    fixture_server_connection.complete_stream_write(3);

    let effects = fixture_server_connection.handle_stream_data_acknowledged(3);

    assert!(!fixture_server_connection.has_stream(3));
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitStreamEvent {
            stream_id: 3,
            event_type: EventType::StreamClosed,
            ..
        }]
    ));
}

#[rstest]
fn test_handle_stream_opened_buffers_early_event(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.handle_stream_opened(0, 4, 1.0);

    assert!(effects.is_empty());
    assert!(!fixture_server_connection.has_stream(4));
    assert_eq!(fixture_server_connection.early_event_count, 1);
    assert!(
        fixture_server_connection
            .early_event_buffer
            .contains_key(&0)
    );
}

#[rstest]
fn test_handle_stream_opened_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

    let effects = fixture_server_connection.handle_stream_opened(0, 4, 2.0);

    assert_eq!(fixture_server_connection.stream_map.get(&4), Some(&0));
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitStreamEvent {
            stream_id: 4,
            event_type: EventType::StreamOpened,
            ..
        }]
    ));
}

#[rstest]
fn test_handle_stream_opened_early_buffer_full_bidirectional(
    mut fixture_server_connection: Connection,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    for _ in 0..100 {
        fixture_server_connection.recv_datagram(0, Bytes::from_static(b"d"), 1.0);
    }

    let effects = fixture_server_connection.handle_stream_opened(0, 4, 2.0);

    assert_eq!(fixture_server_connection.early_event_count, 100);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 4,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            },
            Effect::StopQuicStream {
                stream_id: 4,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            }
        ]
    ));
}

#[rstest]
fn test_handle_stream_opened_early_buffer_full_unidirectional(
    mut fixture_server_connection: Connection,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    for _ in 0..100 {
        fixture_server_connection.recv_datagram(0, Bytes::from_static(b"d"), 1.0);
    }

    let effects = fixture_server_connection.handle_stream_opened(0, 2, 2.0);

    assert_eq!(fixture_server_connection.early_event_count, 100);
    assert!(matches!(
        effects.as_slice(),
        [Effect::StopQuicStream {
            stream_id: 2,
            error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
        }]
    ));
}

#[rstest]
fn test_handle_stream_opened_on_closed_session_not_mapped(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.reject_session(0, 100, vec![], 403, 1.5);

    let effects = fixture_server_connection.handle_stream_opened(0, 4, 2.0);

    assert!(!fixture_server_connection.has_stream(4));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 4,
                error_code: ERR_WT_SESSION_GONE,
            },
            Effect::StopQuicStream {
                stream_id: 4,
                error_code: ERR_WT_SESSION_GONE,
            }
        ]
    ));
}

#[rstest]
fn test_handle_terminated_cleans_up(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    let effects = fixture_server_connection.handle_terminated(0, "reset".into(), 2.0);

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
fn test_handle_terminated_fails_connect_request(mut fixture_client_connection: Connection) {
    fixture_client_connection.request_id = Some(7);
    fixture_client_connection.state = ConnectionState::Connecting;

    let effects = fixture_client_connection.handle_terminated(0, "reset".into(), 2.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitConnectionEvent {
                event_type: EventType::ConnectionClosed,
                ..
            },
            Effect::NotifyRequestFailed {
                request_id: 7,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                reason,
                ..
            }
        ] if reason == "wt_connection abort"
    ));
}

#[rstest]
fn test_handle_terminated_fails_session_requests(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));

    let effects = fixture_client_connection.handle_terminated(0, "reset".into(), 2.0);

    assert!(fixture_client_connection.session_requests.is_empty());
    assert!(matches!(
        effects.last(),
        Some(Effect::NotifyRequestFailed {
            request_id: 100,
            error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
            ..
        })
    ));
}

#[rstest]
fn test_handle_terminated_idempotent(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Closed;

    let effects = fixture_server_connection.handle_terminated(0, "".into(), 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_handle_terminated_when_closing_preserves_reason(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Closing;
    fixture_server_connection.close_code = Some(999);
    fixture_server_connection.close_reason = Some("app close".into());
    fixture_server_connection.closed_at = Some(1.0);

    let effects = fixture_server_connection.handle_terminated(0, "network error".into(), 2.0);

    assert_eq!(fixture_server_connection.state, ConnectionState::Closed);
    assert_eq!(fixture_server_connection.close_code, Some(999));
    assert_eq!(
        fixture_server_connection.close_reason,
        Some("app close".into())
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

#[rstest]
fn test_has_flow_control_with_bidirectional_streams() {
    let mut connection = fixture_server_connection();
    connection.initial_max_streams_bidi = 100;
    connection.peer_initial_max_streams_bidi = 100;

    assert!(connection.has_flow_control());
}

#[rstest]
fn test_has_flow_control_with_data() {
    let mut connection = fixture_server_connection();
    connection.initial_max_data = 100;
    connection.peer_initial_max_data = 100;

    assert!(connection.has_flow_control());
}

#[rstest]
fn test_has_flow_control_with_unidirectional_streams() {
    let mut connection = fixture_server_connection();
    connection.initial_max_streams_uni = 100;
    connection.peer_initial_max_streams_uni = 100;

    assert!(connection.has_flow_control());
}

#[rstest]
fn test_is_session_closed(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    assert!(!fixture_server_connection.is_session_closed(0));
    assert!(!fixture_server_connection.is_session_closed(999));

    fixture_server_connection.reject_session(0, 100, vec![], 403, 1.5);

    assert!(fixture_server_connection.is_session_closed(0));
}

#[rstest]
fn test_prune_early_events_retains_unexpired(mut fixture_server_connection: Connection) {
    fixture_server_connection.handle_stream_opened(0, 4, 1.0);
    fixture_server_connection.handle_stream_opened(0, 8, 9.0);

    let effects = fixture_server_connection.prune_early_events(10.0);

    assert_eq!(fixture_server_connection.early_event_count, 1);
    assert!(
        fixture_server_connection
            .early_event_buffer
            .contains_key(&0)
    );
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream { stream_id: 4, .. },
            Effect::StopQuicStream { stream_id: 4, .. }
        ]
    ));
}

#[rstest]
fn test_prune_early_events_timeout(mut fixture_server_connection: Connection) {
    fixture_server_connection.handle_stream_opened(0, 4, 1.0);
    fixture_server_connection.recv_datagram(0, Bytes::from_static(b"data"), 1.0);

    let effects = fixture_server_connection.prune_early_events(10.0);

    assert_eq!(fixture_server_connection.early_event_count, 0);
    assert!(fixture_server_connection.early_event_buffer.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 4,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            },
            Effect::StopQuicStream {
                stream_id: 4,
                error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
            }
        ]
    ));
}

#[rstest]
fn test_prune_early_events_timeout_unidirectional(mut fixture_server_connection: Connection) {
    fixture_server_connection.handle_stream_opened(0, 2, 1.0);

    let effects = fixture_server_connection.prune_early_events(10.0);

    assert_eq!(fixture_server_connection.early_event_count, 0);
    assert!(fixture_server_connection.early_event_buffer.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::StopQuicStream {
            stream_id: 2,
            error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
        }]
    ));
}

#[rstest]
fn test_prune_resources_cleans_stream_map(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_settings(
        &H3Settings {
            wt_initial_max_data: Some(4 * 1024 * 1024),
            wt_initial_max_streams_bidi: Some(10),
            wt_initial_max_streams_uni: Some(10),
            ..Default::default()
        },
        0.5,
    );

    fixture_server_connection.recv_headers(0, fixture_headers.clone(), 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);
    fixture_server_connection.close_session(0, 101, 0, None, 2.5);
    fixture_server_connection.recv_connect_close(0, 3.0);

    fixture_server_connection.recv_headers(8, fixture_headers, 3.5);
    fixture_server_connection.accept_session(8, 102, vec![], None, 4.0);
    fixture_server_connection.handle_stream_opened(8, 12, 4.5);

    fixture_server_connection.prune_resources();

    assert!(!fixture_server_connection.sessions.contains_key(&0));
    assert!(fixture_server_connection.sessions.contains_key(&8));
    assert!(!fixture_server_connection.has_stream(4));
    assert_eq!(fixture_server_connection.stream_map.get(&12), Some(&8));
}

#[rstest]
fn test_prune_resources_keeps_active_sessions(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);

    fixture_server_connection.prune_resources();

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert_eq!(fixture_server_connection.stream_map.get(&4), Some(&0));
}

#[rstest]
fn test_prune_resources_removes_closed_sessions(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.close_session(0, 101, 0, None, 2.0);
    fixture_server_connection.recv_connect_close(0, 2.5);

    fixture_server_connection.prune_resources();

    assert!(!fixture_server_connection.sessions.contains_key(&0));
}

#[rstest]
fn test_read_datagram_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

    let read_effects = fixture_server_connection.read_datagram(0, 101);
    let datagram_effects =
        fixture_server_connection.recv_datagram(0, Bytes::from_static(b"datagram"), 2.0);

    assert!(read_effects.is_empty());
    assert!(matches!(
        datagram_effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 101,
            result: RequestResult::Datagram(data),
        }] if data.as_ref() == b"datagram"
    ));
}

#[rstest]
fn test_read_datagram_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.read_datagram(999, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 100,
            source: ErrorSource::Session,
            ..
        }]
    ));
}

#[rstest]
fn test_read_stream_delegates(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);

    let effects = fixture_server_connection.read_stream(4, 101, 1024);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 4,
            max_size: 1024,
        }]
    ));
}

#[rstest]
fn test_read_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.read_stream(4, 100, 1024);

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
fn test_read_stream_removes_closed_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);
    fixture_server_connection.reset_stream(4, 101, 0);
    fixture_server_connection.recv_stream_data(4, Bytes::new(), true, 2.5);

    let effects = fixture_server_connection.read_stream(4, 102, 1024);

    assert!(!fixture_server_connection.has_stream(4));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestDone {
                request_id: 102,
                result: RequestResult::ReadData(data),
            },
            Effect::EmitStreamEvent {
                stream_id: 4,
                event_type: EventType::StreamClosed,
                ..
            }
        ] if data.is_empty()
    ));
}

#[rstest]
fn test_recv_capsule_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

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
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    let effects = fixture_server_connection.recv_connect_close(0, 2.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_recv_connect_close_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_connect_close(999, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_datagram_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    fixture_server_connection.recv_datagram(0, Bytes::from_static(b"data"), 2.0);

    assert!(
        !fixture_server_connection
            .early_event_buffer
            .contains_key(&0)
    );
}

#[rstest]
fn test_recv_datagram_early_buffer_full(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    for _ in 0..100 {
        fixture_server_connection.recv_datagram(0, Bytes::from_static(b"d"), 1.0);
    }

    let effects = fixture_server_connection.recv_datagram(0, Bytes::from_static(b"drop"), 2.0);

    assert!(effects.is_empty());
    assert_eq!(fixture_server_connection.early_event_count, 100);
}

#[rstest]
fn test_recv_goaway_drains_sessions(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

    let effects = fixture_server_connection.recv_goaway();

    assert!(fixture_server_connection.peer_goaway_received);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::SendH3Capsule {
            capsule_type: WT_CAPSULE_TYPE_DRAIN_SESSION,
            ..
        }
    )));
}

#[rstest]
fn test_recv_goaway_fails_blocked_session_requests(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));
    fixture_client_connection.defer_session(100);

    let effects = fixture_client_connection.recv_goaway();

    assert!(fixture_client_connection.peer_goaway_received);
    assert!(
        !fixture_client_connection
            .session_requests
            .contains_key(&100)
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 100,
            error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
            reason,
            ..
        }] if reason == "wt_connection validate failed"
    ));
}

#[rstest]
fn test_recv_max_streams_releases_blocked_session_requests(
    mut fixture_client_connection: Connection,
) {
    fixture_client_connection.state = ConnectionState::Connected;
    fixture_client_connection.create_session(100, create_session_request(vec![], None, false));
    let deferred_effects = fixture_client_connection.defer_session(100);

    let effects = fixture_client_connection.recv_max_streams(false);

    assert!(deferred_effects.is_empty());
    assert!(
        fixture_client_connection
            .blocked_session_requests
            .is_empty()
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateH3ConnectStream {
            request_id: 100,
            authority,
            ..
        }] if authority == "example.com"
    ));
}

#[rstest]
fn test_recv_max_streams_releases_blocked_stream_requests(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.create_stream(0, 200, true);
    let deferred_effects = fixture_server_connection.defer_stream(0, 200, true);

    let effects = fixture_server_connection.recv_max_streams(true);

    assert!(deferred_effects.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::CreateQuicStream {
            request_id: 200,
            session_id: 0,
            is_unidirectional: true,
        }]
    ));
}

#[rstest]
fn test_recv_settings_parses_values(mut fixture_client_connection: Connection) {
    fixture_client_connection.state = ConnectionState::Connecting;
    fixture_client_connection.handshake_completed = true;
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
    fixture_client_connection.handshake_completed = true;
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
fn test_recv_stop_sending_aborts_session(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

    let effects = fixture_server_connection.recv_stop_sending(0, ERR_H3_REQUEST_REJECTED, 2.0);

    assert!(fixture_server_connection.is_session_closed(0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_REJECTED,
            },
            Effect::EmitSessionEvent {
                event_type: EventType::SessionClosed,
                error_code: Some(ERR_H3_REQUEST_REJECTED),
                ..
            }
        ]
    ));
}

#[rstest]
fn test_recv_stop_sending_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.0);
    fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    let effects = fixture_server_connection.recv_stop_sending(1, 0, 3.0);

    assert!(fixture_server_connection.has_stream(1));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitStreamEvent {
                event_type: EventType::StreamStopSendingReceived,
                ..
            },
            Effect::ResetQuicStream { stream_id: 1, .. }
        ]
    ));
}

#[rstest]
fn test_recv_stop_sending_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_stop_sending(4, 0, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stop_sending_removes_closed_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.bind_stream(0, 3, 101, true, 2.0);

    let effects = fixture_server_connection.recv_stop_sending(3, 0, 3.0);

    assert!(!fixture_server_connection.has_stream(3));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitStreamEvent {
                event_type: EventType::StreamStopSendingReceived,
                ..
            },
            Effect::ResetQuicStream { stream_id: 3, .. },
            Effect::EmitStreamEvent {
                stream_id: 3,
                event_type: EventType::StreamClosed,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_recv_stream_data_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);
    fixture_server_connection.read_stream(4, 101, 1024);

    let effects =
        fixture_server_connection.recv_stream_data(4, Bytes::from_static(b"data"), false, 3.0);

    assert!(fixture_server_connection.has_stream(4));
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 101,
            result: RequestResult::ReadData(data),
        }] if data.as_ref() == b"data"
    ));
}

#[rstest]
fn test_recv_stream_data_not_found(mut fixture_server_connection: Connection) {
    let effects =
        fixture_server_connection.recv_stream_data(4, Bytes::from_static(b"data"), false, 1.0);

    assert!(effects.is_empty());
    assert_eq!(fixture_server_connection.early_event_count, 0);
}

#[rstest]
fn test_recv_stream_data_removes_closed_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);
    fixture_server_connection.reset_stream(4, 101, 0);
    fixture_server_connection.read_stream(4, 102, 1024);

    let effects = fixture_server_connection.recv_stream_data(4, Bytes::new(), true, 3.0);

    assert!(!fixture_server_connection.has_stream(4));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestDone {
                request_id: 102,
                result: RequestResult::ReadData(data),
            },
            Effect::EmitStreamEvent {
                stream_id: 4,
                event_type: EventType::StreamClosed,
                ..
            }
        ] if data.is_empty()
    ));
}

#[rstest]
fn test_recv_stream_reset_aborts_session(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

    let effects = fixture_server_connection.recv_stream_reset(0, ERR_H3_REQUEST_REJECTED, 2.0);

    assert!(fixture_server_connection.is_session_closed(0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_REJECTED,
            },
            Effect::EmitSessionEvent {
                event_type: EventType::SessionClosed,
                error_code: Some(ERR_H3_REQUEST_REJECTED),
                ..
            }
        ]
    ));
}

#[rstest]
fn test_recv_stream_reset_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    let effects = fixture_server_connection.recv_stream_reset(1, 0, 3.0);

    assert!(fixture_server_connection.has_stream(1));
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitStreamEvent {
            stream_id: 1,
            event_type: EventType::StreamResetReceived,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_stream_reset_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.recv_stream_reset(4, 0, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stream_reset_removes_closed_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 2, 2.0);

    let effects = fixture_server_connection.recv_stream_reset(2, 0, 3.0);

    assert!(!fixture_server_connection.has_stream(2));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitStreamEvent {
                stream_id: 2,
                event_type: EventType::StreamResetReceived,
                ..
            },
            Effect::EmitStreamEvent {
                stream_id: 2,
                event_type: EventType::StreamClosed,
                ..
            }
        ]
    ));
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
    let effects = fixture_client_connection.reject_session(0, 100, vec![], 403, 1.0);

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
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    let effects = fixture_server_connection.reject_session(0, 100, vec![], 403, 2.0);

    assert!(!effects.is_empty());
}

#[rstest]
fn test_reject_session_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.reject_session(999, 100, vec![], 403, 1.0);

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
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.0);
    fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    let effects = fixture_server_connection.reset_stream(1, 102, 0);

    assert!(fixture_server_connection.has_stream(1));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream { stream_id: 1, .. },
            Effect::NotifyRequestDone {
                request_id: 102,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_reset_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.reset_stream(4, 100, 0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 100,
            result: RequestResult::None,
        }]
    ));
}

#[rstest]
fn test_reset_stream_removes_closed_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.bind_stream(0, 3, 101, true, 2.0);

    let effects = fixture_server_connection.reset_stream(3, 102, 0);

    assert!(!fixture_server_connection.has_stream(3));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream { stream_id: 3, .. },
            Effect::NotifyRequestDone {
                request_id: 102,
                ..
            },
            Effect::EmitStreamEvent {
                stream_id: 3,
                event_type: EventType::StreamClosed,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_send_datagram_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_transport_parameters(1200);
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    let data = Bytes::from_static(b"datagram");

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
fn test_server_recv_headers_creates_session(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitSessionEvent {
                event_type: EventType::SessionRequestReceived,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_server_recv_headers_defaults_path(mut fixture_server_connection: Connection) {
    fixture_server_connection.state = ConnectionState::Connected;
    let headers = vec![
        (
            Bytes::from_static(b":method"),
            Bytes::from_static(b"CONNECT"),
        ),
        (
            Bytes::from_static(b":protocol"),
            Bytes::from_static(b"webtransport-h3"),
        ),
    ];

    let effects = fixture_server_connection.recv_headers(0, headers, 1.0);

    assert!(fixture_server_connection.sessions.contains_key(&0));
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitSessionEvent {
            event_type: EventType::SessionRequestReceived,
            path: Some(path),
            ..
        }] if path == "/"
    ));
}

#[rstest]
fn test_server_recv_headers_existing_session_ignored(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers.clone(), 1.0);

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_server_recv_headers_local_goaway_rejects(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.local_goaway_sent = true;

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

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
    fixture_server_connection.recv_headers(0, fixture_headers.clone(), 1.0);

    let effects = fixture_server_connection.recv_headers(1, fixture_headers, 1.0);

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

    let effects = fixture_server_connection.recv_headers(0, headers, 1.0);

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
    fixture_server_connection.recv_headers(0, fixture_headers.clone(), 1.0);

    let effects = fixture_server_connection.recv_headers(1, fixture_headers, 1.0);

    assert_eq!(fixture_server_connection.sessions.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Headers { .. }, Effect::StopQuicStream { .. }]
    ));
}

#[rstest]
fn test_server_recv_headers_replays_early_events(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_datagram(0, Bytes::from_static(b"early"), 0.5);
    fixture_server_connection.handle_stream_opened(0, 4, 0.5);

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    assert_eq!(fixture_server_connection.early_event_count, 0);
    assert_eq!(fixture_server_connection.stream_map.get(&4), Some(&0));
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitSessionEvent {
            event_type: EventType::SessionRequestReceived,
            ..
        }]
    ));

    let accept_effects = fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);

    assert!(accept_effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitStreamEvent {
            stream_id: 4,
            event_type: EventType::StreamOpened,
            ..
        }
    )));
}

#[rstest]
fn test_server_recv_headers_wrong_state_rejects(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Idle;

    let effects = fixture_server_connection.recv_headers(0, fixture_headers, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendH3Headers { .. }, Effect::StopQuicStream { .. }]
    ));
}

#[rstest]
fn test_stop_stream_delegates(mut fixture_server_connection: Connection, fixture_headers: Headers) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.0);
    fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    let effects = fixture_server_connection.stop_stream(1, 102, 0);

    assert!(fixture_server_connection.has_stream(1));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::StopQuicStream { stream_id: 1, .. },
            Effect::NotifyRequestDone {
                request_id: 102,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_stop_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.stop_stream(4, 100, 0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 100,
            result: RequestResult::None,
        }]
    ));
}

#[rstest]
fn test_stop_stream_removes_closed_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 2, 2.0);

    let effects = fixture_server_connection.stop_stream(2, 101, 0);

    assert!(!fixture_server_connection.has_stream(2));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::StopQuicStream { stream_id: 2, .. },
            Effect::NotifyRequestDone {
                request_id: 101,
                ..
            },
            Effect::EmitStreamEvent {
                stream_id: 2,
                event_type: EventType::StreamClosed,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_write_stream_delegates(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.peer_initial_max_data = 1000;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.bind_stream(0, 1, 101, false, 2.0);

    let effects =
        fixture_server_connection.write_stream(1, 102, Bytes::from_static(b"data"), false);

    assert!(fixture_server_connection.has_stream(1));
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            stream_id: 1,
            data,
            end_stream: false,
        }] if data.as_ref() == b"data"
    ));
}

#[rstest]
fn test_write_stream_fin_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.write_stream(4, 100, Bytes::new(), true);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 100,
            result: RequestResult::None,
        }]
    ));
}

#[rstest]
fn test_write_stream_not_found(mut fixture_server_connection: Connection) {
    let effects = fixture_server_connection.write_stream(4, 100, Bytes::new(), false);

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
fn test_write_stream_removes_stale_stream(
    mut fixture_server_connection: Connection,
    fixture_headers: Headers,
) {
    fixture_server_connection.state = ConnectionState::Connected;
    fixture_server_connection.recv_headers(0, fixture_headers, 1.0);
    fixture_server_connection.accept_session(0, 100, vec![], None, 1.5);
    fixture_server_connection.handle_stream_opened(0, 4, 2.0);
    fixture_server_connection.close_session(0, 101, 0, None, 2.5);
    assert!(fixture_server_connection.has_stream(4));

    let effects =
        fixture_server_connection.write_stream(4, 102, Bytes::from_static(b"data"), false);

    assert!(!fixture_server_connection.has_stream(4));
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 102,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}
