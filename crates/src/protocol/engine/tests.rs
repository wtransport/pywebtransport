//! Unit tests for the `crate::protocol::engine` module.

use bytes::{Bytes, BytesMut};
use rstest::*;

use super::*;
use crate::common::constants::{
    ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR, H3_STREAM_TYPE_CONTROL,
};
use crate::common::types::ConnectionState;
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::utils::write_varint;

const MOCK_CONN_ID: &str = "conn-123";
const MOCK_REQUEST_ID: u64 = 100;

fn create_test_engine(is_client: bool) -> WebTransportEngine {
    let res = WebTransportEngine::new(
        MOCK_CONN_ID.to_owned(),
        is_client,
        1200,
        1024 * 1024,
        10,
        10000,
        10,
        10,
        1024,
        1024,
        true,
        1024,
    );
    match res {
        Ok(engine) => engine,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "", "Engine initialization failed");
            unreachable!()
        }
    }
}

#[fixture]
fn fixture_engine_client() -> WebTransportEngine {
    create_test_engine(true)
}

#[fixture]
fn fixture_engine_server() -> WebTransportEngine {
    create_test_engine(false)
}

#[rstest]
fn test_buffer_create_stream_when_connecting(mut fixture_engine_client: WebTransportEngine) {
    let event = ProtocolEvent::UserCreateStream {
        request_id: MOCK_REQUEST_ID,
        session_id: 0,
        is_unidirectional: true,
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::CreateQuicStream { .. }))
    );
    assert_eq!(fixture_engine_client.pending_user_actions.len(), 1);
}

#[rstest]
fn test_buffer_user_actions_when_connecting(mut fixture_engine_client: WebTransportEngine) {
    let event = ProtocolEvent::UserCreateSession {
        request_id: MOCK_REQUEST_ID,
        path: "/".to_owned(),
        headers: vec![],
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::RescheduleQuicTimer))
    );
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::CreateH3Session { .. }))
    );

    assert_eq!(fixture_engine_client.pending_user_actions.len(), 1);
}

#[rstest]
fn test_buffer_user_actions_when_idle(mut fixture_engine_client: WebTransportEngine) {
    fixture_engine_client.connection.state = ConnectionState::Idle;

    let event = ProtocolEvent::UserCreateSession {
        request_id: MOCK_REQUEST_ID,
        path: "/".to_owned(),
        headers: vec![],
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert!(fixture_engine_client.pending_user_actions.len() == 1);
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::CreateH3Session { .. }))
    );
}

#[rstest]
fn test_cleanup_stream(mut fixture_engine_server: WebTransportEngine) {
    fixture_engine_server.cleanup_stream(0);
}

#[rstest]
fn test_client_immediate_actions_when_connected(mut fixture_engine_client: WebTransportEngine) {
    fixture_engine_client.connection.state = ConnectionState::Connected;

    let session_event = ProtocolEvent::UserCreateSession {
        request_id: MOCK_REQUEST_ID,
        path: "/".to_owned(),
        headers: vec![],
    };
    let effects = fixture_engine_client.handle_event(session_event, 0.0);
    assert!(fixture_engine_client.pending_user_actions.is_empty());
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::CreateH3Session { .. }))
    );

    let stream_event = ProtocolEvent::UserCreateStream {
        request_id: MOCK_REQUEST_ID + 1,
        session_id: 0,
        is_unidirectional: true,
    };
    let effects_stream = fixture_engine_client.handle_event(stream_event, 0.0);

    assert!(fixture_engine_client.pending_user_actions.is_empty());
    assert!(!effects_stream.is_empty());
}

#[rstest]
fn test_client_receives_settings_via_raw_data(mut fixture_engine_client: WebTransportEngine) {
    let _unused_init = fixture_engine_client.initialize_h3_transport(0, 4, 8);

    let mut data = BytesMut::new();
    if let Err(e) = write_varint(&mut data, H3_STREAM_TYPE_CONTROL) {
        assert_eq!(format!("{e:?}"), "", "Failed to write control stream type");
    }
    if let Err(e) = write_varint(&mut data, 4) {
        assert_eq!(format!("{e:?}"), "", "Failed to write settings type");
    }
    if let Err(e) = write_varint(&mut data, 0) {
        assert_eq!(format!("{e:?}"), "", "Failed to write settings length");
    }

    let event = ProtocolEvent::TransportStreamDataReceived {
        data: data.freeze(),
        stream_id: 3,
        end_stream: false,
    };

    let _effects = fixture_engine_client.handle_event(event, 0.0);
}

#[rstest]
fn test_connection_close_event_fails_pending_actions(
    mut fixture_engine_client: WebTransportEngine,
) {
    let create_stream_event = ProtocolEvent::UserCreateStream {
        request_id: 1,
        session_id: 0,
        is_unidirectional: true,
    };
    let _unused = fixture_engine_client.handle_event(create_stream_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_actions.len(), 1);

    let create_session_event = ProtocolEvent::UserCreateSession {
        request_id: 2,
        path: "/".to_owned(),
        headers: vec![],
    };
    let _unused = fixture_engine_client.handle_event(create_session_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_actions.len(), 2);

    let close_event = ProtocolEvent::ConnectionClose {
        request_id: 3,
        error_code: 0,
        reason: None,
    };

    let effects = fixture_engine_client.handle_event(close_event, 1.0);

    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::CloseQuicConnection { .. }))
    );

    let failures = effects
        .iter()
        .filter(|e| matches!(e, Effect::NotifyRequestFailed { .. }))
        .count();
    assert_eq!(failures, 2);
    assert!(fixture_engine_client.pending_user_actions.is_empty());
}

#[rstest]
fn test_encode_capsule() {
    let data = Bytes::from_static(b"cap");
    let res = WebTransportEngine::encode_capsule(0, 0x1, data, false);

    let effects = match res {
        Ok(e) => e,
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Encode capsule failed");
            return;
        }
    };
    assert!(matches!(effects.as_slice(), [Effect::SendQuicData { .. }]));
}

#[rstest]
fn test_encode_datagram() {
    let data = Bytes::from_static(b"payload");
    let res = WebTransportEngine::encode_datagram(0, &data);

    let effects = match res {
        Ok(e) => e,
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Encode datagram failed");
            return;
        }
    };
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicDatagram { .. }]
    ));
}

#[rstest]
fn test_encode_goaway(mut fixture_engine_server: WebTransportEngine) {
    if let Err(e) = fixture_engine_server.h3.set_local_stream_ids(3, 7, 11) {
        let msg = format!("{e:?}");
        assert_eq!(msg, "", "Setup failed");
        return;
    }

    let effects = fixture_engine_server.encode_goaway();

    assert!(!effects.is_empty());
    assert!(matches!(effects.as_slice(), [Effect::SendQuicData { .. }]));
}

#[rstest]
fn test_encode_goaway_no_control_stream(mut fixture_engine_server: WebTransportEngine) {
    let effects = fixture_engine_server.encode_goaway();

    assert!(effects.is_empty());
}

#[rstest]
fn test_encode_headers(mut fixture_engine_server: WebTransportEngine) {
    if let Err(e) = fixture_engine_server.h3.set_local_stream_ids(3, 7, 11) {
        let msg = format!("{e:?}");
        assert_eq!(msg, "", "Failed to set local stream IDs");
        return;
    }

    let res = fixture_engine_server.encode_headers(100, 200, true);

    let effects = match res {
        Ok(e) => e,
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Encode headers failed");
            return;
        }
    };

    assert!(!effects.is_empty());
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::SendQuicData { .. }))
    );
}

#[rstest]
fn test_encode_session_request(mut fixture_engine_client: WebTransportEngine) {
    let headers = vec![];
    let res = fixture_engine_client.encode_session_request(
        0,
        "/test".to_owned(),
        "localhost".to_owned(),
        &headers,
    );

    let effects = match res {
        Ok(e) => e,
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Encode session request failed");
            return;
        }
    };

    assert!(!effects.is_empty());
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::SendQuicData { .. }))
    );
}

#[rstest]
fn test_encode_stream_creation(mut fixture_engine_server: WebTransportEngine) {
    let effects = fixture_engine_server.encode_stream_creation(15, 0, true);
    assert!(!effects.is_empty());
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::SendQuicData { .. }))
    );
}

#[rstest]
fn test_fail_pending_actions_on_termination(mut fixture_engine_client: WebTransportEngine) {
    let create_event = ProtocolEvent::UserCreateSession {
        request_id: MOCK_REQUEST_ID,
        path: "/".to_owned(),
        headers: vec![],
    };
    let _unused = fixture_engine_client.handle_event(create_event, 0.0);

    let term_event = ProtocolEvent::TransportConnectionTerminated {
        error_code: 0,
        reason_phrase: "Stop".to_owned(),
    };
    let effects = fixture_engine_client.handle_event(term_event, 1.0);

    assert!(fixture_engine_client.pending_user_actions.is_empty());

    let has_fail = effects.iter().any(|e| {
        matches!(
            e,
            Effect::NotifyRequestFailed {
                error_code: Some(ec),
                ..
            } if *ec == ERR_LIB_CONNECTION_STATE_ERROR
        )
    });
    assert!(has_fail);
}

#[rstest]
fn test_handle_event_server_handshake(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::TransportHandshakeCompleted;
    let effects = fixture_engine_server.handle_event(event, 1.0);

    assert_eq!(
        fixture_engine_server.connection.state,
        ConnectionState::Connected
    );
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionEstablished,
            ..
        }
    )));
}

#[rstest]
fn test_handle_handshake_unexpected_state(mut fixture_engine_server: WebTransportEngine) {
    fixture_engine_server.connection.state = ConnectionState::Closed;

    let event = ProtocolEvent::TransportHandshakeCompleted;
    let effects = fixture_engine_server.handle_event(event, 1.0);

    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::EmitConnectionEvent { .. }))
    );
    assert_eq!(
        fixture_engine_server.connection.state,
        ConnectionState::Closed
    );
}

#[rstest]
fn test_handle_internal_events(mut fixture_engine_server: WebTransportEngine) {
    let events = vec![
        ProtocolEvent::InternalBindQuicStream {
            request_id: MOCK_REQUEST_ID,
            stream_id: 4,
            session_id: 0,
            is_unidirectional: false,
        },
        ProtocolEvent::InternalFailH3Session {
            request_id: MOCK_REQUEST_ID,
            reason: "fail".to_owned(),
            error_code: None,
        },
        ProtocolEvent::InternalFailQuicStream {
            request_id: MOCK_REQUEST_ID,
            session_id: 0,
            is_unidirectional: false,
            error_code: None,
            reason: "fail".to_owned(),
        },
        ProtocolEvent::InternalReturnStreamData {
            stream_id: 4,
            data: Bytes::from_static(b"unused"),
        },
        ProtocolEvent::InternalCleanupResources,
    ];

    for event in events {
        let _unused = fixture_engine_server.handle_event(event, 0.0);
    }
}

#[rstest]
fn test_handle_transport_delegation(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::TransportDatagramFrameReceived {
        data: Bytes::from_static(b"abc"),
    };

    let effects = fixture_engine_server.handle_event(event, 0.0);

    assert_eq!(fixture_engine_server.connection.early_event_count, 1);
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::RescheduleQuicTimer))
    );
}

#[rstest]
fn test_handle_transport_delegation_coverage(mut fixture_engine_server: WebTransportEngine) {
    let capsule_event = ProtocolEvent::CapsuleReceived {
        capsule_data: Bytes::from_static(b""),
        capsule_type: 0,
        stream_id: 0,
    };
    fixture_engine_server.handle_event(capsule_event, 0.0);

    let datagram_event = ProtocolEvent::DatagramReceived {
        data: Bytes::from_static(b"d"),
        stream_id: 0,
    };
    fixture_engine_server.handle_event(datagram_event, 0.0);

    let transport_dgram = ProtocolEvent::TransportDatagramFrameReceived {
        data: Bytes::from_static(b"raw"),
    };
    let effects = fixture_engine_server.handle_event(transport_dgram, 0.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_handle_transport_events(mut fixture_engine_server: WebTransportEngine) {
    let events = vec![
        ProtocolEvent::TransportQuicTimerFired,
        ProtocolEvent::TransportStreamReset {
            stream_id: 0,
            error_code: 0,
        },
        ProtocolEvent::TransportQuicParametersReceived {
            remote_max_datagram_frame_size: 1200,
        },
        ProtocolEvent::ConnectStreamClosed { stream_id: 0 },
        ProtocolEvent::HeadersReceived {
            stream_id: 0,
            headers: vec![],
            stream_ended: false,
        },
        ProtocolEvent::WebTransportStreamDataReceived {
            data: Bytes::new(),
            session_id: 0,
            stream_id: 4,
            stream_ended: false,
        },
        ProtocolEvent::GoawayReceived,
    ];

    for event in events {
        let effects = fixture_engine_server.handle_event(event, 0.0);
        if let Some(Effect::TriggerQuicTimer) = effects.first() {
            assert!(
                effects
                    .iter()
                    .any(|e| matches!(e, Effect::RescheduleQuicTimer))
            );
        }
    }
}

#[rstest]
fn test_handle_user_passthrough_events(mut fixture_engine_server: WebTransportEngine) {
    let events = vec![
        ProtocolEvent::UserAcceptSession {
            request_id: 1,
            session_id: 0,
        },
        ProtocolEvent::UserCloseSession {
            request_id: 2,
            session_id: 0,
            error_code: 0,
            reason: None,
        },
        ProtocolEvent::UserConnectionGracefulClose { request_id: 3 },
        ProtocolEvent::UserGetConnectionDiagnostics { request_id: 4 },
        ProtocolEvent::UserGetSessionDiagnostics {
            request_id: 5,
            session_id: 0,
        },
        ProtocolEvent::UserGetStreamDiagnostics {
            request_id: 6,
            stream_id: 4,
        },
        ProtocolEvent::UserGrantDataCredit {
            request_id: 7,
            session_id: 0,
            max_data: 1000,
        },
        ProtocolEvent::UserGrantStreamsCredit {
            request_id: 8,
            session_id: 0,
            max_streams: 10,
            is_unidirectional: false,
        },
        ProtocolEvent::UserRejectSession {
            request_id: 9,
            session_id: 0,
            status_code: 403,
        },
        ProtocolEvent::UserResetStream {
            request_id: 10,
            stream_id: 4,
            error_code: 0,
        },
        ProtocolEvent::UserSendDatagram {
            request_id: 11,
            session_id: 0,
            data: Bytes::from_static(b"d"),
        },
        ProtocolEvent::UserSendStreamData {
            request_id: 12,
            stream_id: 4,
            data: Bytes::from_static(b"s"),
            end_stream: false,
        },
        ProtocolEvent::UserStopStream {
            request_id: 13,
            stream_id: 4,
            error_code: 0,
        },
        ProtocolEvent::UserStreamRead {
            request_id: 14,
            stream_id: 4,
            max_bytes: 100,
        },
    ];

    for event in events {
        let effects = fixture_engine_server.handle_event(event, 0.0);
        assert!(!effects.is_empty());
    }

    let diag_fail_event = ProtocolEvent::UserGetStreamDiagnostics {
        request_id: 99,
        stream_id: 999,
    };
    let effects = fixture_engine_server.handle_event(diag_fail_event, 0.0);
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyRequestFailed { .. }))
    );
}

#[rstest]
fn test_initialization(fixture_engine_client: WebTransportEngine) {
    assert_eq!(fixture_engine_client.connection.id, MOCK_CONN_ID);
    assert!(fixture_engine_client.connection.is_client);
    assert!(fixture_engine_client.pending_user_actions.is_empty());
}

#[rstest]
fn test_initialize_h3_transport_success(mut fixture_engine_client: WebTransportEngine) {
    let res = fixture_engine_client.initialize_h3_transport(2, 6, 10);

    let effects = match res {
        Ok(e) => e,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "", "H3 init failed");
            return;
        }
    };

    assert_eq!(effects.len(), 6);

    let sent_streams: Vec<_> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendQuicData { stream_id, .. } => Some(*stream_id),
            _ => None,
        })
        .collect();

    assert!(sent_streams.contains(&2));
    assert!(sent_streams.contains(&6));
    assert!(sent_streams.contains(&10));
}

#[rstest]
fn test_internal_bind_session(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::InternalBindH3Session {
        request_id: MOCK_REQUEST_ID,
        stream_id: 0,
    };

    let _unused = fixture_engine_server.handle_event(event, 0.0);

    assert_eq!(
        fixture_engine_server.connection.pending_requests.get(&0),
        Some(&MOCK_REQUEST_ID)
    );
}

#[rstest]
fn test_internal_cleanup_events(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::TransportDatagramFrameReceived {
        data: Bytes::from_static(b"abc"),
    };
    let _unused = fixture_engine_server.handle_event(event, 0.0);

    let cleanup = ProtocolEvent::InternalCleanupEarlyEvents;
    let effects = fixture_engine_server.handle_event(cleanup, 15.0);

    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::ResetQuicStream { .. }))
    );
}

#[rstest]
fn test_replay_user_actions_on_handshake(mut fixture_engine_client: WebTransportEngine) {
    let create_event = ProtocolEvent::UserCreateSession {
        request_id: MOCK_REQUEST_ID,
        path: "/".to_owned(),
        headers: vec![],
    };
    let _unused = fixture_engine_client.handle_event(create_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_actions.len(), 1);

    let handshake_event = ProtocolEvent::TransportHandshakeCompleted;
    let _unused = fixture_engine_client.handle_event(handshake_event, 0.1);
    assert_eq!(
        fixture_engine_client.connection.state,
        ConnectionState::Connecting
    );

    let settings_event = ProtocolEvent::SettingsReceived {
        settings: std::collections::HashMap::new(),
    };
    let effects = fixture_engine_client.handle_event(settings_event, 0.2);

    assert_eq!(
        fixture_engine_client.connection.state,
        ConnectionState::Connected
    );
    assert!(fixture_engine_client.pending_user_actions.is_empty());

    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::CreateH3Session { .. }))
    );
}

#[rstest]
fn test_server_immediate_actions(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::UserCreateSession {
        request_id: MOCK_REQUEST_ID,
        path: "/".to_owned(),
        headers: vec![],
    };

    let _effects = fixture_engine_server.handle_event(event, 0.0);

    assert!(fixture_engine_server.pending_user_actions.is_empty());
}

#[rstest]
#[case::reset(ProtocolEvent::UserResetStream { request_id: 1, stream_id: 999, error_code: 0 })]
#[case::send(ProtocolEvent::UserSendStreamData { request_id: 1, stream_id: 999, data: Bytes::new(), end_stream: false })]
#[case::stop(ProtocolEvent::UserStopStream { request_id: 1, stream_id: 999, error_code: 0 })]
#[case::read(ProtocolEvent::UserStreamRead { request_id: 1, stream_id: 999, max_bytes: 10 })]
fn test_user_stream_actions_not_found(
    mut fixture_engine_server: WebTransportEngine,
    #[case] event: ProtocolEvent,
) {
    let effects = fixture_engine_server.handle_event(event, 0.0);

    let has_fail = effects.iter().any(|e| {
        matches!(
            e,
            Effect::NotifyRequestFailed {
                error_code: Some(ec),
                ..
            } if *ec == ERR_LIB_STREAM_STATE_ERROR
        )
    });
    assert!(has_fail, "Expected NotifyRequestFailed for missing stream");
}

#[rstest]
fn test_user_stream_operations_success(mut fixture_engine_server: WebTransportEngine) {
    fixture_engine_server.connection.state = ConnectionState::Connected;

    let headers = vec![
        (
            Bytes::from_static(b":method"),
            Bytes::from_static(b"CONNECT"),
        ),
        (
            Bytes::from_static(b":protocol"),
            Bytes::from_static(b"webtransport"),
        ),
        (Bytes::from_static(b":scheme"), Bytes::from_static(b"https")),
        (
            Bytes::from_static(b":authority"),
            Bytes::from_static(b"localhost"),
        ),
        (Bytes::from_static(b":path"), Bytes::from_static(b"/")),
    ];

    let header_event = ProtocolEvent::HeadersReceived {
        stream_id: 0,
        headers,
        stream_ended: false,
    };
    fixture_engine_server.handle_event(header_event, 0.0);

    let stream_ids = vec![4, 5, 6, 7];
    for stream_id in &stream_ids {
        let bind_event = ProtocolEvent::InternalBindQuicStream {
            request_id: MOCK_REQUEST_ID,
            stream_id: *stream_id,
            session_id: 0,
            is_unidirectional: false,
        };
        fixture_engine_server.handle_event(bind_event, 0.0);
    }

    let ops = vec![
        ProtocolEvent::UserResetStream {
            request_id: 1,
            stream_id: 4,
            error_code: 100,
        },
        ProtocolEvent::UserStopStream {
            request_id: 2,
            stream_id: 5,
            error_code: 200,
        },
        ProtocolEvent::UserStreamRead {
            request_id: 3,
            stream_id: 6,
            max_bytes: 1024,
        },
        ProtocolEvent::UserSendStreamData {
            request_id: 4,
            stream_id: 7,
            data: Bytes::from_static(b"ok"),
            end_stream: false,
        },
    ];

    for event in ops {
        let effects = fixture_engine_server.handle_event(event, 0.0);

        let stream_error = effects.iter().any(|e| {
            matches!(
                e,
                Effect::NotifyRequestFailed {
                    error_code: Some(ec),
                    ..
                } if *ec == ERR_LIB_STREAM_STATE_ERROR
            )
        });

        assert!(
            !stream_error,
            "Stream operation failed with StreamStateError. Effects: {effects:?}"
        );
        assert!(!effects.is_empty(), "Operation should produce effects");
    }
}
