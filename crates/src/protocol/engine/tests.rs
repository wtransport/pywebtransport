//! Unit tests for the `crate::protocol::engine` module.

use bytes::{BufMut, Bytes, BytesMut};
use rstest::{fixture, rstest};

use super::*;
use crate::common::constants::{
    ERR_H3_GENERAL_PROTOCOL_ERROR, ERR_H3_NO_ERROR, ERR_H3_REQUEST_REJECTED,
    ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR,
    ERR_WT_BUFFERED_STREAM_REJECTED, ERR_WT_SESSION_GONE, H3_FRAME_TYPE_SETTINGS,
    H3_STREAM_TYPE_CONTROL, SETTINGS_WT_ENABLED, WT_CAPSULE_TYPE_CLOSE_SESSION,
};
use crate::common::types::{ErrorSource, EventType};
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};
use crate::protocol::h3::H3Settings;
use crate::protocol::utils;

fn accept_session(engine: &mut WebTransportEngine) {
    let mut client_engine = create_test_engine(true);
    let request_effects =
        match client_engine.encode_connect_request(0, "localhost".into(), "/".into(), &vec![]) {
            Ok(effects) => effects,
            Err(e) => {
                assert_eq!(format!("{e:?}"), "");
                unreachable!()
            }
        };
    let mut request = BytesMut::new();
    for effect in request_effects {
        if let Effect::SendQuicData { data, .. } = effect {
            request.extend_from_slice(&data);
        }
    }
    let request = request.freeze();

    engine.handle_event(ProtocolEvent::TransportHandshakeCompleted, 0.0);
    engine.handle_event(ProtocolEvent::TransportStreamOpened { stream_id: 0 }, 0.0);
    for data in [request.slice(..1), request.slice(1..)] {
        engine.handle_event(
            ProtocolEvent::TransportStreamDataReceived {
                stream_id: 0,
                data,
                end_stream: false,
            },
            0.0,
        );
    }
    let effects = engine.handle_event(
        ProtocolEvent::UserAcceptSession {
            request_id: 1,
            session_id: 0,
            headers: vec![],
            wt_protocol: None,
        },
        0.0,
    );

    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitSessionEvent {
            event_type: EventType::SessionReady,
            ..
        }
    )));
}

fn bind_local_streams(engine: &mut WebTransportEngine) {
    let control_stream_id = if engine.connection.is_client() { 2 } else { 3 };
    let effects = engine.handle_event(
        ProtocolEvent::InternalBindH3LocalStreams {
            control_stream_id,
            encoder_stream_id: control_stream_id + 4,
            decoder_stream_id: control_stream_id + 8,
        },
        0.0,
    );

    assert_eq!(effects.len(), 3);
}

fn create_test_engine(is_client: bool) -> WebTransportEngine {
    let parameters = EngineParameters {
        early_event_ttl: 5.0,
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        max_capsule_size: 65536,
        max_connection_pending_events: 1000,
        max_field_section_size: 65536,
        max_pending_capsules: 20,
        max_pending_datagrams: 100,
        max_pending_streams: 10,
        max_session_pending_events: 100,
        max_sessions: 10,
        max_write_buffer_size: 1024 * 1024,
        session_receive_window: 4 * 1024 * 1024,
    };

    WebTransportEngine::new(42, None, is_client, parameters)
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
fn test_buffer_create_session_optimistic_when_connecting(
    mut fixture_engine_client: WebTransportEngine,
) {
    let event = ProtocolEvent::UserCreateSessionOptimistic {
        request_id: 100,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::CreateH3ConnectStream { .. }))
    );
    assert_eq!(fixture_engine_client.pending_user_events.len(), 1);
}

#[rstest]
fn test_buffer_create_stream_when_connecting(mut fixture_engine_client: WebTransportEngine) {
    let event = ProtocolEvent::UserCreateStream {
        request_id: 100,
        session_id: 0,
        is_unidirectional: true,
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::CreateQuicStream { .. }))
    );
    assert_eq!(fixture_engine_client.pending_user_events.len(), 1);
}

#[rstest]
fn test_buffer_user_events_when_connecting(mut fixture_engine_client: WebTransportEngine) {
    let event = ProtocolEvent::UserCreateSession {
        request_id: 100,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::CreateH3ConnectStream { .. }))
    );

    assert_eq!(fixture_engine_client.pending_user_events.len(), 1);
}

#[rstest]
fn test_buffer_user_events_when_idle(mut fixture_engine_client: WebTransportEngine) {
    let event = ProtocolEvent::UserCreateSession {
        request_id: 100,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert_eq!(fixture_engine_client.pending_user_events.len(), 1);
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::CreateH3ConnectStream { .. }))
    );
}

#[rstest]
fn test_client_immediate_actions_when_connected(mut fixture_engine_client: WebTransportEngine) {
    fixture_engine_client.handle_event(ProtocolEvent::TransportHandshakeCompleted, 0.0);
    fixture_engine_client.handle_event(
        ProtocolEvent::H3SettingsReceived {
            settings: Default::default(),
        },
        0.0,
    );

    let session_event = ProtocolEvent::UserCreateSession {
        request_id: 100,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };
    let effects = fixture_engine_client.handle_event(session_event, 0.0);
    assert!(fixture_engine_client.pending_user_events.is_empty());
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::CreateH3ConnectStream { .. }))
    );

    let stream_event = ProtocolEvent::UserCreateStream {
        request_id: 101,
        session_id: 0,
        is_unidirectional: true,
    };
    let effects_stream = fixture_engine_client.handle_event(stream_event, 0.0);

    assert!(fixture_engine_client.pending_user_events.is_empty());
    assert!(!effects_stream.is_empty());
}

#[rstest]
fn test_client_receives_settings_via_raw_data(mut fixture_engine_client: WebTransportEngine) {
    bind_local_streams(&mut fixture_engine_client);

    let mut payload = BytesMut::new();
    if let Err(e) = utils::write_varint(&mut payload, SETTINGS_WT_ENABLED) {
        assert_eq!(format!("{e:?}"), "");
    }
    if let Err(e) = utils::write_varint(&mut payload, 1) {
        assert_eq!(format!("{e:?}"), "");
    }
    let mut frame = BytesMut::new();
    if let Err(e) = utils::write_varint(&mut frame, H3_FRAME_TYPE_SETTINGS) {
        assert_eq!(format!("{e:?}"), "");
    }
    if let Err(e) = utils::write_varint(&mut frame, payload.len() as u64) {
        assert_eq!(format!("{e:?}"), "");
    }
    frame.extend_from_slice(&payload);

    let mut stream_type = BytesMut::new();
    if let Err(e) = utils::write_varint(&mut stream_type, H3_STREAM_TYPE_CONTROL) {
        assert_eq!(format!("{e:?}"), "");
    }
    fixture_engine_client.handle_event(
        ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: stream_type.freeze(),
            end_stream: false,
        },
        0.0,
    );
    let effects = fixture_engine_client.handle_event(
        ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: frame.freeze(),
            end_stream: false,
        },
        0.0,
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 3,
            max_size: u64::MAX,
        }]
    ));

    let handshake_effects =
        fixture_engine_client.handle_event(ProtocolEvent::TransportHandshakeCompleted, 1.0);

    assert!(fixture_engine_client.connection.is_connected());
    assert!(handshake_effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionEstablished,
            ..
        }
    )));
}

#[rstest]
fn test_client_settings_before_handshake_replays_pending_user_events(
    mut fixture_engine_client: WebTransportEngine,
) {
    let create_stream_event = ProtocolEvent::UserCreateStream {
        request_id: 1,
        session_id: 0,
        is_unidirectional: true,
    };
    fixture_engine_client.handle_event(create_stream_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_events.len(), 1);

    fixture_engine_client.handle_event(
        ProtocolEvent::H3SettingsReceived {
            settings: Default::default(),
        },
        0.0,
    );
    assert_eq!(fixture_engine_client.pending_user_events.len(), 1);

    let effects =
        fixture_engine_client.handle_event(ProtocolEvent::TransportHandshakeCompleted, 1.0);

    assert!(fixture_engine_client.pending_user_events.is_empty());
    assert!(!effects.is_empty());
}

#[rstest]
fn test_connection_close_event_fails_pending_user_events(
    mut fixture_engine_client: WebTransportEngine,
) {
    let create_stream_event = ProtocolEvent::UserCreateStream {
        request_id: 1,
        session_id: 0,
        is_unidirectional: true,
    };
    fixture_engine_client.handle_event(create_stream_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_events.len(), 1);

    let create_session_event = ProtocolEvent::UserCreateSession {
        request_id: 2,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };
    fixture_engine_client.handle_event(create_session_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_events.len(), 2);

    let create_optimistic_event = ProtocolEvent::UserCreateSessionOptimistic {
        request_id: 3,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };
    fixture_engine_client.handle_event(create_optimistic_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_events.len(), 3);

    let close_event = ProtocolEvent::UserCloseConnection {
        request_id: 4,
        error_code: 0,
        reason: None,
    };

    let effects = fixture_engine_client.handle_event(close_event, 1.0);

    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::CloseQuicConnection { .. }))
    );

    let failures = effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                Effect::NotifyRequestFailed {
                    source: ErrorSource::Connection,
                    ..
                }
            )
        })
        .count();
    assert_eq!(failures, 3);
    assert!(fixture_engine_client.pending_user_events.is_empty());
}

#[rstest]
fn test_encode_capsule() {
    let data = Bytes::from_static(b"cap");
    let result = WebTransportEngine::encode_capsule(0, 0x1, data, false);

    let effects = match result {
        Ok(effects) => effects,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            return;
        }
    };

    assert_eq!(effects.len(), 3);
    assert!(
        effects
            .iter()
            .all(|effect| matches!(effect, Effect::SendQuicData { .. }))
    );
}

#[rstest]
fn test_encode_connect_request(mut fixture_engine_client: WebTransportEngine) {
    let headers = vec![(
        Bytes::from_static(b"user-agent"),
        Bytes::from_static(b"rust-test"),
    )];
    let result = fixture_engine_client.encode_connect_request(
        0,
        "localhost".into(),
        "/test".into(),
        &headers,
    );

    let effects = match result {
        Ok(effects) => effects,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            return;
        }
    };

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData { stream_id: 0, .. },
            Effect::SendQuicData {
                stream_id: 0,
                end_stream: false,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_encode_datagram() {
    let data = Bytes::from_static(b"payload");
    let result = WebTransportEngine::encode_datagram(0, data.clone());

    let effects = match result {
        Ok(effects) => effects,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            return;
        }
    };

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicDatagram { header: _, payload }] if payload == &data
    ));
}

#[rstest]
fn test_encode_goaway(mut fixture_engine_server: WebTransportEngine) {
    bind_local_streams(&mut fixture_engine_server);

    let effects = fixture_engine_server.encode_goaway(0);

    assert!(!effects.is_empty());
    assert!(matches!(effects.as_slice(), [Effect::SendQuicData { .. }]));
}

#[rstest]
fn test_encode_goaway_identifier_exceeds_varint_limit(
    mut fixture_engine_server: WebTransportEngine,
) {
    bind_local_streams(&mut fixture_engine_server);

    let effects = fixture_engine_server.encode_goaway(u64::MAX);

    assert!(effects.is_empty());
}

#[rstest]
fn test_encode_goaway_no_control_stream(mut fixture_engine_server: WebTransportEngine) {
    let effects = fixture_engine_server.encode_goaway(0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_encode_headers(mut fixture_engine_server: WebTransportEngine) {
    bind_local_streams(&mut fixture_engine_server);

    let headers = vec![];
    let result = fixture_engine_server.encode_headers(100, &headers, true);

    let effects = match result {
        Ok(effects) => effects,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            return;
        }
    };

    assert!(!effects.is_empty());
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::SendQuicData { .. }))
    );
}

#[rstest]
fn test_encode_stream_header() {
    let effects = WebTransportEngine::encode_stream_header(15, 0, true);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            stream_id: 15,
            data,
            end_stream: false,
        }] if data.as_ref() == [0x40, 0x54, 0x00]
    ));
}

#[rstest]
fn test_fail_pending_user_events_on_termination(mut fixture_engine_client: WebTransportEngine) {
    let create_event = ProtocolEvent::UserCreateSession {
        request_id: 100,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };
    fixture_engine_client.handle_event(create_event, 0.0);

    let term_event = ProtocolEvent::TransportConnectionTerminated {
        error_code: 0,
        reason: "Stop".into(),
    };
    let effects = fixture_engine_client.handle_event(term_event, 1.0);

    assert!(fixture_engine_client.pending_user_events.is_empty());

    let has_fail = effects.iter().any(|effect| {
        matches!(
            effect,
            Effect::NotifyRequestFailed {
                source: ErrorSource::Connection,
                error_code: Some(error_code),
                ..
            } if *error_code == ERR_LIB_CONNECTION_STATE_ERROR
        )
    });
    assert!(has_fail);
}

#[rstest]
fn test_handle_event_server_handshake(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::TransportHandshakeCompleted;
    let effects = fixture_engine_server.handle_event(event, 1.0);

    assert!(fixture_engine_server.connection.is_connected());
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionEstablished,
            ..
        }
    )));
}

#[rstest]
fn test_handle_handshake_unexpected_state(mut fixture_engine_server: WebTransportEngine) {
    fixture_engine_server.handle_event(
        ProtocolEvent::TransportConnectionTerminated {
            error_code: 0,
            reason: "".into(),
        },
        0.0,
    );

    let event = ProtocolEvent::TransportHandshakeCompleted;
    let effects = fixture_engine_server.handle_event(event, 1.0);

    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::EmitConnectionEvent { .. }))
    );
    assert!(!fixture_engine_server.connection.is_connected());
}

#[rstest]
fn test_handle_transport_capsule_close_session_stops_connect_stream(
    mut fixture_engine_server: WebTransportEngine,
) {
    accept_session(&mut fixture_engine_server);
    let mut capsule_value = BytesMut::new();
    capsule_value.put_u32(0);
    let capsule_effects = match WebTransportEngine::encode_capsule(
        0,
        WT_CAPSULE_TYPE_CLOSE_SESSION,
        capsule_value.freeze(),
        false,
    ) {
        Ok(effects) => effects,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };
    let mut data = BytesMut::new();
    for effect in capsule_effects {
        if let Effect::SendQuicData { data: chunk, .. } = effect {
            data.extend_from_slice(&chunk);
        }
    }

    let effects = fixture_engine_server.handle_event(
        ProtocolEvent::TransportStreamDataReceived {
            stream_id: 0,
            data: data.freeze(),
            end_stream: false,
        },
        1.0,
    );

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ReceiveQuicData { stream_id: 0, .. },
            Effect::SendQuicData {
                stream_id: 0,
                end_stream: true,
                ..
            },
            Effect::EmitSessionEvent {
                event_type: EventType::SessionClosed,
                ..
            },
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_WT_SESSION_GONE,
            }
        ]
    ));
}

#[rstest]
#[case::capsule_received(ProtocolEvent::H3CapsuleReceived {
    stream_id: 0,
    capsule_type: 0,
    capsule_value: Bytes::new(),
})]
#[case::connect_stream_closed(ProtocolEvent::H3ConnectStreamClosed { stream_id: 0 })]
#[case::datagram_received(ProtocolEvent::H3DatagramReceived {
    session_id: 0,
    data: Bytes::from_static(b"datagram"),
})]
#[case::goaway_received(ProtocolEvent::H3GoawayReceived)]
#[case::quic_parameters_received(ProtocolEvent::TransportQuicParametersReceived {
    peer_max_datagram_frame_size: 1350,
})]
#[case::stop_sending_received(ProtocolEvent::TransportStopSendingReceived {
    stream_id: 0,
    error_code: 0,
})]
#[case::stream_data_acknowledged(ProtocolEvent::TransportStreamDataAcknowledged { stream_id: 4 })]
#[case::stream_reset_received(ProtocolEvent::TransportResetStreamReceived {
    stream_id: 0,
    error_code: 0,
})]
#[case::webtransport_stream_opened(ProtocolEvent::H3WebTransportStreamOpened {
    session_id: 0,
    stream_id: 4,
})]
fn test_handle_transport_events_without_sessions(
    mut fixture_engine_server: WebTransportEngine,
    #[case] event: ProtocolEvent,
) {
    let effects = fixture_engine_server.handle_event(event, 0.0);

    assert!(effects.is_empty());
}

#[rstest]
fn test_handle_transport_headers_rejected_when_not_connected(
    mut fixture_engine_server: WebTransportEngine,
) {
    let event = ProtocolEvent::H3HeadersReceived {
        stream_id: 0,
        headers: vec![],
    };

    let effects = fixture_engine_server.handle_event(event, 0.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendH3Headers {
                stream_id: 0,
                end_stream: true,
                ..
            },
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_REJECTED,
            }
        ]
    ));
}

#[rstest]
#[case::reset_stream_received(ProtocolEvent::TransportResetStreamReceived {
    stream_id: 0,
    error_code: ERR_H3_REQUEST_REJECTED,
})]
#[case::stop_sending_received(ProtocolEvent::TransportStopSendingReceived {
    stream_id: 0,
    error_code: ERR_H3_REQUEST_REJECTED,
})]
fn test_handle_transport_session_stream_abort_aborts_session(
    mut fixture_engine_server: WebTransportEngine,
    #[case] event: ProtocolEvent,
) {
    accept_session(&mut fixture_engine_server);

    let effects = fixture_engine_server.handle_event(event, 1.0);

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
fn test_handle_transport_stream_abort_delegates_to_stream(
    mut fixture_engine_server: WebTransportEngine,
) {
    accept_session(&mut fixture_engine_server);
    fixture_engine_server.handle_event(
        ProtocolEvent::InternalBindQuicStream {
            request_id: 2,
            stream_id: 1,
            session_id: 0,
            is_unidirectional: false,
        },
        1.0,
    );

    let stop_effects = fixture_engine_server.handle_event(
        ProtocolEvent::TransportStopSendingReceived {
            stream_id: 1,
            error_code: 0,
        },
        1.0,
    );
    let reset_effects = fixture_engine_server.handle_event(
        ProtocolEvent::TransportResetStreamReceived {
            stream_id: 1,
            error_code: 0,
        },
        1.0,
    );

    assert!(matches!(
        stop_effects.as_slice(),
        [
            Effect::EmitStreamEvent {
                stream_id: 1,
                event_type: EventType::StreamStopSendingReceived,
                ..
            },
            Effect::ResetQuicStream { stream_id: 1, .. }
        ]
    ));
    assert!(matches!(
        reset_effects.as_slice(),
        [
            Effect::EmitStreamEvent {
                stream_id: 1,
                event_type: EventType::StreamResetReceived,
                ..
            },
            Effect::EmitStreamEvent {
                stream_id: 1,
                event_type: EventType::StreamClosed,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_handle_transport_stream_data_acknowledged_closes_stream(
    mut fixture_engine_server: WebTransportEngine,
) {
    accept_session(&mut fixture_engine_server);
    fixture_engine_server.handle_event(
        ProtocolEvent::InternalBindQuicStream {
            request_id: 2,
            stream_id: 3,
            session_id: 0,
            is_unidirectional: true,
        },
        1.0,
    );
    fixture_engine_server.handle_event(
        ProtocolEvent::UserWriteStream {
            request_id: 3,
            stream_id: 3,
            data: Bytes::new(),
            end_stream: true,
        },
        1.0,
    );
    fixture_engine_server.handle_event(
        ProtocolEvent::InternalCompleteQuicData { stream_id: 3 },
        1.0,
    );

    let effects = fixture_engine_server.handle_event(
        ProtocolEvent::TransportStreamDataAcknowledged { stream_id: 3 },
        1.0,
    );

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
fn test_handle_transport_stream_opened_delivers_data(
    mut fixture_engine_server: WebTransportEngine,
) {
    accept_session(&mut fixture_engine_server);
    fixture_engine_server.handle_event(ProtocolEvent::TransportStreamOpened { stream_id: 4 }, 1.0);
    for byte in [0x40, 0x41] {
        fixture_engine_server.handle_event(
            ProtocolEvent::TransportStreamDataReceived {
                stream_id: 4,
                data: Bytes::from(vec![byte]),
                end_stream: false,
            },
            1.0,
        );
    }

    let opened_effects = fixture_engine_server.handle_event(
        ProtocolEvent::TransportStreamDataReceived {
            stream_id: 4,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        1.0,
    );
    let read_effects = fixture_engine_server.handle_event(
        ProtocolEvent::UserReadStream {
            request_id: 2,
            stream_id: 4,
            max_size: 1024,
        },
        1.0,
    );
    let data_effects = fixture_engine_server.handle_event(
        ProtocolEvent::TransportStreamDataReceived {
            stream_id: 4,
            data: Bytes::from_static(b"hello"),
            end_stream: false,
        },
        1.0,
    );

    assert!(matches!(
        opened_effects.as_slice(),
        [Effect::EmitStreamEvent {
            stream_id: 4,
            event_type: EventType::StreamOpened,
            ..
        }]
    ));
    assert!(matches!(
        read_effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 4,
            max_size: 1024,
        }]
    ));
    assert!(matches!(
        data_effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 2,
            result: RequestResult::ReadData(data),
        }] if data.as_ref() == b"hello"
    ));
}

#[rstest]
fn test_handle_user_passthrough_events(mut fixture_engine_server: WebTransportEngine) {
    let events = vec![
        ProtocolEvent::UserAcceptSession {
            request_id: 1,
            session_id: 0,
            headers: vec![],
            wt_protocol: None,
        },
        ProtocolEvent::UserCloseConnection {
            request_id: 16,
            error_code: 0,
            reason: None,
        },
        ProtocolEvent::UserCloseConnectionGracefully { request_id: 3 },
        ProtocolEvent::UserCloseSession {
            request_id: 2,
            session_id: 0,
            error_code: 0,
            reason: None,
        },
        ProtocolEvent::UserExportKeyingMaterial {
            request_id: 15,
            session_id: 0,
            label: "test".into(),
            context: Bytes::from_static(b"context"),
            length: 32,
        },
        ProtocolEvent::UserDiagnoseConnection { request_id: 4 },
        ProtocolEvent::UserDiagnoseSession {
            request_id: 5,
            session_id: 0,
        },
        ProtocolEvent::UserDiagnoseStream {
            request_id: 6,
            stream_id: 4,
        },
        ProtocolEvent::UserReadDatagram {
            request_id: 17,
            session_id: 0,
        },
        ProtocolEvent::UserReadStream {
            request_id: 14,
            stream_id: 4,
            max_size: 100,
        },
        ProtocolEvent::UserRejectSession {
            request_id: 9,
            session_id: 0,
            headers: vec![],
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
        ProtocolEvent::UserWriteStream {
            request_id: 12,
            stream_id: 4,
            data: Bytes::from_static(b"s"),
            end_stream: false,
        },
        ProtocolEvent::UserStopSending {
            request_id: 13,
            stream_id: 4,
            error_code: 0,
        },
    ];

    for event in events {
        let effects = fixture_engine_server.handle_event(event, 0.0);
        assert!(!effects.is_empty());
    }

    let diagnose_failure_event = ProtocolEvent::UserDiagnoseStream {
        request_id: 99,
        stream_id: 999,
    };
    let effects = fixture_engine_server.handle_event(diagnose_failure_event, 0.0);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::NotifyRequestFailed {
            source: ErrorSource::Stream,
            ..
        }
    )));
}

#[rstest]
fn test_initialization(fixture_engine_client: WebTransportEngine) {
    assert!(fixture_engine_client.connection.is_client());
    assert!(fixture_engine_client.pending_user_events.is_empty());
}

#[rstest]
fn test_internal_bind_connect_stream(mut fixture_engine_client: WebTransportEngine) {
    let event = ProtocolEvent::InternalBindH3ConnectStream {
        request_id: 100,
        stream_id: 0,
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
}

#[rstest]
fn test_internal_bind_local_streams(mut fixture_engine_client: WebTransportEngine) {
    let event = ProtocolEvent::InternalBindH3LocalStreams {
        control_stream_id: 2,
        encoder_stream_id: 6,
        decoder_stream_id: 10,
    };

    let effects = fixture_engine_client.handle_event(event, 0.0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData { stream_id: 2, .. },
            Effect::SendQuicData { stream_id: 6, .. },
            Effect::SendQuicData { stream_id: 10, .. },
        ]
    ));
}

#[rstest]
fn test_internal_bind_stream_session_not_found(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::InternalBindQuicStream {
        request_id: 100,
        stream_id: 4,
        session_id: 0,
        is_unidirectional: false,
    };

    let effects = fixture_engine_server.handle_event(event, 0.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 100,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_internal_cleanup_events(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::H3WebTransportStreamOpened {
        session_id: 0,
        stream_id: 4,
    };
    fixture_engine_server.handle_event(event, 0.0);

    let cleanup = ProtocolEvent::InternalCleanupEarlyEvents;
    let effects = fixture_engine_server.handle_event(cleanup, 15.0);

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
fn test_internal_cleanup_resources_prunes_closed_sessions(
    mut fixture_engine_server: WebTransportEngine,
) {
    let headers = vec![
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
    ];
    fixture_engine_server.handle_event(ProtocolEvent::TransportHandshakeCompleted, 0.0);
    fixture_engine_server.handle_event(
        ProtocolEvent::H3HeadersReceived {
            stream_id: 0,
            headers,
        },
        0.0,
    );
    fixture_engine_server.handle_event(
        ProtocolEvent::UserAcceptSession {
            request_id: 99,
            session_id: 0,
            headers: vec![],
            wt_protocol: None,
        },
        0.0,
    );
    fixture_engine_server.handle_event(
        ProtocolEvent::UserCloseSession {
            request_id: 100,
            session_id: 0,
            error_code: 0,
            reason: None,
        },
        0.0,
    );
    fixture_engine_server.handle_event(ProtocolEvent::H3ConnectStreamClosed { stream_id: 0 }, 0.0);
    let diagnose_session = |request_id| ProtocolEvent::UserDiagnoseSession {
        request_id,
        session_id: 0,
    };

    let before_effects = fixture_engine_server.handle_event(diagnose_session(101), 1.0);
    fixture_engine_server.handle_event(ProtocolEvent::InternalCleanupResources, 1.0);
    let after_effects = fixture_engine_server.handle_event(diagnose_session(102), 1.0);

    assert!(matches!(
        before_effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 101,
            ..
        }]
    ));
    assert!(matches!(
        after_effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 102,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_internal_close_connection(mut fixture_engine_client: WebTransportEngine) {
    fixture_engine_client.handle_event(
        ProtocolEvent::UserCreateSession {
            request_id: 100,
            authority: "localhost".into(),
            path: "/".into(),
            headers: vec![],
            wt_available_protocols: None,
        },
        0.0,
    );

    let effects = fixture_engine_client.handle_event(ProtocolEvent::InternalCloseConnection, 0.0);

    assert!(fixture_engine_client.pending_user_events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::CloseQuicConnection {
                error_code: ERR_H3_NO_ERROR,
                ..
            },
            Effect::NotifyRequestFailed {
                request_id: 100,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                ..
            }
        ]
    ));
}

#[rstest]
fn test_internal_complete_quic_data_finishes_write(mut fixture_engine_server: WebTransportEngine) {
    accept_session(&mut fixture_engine_server);
    fixture_engine_server.handle_event(
        ProtocolEvent::InternalBindQuicStream {
            request_id: 2,
            stream_id: 1,
            session_id: 0,
            is_unidirectional: false,
        },
        1.0,
    );
    let write_effects = fixture_engine_server.handle_event(
        ProtocolEvent::UserWriteStream {
            request_id: 3,
            stream_id: 1,
            data: Bytes::from_static(b"data"),
            end_stream: false,
        },
        1.0,
    );

    let effects = fixture_engine_server.handle_event(
        ProtocolEvent::InternalCompleteQuicData { stream_id: 1 },
        1.0,
    );

    assert!(matches!(
        write_effects.as_slice(),
        [Effect::SendQuicData {
            stream_id: 1,
            data,
            end_stream: false,
        }] if data.as_ref() == b"data"
    ));
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 3,
            result: RequestResult::None,
        }]
    ));
}

#[rstest]
fn test_internal_defer_session_released_on_max_streams(
    mut fixture_engine_client: WebTransportEngine,
) {
    fixture_engine_client.handle_event(ProtocolEvent::TransportHandshakeCompleted, 0.0);
    fixture_engine_client.handle_event(
        ProtocolEvent::H3SettingsReceived {
            settings: Default::default(),
        },
        0.0,
    );
    fixture_engine_client.handle_event(
        ProtocolEvent::UserCreateSession {
            request_id: 100,
            authority: "localhost".into(),
            path: "/".into(),
            headers: vec![],
            wt_available_protocols: None,
        },
        0.0,
    );

    let deferred_effects = fixture_engine_client.handle_event(
        ProtocolEvent::InternalDeferH3ConnectStream { request_id: 100 },
        0.0,
    );
    let released_effects = fixture_engine_client.handle_event(
        ProtocolEvent::TransportMaxStreamsReceived {
            is_unidirectional: false,
        },
        0.0,
    );

    assert!(deferred_effects.is_empty());
    assert!(matches!(
        released_effects.as_slice(),
        [Effect::CreateH3ConnectStream {
            request_id: 100,
            ..
        }]
    ));
}

#[rstest]
fn test_internal_defer_stream_session_not_found(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::InternalDeferQuicStream {
        request_id: 100,
        session_id: 0,
        is_unidirectional: false,
    };

    let effects = fixture_engine_server.handle_event(event, 0.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 100,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_internal_fail_local_streams(mut fixture_engine_client: WebTransportEngine) {
    let effects =
        fixture_engine_client.handle_event(ProtocolEvent::InternalFailH3LocalStreams, 0.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_GENERAL_PROTOCOL_ERROR,
            ..
        }]
    ));
}

#[rstest]
fn test_internal_fail_session(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::InternalFailH3ConnectStream {
        request_id: 100,
        error_code: None,
        reason: "fail".into(),
    };

    let effects = fixture_engine_server.handle_event(event, 0.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 100,
            reason,
            ..
        }] if reason == "fail"
    ));
}

#[rstest]
fn test_replay_user_events_on_handshake(mut fixture_engine_client: WebTransportEngine) {
    let create_event = ProtocolEvent::UserCreateSession {
        request_id: 100,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };
    fixture_engine_client.handle_event(create_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_events.len(), 1);

    let create_optimistic_event = ProtocolEvent::UserCreateSessionOptimistic {
        request_id: 101,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };
    fixture_engine_client.handle_event(create_optimistic_event, 0.0);
    assert_eq!(fixture_engine_client.pending_user_events.len(), 2);

    let handshake_event = ProtocolEvent::TransportHandshakeCompleted;
    fixture_engine_client.handle_event(handshake_event, 0.1);
    assert!(fixture_engine_client.connection.is_pre_connected());

    let settings_event = ProtocolEvent::H3SettingsReceived {
        settings: Default::default(),
    };
    let effects = fixture_engine_client.handle_event(settings_event, 0.2);

    assert!(fixture_engine_client.connection.is_connected());
    assert!(fixture_engine_client.pending_user_events.is_empty());

    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::CreateH3ConnectStream { .. }))
    );
}

#[rstest]
fn test_server_immediate_actions(mut fixture_engine_server: WebTransportEngine) {
    let event = ProtocolEvent::UserCreateSession {
        request_id: 100,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };

    fixture_engine_server.handle_event(event, 0.0);

    assert!(fixture_engine_server.pending_user_events.is_empty());

    let optimistic_event = ProtocolEvent::UserCreateSessionOptimistic {
        request_id: 101,
        authority: "localhost".into(),
        path: "/".into(),
        headers: vec![],
        wt_available_protocols: None,
    };

    fixture_engine_server.handle_event(optimistic_event, 0.0);
    assert!(fixture_engine_server.pending_user_events.is_empty());
}

#[rstest]
#[case::read(ProtocolEvent::UserReadStream { request_id: 1, stream_id: 999, max_size: 10 })]
#[case::write(ProtocolEvent::UserWriteStream { request_id: 1, stream_id: 999, data: Bytes::new(), end_stream: false })]
fn test_user_stream_actions_not_found(
    mut fixture_engine_server: WebTransportEngine,
    #[case] event: ProtocolEvent,
) {
    let effects = fixture_engine_server.handle_event(event, 0.0);

    let has_fail = effects.iter().any(|effect| {
        matches!(
            effect,
            Effect::NotifyRequestFailed {
                source: ErrorSource::Stream,
                error_code: Some(error_code),
                ..
            } if *error_code == ERR_LIB_STREAM_STATE_ERROR
        )
    });
    assert!(has_fail);
}

#[rstest]
fn test_user_stream_operations_success(mut fixture_engine_server: WebTransportEngine) {
    fixture_engine_server.handle_event(ProtocolEvent::TransportHandshakeCompleted, 0.0);

    let settings_event = ProtocolEvent::H3SettingsReceived {
        settings: H3Settings {
            wt_initial_max_data: Some(4 * 1024 * 1024),
            ..Default::default()
        },
    };
    fixture_engine_server.handle_event(settings_event, 0.0);

    let headers = vec![
        (
            Bytes::from_static(b":method"),
            Bytes::from_static(b"CONNECT"),
        ),
        (
            Bytes::from_static(b":protocol"),
            Bytes::from_static(b"webtransport-h3"),
        ),
        (Bytes::from_static(b":scheme"), Bytes::from_static(b"https")),
        (
            Bytes::from_static(b":authority"),
            Bytes::from_static(b"localhost"),
        ),
        (Bytes::from_static(b":path"), Bytes::from_static(b"/")),
    ];

    let header_event = ProtocolEvent::H3HeadersReceived {
        stream_id: 0,
        headers,
    };
    fixture_engine_server.handle_event(header_event, 0.0);

    let stream_ids = vec![4, 5, 6, 7];
    for stream_id in &stream_ids {
        let bind_event = ProtocolEvent::InternalBindQuicStream {
            request_id: 100,
            stream_id: *stream_id,
            session_id: 0,
            is_unidirectional: false,
        };
        fixture_engine_server.handle_event(bind_event, 0.0);
    }

    let operations = vec![
        ProtocolEvent::UserReadStream {
            request_id: 3,
            stream_id: 6,
            max_size: 1024,
        },
        ProtocolEvent::UserResetStream {
            request_id: 1,
            stream_id: 4,
            error_code: 100,
        },
        ProtocolEvent::UserWriteStream {
            request_id: 4,
            stream_id: 7,
            data: Bytes::from_static(b"ok"),
            end_stream: false,
        },
        ProtocolEvent::UserStopSending {
            request_id: 2,
            stream_id: 5,
            error_code: 200,
        },
    ];

    for event in operations {
        let effects = fixture_engine_server.handle_event(event, 0.0);

        let stream_error = effects.iter().any(|effect| {
            matches!(
                effect,
                Effect::NotifyRequestFailed {
                    source: ErrorSource::Stream,
                    error_code: Some(error_code),
                    ..
                } if *error_code == ERR_LIB_STREAM_STATE_ERROR
            )
        });

        assert!(!stream_error);
        assert!(!effects.is_empty());
    }
}

#[rstest]
#[case::fin(ProtocolEvent::UserWriteStream { request_id: 1, stream_id: 999, data: Bytes::new(), end_stream: true })]
#[case::reset(ProtocolEvent::UserResetStream { request_id: 1, stream_id: 999, error_code: 0 })]
#[case::stop(ProtocolEvent::UserStopSending { request_id: 1, stream_id: 999, error_code: 0 })]
fn test_user_stream_terminations_not_found(
    mut fixture_engine_server: WebTransportEngine,
    #[case] event: ProtocolEvent,
) {
    let effects = fixture_engine_server.handle_event(event, 0.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::None,
        }]
    ));
}
