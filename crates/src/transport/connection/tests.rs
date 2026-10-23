//! Unit tests for the `crate::transport::connection` module.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use quinn_proto::crypto::rustls::{
    QuicClientConfig as QuicCryptoClientConfig, QuicServerConfig as QuicCryptoServerConfig,
};
use quinn_proto::{
    ClientConfig as QuicClientConfig, Connection as QuicConnection,
    ConnectionHandle as QuicConnectionHandle, DatagramEvent as QuicDatagramEvent, Dir as QuicDir,
    Endpoint as QuicEndpoint, EndpointConfig as QuicEndpointConfig,
    ServerConfig as QuicServerConfig, StreamId as QuicStreamId, Transmit as QuicTransmit,
    TransportConfig as QuicTransportConfig, VarInt as QuicVarInt,
};
use rcgen::generate_simple_self_signed;
use rstest::rstest;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::version::TLS13;
use rustls::{
    ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, ServerConfig,
    SignatureScheme,
};

use super::*;
use crate::common::constants::{
    ERR_H3_DATAGRAM_ERROR, ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_INTERNAL_ERROR,
    QUIC_MIN_UDP_PAYLOAD_SIZE_LIMIT,
};
use crate::common::types::{ErrorSource, EventType};
use crate::protocol::engine::{EngineParameters, WebTransportEngine};
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};

struct ConnectedPair {
    client: TransportConnection,
    client_endpoint: QuicEndpoint,
    client_handle: QuicConnectionHandle,
    now: Instant,
    server: TransportConnection,
    server_endpoint: QuicEndpoint,
    server_handle: QuicConnectionHandle,
}

impl ConnectedPair {
    fn drive(&mut self) {
        let mut workspace = Vec::new();

        loop {
            let mut is_progressed = false;

            while let Some(transmit) = self.client.poll_transmit(&mut workspace, self.now) {
                is_progressed = true;
                deliver(
                    &workspace,
                    &transmit,
                    client_address(),
                    &mut self.server_endpoint,
                    &mut self.server,
                    self.now,
                );
                workspace.clear();
            }

            while let Some(transmit) = self.server.poll_transmit(&mut workspace, self.now) {
                is_progressed = true;
                deliver(
                    &workspace,
                    &transmit,
                    server_address(),
                    &mut self.client_endpoint,
                    &mut self.client,
                    self.now,
                );
                workspace.clear();
            }

            for (connection, endpoint, handle) in [
                (
                    &mut self.client,
                    &mut self.client_endpoint,
                    self.client_handle,
                ),
                (
                    &mut self.server,
                    &mut self.server_endpoint,
                    self.server_handle,
                ),
            ] {
                while let Some(event) = connection.poll_endpoint_events() {
                    if let Some(event) = endpoint.handle_event(handle, event) {
                        connection.handle_quic_connection_event(event, 0.0, self.now);
                    }
                }
            }

            if is_progressed {
                continue;
            }

            let deadline = self.now + Duration::from_millis(100);
            let Some(timeout) = [self.client.poll_timeout(), self.server.poll_timeout()]
                .into_iter()
                .flatten()
                .filter(|&timeout| timeout <= deadline)
                .min()
            else {
                break;
            };
            self.now = self.now.max(timeout);
            self.client.handle_timeout(0.0, self.now);
            self.server.handle_timeout(0.0, self.now);
        }
    }
}

#[derive(Debug)]
struct DummyVerifier;

impl ServerCertVerifier for DummyVerifier {
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
        ]
    }

    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }
}

fn client_address() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 50000)
}

fn create_client_config() -> QuicClientConfig {
    let provider = Arc::new(ring::default_provider());
    let Ok(builder) =
        ClientConfig::builder_with_provider(provider).with_protocol_versions(&[&TLS13])
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let root_store = RootCertStore::empty();
    let mut crypto = builder
        .with_root_certificates(root_store)
        .with_no_client_auth();
    crypto
        .dangerous()
        .set_certificate_verifier(Arc::new(DummyVerifier));
    let Ok(quic_crypto) = QuicCryptoClientConfig::try_from(crypto) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let mut client_config = QuicClientConfig::new(Arc::new(quic_crypto));
    client_config.transport_config(Arc::new(QuicTransportConfig::default()));

    client_config
}

fn create_connected_pair(server_transport_config: QuicTransportConfig) -> ConnectedPair {
    let now = Instant::now();
    let mut client_endpoint =
        QuicEndpoint::new(Arc::new(QuicEndpointConfig::default()), None, false, None);
    let mut server_endpoint = QuicEndpoint::new(
        Arc::new(QuicEndpointConfig::default()),
        Some(Arc::new(create_server_config(server_transport_config))),
        false,
        None,
    );

    let Ok((client_handle, quic)) =
        client_endpoint.connect(now, create_client_config(), server_address(), "localhost")
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let mut client = TransportConnection::new(quic, create_test_engine(true), None, None, now);

    let mut workspace = Vec::new();
    let Some(transmit) = client.poll_transmit(&mut workspace, now) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let mut response = Vec::new();
    let Some(QuicDatagramEvent::NewConnection(incoming)) = server_endpoint.handle(
        now,
        client_address(),
        None,
        None,
        BytesMut::from(workspace.get(..transmit.size).unwrap_or_default()),
        &mut response,
    ) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let Ok((server_handle, quic)) = server_endpoint.accept(incoming, now, &mut response, None)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let server = TransportConnection::new(quic, create_test_engine(false), None, None, now);

    let mut pair = ConnectedPair {
        client,
        client_endpoint,
        client_handle,
        now,
        server,
        server_endpoint,
        server_handle,
    };
    pair.drive();

    assert!(!pair.client.connection.is_handshaking());
    assert!(!pair.server.connection.is_handshaking());
    for connection in [&mut pair.client, &mut pair.server] {
        assert!(matches!(
            drain_effects(connection).as_slice(),
            [Effect::EmitConnectionEvent {
                event_type: EventType::ConnectionEstablished,
                ..
            }]
        ));
    }

    pair
}

fn create_dummy_quic_connection() -> QuicConnection {
    let mut endpoint =
        QuicEndpoint::new(Arc::new(QuicEndpointConfig::default()), None, false, None);

    let Ok((_, connection)) = endpoint.connect(
        Instant::now(),
        create_client_config(),
        server_address(),
        "localhost",
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    connection
}

fn create_server_config(transport_config: QuicTransportConfig) -> QuicServerConfig {
    let Ok(certified_key) = generate_simple_self_signed(vec!["localhost".to_owned()]) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let certificate = certified_key.cert.der().clone();
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified_key.signing_key.serialize_der(),
    ));
    let provider = Arc::new(ring::default_provider());
    let Ok(builder) =
        ServerConfig::builder_with_provider(provider).with_protocol_versions(&[&TLS13])
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(crypto) = builder
        .with_no_client_auth()
        .with_single_cert(vec![certificate], private_key)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(quic_crypto) = QuicCryptoServerConfig::try_from(crypto) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let mut server_config = QuicServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport_config(Arc::new(transport_config));

    server_config
}

fn create_test_connection() -> TransportConnection {
    let quic = create_dummy_quic_connection();
    let engine = create_test_engine(true);
    let now = Instant::now();

    TransportConnection::new(quic, engine, None, None, now)
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

fn deliver(
    workspace: &[u8],
    transmit: &QuicTransmit,
    source_address: SocketAddr,
    endpoint: &mut QuicEndpoint,
    connection: &mut TransportConnection,
    now: Instant,
) {
    let payload = workspace.get(..transmit.size).unwrap_or_default();
    let segment_size = transmit.segment_size.unwrap_or(transmit.size);

    for datagram in payload.chunks(segment_size) {
        let mut response = Vec::new();
        if let Some(QuicDatagramEvent::ConnectionEvent(_, event)) = endpoint.handle(
            now,
            source_address,
            None,
            None,
            BytesMut::from(datagram),
            &mut response,
        ) {
            connection.handle_quic_connection_event(event, 0.0, now);
        }
    }
}

fn drain_effects(connection: &mut TransportConnection) -> Vec<Effect> {
    let mut effects = Vec::new();

    while let Some(effect) = connection.poll_effects() {
        effects.push(effect);
    }

    effects
}

fn open_server_webtransport_stream(pair: &mut ConnectedPair, payload: &[u8]) -> QuicStreamId {
    let Some(quic_stream_id) = pair.server.connection.streams().open(QuicDir::Uni) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let mut data = vec![0x40, 0x54, 0x00];
    data.extend_from_slice(payload);
    let mut send_stream = pair.server.connection.send_stream(quic_stream_id);
    assert!(
        send_stream
            .write(&data)
            .is_ok_and(|written| written == data.len())
    );
    pair.drive();

    quic_stream_id
}

fn server_address() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443)
}

#[test]
fn test_flush_stream_blocked_resumes_on_writable() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let Some(quic_stream_id) = pair.client.connection.streams().open(QuicDir::Uni) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let mut payload = BytesMut::new();
    payload.put_slice(&[0x40, 0x54, 0x00]);
    payload.put_slice(&vec![0u8; 4 * 1024 * 1024]);
    let mut event_queue = VecDeque::new();

    pair.client.process_effects(
        vec![Effect::SendQuicData {
            stream_id: u64::from(quic_stream_id),
            data: payload.freeze(),
            end_stream: true,
        }],
        &mut event_queue,
        pair.now,
    );

    assert!(event_queue.is_empty());
    assert!(pair.client.send_buffers.contains_key(&quic_stream_id));

    let mut received_size = 0;
    let mut is_finished = false;
    while !is_finished {
        pair.drive();
        let mut stream = pair.server.connection.recv_stream(quic_stream_id);
        let Ok(mut chunks) = stream.read(true) else {
            assert_eq!("ok", "err");
            unreachable!()
        };
        loop {
            match chunks.next(usize::MAX) {
                Ok(Some(chunk)) => received_size += chunk.bytes.len(),
                Ok(None) => {
                    is_finished = true;
                    break;
                }
                Err(_) => break,
            }
        }
        let _transmit = chunks.finalize();
    }
    pair.drive();

    assert_eq!(received_size, 4 * 1024 * 1024);
    assert!(!pair.client.send_buffers.contains_key(&quic_stream_id));
}

#[test]
fn test_flush_stream_closed_stream_discards_buffer() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let Some(quic_stream_id) = pair.client.connection.streams().open(QuicDir::Uni) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let mut event_queue = VecDeque::new();
    pair.client.process_effects(
        vec![Effect::ResetQuicStream {
            stream_id: u64::from(quic_stream_id),
            error_code: 0,
        }],
        &mut event_queue,
        pair.now,
    );
    let mut buffer = SendBuffer::default();
    buffer.chunks.push_back(Bytes::from_static(b"data"));
    pair.client.send_buffers.insert(quic_stream_id, buffer);

    let event = pair.client.flush_stream(quic_stream_id);

    assert!(event.is_none());
    assert!(!pair.client.send_buffers.contains_key(&quic_stream_id));
}

#[test]
fn test_flush_stream_finished_buffer_removes_from_map() {
    let mut connection = create_test_connection();
    let quic_stream_id = QuicStreamId::from(QuicVarInt::from_u32(1));
    let buffer = SendBuffer {
        finished: true,
        ..Default::default()
    };
    connection.send_buffers.insert(quic_stream_id, buffer);

    let event = connection.flush_stream(quic_stream_id);

    assert!(matches!(
        event,
        Some(ProtocolEvent::InternalCompleteQuicData { stream_id: 1 })
    ));
    assert!(!connection.send_buffers.contains_key(&quic_stream_id));
}

#[test]
fn test_flush_stream_without_buffer_returns_none() {
    let mut connection = create_test_connection();
    let quic_stream_id = QuicStreamId::from(QuicVarInt::from_u32(1));

    let event = connection.flush_stream(quic_stream_id);

    assert!(event.is_none());
}

#[test]
fn test_handle_protocol_event_dispatches_cleanly() {
    let mut connection = create_test_connection();
    let event = ProtocolEvent::InternalCleanupResources;

    connection.handle_protocol_event(event, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_handle_timeout_idle_connection_reports_termination() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());

    pair.client
        .handle_timeout(0.0, pair.now + Duration::from_mins(2));

    assert!(
        drain_effects(&mut pair.client)
            .iter()
            .any(|effect| matches!(
                effect,
                Effect::EmitConnectionEvent {
                    event_type: EventType::ConnectionClosed,
                    error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                    ..
                }
            ))
    );
}

#[test]
fn test_handle_timeout_past_early_event_time_updates_next_ttl() {
    let mut connection = create_test_connection();
    let now = Instant::now();
    let interval = Duration::from_secs(2);
    connection.early_event_ttl = Some(interval);
    let Some(past_time) = now.checked_sub(Duration::from_secs(1)) else {
        assert_eq!("valid", "underflow");
        unreachable!()
    };
    connection.next_early_event_time = Some(past_time);

    connection.handle_timeout(0.0, now);

    assert!(
        connection
            .next_early_event_time
            .is_some_and(|scheduled_time| scheduled_time > now)
    );
}

#[test]
fn test_handle_timeout_past_resource_cleanup_time_updates_next_resource_cleanup_time() {
    let mut connection = create_test_connection();
    let now = Instant::now();
    let interval = Duration::from_secs(5);
    connection.resource_cleanup_interval = Some(interval);
    let Some(past_time) = now.checked_sub(Duration::from_secs(1)) else {
        assert_eq!("valid", "underflow");
        unreachable!()
    };
    connection.next_resource_cleanup_time = Some(past_time);

    connection.handle_timeout(0.0, now);

    assert!(
        connection
            .next_resource_cleanup_time
            .is_some_and(|scheduled_time| scheduled_time > now)
    );
}

#[rstest]
#[case(None, None)]
#[case(None, Some(Duration::from_secs(10)))]
#[case(Some(Duration::from_secs(5)), None)]
#[case(Some(Duration::from_secs(5)), Some(Duration::from_secs(10)))]
fn test_new_initialization_sets_correct_timer_states(
    #[case] resource_cleanup_interval: Option<Duration>,
    #[case] early_event_ttl: Option<Duration>,
) {
    let quic = create_dummy_quic_connection();
    let engine = create_test_engine(true);
    let now = Instant::now();

    let connection = TransportConnection::new(
        quic,
        engine,
        resource_cleanup_interval,
        early_event_ttl,
        now,
    );

    assert_eq!(
        connection.next_resource_cleanup_time,
        resource_cleanup_interval.map(|interval| now + interval)
    );
    assert_eq!(
        connection.next_early_event_time,
        early_event_ttl.map(|interval| now + interval)
    );
}

#[test]
fn test_poll_effects_returns_none_initially() {
    let mut connection = create_test_connection();

    assert!(connection.poll_effects().is_none());
}

#[test]
fn test_poll_endpoint_events_initially_returns_none() {
    let mut connection = create_test_connection();

    let endpoint_event = connection.poll_endpoint_events();

    assert!(endpoint_event.is_none());
}

#[test]
fn test_poll_timeout_connection_timer_precedes_resource_cleanup() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let Some(connection_timeout) = pair.client.connection.poll_timeout() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    pair.client.next_resource_cleanup_time = Some(connection_timeout + Duration::from_hours(1));

    let result = pair.client.poll_timeout();

    assert_eq!(result, Some(connection_timeout));
}

#[rstest]
#[case(None, Some(Duration::from_secs(10)), 10)]
#[case(Some(Duration::from_secs(10)), Some(Duration::from_secs(5)), 5)]
#[case(Some(Duration::from_secs(5)), None, 5)]
#[case(Some(Duration::from_secs(5)), Some(Duration::from_secs(10)), 5)]
fn test_poll_timeout_multiple_timers_returns_earliest_deadline(
    #[case] resource_cleanup_interval: Option<Duration>,
    #[case] early_event_ttl: Option<Duration>,
    #[case] expected_offset: u64,
) {
    let quic = create_dummy_quic_connection();
    let engine = create_test_engine(true);
    let now = Instant::now();
    let mut connection = TransportConnection::new(
        quic,
        engine,
        resource_cleanup_interval,
        early_event_ttl,
        now,
    );

    let result = connection.poll_timeout();

    assert!(result.is_some_and(|scheduled_time| {
        scheduled_time == now + Duration::from_secs(expected_offset)
    }));
}

#[test]
fn test_poll_transmit_returns_padded_initial_packet() {
    let mut connection = create_test_connection();
    let mut workspace = Vec::new();

    let transmit = connection.poll_transmit(&mut workspace, Instant::now());

    assert!(transmit.is_some());
    assert!(
        u64::try_from(workspace.len())
            .is_ok_and(|length| length >= QUIC_MIN_UDP_PAYLOAD_SIZE_LIMIT)
    );
}

#[test]
fn test_process_effects_close_quic_connection_already_closed_skipped() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![
        Effect::CloseQuicConnection {
            error_code: 0,
            reason: None,
        },
        Effect::CloseQuicConnection {
            error_code: 1,
            reason: None,
        },
    ];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::TransportConnectionTerminated { error_code: 0, .. })
    ));
    assert!(event_queue.is_empty());
}

#[test]
fn test_process_effects_close_quic_connection_reports_termination() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CloseQuicConnection {
        error_code: 0,
        reason: None,
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(connection.pending_effects.is_empty());
    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::TransportConnectionTerminated {
            error_code: 0,
            reason,
        }) if reason.is_empty()
    ));
}

#[rstest]
#[case::borrowed(Cow::Borrowed("shutdown"))]
#[case::owned(Cow::Owned("shutdown".to_owned()))]
fn test_process_effects_close_quic_connection_with_reason_reports_termination(
    #[case] reason: Cow<'static, str>,
) {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CloseQuicConnection {
        error_code: 0,
        reason: Some(reason),
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(connection.pending_effects.is_empty());
    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::TransportConnectionTerminated {
            error_code: 0,
            reason,
        }) if reason == "shutdown"
    ));
}

#[test]
fn test_process_effects_create_h3_connect_stream_encode_failure_fails_request() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CreateH3ConnectStream {
        request_id: 1,
        authority: "localhost".to_owned(),
        path: "/".to_owned(),
        headers: vec![(
            Bytes::from_static(b"x-large"),
            Bytes::from(vec![b'a'; 70000]),
        )],
    }];

    pair.client
        .process_effects(effects, &mut event_queue, pair.now);

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalFailH3ConnectStream {
            request_id: 1,
            error_code: None,
            ..
        })
    ));
    assert!(event_queue.is_empty());
}

#[test]
fn test_process_effects_create_h3_connect_stream_opens_stream() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CreateH3ConnectStream {
        request_id: 1,
        authority: "localhost".to_owned(),
        path: "/".to_owned(),
        headers: vec![],
    }];

    pair.client
        .process_effects(effects, &mut event_queue, pair.now);

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalCompleteQuicData { stream_id: 0 })
    ));
    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalCompleteQuicData { stream_id: 0 })
    ));
    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalBindH3ConnectStream {
            request_id: 1,
            stream_id: 0,
        })
    ));
    assert!(event_queue.is_empty());
}

#[test]
fn test_process_effects_create_h3_local_streams_unconnected_fails() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CreateH3LocalStreams];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalFailH3LocalStreams)
    ));
    assert!(event_queue.is_empty());
}

#[test]
fn test_process_effects_create_h3_session_unconnected_defers_request() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CreateH3ConnectStream {
        request_id: 1,
        authority: "localhost".to_owned(),
        path: "/".to_owned(),
        headers: vec![],
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalDeferH3ConnectStream { request_id: 1 })
    ));
    assert!(event_queue.is_empty());
}

#[test]
fn test_process_effects_create_quic_stream_bidirectional_unconnected_defers_request() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CreateQuicStream {
        request_id: 1,
        session_id: 2,
        is_unidirectional: false,
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalDeferQuicStream {
            request_id: 1,
            session_id: 2,
            is_unidirectional: false,
        })
    ));
    assert!(event_queue.is_empty());
}

#[rstest]
#[case::bidirectional(false, 0)]
#[case::unidirectional(true, 14)]
fn test_process_effects_create_quic_stream_opens_stream(
    #[case] is_unidirectional: bool,
    #[case] expected_stream_id: u64,
) {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CreateQuicStream {
        request_id: 1,
        session_id: 0,
        is_unidirectional,
    }];

    pair.client
        .process_effects(effects, &mut event_queue, pair.now);

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalCompleteQuicData { stream_id }) if stream_id == expected_stream_id
    ));
    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalBindQuicStream {
            request_id: 1,
            stream_id,
            session_id: 0,
            is_unidirectional: bound_unidirectional,
        }) if stream_id == expected_stream_id && bound_unidirectional == is_unidirectional
    ));

    assert!(pair.client.send_buffers.is_empty());
}

#[test]
fn test_process_effects_create_quic_stream_unidirectional_unconnected_defers_request() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::CreateQuicStream {
        request_id: 1,
        session_id: 2,
        is_unidirectional: true,
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::InternalDeferQuicStream {
            request_id: 1,
            session_id: 2,
            is_unidirectional: true,
        })
    ));
    assert!(event_queue.is_empty());
}

#[test]
fn test_process_effects_export_tls_keying_material_connected_succeeds() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    drain_effects(&mut pair.client);
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::ExportTlsKeyingMaterial {
        request_id: 1,
        label: "label".to_owned(),
        context: Bytes::from_static(b"context"),
        length: 32,
    }];

    pair.client
        .process_effects(effects, &mut event_queue, pair.now);

    assert!(matches!(
        pair.client.pending_effects.pop_front(),
        Some(Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::KeyingMaterial(material),
        }) if material.len() == 32
    ));
}

#[test]
fn test_process_effects_export_tls_keying_material_unconnected_fails() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::ExportTlsKeyingMaterial {
        request_id: 1,
        label: "label".to_owned(),
        context: Bytes::from_static(b"context"),
        length: 32,
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(matches!(
        connection.pending_effects.pop_front(),
        Some(Effect::NotifyRequestFailed {
            request_id: 1,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_INTERNAL_ERROR),
            ..
        })
    ));
}

#[rstest]
#[case::receive_quic_data(Effect::ReceiveQuicData {
    stream_id: u64::MAX,
    max_size: 1,
})]
#[case::reset_quic_stream(Effect::ResetQuicStream {
    stream_id: u64::MAX,
    error_code: 0,
})]
#[case::send_quic_data(Effect::SendQuicData {
    stream_id: u64::MAX,
    data: Bytes::from_static(b"data"),
    end_stream: false,
})]
#[case::stop_quic_stream(Effect::StopQuicStream {
    stream_id: u64::MAX,
    error_code: 0,
})]
fn test_process_effects_invalid_stream_id_ignored(#[case] effect: Effect) {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();

    connection.process_effects(vec![effect], &mut event_queue, Instant::now());

    assert!(event_queue.is_empty());
    assert!(connection.pending_effects.is_empty());
    assert!(connection.pending_receives.is_empty());
    assert!(connection.send_buffers.is_empty());
}

#[test]
fn test_process_effects_receive_quic_data_closed_stream_ignored() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let quic_stream_id = open_server_webtransport_stream(&mut pair, b"abc");
    let mut send_stream = pair.server.connection.send_stream(quic_stream_id);
    assert!(matches!(send_stream.finish(), Ok(())));
    pair.drive();
    let stream_id = u64::from(quic_stream_id);
    let mut event_queue = VecDeque::new();
    pair.client.process_effects(
        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: 100,
        }],
        &mut event_queue,
        pair.now,
    );
    event_queue.clear();

    pair.client.process_effects(
        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: 100,
        }],
        &mut event_queue,
        pair.now,
    );

    assert!(event_queue.is_empty());
    assert!(!pair.client.pending_receives.contains_key(&quic_stream_id));
}

#[test]
fn test_process_effects_receive_quic_data_concatenates_chunks() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let quic_stream_id = open_server_webtransport_stream(&mut pair, b"abc");
    let mut send_stream = pair.server.connection.send_stream(quic_stream_id);
    assert!(matches!(send_stream.write(b"def"), Ok(3)));
    assert!(matches!(send_stream.finish(), Ok(())));
    pair.drive();
    let mut event_queue = VecDeque::new();

    pair.client.process_effects(
        vec![Effect::ReceiveQuicData {
            stream_id: u64::from(quic_stream_id),
            max_size: 100,
        }],
        &mut event_queue,
        pair.now,
    );

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::TransportStreamDataReceived {
            data,
            end_stream: true,
            ..
        }) if data.as_ref() == b"abcdef"
    ));
    assert!(!pair.client.pending_receives.contains_key(&quic_stream_id));
}

#[test]
fn test_process_effects_receive_quic_data_reads_up_to_limit() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let quic_stream_id = open_server_webtransport_stream(&mut pair, b"hello");
    let stream_id = u64::from(quic_stream_id);
    let mut event_queue = VecDeque::new();

    pair.client.process_effects(
        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: 3,
        }],
        &mut event_queue,
        pair.now,
    );
    pair.client.process_effects(
        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: 100,
        }],
        &mut event_queue,
        pair.now,
    );

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::TransportStreamDataReceived {
            data,
            end_stream: false,
            ..
        }) if data.as_ref() == b"hel"
    ));
    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::TransportStreamDataReceived {
            data,
            end_stream: false,
            ..
        }) if data.as_ref() == b"lo"
    ));
    assert!(!pair.client.pending_receives.contains_key(&quic_stream_id));
}

#[test]
fn test_process_effects_receive_quic_data_reports_reset() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let quic_stream_id = open_server_webtransport_stream(&mut pair, b"");
    let mut send_stream = pair.server.connection.send_stream(quic_stream_id);
    assert!(matches!(send_stream.reset(QuicVarInt::from_u32(7)), Ok(())));
    pair.drive();
    let mut event_queue = VecDeque::new();

    pair.client.process_effects(
        vec![Effect::ReceiveQuicData {
            stream_id: u64::from(quic_stream_id),
            max_size: 100,
        }],
        &mut event_queue,
        pair.now,
    );

    assert!(matches!(
        event_queue.pop_front(),
        Some(ProtocolEvent::TransportResetStreamReceived { error_code: 7, .. })
    ));
    assert!(!pair.client.pending_receives.contains_key(&quic_stream_id));
}

#[test]
fn test_process_effects_receive_quic_data_waits_for_data() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let Some(quic_stream_id) = pair.server.connection.streams().open(QuicDir::Uni) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let mut send_stream = pair.server.connection.send_stream(quic_stream_id);
    assert!(matches!(send_stream.write(&[0x40]), Ok(1)));
    pair.drive();

    assert_eq!(pair.client.pending_receives.get(&quic_stream_id), Some(&1));

    let mut send_stream = pair.server.connection.send_stream(quic_stream_id);
    assert!(matches!(send_stream.write(&[0x54, 0x00]), Ok(2)));
    pair.drive();

    assert!(!pair.client.pending_receives.contains_key(&quic_stream_id));
}

#[test]
fn test_process_effects_reset_quic_stream_removes_send_buffer() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let quic_stream_id = QuicStreamId::from(QuicVarInt::from_u32(1));
    let effects = vec![Effect::ResetQuicStream {
        stream_id: 1,
        error_code: 0,
    }];
    connection
        .send_buffers
        .insert(quic_stream_id, SendBuffer::default());

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(!connection.send_buffers.contains_key(&quic_stream_id));
}

#[test]
fn test_process_effects_send_h3_capsule_processes_safely() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendH3Capsule {
        stream_id: 1,
        capsule_type: 0,
        capsule_value: Bytes::from_static(b"test"),
        end_stream: false,
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_send_h3_capsule_writes_connect_stream() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let Some(quic_stream_id) = pair.client.connection.streams().open(QuicDir::Bi) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendH3Capsule {
        stream_id: u64::from(quic_stream_id),
        capsule_type: 0,
        capsule_value: Bytes::from_static(b"test"),
        end_stream: false,
    }];

    pair.client
        .process_effects(effects, &mut event_queue, pair.now);

    assert_eq!(event_queue.len(), 3);
    assert!(event_queue.iter().all(|event| matches!(
        event,
        ProtocolEvent::InternalCompleteQuicData { stream_id: 0 }
    )));
    assert!(pair.client.send_buffers.is_empty());
}

#[test]
fn test_process_effects_send_h3_datagram_processes_safely() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendH3Datagram {
        stream_id: 1,
        data: Bytes::from_static(b"test"),
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_send_h3_datagram_transmits_datagram() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendH3Datagram {
        stream_id: 0,
        data: Bytes::from_static(b"datagram"),
    }];

    pair.client
        .process_effects(effects, &mut event_queue, pair.now);
    pair.drive();

    assert!(event_queue.is_empty());
    assert!(drain_effects(&mut pair.client).is_empty());
    assert_eq!(pair.client.connection.stats().frame_tx.datagram, 1);
    assert_eq!(pair.server.connection.stats().frame_rx.datagram, 1);
}

#[test]
fn test_process_effects_send_h3_goaway_processes_safely() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendH3Goaway { identifier: 0 }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_send_h3_headers_encode_failure_skipped() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendH3Headers {
        stream_id: 0,
        headers: vec![(
            Bytes::from_static(b"x-large"),
            Bytes::from(vec![b'a'; 70000]),
        )],
        end_stream: false,
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(event_queue.is_empty());
    assert!(connection.pending_effects.is_empty());
    assert!(connection.send_buffers.is_empty());
}

#[test]
fn test_process_effects_send_h3_headers_processes_safely() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendH3Headers {
        stream_id: 1,
        headers: vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))],
        end_stream: false,
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_send_quic_data_appends_to_send_buffer() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let stream_id = 1u64;
    let data = Bytes::from_static(b"payload");
    let effects = vec![Effect::SendQuicData {
        stream_id,
        data,
        end_stream: true,
    }];
    let quic_stream_id = QuicStreamId::from(QuicVarInt::from_u32(1));

    connection.process_effects(effects, &mut event_queue, Instant::now());

    let Some(buffer) = connection.send_buffers.get(&quic_stream_id) else {
        assert_eq!("found", "missing");
        unreachable!()
    };
    assert!(buffer.finished);
    assert_eq!(buffer.chunks.len(), 1);
}

#[test]
fn test_process_effects_send_quic_datagram_processes_safely() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendQuicDatagram {
        header: Bytes::from_static(b"head"),
        payload: Bytes::from_static(b"load"),
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_stop_quic_stream_processes_safely() {
    let mut connection = create_test_connection();
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::StopQuicStream {
        stream_id: 1,
        error_code: 0,
    }];

    connection.process_effects(effects, &mut event_queue, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_quic_events_connection_lost_reports_application_close() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());

    pair.server.handle_protocol_event(
        ProtocolEvent::UserCloseConnection {
            request_id: 1,
            error_code: 7,
            reason: None,
        },
        0.0,
        pair.now,
    );
    pair.drive();

    assert!(
        drain_effects(&mut pair.client)
            .iter()
            .any(|effect| matches!(
                effect,
                Effect::EmitConnectionEvent {
                    event_type: EventType::ConnectionClosed,
                    error_code: Some(7),
                    ..
                }
            ))
    );
}

#[test]
fn test_process_quic_events_connection_refused_reports_transport_close() {
    let now = Instant::now();
    let mut client_endpoint =
        QuicEndpoint::new(Arc::new(QuicEndpointConfig::default()), None, false, None);
    let mut server_endpoint = QuicEndpoint::new(
        Arc::new(QuicEndpointConfig::default()),
        Some(Arc::new(create_server_config(
            QuicTransportConfig::default(),
        ))),
        false,
        None,
    );
    let Ok((_, quic)) =
        client_endpoint.connect(now, create_client_config(), server_address(), "localhost")
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let mut client = TransportConnection::new(quic, create_test_engine(true), None, None, now);
    let mut workspace = Vec::new();
    let Some(transmit) = client.poll_transmit(&mut workspace, now) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let mut response = Vec::new();
    let Some(QuicDatagramEvent::NewConnection(incoming)) = server_endpoint.handle(
        now,
        client_address(),
        None,
        None,
        BytesMut::from(workspace.get(..transmit.size).unwrap_or_default()),
        &mut response,
    ) else {
        assert_eq!("some", "none");
        unreachable!()
    };

    let refusal = server_endpoint.refuse(incoming, &mut response);
    deliver(
        &response,
        &refusal,
        server_address(),
        &mut client_endpoint,
        &mut client,
        now,
    );

    assert!(drain_effects(&mut client).iter().any(|effect| matches!(
        effect,
        Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionClosed,
            error_code: Some(0x2),
            ..
        }
    )));
}

#[test]
fn test_process_quic_events_malformed_datagram_closes_connection() {
    let mut pair = create_connected_pair(QuicTransportConfig::default());
    assert!(matches!(
        pair.server.connection.datagrams().send(Bytes::new(), false),
        Ok(())
    ));

    pair.drive();

    assert!(
        drain_effects(&mut pair.client)
            .iter()
            .any(|effect| matches!(
                effect,
                Effect::EmitConnectionEvent {
                    event_type: EventType::ConnectionClosed,
                    error_code: Some(ERR_H3_DATAGRAM_ERROR),
                    ..
                }
            ))
    );
}

#[test]
fn test_process_quic_events_stream_available_opens_deferred_connect_stream() {
    let mut server_transport_config = QuicTransportConfig::default();
    server_transport_config.max_concurrent_bidi_streams(QuicVarInt::from_u32(0));
    let mut pair = create_connected_pair(server_transport_config);
    let send_streams = pair.client.connection.streams().send_streams();

    pair.client.handle_protocol_event(
        ProtocolEvent::UserCreateSession {
            request_id: 1,
            authority: "localhost".to_owned(),
            path: "/".to_owned(),
            headers: vec![],
            wt_available_protocols: None,
        },
        0.0,
        pair.now,
    );
    pair.drive();

    assert!(drain_effects(&mut pair.client).is_empty());
    assert_eq!(
        pair.client.connection.streams().send_streams(),
        send_streams
    );

    pair.server
        .connection
        .set_max_concurrent_streams(QuicDir::Bi, QuicVarInt::from_u32(1));
    pair.drive();

    assert_eq!(
        pair.client.connection.streams().send_streams(),
        send_streams + 1
    );
    assert!(pair.client.send_buffers.is_empty());
}

#[test]
fn test_process_quic_events_stream_stopped_discards_send_buffer() {
    let mut server_transport_config = QuicTransportConfig::default();
    server_transport_config.stream_receive_window(QuicVarInt::from_u32(16));
    let mut pair = create_connected_pair(server_transport_config);
    let Some(quic_stream_id) = pair.client.connection.streams().open(QuicDir::Bi) else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let mut data = vec![0x40, 0x41, 0x00];
    data.extend_from_slice(&[0u8; 1024]);
    let mut event_queue = VecDeque::new();
    let effects = vec![Effect::SendQuicData {
        stream_id: u64::from(quic_stream_id),
        data: Bytes::from(data),
        end_stream: false,
    }];
    pair.client
        .process_effects(effects, &mut event_queue, pair.now);
    pair.drive();
    assert!(pair.client.send_buffers.contains_key(&quic_stream_id));

    assert!(matches!(
        pair.server
            .connection
            .recv_stream(quic_stream_id)
            .stop(QuicVarInt::from_u32(0)),
        Ok(())
    ));
    pair.drive();

    assert!(!pair.client.send_buffers.contains_key(&quic_stream_id));
    assert!(drain_effects(&mut pair.client).is_empty());
}
