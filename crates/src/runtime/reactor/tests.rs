//! Unit tests for the `crate::runtime::reactor` module.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use quinn_proto::crypto::rustls::QuicClientConfig as QuicCryptoClientConfig;
use quinn_proto::{
    ClientConfig as QuicClientConfig, ConnectionHandle as QuicConnectionHandle,
    Endpoint as QuicEndpoint, EndpointConfig as QuicEndpointConfig, Transmit as QuicTransmit,
    TransportConfig as QuicTransportConfig, VarInt as QuicVarInt,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::version::TLS13;
use rustls::{
    ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme,
};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::{self, LocalSet};
use tokio::test as tokio_test;
use tokio::time::{self, Instant as TokioInstant};

use super::*;
use crate::common::config::{RustBaseConfig, RustServerConfig};
use crate::common::constants::ERR_LIB_CONNECTION_STATE_ERROR;
use crate::common::types::{EventType, RequestId};
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::runtime::channel::{RuntimeCommand, RuntimeEvent};
use crate::transport::endpoint::{TransportEndpoint, TransportEvent};

#[derive(Debug)]
struct DummyVerifier;

impl ServerCertVerifier for DummyVerifier {
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::RSA_PSS_SHA256]
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

async fn create_client_reactor(
    event_capacity: usize,
) -> (
    Reactor,
    Sender<RuntimeCommand>,
    Arc<ArrayQueue<RuntimeEvent>>,
    Arc<AtomicBool>,
) {
    let (command_tx, command_rx) = mpsc::channel(10);
    let event_tx = Arc::new(ArrayQueue::new(event_capacity));
    let waker_called = Arc::new(AtomicBool::new(false));
    let waker_called_clone = Arc::clone(&waker_called);
    let waker: WakerCallback = Arc::new(move || {
        waker_called_clone.store(true, Ordering::SeqCst);
    });
    let Ok(socket) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let endpoint = create_dummy_endpoint(false);
    let reactor = Reactor::new(
        command_rx,
        endpoint,
        Arc::clone(&event_tx),
        vec![socket],
        waker,
    );

    (reactor, command_tx, event_tx, waker_called)
}

fn create_dummy_client_config() -> QuicClientConfig {
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
    let Ok(crypto) = QuicCryptoClientConfig::try_from(crypto) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let mut client_config = QuicClientConfig::new(Arc::new(crypto));
    client_config.transport_config(Arc::new(QuicTransportConfig::default()));

    client_config
}

fn create_dummy_endpoint(is_server: bool) -> TransportEndpoint {
    let quic_endpoint =
        QuicEndpoint::new(Arc::new(QuicEndpointConfig::default()), None, false, None);
    let base_config = mock_base_config();

    if is_server {
        let server_config = create_dummy_rust_server_config();
        let Ok(endpoint) = TransportEndpoint::new_server(quic_endpoint, base_config, server_config)
        else {
            assert_eq!("ok", "err");
            unreachable!()
        };
        endpoint
    } else {
        let client_config = create_dummy_client_config();
        let Ok(endpoint) = TransportEndpoint::new_client(quic_endpoint, base_config, client_config)
        else {
            assert_eq!("ok", "err");
            unreachable!()
        };
        endpoint
    }
}

fn create_dummy_rust_server_config() -> RustServerConfig {
    RustServerConfig {
        base: mock_base_config(),
        bind_host: "127.0.0.1".to_owned(),
        bind_port: 4433,
        ca_certs: None,
        certfile: PathBuf::from("dummy.crt"),
        keyfile: PathBuf::from("dummy.key"),
        require_client_auth: false,
    }
}

fn create_dummy_socket_address() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443)
}

async fn create_idle_client_reactor(
    event_capacity: usize,
) -> (
    Reactor,
    Sender<RuntimeCommand>,
    Arc<ArrayQueue<RuntimeEvent>>,
    Arc<AtomicBool>,
) {
    let (command_tx, command_rx) = mpsc::channel(10);
    let event_tx = Arc::new(ArrayQueue::new(event_capacity));
    let waker_called = Arc::new(AtomicBool::new(false));
    let waker_called_clone = Arc::clone(&waker_called);
    let waker: WakerCallback = Arc::new(move || {
        waker_called_clone.store(true, Ordering::SeqCst);
    });
    let Ok(socket) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let mut transport_config = QuicTransportConfig::default();
    transport_config
        .initial_rtt(Duration::from_millis(1))
        .max_idle_timeout(Some(QuicVarInt::from_u32(10).into()));
    let mut client_config = create_dummy_client_config();
    client_config.transport_config(Arc::new(transport_config));
    let quic_endpoint =
        QuicEndpoint::new(Arc::new(QuicEndpointConfig::default()), None, false, None);
    let Ok(endpoint) =
        TransportEndpoint::new_client(quic_endpoint, mock_base_config(), client_config)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let reactor = Reactor::new(
        command_rx,
        endpoint,
        Arc::clone(&event_tx),
        vec![socket],
        waker,
    );

    (reactor, command_tx, event_tx, waker_called)
}

async fn create_test_reactor(
    event_capacity: usize,
) -> (
    Reactor,
    Sender<RuntimeCommand>,
    Arc<ArrayQueue<RuntimeEvent>>,
    Arc<AtomicBool>,
) {
    let (command_tx, command_rx) = mpsc::channel(10);
    let event_tx = Arc::new(ArrayQueue::new(event_capacity));
    let waker_called = Arc::new(AtomicBool::new(false));
    let waker_called_clone = Arc::clone(&waker_called);
    let waker: WakerCallback = Arc::new(move || {
        waker_called_clone.store(true, Ordering::SeqCst);
    });
    let Ok(socket) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let endpoint = create_dummy_endpoint(true);
    let reactor = Reactor::new(
        command_rx,
        endpoint,
        Arc::clone(&event_tx),
        vec![socket],
        waker,
    );

    (reactor, command_tx, event_tx, waker_called)
}

fn mock_base_config() -> RustBaseConfig {
    RustBaseConfig {
        congestion_control_algorithm: "cubic".to_owned(),
        connection_receive_window: 16 * 1024 * 1024,
        connection_send_window: 16 * 1024 * 1024,
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        keep_alive_interval: Some(Duration::from_secs_f64(30.0)),
        max_capsule_size: 65536,
        max_connection_pending_events: 1000,
        max_connections: 100,
        max_datagram_size: 1350,
        max_field_section_size: 65536,
        max_idle_timeout: Duration::from_secs_f64(60.0),
        max_pending_capsules: 20,
        max_pending_datagrams: 100,
        max_pending_streams: 10,
        max_sessions: 10,
        max_session_pending_events: 100,
        max_streams_bidi: 100,
        max_streams_uni: 100,
        max_udp_payload_size: 1472,
        max_write_buffer_size: 1024 * 1024,
        pending_event_ttl: Duration::from_secs_f64(5.0),
        resource_cleanup_interval: Duration::from_secs_f64(15.0),
        session_receive_window: 4 * 1024 * 1024,
        stream_receive_window: 1024 * 1024,
    }
}

async fn run_timers_until_idle(reactor: &mut Reactor) {
    let result = time::timeout(Duration::from_secs(5), async {
        while let Some(timeout) = reactor.endpoint.poll_timeout() {
            time::sleep_until(TokioInstant::from_std(timeout)).await;
            reactor.handle_timeout();
        }
    })
    .await;

    assert!(matches!(result, Ok(())));
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let result = time::timeout(Duration::from_secs(5), async {
        while !condition() {
            time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;

    assert!(matches!(result, Ok(())));
}

#[tokio_test]
async fn test_flush_transmits_after_connect_sends_data() {
    let (mut reactor, _, _, _) = create_client_reactor(10).await;
    let remote_address = create_dummy_socket_address();
    let now = Instant::now();

    let TransportEvent::ConnectionSpawned { .. } =
        reactor
            .endpoint
            .connect(1, remote_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    reactor.flush_transmits().await;

    assert!(!reactor.events_emitted);
    assert!(reactor.endpoint.poll_transmit(now).is_none());
}

#[tokio_test]
async fn test_flush_transmits_empty() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;

    reactor.flush_transmits().await;

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_handle_command_create_connection_failure_emits_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(10).await;
    let command = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(1u64),
        remote_address: create_dummy_socket_address(),
        server_name: "localhost".to_owned(),
    };

    let continue_loop = reactor.handle_command(command).await;

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(continue_loop);
    assert!(reactor.events_emitted);
    assert!(matches!(event, RuntimeEvent::CommandFailed { .. }));
}

#[tokio_test]
async fn test_handle_command_create_connection_queue_full_drops_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(1).await;
    let command = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(1u64),
        remote_address: create_dummy_socket_address(),
        server_name: "localhost".to_owned(),
    };
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutdown) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let continue_loop = reactor.handle_command(command).await;

    assert!(continue_loop);
    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_handle_command_protocol_dispatches_event() {
    let (mut reactor, _, _, _) = create_client_reactor(10).await;
    let remote_address = create_dummy_socket_address();
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { handle, .. } =
        reactor
            .endpoint
            .connect(1, remote_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let command = RuntimeCommand::ProtocolEvent {
        request_id: 100,
        handle,
        event: ProtocolEvent::UserDiagnoseConnection { request_id: 100 },
    };

    let continue_loop = reactor.handle_command(command).await;

    assert!(continue_loop);
    assert!(reactor.events_emitted);
}

#[tokio_test]
async fn test_handle_command_shutdown_emits_final_effects() {
    let (mut reactor, _, event_tx, _) = create_client_reactor(10).await;
    let remote_address = create_dummy_socket_address();
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { handle, .. } =
        reactor
            .endpoint
            .connect(1, remote_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let continue_loop = reactor
        .handle_command(RuntimeCommand::ShutdownReactor)
        .await;

    let Some(emitted) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(!continue_loop);
    assert!(reactor.events_emitted);
    assert!(matches!(
        emitted,
        RuntimeEvent::ConnectionEffects {
            handle: event_handle,
            effects,
        } if event_handle == handle && effects.iter().any(|effect| matches!(
            effect,
            Effect::EmitConnectionEvent {
                event_type: EventType::ConnectionClosed,
                ..
            }
        ))
    ));
}

#[tokio_test]
async fn test_handle_command_shutdown_queue_full_drops_effects() {
    let (mut reactor, _, event_tx, _) = create_client_reactor(1).await;
    let TransportEvent::ConnectionSpawned { .. } = reactor.endpoint.connect(
        1,
        create_dummy_socket_address(),
        "localhost",
        Instant::now(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutdown) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let continue_loop = reactor
        .handle_command(RuntimeCommand::ShutdownReactor)
        .await;

    assert!(!continue_loop);
    assert!(!reactor.events_emitted);
    assert!(matches!(
        event_tx.pop(),
        Some(RuntimeEvent::ReactorShutdown)
    ));
}

#[tokio_test]
async fn test_handle_command_shutdown_returns_false() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;
    let command = RuntimeCommand::ShutdownReactor;

    let continue_loop = reactor.handle_command(command).await;

    assert!(!continue_loop);
}

#[tokio_test]
async fn test_handle_datagram_processes_safely() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;

    reactor
        .handle_datagram(create_dummy_socket_address(), None, BytesMut::new())
        .await;

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_handle_timeout_emits_connection_effects() {
    let (mut reactor, _, event_tx, _) = create_idle_client_reactor(10).await;
    let TransportEvent::ConnectionSpawned { handle, .. } = reactor.endpoint.connect(
        1,
        create_dummy_socket_address(),
        "localhost",
        Instant::now(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    reactor.flush_transmits().await;

    run_timers_until_idle(&mut reactor).await;

    let Some(emitted) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(reactor.events_emitted);
    assert!(matches!(
        emitted,
        RuntimeEvent::ConnectionEffects {
            handle: event_handle,
            effects,
        } if event_handle == handle && effects.iter().any(|effect| matches!(
            effect,
            Effect::EmitConnectionEvent {
                event_type: EventType::ConnectionClosed,
                ..
            }
        ))
    ));
}

#[tokio_test]
async fn test_handle_timeout_processes_safely() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;

    reactor.handle_timeout();

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_handle_timeout_queue_full_drops_effects() {
    let (mut reactor, _, event_tx, _) = create_idle_client_reactor(1).await;
    let TransportEvent::ConnectionSpawned { .. } = reactor.endpoint.connect(
        1,
        create_dummy_socket_address(),
        "localhost",
        Instant::now(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutdown) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    reactor.flush_transmits().await;

    run_timers_until_idle(&mut reactor).await;

    assert!(!reactor.events_emitted);
    assert!(matches!(
        event_tx.pop(),
        Some(RuntimeEvent::ReactorShutdown)
    ));
}

#[tokio_test]
async fn test_notify_shutdown_emits_event() {
    let (reactor, _, event_tx, waker_called) = create_test_reactor(10).await;

    reactor.notify_shutdown();

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(waker_called.load(Ordering::SeqCst));
    assert!(matches!(event, RuntimeEvent::ReactorShutdown));
}

#[tokio_test]
async fn test_notify_shutdown_queue_full_drops_event() {
    let (reactor, _, event_tx, waker_called) = create_test_reactor(1).await;
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutdown) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    reactor.notify_shutdown();

    assert!(waker_called.load(Ordering::SeqCst));
}

#[tokio_test]
async fn test_now_context_returns_valid_timestamps() {
    let (reactor, _, _, _) = create_test_reactor(10).await;

    let (now, now_instant) = reactor.now_context();

    assert!(now >= 0.0);
    assert!(now_instant >= reactor.start_instant);
}

#[tokio_test]
async fn test_process_transport_event_connection_effects_emits_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(10).await;
    let event = TransportEvent::ConnectionEffects {
        handle: QuicConnectionHandle(0),
        effects: vec![Effect::SendH3Goaway { identifier: 0 }],
    };

    reactor.process_transport_event(event).await;

    let Some(emitted) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(reactor.events_emitted);
    assert!(matches!(emitted, RuntimeEvent::ConnectionEffects { .. }));
}

#[tokio_test]
async fn test_process_transport_event_connection_effects_queue_full_drops_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(1).await;
    let event = TransportEvent::ConnectionEffects {
        handle: QuicConnectionHandle(0),
        effects: vec![Effect::SendH3Goaway { identifier: 0 }],
    };
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutdown) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    reactor.process_transport_event(event).await;

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_process_transport_event_connection_effects_without_effects_skipped() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(10).await;
    let event = TransportEvent::ConnectionEffects {
        handle: QuicConnectionHandle(0),
        effects: vec![],
    };

    reactor.process_transport_event(event).await;

    assert!(!reactor.events_emitted);
    assert!(event_tx.is_empty());
}

#[tokio_test]
async fn test_process_transport_event_connection_spawned_emits_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(10).await;
    let event = TransportEvent::ConnectionSpawned {
        handle: QuicConnectionHandle(0),
        effects: vec![],
        remote_address: create_dummy_socket_address(),
    };

    reactor.process_transport_event(event).await;

    let Some(emitted) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(reactor.events_emitted);
    assert!(matches!(emitted, RuntimeEvent::ConnectionSpawned { .. }));
}

#[tokio_test]
async fn test_process_transport_event_connection_spawned_queue_full_drops_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(1).await;
    let event = TransportEvent::ConnectionSpawned {
        handle: QuicConnectionHandle(0),
        effects: vec![],
        remote_address: create_dummy_socket_address(),
    };
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutdown) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    reactor.process_transport_event(event).await;

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_process_transport_event_transmit_executes() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;
    let transmit = QuicTransmit {
        destination: create_dummy_socket_address(),
        size: 0,
        ecn: None,
        segment_size: None,
        src_ip: None,
    };
    let event = TransportEvent::Transmit(transmit);

    reactor.process_transport_event(event).await;

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_reactor_initialization_succeeds() {
    let (_command_tx, command_rx) = mpsc::channel(10);
    let event_tx = Arc::new(ArrayQueue::new(10));
    let waker: WakerCallback = Arc::new(|| {});
    let Ok(socket) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let endpoint = create_dummy_endpoint(true);

    let reactor = Reactor::new(
        command_rx,
        endpoint,
        Arc::clone(&event_tx),
        vec![socket],
        waker,
    );

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_reactor_run_loop_emits_final_effects_on_shutdown() {
    let (reactor, command_tx, event_tx, waker_called) = create_client_reactor(10).await;
    let command_create = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(1u64),
        remote_address: create_dummy_socket_address(),
        server_name: "localhost".to_owned(),
    };
    let local = LocalSet::new();

    local
        .run_until(async {
            let handle = task::spawn_local(reactor.run());
            let Ok(()) = command_tx.send(command_create).await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            wait_until(|| !event_tx.is_empty()).await;
            waker_called.store(false, Ordering::SeqCst);
            let Ok(()) = command_tx.send(RuntimeCommand::ShutdownReactor).await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            let Ok(()) = handle.await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            assert!(waker_called.load(Ordering::SeqCst));
        })
        .await;

    assert!(matches!(
        event_tx.pop(),
        Some(RuntimeEvent::ConnectionSpawned { .. })
    ));
    assert!(matches!(
        event_tx.pop(),
        Some(RuntimeEvent::ConnectionEffects { effects, .. }) if effects.iter().any(|effect| matches!(
            effect,
            Effect::NotifyRequestFailed { request_id: 1, .. }
        ))
    ));
    assert!(matches!(
        event_tx.pop(),
        Some(RuntimeEvent::ReactorShutdown)
    ));
}

#[tokio_test]
async fn test_reactor_run_loop_full_tick_and_waker() {
    let (reactor, command_tx, event_tx, waker_called) = create_test_reactor(10).await;
    let command_create = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(99u64),
        remote_address: create_dummy_socket_address(),
        server_name: "localhost".to_owned(),
    };
    let command_shutdown = RuntimeCommand::ShutdownReactor;
    let local = LocalSet::new();

    local
        .run_until(async {
            let handle = task::spawn_local(reactor.run());
            let Ok(()) = command_tx.send(command_create).await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            wait_until(|| waker_called.load(Ordering::SeqCst)).await;
            let Ok(()) = command_tx.send(command_shutdown).await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            let Ok(()) = handle.await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
        })
        .await;

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(waker_called.load(Ordering::SeqCst));
    assert!(matches!(event, RuntimeEvent::CommandFailed { .. }));
}

#[tokio_test]
async fn test_reactor_run_loop_handles_timeout() {
    let (reactor, command_tx, event_tx, waker_called) = create_idle_client_reactor(10).await;
    let command_create = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(1u64),
        remote_address: create_dummy_socket_address(),
        server_name: "localhost".to_owned(),
    };
    let local = LocalSet::new();

    local
        .run_until(async move {
            let handle = task::spawn_local(reactor.run());
            let Ok(()) = command_tx.send(command_create).await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            wait_until(|| event_tx.len() == 2).await;

            assert!(waker_called.load(Ordering::SeqCst));
            assert!(matches!(
                event_tx.pop(),
                Some(RuntimeEvent::ConnectionSpawned { .. })
            ));
            assert!(matches!(
                event_tx.pop(),
                Some(RuntimeEvent::ConnectionEffects { effects, .. }) if effects.iter().any(|effect| matches!(
                    effect,
                    Effect::EmitConnectionEvent {
                        event_type: EventType::ConnectionClosed,
                        error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                        ..
                    }
                ))
            ));

            let Ok(()) = command_tx.send(RuntimeCommand::ShutdownReactor).await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            let Ok(()) = handle.await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
        })
        .await;
}

#[tokio_test]
async fn test_reactor_run_loop_processes_multiple_datagrams() {
    let (reactor, command_tx, event_tx, _) = create_test_reactor(10).await;
    let local = LocalSet::new();
    let Some(bound_socket) = reactor.sockets.first() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let Ok(address) = bound_socket.socket.local_addr() else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    for _ in 0..3 {
        let Ok(_) = sender.send_to(b"test", address).await else {
            assert_eq!("ok", "err");
            unreachable!()
        };
    }

    wait_until(|| reactor.datagram_rx.len() == 3).await;

    local
        .run_until(async move {
            let handle = task::spawn_local(reactor.run());
            task::yield_now().await;
            let Ok(()) = command_tx.send(RuntimeCommand::ShutdownReactor).await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            let Ok(()) = handle.await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
        })
        .await;

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(matches!(event, RuntimeEvent::ReactorShutdown));
}

#[tokio_test]
async fn test_reactor_run_loop_terminates_on_shutdown() {
    let (reactor, command_tx, event_tx, _) = create_test_reactor(10).await;
    let local = LocalSet::new();
    let command_shutdown = RuntimeCommand::ShutdownReactor;

    local
        .run_until(async move {
            let handle = task::spawn_local(reactor.run());
            let Ok(()) = command_tx.send(command_shutdown).await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
            let Ok(()) = handle.await else {
                assert_eq!("ok", "err");
                unreachable!()
            };
        })
        .await;

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert!(matches!(event, RuntimeEvent::ReactorShutdown));
}

#[tokio_test]
async fn test_reactor_udp_receive_loop_forwards_datagrams() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;
    let payload = b"hello_world";
    let Some(bound_socket) = reactor.sockets.first() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let Ok(address) = bound_socket.socket.local_addr() else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(sender_address) = sender.local_addr() else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let Ok(size) = sender.send_to(payload, address).await else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Some((remote_address, local_address, data)) = reactor.datagram_rx.recv().await else {
        assert_eq!("some", "none");
        unreachable!()
    };

    assert_eq!(size, 11);
    assert_eq!(data.as_ref(), payload);
    assert_eq!(remote_address.port(), sender_address.port());
    assert!(local_address.is_some());
}

#[tokio_test]
async fn test_reactor_udp_receive_task_stops_after_reactor_dropped() {
    let (reactor, _, _, _) = create_test_reactor(10).await;
    let Some(bound_socket) = reactor.sockets.first() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let socket = Arc::clone(&bound_socket.socket);
    let Ok(address) = socket.local_addr() else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    drop(reactor);

    let Ok(_) = sender.send_to(b"test", address).await else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    wait_until(|| Arc::strong_count(&socket) == 1).await;
}

#[tokio_test]
async fn test_reactor_udp_slab_reallocation_is_triggered() {
    let (mut reactor, _, _, _) = create_test_reactor(100).await;
    let payload = vec![0u8; 1200];
    let mut count = 0;
    let Some(bound_socket) = reactor.sockets.first() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    let Ok(address) = bound_socket.socket.local_addr() else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    for _ in 0..60 {
        let Ok(size) = sender.send_to(&payload, address).await else {
            assert_eq!("ok", "err");
            unreachable!()
        };
        assert_eq!(size, 1200);
    }
    while count < 60 {
        let Ok(Some(_)) = time::timeout(Duration::from_secs(5), reactor.datagram_rx.recv()).await
        else {
            break;
        };
        count += 1;
    }

    assert_eq!(count, 60);
}

#[tokio_test]
async fn test_send_transmit_beyond_workspace_skipped() {
    let (reactor, _, _, _) = create_test_reactor(10).await;
    let transmit = QuicTransmit {
        destination: create_dummy_socket_address(),
        size: 10,
        ecn: None,
        segment_size: None,
        src_ip: None,
    };

    reactor.send_transmit(&transmit).await;

    assert!(reactor.endpoint.transmit_workspace().is_empty());
    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_send_transmit_executes_safely() {
    let (reactor, _, _, _) = create_test_reactor(10).await;
    let transmit = QuicTransmit {
        destination: create_dummy_socket_address(),
        size: 0,
        ecn: None,
        segment_size: None,
        src_ip: None,
    };

    reactor.send_transmit(&transmit).await;

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_send_transmit_falls_back_to_first_socket() {
    let (mut reactor, _, _, _) = create_client_reactor(10).await;
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { .. } =
        reactor
            .endpoint
            .connect(1, create_dummy_socket_address(), "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Some(mut transmit) = reactor.endpoint.poll_transmit(now) else {
        assert_eq!("some", "none");
        unreachable!()
    };

    transmit.destination = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 443);
    reactor.send_transmit(&transmit).await;

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_send_transmit_segments_data_safely() {
    let (mut reactor, _, _, _) = create_client_reactor(10).await;
    let remote_address = create_dummy_socket_address();
    let now = Instant::now();

    let TransportEvent::ConnectionSpawned { .. } =
        reactor
            .endpoint
            .connect(1, remote_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Some(mut transmit) = reactor.endpoint.poll_transmit(now) else {
        assert_eq!("some", "none");
        unreachable!()
    };

    transmit.segment_size = Some(100);
    reactor.send_transmit(&transmit).await;

    assert!(!reactor.events_emitted);
}

#[tokio_test]
async fn test_send_transmit_without_sockets_skipped() {
    let (_command_tx, command_rx) = mpsc::channel(10);
    let event_tx = Arc::new(ArrayQueue::new(10));
    let waker: WakerCallback = Arc::new(|| {});
    let mut reactor = Reactor::new(
        command_rx,
        create_dummy_endpoint(false),
        Arc::clone(&event_tx),
        vec![],
        waker,
    );
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { .. } =
        reactor
            .endpoint
            .connect(1, create_dummy_socket_address(), "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Some(transmit) = reactor.endpoint.poll_transmit(now) else {
        assert_eq!("some", "none");
        unreachable!()
    };

    reactor.send_transmit(&transmit).await;

    assert!(reactor.sockets.is_empty());
    assert!(!reactor.events_emitted);
}
