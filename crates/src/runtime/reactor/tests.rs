//! Unit tests for the `crate::runtime::reactor` module.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use quinn_proto::crypto::rustls::QuicClientConfig;
use quinn_proto::{
    ClientConfig, ConnectionHandle, Endpoint as QuinnEndpoint, EndpointConfig, Transmit,
    TransportConfig,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::version::TLS13;
use rustls::{DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use super::*;
use crate::common::config::{RustBaseConfig, RustServerConfig};
use crate::common::types::RequestId;
use crate::protocol::events::{Effect, ProtocolEvent};

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

fn create_dummy_client_config() -> ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let Ok(builder) =
        rustls::ClientConfig::builder_with_provider(provider).with_protocol_versions(&[&TLS13])
    else {
        assert_eq!("ok", "err", "Failed to build rustls client config");
        unreachable!()
    };
    let root_store = RootCertStore::empty();
    let mut crypto = builder
        .with_root_certificates(root_store)
        .with_no_client_auth();
    crypto
        .dangerous()
        .set_certificate_verifier(Arc::new(DummyVerifier));
    let Ok(quic_crypto) = QuicClientConfig::try_from(crypto) else {
        assert_eq!("ok", "err", "Failed to build QuicClientConfig");
        unreachable!()
    };
    let mut client_config = ClientConfig::new(Arc::new(quic_crypto));
    client_config.transport_config(Arc::new(TransportConfig::default()));

    client_config
}

fn create_dummy_endpoint(is_server: bool) -> TransportEndpoint {
    let quinn_ep = QuinnEndpoint::new(Arc::new(EndpointConfig::default()), None, false, None);
    let b_cfg = mock_base_config();

    if is_server {
        let s_cfg = create_dummy_rust_server_config();
        let Ok(ep) = TransportEndpoint::new_server(quinn_ep, b_cfg, s_cfg) else {
            assert_eq!("ok", "err", "Failed to create TransportEndpoint server");
            unreachable!()
        };
        ep
    } else {
        let c_cfg = create_dummy_client_config();
        let Ok(ep) = TransportEndpoint::new_client(quinn_ep, b_cfg, c_cfg) else {
            assert_eq!("ok", "err", "Failed to create TransportEndpoint client");
            unreachable!()
        };
        ep
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

fn create_dummy_socket_addr() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443)
}

async fn create_client_reactor(
    event_capacity: usize,
) -> (
    Reactor,
    mpsc::Sender<RuntimeCommand>,
    Arc<ArrayQueue<RuntimeEvent>>,
    Arc<AtomicBool>,
) {
    let (cmd_tx, cmd_rx) = mpsc::channel(10);
    let event_tx = Arc::new(ArrayQueue::new(event_capacity));
    let waker_called = Arc::new(AtomicBool::new(false));
    let waker_called_clone = Arc::clone(&waker_called);
    let waker: WakerCallback = Arc::new(move || {
        waker_called_clone.store(true, Ordering::SeqCst);
    });
    let Ok(socket) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err", "Failed to bind UDP socket");
        unreachable!()
    };
    let endpoint = create_dummy_endpoint(false);
    let reactor = Reactor::new(cmd_rx, endpoint, Arc::clone(&event_tx), vec![socket], waker);

    (reactor, cmd_tx, event_tx, waker_called)
}

async fn create_test_reactor(
    event_capacity: usize,
) -> (
    Reactor,
    mpsc::Sender<RuntimeCommand>,
    Arc<ArrayQueue<RuntimeEvent>>,
    Arc<AtomicBool>,
) {
    let (cmd_tx, cmd_rx) = mpsc::channel(10);
    let event_tx = Arc::new(ArrayQueue::new(event_capacity));
    let waker_called = Arc::new(AtomicBool::new(false));
    let waker_called_clone = Arc::clone(&waker_called);
    let waker: WakerCallback = Arc::new(move || {
        waker_called_clone.store(true, Ordering::SeqCst);
    });
    let Ok(socket) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err", "Failed to bind UDP socket");
        unreachable!()
    };
    let endpoint = create_dummy_endpoint(true);
    let reactor = Reactor::new(cmd_rx, endpoint, Arc::clone(&event_tx), vec![socket], waker);

    (reactor, cmd_tx, event_tx, waker_called)
}

fn mock_base_config() -> RustBaseConfig {
    RustBaseConfig {
        alpn_protocols: vec!["h3".to_owned()],
        congestion_control_algorithm: "cubic".to_owned(),
        connection_idle_timeout: Duration::from_secs_f64(60.0),
        flow_control_window: 4 * 1024 * 1024,
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        keep_alive_interval: Some(Duration::from_secs_f64(30.0)),
        max_capsule_size: 65536,
        max_datagram_size: 1350,
        max_field_section_size: 65536,
        max_session_pending_events: 100,
        max_sessions: 10,
        max_stream_read_buffer_size: 1024 * 1024,
        max_stream_write_buffer_size: 1024 * 1024,
        max_total_pending_events: 1000,
        pending_event_ttl: Duration::from_secs_f64(5.0),
        quic_max_concurrent_bidi_streams: 100,
        quic_max_concurrent_uni_streams: 100,
        quic_receive_window: 16 * 1024 * 1024,
        quic_send_window: 16 * 1024 * 1024,
        quic_stream_receive_window: 1024 * 1024,
        resource_cleanup_interval: Duration::from_secs_f64(15.0),
    }
}

#[tokio::test]
async fn test_flush_transmits_empty() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;

    reactor.flush_transmits().await;

    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_handle_command_create_connection_failure_emits_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(10).await;
    let cmd = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(1u64),
        remote_address: create_dummy_socket_addr(),
        server_name: "localhost".to_owned(),
    };

    let continue_loop = reactor.handle_command(cmd).await;

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none", "Expected event in queue");
        unreachable!()
    };
    assert!(continue_loop);
    assert!(reactor.events_emitted);
    assert!(matches!(event, RuntimeEvent::CommandFailed { .. }));
}

#[tokio::test]
async fn test_handle_command_create_connection_queue_full_drops_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(1).await;
    let cmd = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(1u64),
        remote_address: create_dummy_socket_addr(),
        server_name: "localhost".to_owned(),
    };
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutDown) else {
        assert_eq!("ok", "err", "Failed to push event");
        unreachable!()
    };

    let continue_loop = reactor.handle_command(cmd).await;

    assert!(continue_loop);
    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_handle_command_protocol_dispatches_event() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;
    let cmd = RuntimeCommand::Protocol {
        handle: ConnectionHandle(0),
        event: ProtocolEvent::InternalCleanupResources,
    };

    let continue_loop = reactor.handle_command(cmd).await;

    assert!(continue_loop);
}

#[tokio::test]
async fn test_handle_command_shutdown_returns_false() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;
    let cmd = RuntimeCommand::Shutdown;

    let continue_loop = reactor.handle_command(cmd).await;

    assert!(!continue_loop);
}

#[tokio::test]
async fn test_handle_datagram_processes_safely() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;

    reactor
        .handle_datagram(create_dummy_socket_addr(), None, BytesMut::new())
        .await;

    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_handle_timeout_processes_safely() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;

    reactor.handle_timeout();

    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_notify_shutdown_emits_event() {
    let (reactor, _, event_tx, waker_called) = create_test_reactor(10).await;

    reactor.notify_shutdown();

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none", "Expected event in queue");
        unreachable!()
    };
    assert!(waker_called.load(Ordering::SeqCst));
    assert!(matches!(event, RuntimeEvent::ReactorShutDown));
}

#[tokio::test]
async fn test_notify_shutdown_queue_full_drops_event() {
    let (reactor, _, event_tx, waker_called) = create_test_reactor(1).await;
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutDown) else {
        assert_eq!("ok", "err", "Failed to push event");
        unreachable!()
    };

    reactor.notify_shutdown();

    assert!(waker_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_now_context_returns_valid_timestamps() {
    let (reactor, _, _, _) = create_test_reactor(10).await;

    let (now, inst) = reactor.now_context();

    assert!(now >= 0.0);
    assert!(inst >= reactor.start_instant);
}

#[tokio::test]
async fn test_process_transport_event_connection_effects_emits_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(10).await;
    let event = TransportEvent::ConnectionEffects {
        handle: ConnectionHandle(0),
        effects: vec![Effect::SendH3Goaway],
    };

    reactor.process_transport_event(event).await;

    let Some(emitted) = event_tx.pop() else {
        assert_eq!("some", "none", "Expected event in queue");
        unreachable!()
    };
    assert!(reactor.events_emitted);
    assert!(matches!(emitted, RuntimeEvent::ConnectionEffects { .. }));
}

#[tokio::test]
async fn test_process_transport_event_connection_effects_queue_full_drops_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(1).await;
    let event = TransportEvent::ConnectionEffects {
        handle: ConnectionHandle(0),
        effects: vec![Effect::SendH3Goaway],
    };
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutDown) else {
        assert_eq!("ok", "err", "Failed to push event");
        unreachable!()
    };

    reactor.process_transport_event(event).await;

    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_process_transport_event_connection_spawned_emits_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(10).await;
    let event = TransportEvent::ConnectionSpawned {
        handle: ConnectionHandle(0),
        effects: vec![],
        remote_address: create_dummy_socket_addr(),
    };

    reactor.process_transport_event(event).await;

    let Some(emitted) = event_tx.pop() else {
        assert_eq!("some", "none", "Expected event in queue");
        unreachable!()
    };
    assert!(reactor.events_emitted);
    assert!(matches!(emitted, RuntimeEvent::ConnectionSpawned { .. }));
}

#[tokio::test]
async fn test_process_transport_event_connection_spawned_queue_full_drops_event() {
    let (mut reactor, _, event_tx, _) = create_test_reactor(1).await;
    let event = TransportEvent::ConnectionSpawned {
        handle: ConnectionHandle(0),
        effects: vec![],
        remote_address: create_dummy_socket_addr(),
    };
    let Ok(()) = event_tx.push(RuntimeEvent::ReactorShutDown) else {
        assert_eq!("ok", "err", "Failed to push event");
        unreachable!()
    };

    reactor.process_transport_event(event).await;

    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_process_transport_event_transmit_executes() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;
    let transmit = Transmit {
        destination: create_dummy_socket_addr(),
        size: 0,
        ecn: None,
        segment_size: None,
        src_ip: None,
    };
    let event = TransportEvent::Transmit(transmit);

    reactor.process_transport_event(event).await;

    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_reactor_initialization_succeeds() {
    let (_cmd_tx, cmd_rx) = mpsc::channel(10);
    let event_tx = Arc::new(ArrayQueue::new(10));
    let waker: WakerCallback = Arc::new(|| {});
    let Ok(socket) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err", "Failed to bind socket");
        unreachable!()
    };
    let endpoint = create_dummy_endpoint(true);

    let reactor = Reactor::new(cmd_rx, endpoint, Arc::clone(&event_tx), vec![socket], waker);

    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_reactor_run_loop_full_tick_and_waker() {
    let (reactor, cmd_tx, event_tx, waker_called) = create_test_reactor(10).await;
    let cmd_create = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(99u64),
        remote_address: create_dummy_socket_addr(),
        server_name: "localhost".to_owned(),
    };
    let cmd_shutdown = RuntimeCommand::Shutdown;
    let local = tokio::task::LocalSet::new();

    local
        .run_until(async move {
            let handle = tokio::task::spawn_local(reactor.run());
            let Ok(()) = cmd_tx.send(cmd_create).await else {
                assert_eq!("ok", "err", "Failed to send command to reactor loop");
                unreachable!()
            };
            tokio::time::sleep(Duration::from_millis(50)).await;
            let Ok(()) = cmd_tx.send(cmd_shutdown).await else {
                assert_eq!("ok", "err", "Failed to send shutdown command");
                unreachable!()
            };
            let Ok(()) = handle.await else {
                assert_eq!("ok", "err", "Reactor task panicked");
                unreachable!()
            };
        })
        .await;

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none", "Expected event emitted by loop");
        unreachable!()
    };
    assert!(waker_called.load(Ordering::SeqCst));
    assert!(matches!(event, RuntimeEvent::CommandFailed { .. }));
}

#[tokio::test]
async fn test_reactor_run_loop_processes_multiple_datagrams() {
    let (reactor, cmd_tx, event_tx, _) = create_test_reactor(10).await;
    let local = tokio::task::LocalSet::new();
    let Some(bound_socket) = reactor.sockets.first() else {
        assert_eq!("some", "none", "Expected socket in reactor");
        unreachable!()
    };
    let Ok(addr) = bound_socket.socket.local_addr() else {
        assert_eq!("ok", "err", "Failed to read local socket address");
        unreachable!()
    };
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err", "Failed to bind sender socket");
        unreachable!()
    };

    for _ in 0..3 {
        let Ok(_) = sender.send_to(b"test", addr).await else {
            assert_eq!("ok", "err", "Failed to send datagram");
            unreachable!()
        };
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    local
        .run_until(async move {
            let handle = tokio::task::spawn_local(reactor.run());
            let Ok(()) = cmd_tx.send(RuntimeCommand::Shutdown).await else {
                assert_eq!("ok", "err", "Failed to send shutdown command");
                unreachable!()
            };
            let Ok(()) = handle.await else {
                assert_eq!("ok", "err", "Reactor task panicked");
                unreachable!()
            };
        })
        .await;

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none", "Expected shutdown event in queue");
        unreachable!()
    };
    assert!(matches!(event, RuntimeEvent::ReactorShutDown));
}

#[tokio::test]
async fn test_reactor_run_loop_terminates_on_shutdown() {
    let (reactor, cmd_tx, event_tx, _) = create_test_reactor(10).await;
    let local = tokio::task::LocalSet::new();
    let cmd_shutdown = RuntimeCommand::Shutdown;

    local
        .run_until(async move {
            let handle = tokio::task::spawn_local(reactor.run());
            let Ok(()) = cmd_tx.send(cmd_shutdown).await else {
                assert_eq!("ok", "err", "Failed to send shutdown command");
                unreachable!()
            };
            let Ok(()) = handle.await else {
                assert_eq!("ok", "err", "Reactor task panicked");
                unreachable!()
            };
        })
        .await;

    let Some(event) = event_tx.pop() else {
        assert_eq!("some", "none", "Expected shutdown event in queue");
        unreachable!()
    };
    assert!(matches!(event, RuntimeEvent::ReactorShutDown));
}

#[tokio::test]
async fn test_reactor_udp_receive_loop_forwards_datagrams() {
    let (mut reactor, _, _, _) = create_test_reactor(10).await;
    let payload = b"hello_world";
    let Some(bound_socket) = reactor.sockets.first() else {
        assert_eq!("some", "none", "Expected socket in reactor");
        unreachable!()
    };
    let Ok(addr) = bound_socket.socket.local_addr() else {
        assert_eq!("ok", "err", "Failed to read local socket address");
        unreachable!()
    };
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err", "Failed to bind sender socket");
        unreachable!()
    };
    let Ok(sender_addr) = sender.local_addr() else {
        assert_eq!("ok", "err", "Failed to get sender address");
        unreachable!()
    };

    let Ok(size) = sender.send_to(payload, addr).await else {
        assert_eq!("ok", "err", "Failed to transmit datagram");
        unreachable!()
    };
    let Some((remote, local, data)) = reactor.datagram_rx.recv().await else {
        assert_eq!("some", "none", "Reactor failed to receive datagram");
        unreachable!()
    };

    assert_eq!(size, 11);
    assert_eq!(data.as_ref(), payload);
    assert_eq!(remote.port(), sender_addr.port());
    assert!(local.is_some());
}

#[tokio::test]
async fn test_reactor_udp_slab_reallocation_is_triggered() {
    let (mut reactor, _, _, _) = create_test_reactor(100).await;
    let payload = vec![0u8; 1200];
    let mut count = 0;
    let Some(bound_socket) = reactor.sockets.first() else {
        assert_eq!("some", "none", "Expected socket in reactor");
        unreachable!()
    };
    let Ok(addr) = bound_socket.socket.local_addr() else {
        assert_eq!("ok", "err", "Failed to read local socket address");
        unreachable!()
    };
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
        assert_eq!("ok", "err", "Failed to bind sender socket");
        unreachable!()
    };

    for _ in 0..60 {
        let Ok(size) = sender.send_to(&payload, addr).await else {
            assert_eq!("ok", "err", "Failed to transmit datagram");
            unreachable!()
        };
        assert_eq!(size, 1200);
    }
    loop {
        let result =
            tokio::time::timeout(Duration::from_millis(50), reactor.datagram_rx.recv()).await;
        if let Ok(Some(_)) = result {
            count += 1;
            if count == 60 {
                break;
            }
        } else {
            break;
        }
    }

    assert_eq!(count, 60);
}

#[tokio::test]
async fn test_send_transmit_executes_safely() {
    let (reactor, _, _, _) = create_test_reactor(10).await;
    let transmit = Transmit {
        destination: create_dummy_socket_addr(),
        size: 0,
        ecn: None,
        segment_size: None,
        src_ip: None,
    };

    reactor.send_transmit(&transmit).await;

    assert!(!reactor.events_emitted);
}

#[tokio::test]
async fn test_send_transmit_segments_data_safely() {
    let (mut reactor, _, _, _) = create_client_reactor(10).await;
    let remote = create_dummy_socket_addr();
    let now = Instant::now();

    let Ok(_) = reactor.endpoint.connect(remote, "localhost", now) else {
        assert_eq!("ok", "err", "Failed to connect endpoint");
        unreachable!()
    };
    let Some(mut transmit) = reactor.endpoint.poll_transmit(now) else {
        assert_eq!("some", "none", "Expected transmit after connect");
        unreachable!()
    };

    transmit.segment_size = Some(100);
    reactor.send_transmit(&transmit).await;

    assert!(!reactor.events_emitted);
}
