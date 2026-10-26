//! Unit tests for the `crate::transport::endpoint` module.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use quinn_proto::crypto::rustls::{
    QuicClientConfig as QuicCryptoClientConfig, QuicServerConfig as QuicCryptoServerConfig,
};
use quinn_proto::{
    ClientConfig as QuicClientConfig, ConnectionHandle as QuicConnectionHandle,
    Endpoint as QuicEndpoint, EndpointConfig as QuicEndpointConfig,
    ServerConfig as QuicServerConfig, TransportConfig as QuicTransportConfig,
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
use crate::common::config::{RustBaseConfig, RustServerConfig};
use crate::common::constants::{
    ERR_H3_NO_ERROR, ERR_LIB_CONNECTION_LIMIT_EXCEEDED, ERR_LIB_CONNECTION_STATE_ERROR,
    ERR_LIB_INTERNAL_ERROR,
};
use crate::protocol::events::{Effect, ProtocolEvent};

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

fn create_quic_server_endpoint() -> QuicEndpoint {
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
    let Ok(crypto) = QuicCryptoServerConfig::try_from(crypto) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    QuicEndpoint::new(
        Arc::new(QuicEndpointConfig::default()),
        Some(Arc::new(QuicServerConfig::with_crypto(Arc::new(crypto)))),
        false,
        None,
    )
}

fn create_test_client_endpoint() -> TransportEndpoint {
    let quic_endpoint =
        QuicEndpoint::new(Arc::new(QuicEndpointConfig::default()), None, false, None);
    let base_config = mock_base_config();
    let client_config = create_dummy_client_config();

    let Ok(endpoint) = TransportEndpoint::new_client(quic_endpoint, base_config, client_config)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    endpoint
}

fn create_test_server_endpoint() -> TransportEndpoint {
    let quic_endpoint =
        QuicEndpoint::new(Arc::new(QuicEndpointConfig::default()), None, false, None);
    let base_config = mock_base_config();
    let server_config = create_dummy_rust_server_config();

    let Ok(endpoint) = TransportEndpoint::new_server(quic_endpoint, base_config, server_config)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    endpoint
}

fn deliver_transmits(
    source: &mut TransportEndpoint,
    destination: &mut TransportEndpoint,
    source_address: SocketAddr,
    now: Instant,
) -> Vec<TransportEvent> {
    let mut events = Vec::new();

    while let Some(transmit) = source.poll_transmit(now) {
        let payload = source
            .transmit_workspace()
            .get(..transmit.size)
            .unwrap_or_default();
        let segment_size = transmit.segment_size.unwrap_or(transmit.size);

        for datagram in payload.chunks(segment_size) {
            events.push(destination.handle_datagram(
                source_address,
                None,
                BytesMut::from(datagram),
                0.0,
                now,
            ));
        }
    }

    events
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
        max_session_pending_events: 100,
        max_sessions: 10,
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

#[rstest]
#[case("127.0.0.1", 8080)]
#[case("localhost", 443)]
fn test_connect_creates_connection_and_routes_correctly(#[case] host: &str, #[case] port: u16) {
    let mut endpoint = create_test_client_endpoint();
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
    let now = Instant::now();

    let TransportEvent::ConnectionSpawned {
        remote_address: spawned_remote_address,
        ..
    } = endpoint.connect(1, remote_address, host, now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert_eq!(spawned_remote_address.port(), port);
}

#[test]
fn test_connect_exceeding_max_connections_returns_error() {
    let mut endpoint = create_test_client_endpoint();
    endpoint.base_config.max_connections = 1;
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();

    let first_event = endpoint.connect(1, remote_address, "localhost", now);
    let second_event = endpoint.connect(2, remote_address, "localhost", now);

    assert!(matches!(
        first_event,
        TransportEvent::ConnectionSpawned { .. }
    ));
    assert!(matches!(
        second_event,
        TransportEvent::CommandFailed {
            request_id: 2,
            error_code: Some(ERR_LIB_CONNECTION_LIMIT_EXCEEDED),
            ..
        }
    ));
    assert_eq!(endpoint.connections.len(), 1);
}

#[test]
fn test_connect_on_server_endpoint_returns_error() {
    let mut endpoint = create_test_server_endpoint();
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();

    let event = endpoint.connect(1, remote_address, "localhost", now);

    assert!(matches!(
        event,
        TransportEvent::CommandFailed {
            request_id: 1,
            error_code: Some(ERR_LIB_INTERNAL_ERROR),
            ..
        }
    ));
}

#[test]
fn test_connect_with_invalid_server_name_returns_error() {
    let mut endpoint = create_test_client_endpoint();
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();

    let event = endpoint.connect(1, remote_address, "", now);

    assert!(matches!(
        event,
        TransportEvent::CommandFailed {
            request_id: 1,
            error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
            ..
        }
    ));
}

#[test]
fn test_connect_with_zero_timers_sets_none_boundaries() {
    let mut endpoint = create_test_client_endpoint();
    endpoint.base_config.resource_cleanup_interval = Duration::ZERO;
    endpoint.base_config.pending_event_ttl = Duration::ZERO;
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();

    let event = endpoint.connect(1, remote_address, "localhost", now);

    assert!(matches!(event, TransportEvent::ConnectionSpawned { .. }));
}

#[test]
fn test_handle_datagram_accepts_new_connection() {
    let mut client = create_test_client_endpoint();
    let Ok(mut server) = TransportEndpoint::new_server(
        create_quic_server_endpoint(),
        mock_base_config(),
        create_dummy_rust_server_config(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let client_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 50000);
    let server_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    client.connect(1, server_address, "localhost", now);

    let server_events = deliver_transmits(&mut client, &mut server, client_address, now);
    let mut client_events = Vec::new();
    loop {
        let events = deliver_transmits(&mut server, &mut client, server_address, now);
        if events.is_empty() {
            break;
        }
        client_events.extend(events);
        deliver_transmits(&mut client, &mut server, client_address, now);
    }

    assert!(matches!(
        server_events.first(),
        Some(TransportEvent::ConnectionSpawned {
            remote_address,
            ..
        }) if *remote_address == client_address
    ));
    assert_eq!(server.connections.len(), 1);
    assert!(client_events.iter().any(|event| matches!(
        event,
        TransportEvent::ConnectionEffects { effects, .. }
            if effects.iter().any(|effect| matches!(
                effect,
                Effect::NotifyRequestDone { request_id: 1, .. }
            ))
    )));
}

#[test]
fn test_handle_datagram_exceeding_max_connections_refuses() {
    let mut client = create_test_client_endpoint();
    let mut base_config = mock_base_config();
    base_config.max_connections = 0;
    let Ok(mut server) = TransportEndpoint::new_server(
        create_quic_server_endpoint(),
        base_config,
        create_dummy_rust_server_config(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let client_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 50000);
    let server_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    client.connect(1, server_address, "localhost", now);

    let server_events = deliver_transmits(&mut client, &mut server, client_address, now);

    assert!(matches!(
        server_events.as_slice(),
        [TransportEvent::Transmit(transmit)] if transmit.destination == client_address
    ));
    assert!(server.connections.is_empty());
}

#[test]
fn test_handle_datagram_on_client_endpoint_refuses_incoming_connection() {
    let mut client = create_test_client_endpoint();
    let Ok(mut peer) = TransportEndpoint::new_client(
        create_quic_server_endpoint(),
        mock_base_config(),
        create_dummy_client_config(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let client_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 50000);
    let server_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    client.connect(1, server_address, "localhost", now);

    let peer_events = deliver_transmits(&mut client, &mut peer, client_address, now);

    assert!(matches!(
        peer_events.as_slice(),
        [TransportEvent::Transmit(transmit)] if transmit.destination == client_address
    ));
    assert!(peer.connections.is_empty());
}

#[test]
fn test_handle_datagram_unknown_connection_returns_consumed() {
    let mut client = create_test_client_endpoint();
    let Ok(mut server) = TransportEndpoint::new_server(
        create_quic_server_endpoint(),
        mock_base_config(),
        create_dummy_rust_server_config(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let client_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 50000);
    let server_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    client.connect(1, server_address, "localhost", now);
    deliver_transmits(&mut client, &mut server, client_address, now);
    deliver_transmits(&mut server, &mut client, server_address, now);
    server.connections.clear();

    let server_events = deliver_transmits(&mut client, &mut server, client_address, now);

    assert!(!server_events.is_empty());
    assert!(
        server_events
            .iter()
            .all(|event| matches!(event, TransportEvent::Consumed))
    );
}

#[test]
fn test_handle_datagram_unsupported_version_returns_transmit() {
    let Ok(mut server) = TransportEndpoint::new_server(
        create_quic_server_endpoint(),
        mock_base_config(),
        create_dummy_rust_server_config(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let client_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 50000);
    let mut data = BytesMut::new();
    data.extend_from_slice(&[0xC0, 0x1A, 0x2A, 0x3A, 0x4A, 0x08]);
    data.extend_from_slice(&[0x01; 8]);
    data.extend_from_slice(&[0x08]);
    data.extend_from_slice(&[0x02; 8]);
    data.resize(1200, 0);

    let event = server.handle_datagram(client_address, None, data, 0.0, Instant::now());

    assert!(matches!(
        event,
        TransportEvent::Transmit(transmit) if transmit.destination == client_address
    ));
}

#[test]
fn test_handle_datagram_without_connections_returns_consumed() {
    let mut endpoint = create_test_client_endpoint();
    let data = BytesMut::from(&b"dummy_packet"[..]);
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now_instant = Instant::now();

    let event = endpoint.handle_datagram(remote_address, None, data, 0.0, now_instant);

    assert!(matches!(event, TransportEvent::Consumed));
}

#[test]
fn test_handle_protocol_event_with_invalid_handle_returns_command_failed() {
    let mut endpoint = create_test_client_endpoint();
    let handle = QuicConnectionHandle(999);
    let event = ProtocolEvent::InternalCleanupResources;
    let now = Instant::now();

    let transport_event = endpoint.handle_protocol_event(handle, 100, event, 0.0, now);

    assert!(matches!(
        transport_event,
        TransportEvent::CommandFailed {
            request_id: 100,
            error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
            ..
        }
    ));
}

#[test]
fn test_handle_protocol_event_yielding_effects_collects_properly() {
    let mut endpoint = create_test_client_endpoint();
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { handle, .. } =
        endpoint.connect(1, remote_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let event = ProtocolEvent::UserDiagnoseConnection { request_id: 100 };

    let transport_event = endpoint.handle_protocol_event(handle, 100, event, 0.0, now);

    assert!(matches!(
        transport_event,
        TransportEvent::ConnectionEffects { effects, .. }
            if effects.iter().any(|effect| matches!(
                effect,
                Effect::NotifyRequestDone { request_id: 100, .. }
            ))
    ));
}

#[test]
fn test_handle_timeout_removes_drained_connection() {
    let mut endpoint = create_test_client_endpoint();
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { handle, .. } =
        endpoint.connect(1, remote_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    endpoint.handle_protocol_event(
        handle,
        2,
        ProtocolEvent::UserCloseConnection {
            request_id: 2,
            error_code: 0,
            reason: None,
        },
        0.0,
        now,
    );

    endpoint.handle_timeout(60.0, now + Duration::from_mins(1));

    assert!(endpoint.connections.is_empty());
}

#[test]
fn test_handle_timeout_reports_connection_effects() {
    let mut client = create_test_client_endpoint();
    let Ok(mut server) = TransportEndpoint::new_server(
        create_quic_server_endpoint(),
        mock_base_config(),
        create_dummy_rust_server_config(),
    ) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let client_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 50000);
    let server_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { handle, .. } =
        client.connect(1, server_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    loop {
        let server_events = deliver_transmits(&mut client, &mut server, client_address, now);
        let client_events = deliver_transmits(&mut server, &mut client, server_address, now);
        if server_events.is_empty() && client_events.is_empty() {
            break;
        }
    }

    let results = client.handle_timeout(120.0, now + Duration::from_mins(2));

    assert!(matches!(
        results.as_slice(),
        [(result_handle, effects)] if *result_handle == handle
            && effects.iter().any(|effect| matches!(
                effect,
                Effect::EmitConnectionEvent {
                    error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                    ..
                }
            ))
    ));
}

#[test]
fn test_handle_timeout_with_no_connections_returns_empty() {
    let mut endpoint = create_test_client_endpoint();
    let now = Instant::now();

    let results = endpoint.handle_timeout(0.0, now);

    assert!(results.is_empty());
}

#[test]
fn test_poll_timeout_with_multiple_connections_returns_minimum() {
    let mut endpoint = create_test_client_endpoint();
    let remote_address1 = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let remote_address2 = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 444);
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { .. } =
        endpoint.connect(1, remote_address1, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let TransportEvent::ConnectionSpawned { .. } =
        endpoint.connect(2, remote_address2, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let timeout = endpoint.poll_timeout();

    assert!(timeout.is_some());
}

#[test]
fn test_poll_timeout_with_no_connections_returns_none() {
    let mut endpoint = create_test_client_endpoint();

    let timeout = endpoint.poll_timeout();

    assert!(timeout.is_none());
}

#[test]
fn test_poll_transmit_with_no_connections_returns_none() {
    let mut endpoint = create_test_client_endpoint();
    let now = Instant::now();

    let transmit = endpoint.poll_transmit(now);

    assert!(transmit.is_none());
}

#[test]
fn test_routing_with_active_connection() {
    let mut endpoint = create_test_client_endpoint();
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { handle, .. } =
        endpoint.connect(1, remote_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let event = ProtocolEvent::InternalCleanupResources;
    let future_time = now + Duration::from_secs_f64(60.0);

    let transport_event = endpoint.handle_protocol_event(handle, 100, event, 0.0, now);
    let earliest_timeout = endpoint.poll_timeout();
    let timeout_results = endpoint.handle_timeout(60.0, future_time);
    let transmit = endpoint.poll_transmit(now);
    assert!(matches!(
        transport_event,
        TransportEvent::ConnectionEffects {
            handle: event_handle,
            effects,
        } if event_handle == handle && effects.is_empty()
    ));
    assert!(earliest_timeout.is_some());
    assert!(timeout_results.is_empty());
    assert!(transmit.is_some());
    assert_eq!(endpoint.connections.len(), 1);
}

#[test]
fn test_server_endpoint_creation_succeeds() {
    let endpoint = create_test_server_endpoint();

    assert!(endpoint.client_config.is_none());
    assert!(endpoint.server_config.is_some());
}

#[test]
fn test_shutdown_closes_connections_through_protocol_core() {
    let mut endpoint = create_test_client_endpoint();
    let remote_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    let TransportEvent::ConnectionSpawned { handle, .. } =
        endpoint.connect(1, remote_address, "localhost", now)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let results = endpoint.shutdown(0.0, now);

    assert!(matches!(
        results.as_slice(),
        [(result_handle, effects)] if *result_handle == handle && matches!(
            effects.as_slice(),
            [
                Effect::EmitConnectionEvent {
                    error_code: Some(ERR_H3_NO_ERROR),
                    ..
                },
                Effect::NotifyRequestFailed {
                    request_id: 1,
                    error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                    reason,
                    ..
                }
            ] if reason == "wt_connection close"
        )
    ));
}

#[test]
fn test_transmit_workspace_is_initially_empty() {
    let endpoint = create_test_client_endpoint();

    let workspace = endpoint.transmit_workspace();

    assert!(workspace.is_empty());
}
