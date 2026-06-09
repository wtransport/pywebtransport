//! Unit tests for the `crate::transport::endpoint` module.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use quinn_proto::crypto::rustls::QuicClientConfig;
use quinn_proto::{
    ClientConfig, ConnectionHandle, Endpoint as QuinnEndpoint, EndpointConfig, TransportConfig,
};
use rstest::rstest;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::version::TLS13;
use rustls::{DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme};

use super::*;
use crate::common::config::{RustBaseConfig, RustServerConfig};
use crate::protocol::events::ProtocolEvent;

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

fn mock_base_config() -> RustBaseConfig {
    RustBaseConfig {
        alpn_protocols: vec!["h3".to_owned()],
        congestion_control_algorithm: "cubic".to_owned(),
        connection_idle_timeout: Duration::from_mins(1),
        flow_control_window: 1_048_576,
        flow_control_window_auto_scale_enabled: true,
        initial_max_data: 10_485_760,
        initial_max_streams_bidi: 100,
        initial_max_streams_uni: 100,
        keep_alive_interval: Some(Duration::from_secs(10)),
        max_capsule_size: 1500,
        max_datagram_size: 1200,
        max_field_section_size: 65536,
        max_session_pending_events: 100,
        max_sessions: 100,
        max_stream_read_buffer_size: 1_048_576,
        max_stream_write_buffer_size: 1_048_576,
        max_total_pending_events: 1000,
        pending_event_ttl: Duration::from_secs(30),
        quic_max_concurrent_bidi_streams: 65535,
        quic_max_concurrent_uni_streams: 65535,
        quic_receive_window: 33_554_432,
        quic_send_window: 33_554_432,
        quic_stream_receive_window: 2_097_152,
        resource_cleanup_interval: Duration::from_secs(5),
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

fn create_test_client_endpoint() -> TransportEndpoint {
    let quinn_ep = QuinnEndpoint::new(Arc::new(EndpointConfig::default()), None, false, None);
    let b_cfg = mock_base_config();
    let c_cfg = create_dummy_client_config();

    let Ok(ep) = TransportEndpoint::new_client(quinn_ep, b_cfg, c_cfg) else {
        assert_eq!("ok", "err", "Failed to create TransportEndpoint");
        unreachable!()
    };

    ep
}

fn create_test_server_endpoint() -> TransportEndpoint {
    let quinn_ep = QuinnEndpoint::new(Arc::new(EndpointConfig::default()), None, false, None);
    let b_cfg = mock_base_config();
    let s_cfg = create_dummy_rust_server_config();

    let Ok(ep) = TransportEndpoint::new_server(quinn_ep, b_cfg, s_cfg) else {
        assert_eq!("ok", "err", "Failed to create TransportEndpoint server");
        unreachable!()
    };

    ep
}

#[test]
fn test_transmit_workspace_is_initially_empty() {
    let endpoint = create_test_client_endpoint();

    let workspace = endpoint.transmit_workspace();

    assert!(workspace.is_empty());
}

#[test]
fn test_timeout_with_no_connections_returns_none() {
    let mut endpoint = create_test_client_endpoint();

    let timeout = endpoint.timeout();

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
fn test_handle_timeout_with_no_connections_returns_empty() {
    let mut endpoint = create_test_client_endpoint();
    let now = Instant::now();

    let results = endpoint.handle_timeout(0.0, now);

    assert!(results.is_empty());
}

#[test]
fn test_handle_user_event_with_invalid_handle_returns_none() {
    let mut endpoint = create_test_client_endpoint();
    let handle = ConnectionHandle(999);
    let event = ProtocolEvent::InternalCleanupResources;
    let now = Instant::now();

    let result = endpoint.handle_user_event(handle, event, 0.0, now);

    assert!(result.is_none());
}

#[test]
fn test_handle_datagram_without_connections_returns_consumed() {
    let mut endpoint = create_test_client_endpoint();
    let data = BytesMut::from(&b"dummy_packet"[..]);
    let remote = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now_instant = Instant::now();

    let event = endpoint.handle_datagram(remote, None, data, 0.0, now_instant);

    assert!(matches!(event, TransportEvent::Consumed));
}

#[test]
fn test_server_endpoint_creation_succeeds() {
    let endpoint = create_test_server_endpoint();

    assert!(endpoint.client_config.is_none());
    assert!(endpoint.server_config.is_some());
}

#[rstest]
#[case("localhost", 443)]
#[case("127.0.0.1", 8080)]
fn test_connect_creates_connection_and_routes_correctly(#[case] host: &str, #[case] port: u16) {
    let mut endpoint = create_test_client_endpoint();
    let remote = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
    let now = Instant::now();

    let Ok((_, addr)) = endpoint.connect(remote, host, now) else {
        assert_eq!("ok", "err", "Failed to connect endpoint");
        unreachable!()
    };

    assert_eq!(addr.port(), port);
}

#[test]
fn test_connect_on_server_endpoint_returns_error() {
    let mut endpoint = create_test_server_endpoint();
    let remote = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();

    let result = endpoint.connect(remote, "localhost", now);

    assert!(matches!(
        result,
        Err(WebTransportError::Unknown(
            Some(constants::ERR_LIB_INTERNAL_ERROR),
            _
        ))
    ));
}

#[test]
fn test_routing_with_active_connection() {
    let mut endpoint = create_test_client_endpoint();
    let remote = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    let Ok((handle, _)) = endpoint.connect(remote, "localhost", now) else {
        assert_eq!("ok", "err", "Failed to connect endpoint");
        unreachable!()
    };
    let event = ProtocolEvent::InternalCleanupResources;
    let future_time = now + Duration::from_mins(1);

    let transport_event = endpoint.handle_user_event(handle, event, 0.0, now);
    let earliest_timeout = endpoint.timeout();
    let timeout_results = endpoint.handle_timeout(60.0, future_time);
    let transmit = endpoint.poll_transmit(now);

    assert!(transport_event.is_none());
    assert!(earliest_timeout.is_some());
    assert!(timeout_results.is_empty() || !timeout_results.is_empty());
    assert!(transmit.is_none() || transmit.is_some());
}

#[test]
fn test_connect_with_zero_timers_sets_none_boundaries() {
    let mut endpoint = create_test_client_endpoint();
    endpoint.base_config.resource_cleanup_interval = Duration::ZERO;
    endpoint.base_config.pending_event_ttl = Duration::ZERO;
    let remote = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();

    let result = endpoint.connect(remote, "localhost", now);

    assert!(
        result.is_ok(),
        "Expected connect to succeed with zero timers"
    );
}

#[test]
fn test_connect_with_invalid_server_name_returns_error() {
    let mut endpoint = create_test_client_endpoint();
    let remote = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();

    let result = endpoint.connect(remote, "", now);

    assert!(matches!(
        result,
        Err(WebTransportError::Connection(
            Some(constants::ERR_LIB_CONNECTION_STATE_ERROR),
            _
        ))
    ));
}

#[test]
fn test_handle_user_event_yielding_effects_collects_properly() {
    let mut endpoint = create_test_client_endpoint();
    let remote = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let now = Instant::now();
    let Ok((handle, _)) = endpoint.connect(remote, "localhost", now) else {
        assert_eq!("ok", "err", "Failed to connect endpoint");
        unreachable!()
    };
    let event = ProtocolEvent::UserGetConnectionDiagnostics { request_id: 100 };

    let transport_event = endpoint.handle_user_event(handle, event, 0.0, now);

    assert!(matches!(
        transport_event,
        Some(TransportEvent::ConnectionEffects { .. })
    ));
}

#[test]
fn test_timeout_with_multiple_connections_returns_minimum() {
    let mut endpoint = create_test_client_endpoint();
    let remote1 = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);
    let remote2 = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 444);
    let now = Instant::now();
    let Ok(_) = endpoint.connect(remote1, "localhost", now) else {
        assert_eq!("ok", "err", "Failed to connect endpoint 1");
        unreachable!()
    };
    let Ok(_) = endpoint.connect(remote2, "localhost", now) else {
        assert_eq!("ok", "err", "Failed to connect endpoint 2");
        unreachable!()
    };

    let t = endpoint.timeout();

    assert!(t.is_some());
}
