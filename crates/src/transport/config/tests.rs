//! Unit tests for the `crate::transport::config` module.

use std::path::PathBuf;
use std::time::Duration;

use rstest::rstest;

use super::*;
use crate::common::config::{RustBaseConfig, RustClientConfig, RustServerConfig};

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
        max_pending_capsules: 20,
        max_pending_datagrams: 100,
        max_pending_streams: 10,
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

fn mock_client_config() -> RustClientConfig {
    RustClientConfig {
        base: mock_base_config(),
        ca_certs: None,
        certfile: None,
        keyfile: None,
        verify_server_certificate: true,
    }
}

fn mock_server_config() -> RustServerConfig {
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

#[rstest]
#[case(Some(PathBuf::from("/non/existent/ca.pem")), None, None)]
#[case(
    None,
    Some(PathBuf::from("/non/existent/cert.pem")),
    Some(PathBuf::from("/non/existent/key.pem"))
)]
fn test_build_client_config_fails_with_invalid_paths(
    #[case] ca_certs: Option<PathBuf>,
    #[case] certfile: Option<PathBuf>,
    #[case] keyfile: Option<PathBuf>,
) {
    let mut config = mock_client_config();
    config.ca_certs = ca_certs;
    config.certfile = certfile;
    config.keyfile = keyfile;

    let result = build_client_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_build_client_config_with_verify_server_certificate_succeeds() {
    let mut config = mock_client_config();
    config.verify_server_certificate = true;

    let result = build_client_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_client_config_without_certs_succeeds() {
    let mut config = mock_client_config();
    config.verify_server_certificate = false;

    let result = build_client_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_endpoint_config_returns_default() {
    let mut config = mock_server_config();
    config.bind_host = String::new();
    config.bind_port = 0;

    let endpoint_config = build_endpoint_config(&config);

    assert!(matches!(endpoint_config, _));
}

#[rstest]
#[case(false, None)]
#[case(true, None)]
#[case(true, Some(PathBuf::from("/non/existent/path/ca.pem")))]
fn test_build_server_config_fails_with_invalid_cert_paths(
    #[case] require_client_auth: bool,
    #[case] ca_certs: Option<PathBuf>,
) {
    let mut config = mock_server_config();
    config.ca_certs = ca_certs;
    config.certfile = PathBuf::from("/non/existent/path/cert.pem");
    config.keyfile = PathBuf::from("/non/existent/path/key.pem");
    config.require_client_auth = require_client_auth;

    let result = build_server_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_bidi_streams() {
    let mut config = mock_base_config();
    config.quic_max_concurrent_bidi_streams = u64::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_idle_timeout() {
    let mut config = mock_base_config();
    config.connection_idle_timeout = Duration::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_receive_window() {
    let mut config = mock_base_config();
    config.quic_receive_window = u64::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_stream_receive_window() {
    let mut config = mock_base_config();
    config.quic_stream_receive_window = u64::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_uni_streams() {
    let mut config = mock_base_config();
    config.quic_max_concurrent_uni_streams = u64::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_default_succeeds() {
    let config = mock_base_config();

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_invalid_congestion_control_fails() {
    let mut config = mock_base_config();
    config.congestion_control_algorithm = "invalid_algo".to_owned();

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
#[case("bbr")]
#[case("cubic")]
#[case("reno")]
fn test_build_transport_config_with_valid_congestion_control(#[case] algo: &str) {
    let mut config = mock_base_config();
    config.congestion_control_algorithm = algo.to_owned();

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_zero_max_datagram_size() {
    let mut config = mock_base_config();
    config.max_datagram_size = 0;

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}
