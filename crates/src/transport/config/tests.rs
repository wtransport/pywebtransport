//! Unit tests for the `crate::transport::config` module.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use rstest::rstest;

use super::*;
use crate::common::config::{RustBaseConfig, RustClientConfig, RustServerConfig};
use crate::tls::certificate;

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
#[case(
    None,
    Some(PathBuf::from("/non/existent/cert.pem")),
    Some(PathBuf::from("/non/existent/key.pem"))
)]
#[case(Some(PathBuf::from("/non/existent/ca.pem")), None, None)]
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
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_client_config_with_client_auth_cert_succeeds() {
    let output_dir = env::temp_dir().join("pywebtransport_test_client_auth");
    let Ok((_ca_path, cert_path, key_path)) =
        certificate::generate_self_signed_cert("localhost", &output_dir, 1)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let mut config = mock_client_config();
    config.certfile = Some(PathBuf::from(cert_path));
    config.keyfile = Some(PathBuf::from(key_path));

    let result = build_client_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[rstest]
fn test_build_client_config_with_custom_ca_succeeds() {
    let output_dir = env::temp_dir().join("pywebtransport_test_client_ca");
    let Ok((ca_path, _cert_path, _key_path)) =
        certificate::generate_self_signed_cert("localhost", &output_dir, 1)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let mut config = mock_client_config();
    config.ca_certs = Some(PathBuf::from(ca_path));

    let result = build_client_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[rstest]
fn test_build_client_config_with_verify_server_certificate_succeeds() {
    let mut config = mock_client_config();
    config.verify_server_certificate = true;

    let result = build_client_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[rstest]
fn test_build_client_config_without_certs_succeeds() {
    let mut config = mock_client_config();
    config.verify_server_certificate = false;

    let result = build_client_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[rstest]
fn test_build_endpoint_config_fails_with_oversized_max_udp_payload_size() {
    let mut config = mock_base_config();
    config.max_udp_payload_size = u64::from(u16::MAX) + 1;

    let result = build_endpoint_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_endpoint_config_fails_with_undersized_max_udp_payload_size() {
    let mut config = mock_base_config();
    config.max_udp_payload_size = 100;

    let result = build_endpoint_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_endpoint_config_with_default_succeeds() {
    let config = mock_base_config();

    let result = build_endpoint_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
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
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_server_config_with_client_auth_succeeds() {
    let output_dir = env::temp_dir().join("pywebtransport_test_server_client_auth");
    let Ok((ca_path, cert_path, key_path)) =
        certificate::generate_self_signed_cert("localhost", &output_dir, 1)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let mut config = mock_server_config();
    config.certfile = PathBuf::from(cert_path);
    config.keyfile = PathBuf::from(key_path);
    config.require_client_auth = true;
    config.ca_certs = Some(PathBuf::from(ca_path));

    let result = build_server_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[rstest]
fn test_build_server_config_with_client_auth_without_ca_fails() {
    let output_dir = env::temp_dir().join("pywebtransport_test_server_client_auth_without_ca");
    let Ok((_ca_path, cert_path, key_path)) =
        certificate::generate_self_signed_cert("localhost", &output_dir, 1)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let mut config = mock_server_config();
    config.certfile = PathBuf::from(cert_path);
    config.keyfile = PathBuf::from(key_path);
    config.require_client_auth = true;
    config.ca_certs = None;

    let result = build_server_config(&config);

    let Err(WebTransportError::Configuration(None, _)) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_server_config_with_valid_certs_succeeds() {
    let output_dir = env::temp_dir().join("pywebtransport_test_server_valid");
    let Ok((_ca_path, cert_path, key_path)) =
        certificate::generate_self_signed_cert("localhost", &output_dir, 1)
    else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let mut config = mock_server_config();
    config.certfile = PathBuf::from(cert_path);
    config.keyfile = PathBuf::from(key_path);
    config.require_client_auth = false;

    let result = build_server_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_bidirectional_streams() {
    let mut config = mock_base_config();
    config.max_streams_bidi = u64::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_connection_receive_window() {
    let mut config = mock_base_config();
    config.connection_receive_window = u64::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_idle_timeout() {
    let mut config = mock_base_config();
    config.max_idle_timeout = Duration::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_stream_receive_window() {
    let mut config = mock_base_config();
    config.stream_receive_window = u64::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_fails_with_invalid_unidirectional_streams() {
    let mut config = mock_base_config();
    config.max_streams_uni = u64::MAX;

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_default_succeeds() {
    let config = mock_base_config();

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_invalid_congestion_control_fails() {
    let mut config = mock_base_config();
    config.congestion_control_algorithm = "invalid_algo".to_owned();

    let result = build_transport_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[rstest]
#[case("bbr")]
#[case("cubic")]
#[case("reno")]
fn test_build_transport_config_with_valid_congestion_control(#[case] algorithm: &str) {
    let mut config = mock_base_config();
    config.congestion_control_algorithm = algorithm.to_owned();

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_zero_max_datagram_size() {
    let mut config = mock_base_config();
    config.max_datagram_size = 0;

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}
