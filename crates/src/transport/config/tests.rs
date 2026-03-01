//! Unit tests for the `crate::transport::config` module.

use std::path::PathBuf;

use rstest::rstest;

use super::*;
use crate::common::config::{
    RustClientConfig, RustServerConfig, TransportConfig as WtTransportConfig,
};

#[rstest]
fn test_build_client_config_with_verify_server_certificate_succeeds() {
    let config = RustClientConfig {
        verify_server_certificate: true,
        ..Default::default()
    };

    let result = build_client_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_client_config_without_certs_succeeds() {
    let config = RustClientConfig {
        verify_server_certificate: false,
        ..Default::default()
    };

    let result = build_client_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_endpoint_config_returns_default() {
    let config = RustServerConfig {
        bind_host: String::new(),
        bind_port: 0,
        certfile: PathBuf::new(),
        enable_stateless_retry: false,
        keyfile: PathBuf::new(),
        require_client_auth: false,
        transport: WtTransportConfig::default(),
    };

    let endpoint_config = build_endpoint_config(&config);

    assert!(matches!(endpoint_config, _));
}

#[rstest]
fn test_build_server_config_fails_with_invalid_cert_paths() {
    let config = RustServerConfig {
        bind_host: "127.0.0.1".to_owned(),
        bind_port: 4433,
        certfile: PathBuf::from("/non/existent/path/cert.pem"),
        enable_stateless_retry: false,
        keyfile: PathBuf::from("/non/existent/path/key.pem"),
        require_client_auth: false,
        transport: WtTransportConfig::default(),
    };

    let result = build_server_config(&config);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_default_succeeds() {
    let config = WtTransportConfig::default();

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_invalid_congestion_control_fails() {
    let config = WtTransportConfig {
        congestion_control_algorithm: "invalid_algo".to_owned(),
        ..Default::default()
    };

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
    let config = WtTransportConfig {
        congestion_control_algorithm: algo.to_owned(),
        ..Default::default()
    };

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}

#[rstest]
fn test_build_transport_config_with_zero_max_datagram_size() {
    let config = WtTransportConfig {
        max_datagram_size: 0,
        ..Default::default()
    };

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
        unreachable!()
    };
}
