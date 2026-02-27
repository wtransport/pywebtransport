//! Unit tests for the `crate::transport::config` module.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use rstest::{fixture, rstest};

use super::*;
use crate::common::config::{
    RustClientConfig, RustServerConfig, TransportConfig as WtTransportConfig,
};

#[fixture]
fn temp_cert_file() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push("dummy_cert.pem");
    let Ok(mut file) = File::create(&path) else {
        assert_eq!("ok", "err", "Failed to create temp cert file");
        unreachable!()
    };
    let cert_data = b"-----BEGIN CERTIFICATE-----\n\
        MIICXTCCAcWgAwIBAgIUHTYyNzkzODAzMjQxMzUzMTM5MA0GCSqGSIb3DQEBCwUA\n\
        MB4xHDAaBgNVBAMTE2R1bW15X3Rlc3RfY2VydGlmaWNhdGUwHhcNMjMwMTAxMDAw\n\
        MDAwWhcNMzMwMTAxMDAwMDAwWjAeMRwwGgYDVQQDExNkdW1teV90ZXN0X2NlcnRp\n\
        ZmljYXRlMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzqXg7x7B+j4u\n\
        Y+Yy3V+1V2z1g2pG6y4M4o+pY9nO7m4U9vV2c5vV2r2q0u5q8u6v5V2r2q0u5q8\n\
        v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2\n\
        v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2\n\
        v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2\n\
        v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2\n\
        AwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBV2r2q0u5q8v5V2r2q0u5q8v5V2r2q\n\
        v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2\n\
        v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2\n\
        v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2\n\
        v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2r2q0u5q8v5V2\n\
        -----END CERTIFICATE-----\n";
    let Ok(()) = file.write_all(cert_data) else {
        assert_eq!("ok", "err", "Failed to write temp cert file");
        unreachable!()
    };
    path
}

#[fixture]
fn temp_key_file() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push("dummy_key.pem");
    let Ok(mut file) = File::create(&path) else {
        assert_eq!("ok", "err", "Failed to create temp key file");
        unreachable!()
    };
    let key_data = b"-----BEGIN PRIVATE KEY-----\n\
        MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDOpeDvHsH6Pi5j\n\
        5jLdX7VXbPWDakbrLgzij6lj2c7ubhT29XZzm9XavaqS7mry7q/lXavaqS7mry/l\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        AgMBAAECggEAMIIBCgKCAQEAzqXg7x7B+j4uY+Yy3V+1V2z1g2pG6y4M4o+pY9nO\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        XavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXavaqS7mry/lXava\n\
        -----END PRIVATE KEY-----\n";
    let Ok(()) = file.write_all(key_data) else {
        assert_eq!("ok", "err", "Failed to write temp key file");
        unreachable!()
    };
    path
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
fn test_build_transport_config_with_default_succeeds() {
    let config = WtTransportConfig::default();

    let result = build_transport_config(&config);

    let Ok(_) = result else {
        assert_eq!("ok", "err", "Expected ok");
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

#[rstest]
fn test_load_certs_fails_with_invalid_path() {
    let path = Path::new("/non/existent/path/certs.pem");

    let result = load_certs(path);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
        unreachable!()
    };
}

#[rstest]
fn test_load_private_key_fails_with_invalid_path() {
    let path = Path::new("/non/existent/path/key.pem");

    let result = load_private_key(path);

    let Err(_) = result else {
        assert_eq!("err", "ok", "Expected err");
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
