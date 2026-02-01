//! Unit tests for the `crate::common::config` module.

use std::path::PathBuf;

use rstest::*;

use super::*;

#[test]
fn test_config_clone_trait_behavior_success() {
    let original_client = RustClientConfig {
        user_agent: Some("Original".to_owned()),
        ..Default::default()
    };
    let cert_path = PathBuf::from("cert.pem");
    let key_path = PathBuf::from("key.pem");
    let original_server = RustServerConfig {
        bind_host: "localhost".to_owned(),
        bind_port: 4433,
        certfile: cert_path.clone(),
        keyfile: key_path.clone(),
        require_client_auth: false,
        transport: TransportConfig::default(),
    };

    let cloned_client = original_client.clone();
    let cloned_server = original_server.clone();

    assert_eq!(original_client.user_agent, cloned_client.user_agent);
    assert_eq!(format!("{original_client:?}"), format!("{cloned_client:?}"));
    assert_eq!(original_server.bind_port, cloned_server.bind_port);
    assert_eq!(original_server.certfile, cloned_server.certfile);
    assert_eq!(format!("{original_server:?}"), format!("{cloned_server:?}"));
}

#[test]
fn test_rust_client_config_default_values_sanity_check_success() {
    let client_config = RustClientConfig::default();

    assert!(client_config.verify_server_certificate);
    assert!(client_config.headers.is_empty());
    assert!(client_config.ca_certs.is_none());
    assert!(client_config.certfile.is_none());
    assert!(client_config.keyfile.is_none());
    assert!(client_config.transport.max_connections > 0);
}

#[test]
fn test_rust_client_config_modification_success() {
    let custom_path = PathBuf::from("/tmp/cert.pem");
    let custom_header = ("User-Agent".to_owned(), "TestClient/1.0".to_owned());
    let mut config = RustClientConfig {
        certfile: Some(custom_path.clone()),
        ..Default::default()
    };

    config.headers.push(custom_header.clone());
    config.transport.max_datagram_size = 1500;

    assert_eq!(config.certfile, Some(custom_path));
    assert_eq!(config.headers.len(), 1);
    assert_eq!(config.headers.first(), Some(&custom_header));
    assert_eq!(config.transport.max_datagram_size, 1500);
}

#[rstest]
#[case("0.0.0.0", 443)]
#[case("127.0.0.1", 8080)]
fn test_rust_server_config_manual_instantiation_success(
    #[case] bind_host: String,
    #[case] bind_port: u16,
) {
    let certfile = PathBuf::from("server.crt");
    let keyfile = PathBuf::from("server.key");
    let transport = TransportConfig::default();

    let server_config = RustServerConfig {
        bind_host: bind_host.clone(),
        bind_port,
        certfile: certfile.clone(),
        keyfile: keyfile.clone(),
        require_client_auth: true,
        transport,
    };

    assert_eq!(server_config.bind_host, bind_host);
    assert_eq!(server_config.bind_port, bind_port);
    assert_eq!(server_config.certfile, certfile);
    assert_eq!(server_config.keyfile, keyfile);
    assert!(server_config.require_client_auth);
    assert!(server_config.transport.max_connections > 0);
}

#[test]
fn test_transport_config_default_values_sanity_check_success() {
    let config = TransportConfig::default();

    assert!(!config.alpn_protocols.is_empty());
    assert!(!config.congestion_control_algorithm.is_empty());
    assert!(config.max_connections > 0);
    assert!(config.max_stream_read_buffer > 0);
    assert!(config.read_timeout.is_some());
    assert!(config.write_timeout.is_some());
    assert!(config.close_timeout.as_nanos() > 0);
}

#[rstest]
#[case(false, 2048)]
#[case(true, 1024)]
fn test_transport_config_initialization_override_success(
    #[case] keep_alive: bool,
    #[case] max_message_size: u64,
) {
    let config = TransportConfig {
        keep_alive,
        max_message_size,
        ..Default::default()
    };

    assert_eq!(config.keep_alive, keep_alive);
    assert_eq!(config.max_message_size, max_message_size);
}
