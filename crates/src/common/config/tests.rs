//! Unit tests for the `crate::common::config` module.

use std::path::PathBuf;
use std::time::Duration;

use super::*;

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn test_config_structs_are_thread_safe() {
    assert_send_sync::<RustBaseConfig>();
    assert_send_sync::<RustClientConfig>();
    assert_send_sync::<RustServerConfig>();
}

#[test]
fn test_config_memory_footprint() {
    let base_size = size_of::<RustBaseConfig>();
    assert!(base_size <= 384);

    let client_size = size_of::<RustClientConfig>();
    assert!(client_size <= 512);

    let server_size = size_of::<RustServerConfig>();
    assert!(server_size <= 512);
}

#[test]
fn test_config_structs_instantiation_and_derives() {
    let base = RustBaseConfig {
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
    };

    let server = RustServerConfig {
        base: base.clone(),
        bind_host: "127.0.0.1".to_owned(),
        bind_port: 4433,
        ca_certs: None,
        certfile: PathBuf::from("/dummy/cert.pem"),
        keyfile: PathBuf::from("/dummy/key.pem"),
        require_client_auth: false,
    };

    assert_eq!(server.bind_port, 4433);
    assert!(server.certfile.ends_with("cert.pem"));

    let server_debug_str = format!("{server:?}");
    assert!(server_debug_str.contains("RustServerConfig"));
    assert!(server_debug_str.contains("bind_port: 4433"));

    let client = RustClientConfig {
        base: base.clone(),
        ca_certs: None,
        certfile: None,
        keyfile: None,
        verify_server_certificate: true,
    };

    assert!(client.verify_server_certificate);

    let client_debug_str = format!("{client:?}");
    assert!(client_debug_str.contains("RustClientConfig"));
    assert!(client_debug_str.contains("verify_server_certificate: true"));
}
