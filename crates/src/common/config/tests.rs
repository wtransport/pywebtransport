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
    assert!(
        base_size <= 384,
        "RustBaseConfig is too large ({base_size} bytes). Consider boxing large fields."
    );

    let client_size = size_of::<RustClientConfig>();
    assert!(
        client_size <= 512,
        "RustClientConfig is too large ({client_size} bytes)."
    );

    let server_size = size_of::<RustServerConfig>();
    assert!(
        server_size <= 512,
        "RustServerConfig is too large ({server_size} bytes)."
    );
}

#[test]
fn test_config_structs_instantiation_and_derives() {
    let base = RustBaseConfig {
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
        quic_receive_window: 15_728_640,
        quic_send_window: 15_728_640,
        quic_stream_receive_window: 2_097_152,
        resource_cleanup_interval: Duration::from_secs(5),
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
