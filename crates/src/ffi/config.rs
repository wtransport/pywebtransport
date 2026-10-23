//! FFI conversion logic for common configuration.

use std::path::PathBuf;
use std::time::Duration;

use pyo3::prelude::*;

use crate::common::config::{RustBaseConfig, RustClientConfig, RustServerConfig};

impl<'a> TryFrom<&Bound<'a, PyAny>> for RustBaseConfig {
    type Error = PyErr;

    fn try_from(config: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let congestion_control_algorithm: String =
            config.getattr("congestion_control_algorithm")?.extract()?;
        let connection_receive_window: u64 =
            config.getattr("connection_receive_window")?.extract()?;
        let connection_send_window: u64 = config.getattr("connection_send_window")?.extract()?;
        let initial_max_data: u64 = config.getattr("initial_max_data")?.extract()?;
        let initial_max_streams_bidi: u64 =
            config.getattr("initial_max_streams_bidi")?.extract()?;
        let initial_max_streams_uni: u64 = config.getattr("initial_max_streams_uni")?.extract()?;
        let keep_alive_interval: Option<f64> = config.getattr("keep_alive_interval")?.extract()?;
        let max_capsule_size: u64 = config.getattr("max_capsule_size")?.extract()?;
        let max_connection_pending_events: u64 =
            config.getattr("max_connection_pending_events")?.extract()?;
        let max_connections: u64 = config.getattr("max_connections")?.extract()?;
        let max_datagram_size: u64 = config.getattr("max_datagram_size")?.extract()?;
        let max_field_section_size: u64 = config.getattr("max_field_section_size")?.extract()?;
        let max_idle_timeout: f64 = config.getattr("max_idle_timeout")?.extract()?;
        let max_pending_capsules: u64 = config.getattr("max_pending_capsules")?.extract()?;
        let max_pending_datagrams: u64 = config.getattr("max_pending_datagrams")?.extract()?;
        let max_pending_streams: u64 = config.getattr("max_pending_streams")?.extract()?;
        let max_session_pending_events: u64 =
            config.getattr("max_session_pending_events")?.extract()?;
        let max_sessions: u64 = config.getattr("max_sessions")?.extract()?;
        let max_streams_bidi: u64 = config.getattr("max_streams_bidi")?.extract()?;
        let max_streams_uni: u64 = config.getattr("max_streams_uni")?.extract()?;
        let max_udp_payload_size: u64 = config.getattr("max_udp_payload_size")?.extract()?;
        let max_write_buffer_size: u64 = config.getattr("max_write_buffer_size")?.extract()?;
        let pending_event_ttl: f64 = config.getattr("pending_event_ttl")?.extract()?;
        let resource_cleanup_interval: f64 =
            config.getattr("resource_cleanup_interval")?.extract()?;
        let session_receive_window: u64 = config.getattr("session_receive_window")?.extract()?;
        let stream_receive_window: u64 = config.getattr("stream_receive_window")?.extract()?;

        Ok(RustBaseConfig {
            congestion_control_algorithm,
            connection_receive_window,
            connection_send_window,
            initial_max_data,
            initial_max_streams_bidi,
            initial_max_streams_uni,
            keep_alive_interval: keep_alive_interval.map(Duration::from_secs_f64),
            max_capsule_size,
            max_connection_pending_events,
            max_connections,
            max_datagram_size,
            max_field_section_size,
            max_idle_timeout: Duration::from_secs_f64(max_idle_timeout),
            max_pending_capsules,
            max_pending_datagrams,
            max_pending_streams,
            max_session_pending_events,
            max_sessions,
            max_streams_bidi,
            max_streams_uni,
            max_udp_payload_size,
            max_write_buffer_size,
            pending_event_ttl: Duration::from_secs_f64(pending_event_ttl),
            resource_cleanup_interval: Duration::from_secs_f64(resource_cleanup_interval),
            session_receive_window,
            stream_receive_window,
        })
    }
}

impl<'a> TryFrom<&Bound<'a, PyAny>> for RustClientConfig {
    type Error = PyErr;

    fn try_from(config: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let base = RustBaseConfig::try_from(config)?;

        let ca_certs: Option<String> = config.getattr("ca_certs")?.extract()?;
        let certfile: Option<String> = config.getattr("certfile")?.extract()?;
        let keyfile: Option<String> = config.getattr("keyfile")?.extract()?;

        let verify_mode_object = config.getattr("verify_mode")?;
        let verify_server_certificate = if verify_mode_object.is_none() {
            true
        } else {
            let mode_value: i32 = verify_mode_object.extract()?;
            mode_value != 0
        };

        Ok(RustClientConfig {
            base,
            ca_certs: ca_certs.map(PathBuf::from),
            certfile: certfile.map(PathBuf::from),
            keyfile: keyfile.map(PathBuf::from),
            verify_server_certificate,
        })
    }
}

impl<'a> TryFrom<&Bound<'a, PyAny>> for RustServerConfig {
    type Error = PyErr;

    fn try_from(config: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let base = RustBaseConfig::try_from(config)?;

        let bind_host: String = config.getattr("bind_host")?.extract()?;
        let bind_port: u16 = config.getattr("bind_port")?.extract()?;
        let ca_certs: Option<String> = config.getattr("ca_certs")?.extract()?;
        let certfile: String = config.getattr("certfile")?.extract()?;
        let keyfile: String = config.getattr("keyfile")?.extract()?;

        let verify_mode_object = config.getattr("verify_mode")?;
        let require_client_auth = if verify_mode_object.is_none() {
            false
        } else {
            let mode_value: i32 = verify_mode_object.extract()?;
            mode_value == 2
        };

        Ok(RustServerConfig {
            base,
            bind_host,
            bind_port,
            ca_certs: ca_certs.map(PathBuf::from),
            certfile: PathBuf::from(certfile),
            keyfile: PathBuf::from(keyfile),
            require_client_auth,
        })
    }
}
