//! FFI conversion logic for configuration objects.

use std::path::PathBuf;
use std::time::Duration;

use pyo3::prelude::*;

use crate::common::config::{RustBaseConfig, RustClientConfig, RustServerConfig};

impl<'a> TryFrom<&Bound<'a, PyAny>> for RustBaseConfig {
    type Error = PyErr;

    fn try_from(conf: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let alpn_protocols: Vec<String> = conf.getattr("alpn_protocols")?.extract()?;
        let congestion_control_algorithm: String =
            conf.getattr("congestion_control_algorithm")?.extract()?;
        let connection_idle_timeout: f64 = conf.getattr("connection_idle_timeout")?.extract()?;
        let flow_control_window: u64 = conf.getattr("flow_control_window")?.extract()?;
        let flow_control_window_auto_scale_enabled: bool = conf
            .getattr("flow_control_window_auto_scale_enabled")?
            .extract()?;
        let initial_max_data: u64 = conf.getattr("initial_max_data")?.extract()?;
        let initial_max_streams_bidi: u64 = conf.getattr("initial_max_streams_bidi")?.extract()?;
        let initial_max_streams_uni: u64 = conf.getattr("initial_max_streams_uni")?.extract()?;
        let keep_alive_interval: Option<f64> = conf.getattr("keep_alive_interval")?.extract()?;
        let max_capsule_size: u64 = conf.getattr("max_capsule_size")?.extract()?;
        let max_datagram_size: u64 = conf.getattr("max_datagram_size")?.extract()?;
        let max_field_section_size: u64 = conf.getattr("max_field_section_size")?.extract()?;
        let max_session_pending_events: u64 =
            conf.getattr("max_session_pending_events")?.extract()?;
        let max_sessions: u64 = conf.getattr("max_sessions")?.extract()?;
        let max_stream_read_buffer_size: u64 =
            conf.getattr("max_stream_read_buffer_size")?.extract()?;
        let max_stream_write_buffer_size: u64 =
            conf.getattr("max_stream_write_buffer_size")?.extract()?;
        let max_total_pending_events: u64 = conf.getattr("max_total_pending_events")?.extract()?;
        let pending_event_ttl: f64 = conf.getattr("pending_event_ttl")?.extract()?;
        let quic_max_concurrent_bidi_streams: u64 = conf
            .getattr("quic_max_concurrent_bidi_streams")?
            .extract()?;
        let quic_max_concurrent_uni_streams: u64 =
            conf.getattr("quic_max_concurrent_uni_streams")?.extract()?;
        let quic_receive_window: u64 = conf.getattr("quic_receive_window")?.extract()?;
        let quic_send_window: u64 = conf.getattr("quic_send_window")?.extract()?;
        let quic_stream_receive_window: u64 =
            conf.getattr("quic_stream_receive_window")?.extract()?;
        let resource_cleanup_interval: f64 =
            conf.getattr("resource_cleanup_interval")?.extract()?;

        Ok(RustBaseConfig {
            alpn_protocols,
            congestion_control_algorithm,
            connection_idle_timeout: Duration::from_secs_f64(connection_idle_timeout),
            flow_control_window,
            flow_control_window_auto_scale_enabled,
            initial_max_data,
            initial_max_streams_bidi,
            initial_max_streams_uni,
            keep_alive_interval: keep_alive_interval.map(Duration::from_secs_f64),
            max_capsule_size,
            max_datagram_size,
            max_field_section_size,
            max_session_pending_events,
            max_sessions,
            max_stream_read_buffer_size,
            max_stream_write_buffer_size,
            max_total_pending_events,
            pending_event_ttl: Duration::from_secs_f64(pending_event_ttl),
            quic_max_concurrent_bidi_streams,
            quic_max_concurrent_uni_streams,
            quic_receive_window,
            quic_send_window,
            quic_stream_receive_window,
            resource_cleanup_interval: Duration::from_secs_f64(resource_cleanup_interval),
        })
    }
}

impl<'a> TryFrom<&Bound<'a, PyAny>> for RustClientConfig {
    type Error = PyErr;

    fn try_from(conf: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let base = RustBaseConfig::try_from(conf)?;

        let ca_certs: Option<String> = conf.getattr("ca_certs")?.extract()?;
        let certfile: Option<String> = conf.getattr("certfile")?.extract()?;
        let keyfile: Option<String> = conf.getattr("keyfile")?.extract()?;

        let verify_mode_obj = conf.getattr("verify_mode")?;
        let verify_server_certificate = if verify_mode_obj.is_none() {
            true
        } else {
            let mode_val: i32 = verify_mode_obj.extract()?;
            mode_val != 0
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

    fn try_from(conf: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let base = RustBaseConfig::try_from(conf)?;

        let bind_host: String = conf.getattr("bind_host")?.extract()?;
        let bind_port: u16 = conf.getattr("bind_port")?.extract()?;
        let ca_certs: Option<String> = conf.getattr("ca_certs")?.extract()?;
        let certfile: String = conf.getattr("certfile")?.extract()?;
        let keyfile: String = conf.getattr("keyfile")?.extract()?;

        let verify_mode_obj = conf.getattr("verify_mode")?;
        let require_client_auth = if verify_mode_obj.is_none() {
            false
        } else {
            let mode_val: i32 = verify_mode_obj.extract()?;
            mode_val == 2
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
