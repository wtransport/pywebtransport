//! FFI conversion logic for configuration objects.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use pyo3::prelude::*;

use crate::common::config::{RustClientConfig, RustServerConfig, TransportConfig};

impl<'a> TryFrom<&Bound<'a, PyAny>> for TransportConfig {
    type Error = PyErr;

    // Python dictionary to TransportConfig conversion.
    fn try_from(conf: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let alpn_protocols: Vec<String> = conf.getattr("alpn_protocols")?.extract()?;
        let close_timeout: f64 = conf.getattr("close_timeout")?.extract()?;
        let congestion_control_algorithm: String =
            conf.getattr("congestion_control_algorithm")?.extract()?;
        let connection_idle_timeout: f64 = conf.getattr("connection_idle_timeout")?.extract()?;
        let flow_control_window_auto_scale: bool =
            conf.getattr("flow_control_window_auto_scale")?.extract()?;
        let flow_control_window_size: u64 = conf.getattr("flow_control_window_size")?.extract()?;
        let initial_max_data: u64 = conf.getattr("initial_max_data")?.extract()?;
        let initial_max_streams_bidi: u64 = conf.getattr("initial_max_streams_bidi")?.extract()?;
        let initial_max_streams_uni: u64 = conf.getattr("initial_max_streams_uni")?.extract()?;
        let keep_alive: bool = conf.getattr("keep_alive")?.extract()?;
        let max_capsule_size: u64 = conf.getattr("max_capsule_size")?.extract()?;
        let max_connections: u64 = conf.getattr("max_connections")?.extract()?;
        let max_datagram_size: u64 = conf.getattr("max_datagram_size")?.extract()?;
        let max_event_history_size: u64 = conf.getattr("max_event_history_size")?.extract()?;
        let max_event_listeners: u64 = conf.getattr("max_event_listeners")?.extract()?;
        let max_event_queue_size: u64 = conf.getattr("max_event_queue_size")?.extract()?;
        let max_message_size: u64 = conf.getattr("max_message_size")?.extract()?;
        let max_pending_events_per_session: u64 =
            conf.getattr("max_pending_events_per_session")?.extract()?;
        let max_sessions: u64 = conf.getattr("max_sessions")?.extract()?;
        let max_stream_read_buffer: u64 = conf.getattr("max_stream_read_buffer")?.extract()?;
        let max_stream_write_buffer: u64 = conf.getattr("max_stream_write_buffer")?.extract()?;
        let max_total_pending_events: u64 = conf.getattr("max_total_pending_events")?.extract()?;
        let pending_event_ttl: f64 = conf.getattr("pending_event_ttl")?.extract()?;
        let resource_cleanup_interval: f64 =
            conf.getattr("resource_cleanup_interval")?.extract()?;
        let stream_creation_timeout: f64 = conf.getattr("stream_creation_timeout")?.extract()?;

        let read_timeout: Option<f64> = conf.getattr("read_timeout")?.extract()?;
        let write_timeout: Option<f64> = conf.getattr("write_timeout")?.extract()?;

        Ok(TransportConfig {
            alpn_protocols,
            close_timeout: Duration::from_secs_f64(close_timeout),
            congestion_control_algorithm,
            connection_idle_timeout: Duration::from_secs_f64(connection_idle_timeout),
            flow_control_window_auto_scale,
            flow_control_window_size,
            initial_max_data,
            initial_max_streams_bidi,
            initial_max_streams_uni,
            keep_alive,
            max_capsule_size,
            max_connections,
            max_datagram_size,
            max_event_history_size,
            max_event_listeners,
            max_event_queue_size,
            max_message_size,
            max_pending_events_per_session,
            max_sessions,
            max_stream_read_buffer,
            max_stream_write_buffer,
            max_total_pending_events,
            pending_event_ttl: Duration::from_secs_f64(pending_event_ttl),
            read_timeout: read_timeout.map(Duration::from_secs_f64),
            resource_cleanup_interval: Duration::from_secs_f64(resource_cleanup_interval),
            stream_creation_timeout: Duration::from_secs_f64(stream_creation_timeout),
            write_timeout: write_timeout.map(Duration::from_secs_f64),
        })
    }
}

impl<'a> TryFrom<&Bound<'a, PyAny>> for RustClientConfig {
    type Error = PyErr;

    // Python dictionary to RustClientConfig conversion.
    fn try_from(conf: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let transport = TransportConfig::try_from(conf)?;

        let connect_timeout: f64 = conf.getattr("connect_timeout")?.extract()?;
        let max_connection_retries: u64 = conf.getattr("max_connection_retries")?.extract()?;
        let max_retry_delay: f64 = conf.getattr("max_retry_delay")?.extract()?;
        let retry_backoff: f64 = conf.getattr("retry_backoff")?.extract()?;
        let retry_delay: f64 = conf.getattr("retry_delay")?.extract()?;

        let ca_certs: Option<String> = conf.getattr("ca_certs")?.extract()?;
        let certfile: Option<String> = conf.getattr("certfile")?.extract()?;
        let keyfile: Option<String> = conf.getattr("keyfile")?.extract()?;
        let user_agent: Option<String> = conf.getattr("user_agent")?.extract()?;

        let verify_mode_obj = conf.getattr("verify_mode")?;
        let verify_server_certificate = if verify_mode_obj.is_none() {
            true
        } else {
            let mode_val: i32 = verify_mode_obj.extract()?;
            mode_val != 0
        };

        let headers_map: HashMap<String, String> = conf.getattr("headers")?.extract()?;
        let headers = headers_map.into_iter().collect();

        Ok(RustClientConfig {
            ca_certs: ca_certs.map(PathBuf::from),
            certfile: certfile.map(PathBuf::from),
            connect_timeout: Duration::from_secs_f64(connect_timeout),
            headers,
            keyfile: keyfile.map(PathBuf::from),
            max_connection_retries,
            max_retry_delay: Duration::from_secs_f64(max_retry_delay),
            retry_backoff,
            retry_delay: Duration::from_secs_f64(retry_delay),
            transport,
            user_agent,
            verify_server_certificate,
        })
    }
}

impl<'a> TryFrom<&Bound<'a, PyAny>> for RustServerConfig {
    type Error = PyErr;

    // Python dictionary to RustServerConfig conversion.
    fn try_from(conf: &Bound<'a, PyAny>) -> Result<Self, Self::Error> {
        let transport = TransportConfig::try_from(conf)?;

        let bind_host: String = conf.getattr("bind_host")?.extract()?;
        let bind_port: u16 = conf.getattr("bind_port")?.extract()?;
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
            bind_host,
            bind_port,
            certfile: PathBuf::from(certfile),
            keyfile: PathBuf::from(keyfile),
            require_client_auth,
            transport,
        })
    }
}
