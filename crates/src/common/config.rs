//! Configuration definitions for transport and application protocols.

use std::path::PathBuf;
use std::time::Duration;

use crate::common::constants;

/// Core transport protocol configuration shared between client and server.
#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub(crate) alpn_protocols: Vec<String>,
    pub(crate) congestion_control_algorithm: String,
    pub(crate) connection_idle_timeout: Duration,
    pub(crate) flow_control_window_auto_scale: bool,
    pub(crate) flow_control_window_size: u64,
    pub(crate) initial_max_data: u64,
    pub(crate) initial_max_streams_bidi: u64,
    pub(crate) initial_max_streams_uni: u64,
    pub(crate) keep_alive: Option<Duration>,
    pub(crate) max_capsule_size: u64,
    pub(crate) max_datagram_size: u64,
    pub(crate) max_pending_events_per_session: u64,
    pub(crate) max_sessions: u64,
    pub(crate) max_stream_read_buffer: u64,
    pub(crate) max_stream_write_buffer: u64,
    pub(crate) max_total_pending_events: u64,
    pub(crate) pending_event_ttl: Duration,
    pub(crate) resource_cleanup_interval: Duration,
    pub(crate) transport_streams_cap: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            alpn_protocols: constants::DEFAULT_ALPN_PROTOCOLS
                .iter()
                .map(|&s| s.to_owned())
                .collect(),
            congestion_control_algorithm: constants::DEFAULT_CONGESTION_CONTROL_ALGORITHM
                .to_owned(),
            connection_idle_timeout: Duration::from_secs_f64(
                constants::DEFAULT_CONNECTION_IDLE_TIMEOUT,
            ),
            flow_control_window_auto_scale: constants::DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE,
            flow_control_window_size: constants::DEFAULT_FLOW_CONTROL_WINDOW_SIZE,
            initial_max_data: constants::DEFAULT_INITIAL_MAX_DATA,
            initial_max_streams_bidi: constants::DEFAULT_INITIAL_MAX_STREAMS_BIDI,
            initial_max_streams_uni: constants::DEFAULT_INITIAL_MAX_STREAMS_UNI,
            keep_alive: Some(Duration::from_secs_f64(constants::DEFAULT_KEEP_ALIVE)),
            max_capsule_size: constants::DEFAULT_MAX_CAPSULE_SIZE,
            max_datagram_size: constants::DEFAULT_MAX_DATAGRAM_SIZE,
            max_pending_events_per_session: constants::DEFAULT_MAX_PENDING_EVENTS_PER_SESSION,
            max_sessions: constants::DEFAULT_CLIENT_MAX_SESSIONS,
            max_stream_read_buffer: constants::DEFAULT_MAX_STREAM_READ_BUFFER,
            max_stream_write_buffer: constants::DEFAULT_MAX_STREAM_WRITE_BUFFER,
            max_total_pending_events: constants::DEFAULT_MAX_TOTAL_PENDING_EVENTS,
            pending_event_ttl: Duration::from_secs_f64(constants::DEFAULT_PENDING_EVENT_TTL),
            resource_cleanup_interval: Duration::from_secs_f64(
                constants::DEFAULT_RESOURCE_CLEANUP_INTERVAL,
            ),
            transport_streams_cap: constants::DEFAULT_TRANSPORT_STREAMS_CAP,
        }
    }
}

/// WebTransport client configuration options.
#[derive(Clone, Debug)]
pub struct RustClientConfig {
    pub(crate) ca_certs: Option<PathBuf>,
    pub(crate) certfile: Option<PathBuf>,
    pub(crate) keyfile: Option<PathBuf>,
    pub(crate) transport: TransportConfig,
    pub(crate) verify_server_certificate: bool,
}

impl Default for RustClientConfig {
    fn default() -> Self {
        Self {
            ca_certs: None,
            certfile: None,
            keyfile: None,
            transport: TransportConfig::default(),
            verify_server_certificate: true,
        }
    }
}

/// WebTransport server configuration options.
#[derive(Clone, Debug)]
pub struct RustServerConfig {
    pub(crate) bind_host: String,
    pub(crate) bind_port: u16,
    pub(crate) ca_certs: Option<PathBuf>,
    pub(crate) certfile: PathBuf,
    pub(crate) keyfile: PathBuf,
    pub(crate) require_client_auth: bool,
    pub(crate) transport: TransportConfig,
}

#[cfg(test)]
mod tests;
