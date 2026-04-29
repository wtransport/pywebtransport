//! Configuration definitions for transport and application protocols.

use std::path::PathBuf;
use std::time::Duration;

// Core common configuration shared between client and server.
#[derive(Clone, Debug)]
pub(crate) struct RustBaseConfig {
    pub(crate) alpn_protocols: Vec<String>,
    pub(crate) congestion_control_algorithm: String,
    pub(crate) connection_idle_timeout: Duration,
    pub(crate) flow_control_window: u64,
    pub(crate) flow_control_window_auto_scale_enabled: bool,
    pub(crate) initial_max_data: u64,
    pub(crate) initial_max_streams_bidi: u64,
    pub(crate) initial_max_streams_uni: u64,
    pub(crate) keep_alive_interval: Option<Duration>,
    pub(crate) max_capsule_size: u64,
    pub(crate) max_datagram_size: u64,
    pub(crate) max_field_section_size: u64,
    pub(crate) max_session_pending_events: u64,
    pub(crate) max_sessions: u64,
    pub(crate) max_stream_read_buffer_size: u64,
    pub(crate) max_stream_write_buffer_size: u64,
    pub(crate) max_total_pending_events: u64,
    pub(crate) max_transport_streams: u64,
    pub(crate) pending_event_ttl: Duration,
    pub(crate) resource_cleanup_interval: Duration,
}

// WebTransport client configuration options.
#[derive(Clone, Debug)]
pub(crate) struct RustClientConfig {
    pub(crate) base: RustBaseConfig,
    pub(crate) ca_certs: Option<PathBuf>,
    pub(crate) certfile: Option<PathBuf>,
    pub(crate) keyfile: Option<PathBuf>,
    pub(crate) verify_server_certificate: bool,
}

// WebTransport server configuration options.
#[derive(Clone, Debug)]
pub(crate) struct RustServerConfig {
    pub(crate) base: RustBaseConfig,
    pub(crate) bind_host: String,
    pub(crate) bind_port: u16,
    pub(crate) ca_certs: Option<PathBuf>,
    pub(crate) certfile: PathBuf,
    pub(crate) keyfile: PathBuf,
    pub(crate) require_client_auth: bool,
}

#[cfg(test)]
mod tests;
