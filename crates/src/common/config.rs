//! Common configuration.

use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct RustBaseConfig {
    pub(crate) congestion_control_algorithm: String,
    pub(crate) connection_receive_window: u64,
    pub(crate) connection_send_window: u64,
    pub(crate) initial_max_data: u64,
    pub(crate) initial_max_streams_bidi: u64,
    pub(crate) initial_max_streams_uni: u64,
    pub(crate) keep_alive_interval: Option<Duration>,
    pub(crate) max_capsule_size: u64,
    pub(crate) max_connection_pending_events: u64,
    pub(crate) max_connections: u64,
    pub(crate) max_datagram_size: u64,
    pub(crate) max_field_section_size: u64,
    pub(crate) max_idle_timeout: Duration,
    pub(crate) max_pending_capsules: u64,
    pub(crate) max_pending_datagrams: u64,
    pub(crate) max_pending_streams: u64,
    pub(crate) max_session_pending_events: u64,
    pub(crate) max_sessions: u64,
    pub(crate) max_streams_bidi: u64,
    pub(crate) max_streams_uni: u64,
    pub(crate) max_udp_payload_size: u64,
    pub(crate) max_write_buffer_size: u64,
    pub(crate) pending_event_ttl: Duration,
    pub(crate) resource_cleanup_interval: Duration,
    pub(crate) session_receive_window: u64,
    pub(crate) stream_receive_window: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct RustClientConfig {
    pub(crate) base: RustBaseConfig,
    pub(crate) ca_certs: Option<PathBuf>,
    pub(crate) certfile: Option<PathBuf>,
    pub(crate) keyfile: Option<PathBuf>,
    pub(crate) verify_server_certificate: bool,
}

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
