//! Configuration definitions for transport and application protocols.

use std::path::PathBuf;
use std::time::Duration;

use crate::common::constants;

/// Core transport protocol configuration shared between client and server.
#[derive(Clone, Debug)]
pub struct TransportConfig {
    /// Supported ALPN protocol list.
    pub alpn_protocols: Vec<String>,

    /// Connection close timeout.
    pub close_timeout: Duration,

    /// Congestion control algorithm.
    pub congestion_control_algorithm: String,

    /// Connection idle timeout.
    pub connection_idle_timeout: Duration,

    /// Flow control window auto-scaling enablement flag.
    pub flow_control_window_auto_scale: bool,

    /// Initial stream flow control window size.
    pub flow_control_window_size: u64,

    /// Initial connection max data limit.
    pub initial_max_data: u64,

    /// Initial max bidirectional streams.
    pub initial_max_streams_bidi: u64,

    /// Initial max unidirectional streams.
    pub initial_max_streams_uni: u64,

    /// Keep-alive packet enablement flag.
    pub keep_alive: bool,

    /// Maximum H3 capsule size.
    pub max_capsule_size: u64,

    /// Maximum concurrent connections.
    pub max_connections: u64,

    /// Maximum UDP datagram size.
    pub max_datagram_size: u64,

    /// Maximum event history capacity (0 disables).
    pub max_event_history_size: u64,

    /// Maximum event listener count.
    pub max_event_listeners: u64,

    /// Maximum event queue capacity.
    pub max_event_queue_size: u64,

    /// Maximum WebTransport message size.
    pub max_message_size: u64,

    /// Maximum pending events per session.
    pub max_pending_events_per_session: u64,

    /// Maximum concurrent sessions.
    pub max_sessions: u64,

    /// Maximum stream read buffer capacity.
    pub max_stream_read_buffer: u64,

    /// Maximum stream write buffer capacity.
    pub max_stream_write_buffer: u64,

    /// Maximum total pending events.
    pub max_total_pending_events: u64,

    /// Pending event time-to-live.
    pub pending_event_ttl: Duration,

    /// Stream read operation timeout.
    pub read_timeout: Option<Duration>,

    /// Resource cleanup interval.
    pub resource_cleanup_interval: Duration,

    /// Stream creation timeout.
    pub stream_creation_timeout: Duration,

    /// Stream write operation timeout.
    pub write_timeout: Option<Duration>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            alpn_protocols: vec![constants::ALPN_H3.to_owned()],
            close_timeout: Duration::from_secs_f64(constants::DEFAULT_CLOSE_TIMEOUT),
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
            keep_alive: constants::DEFAULT_KEEP_ALIVE,
            max_capsule_size: constants::DEFAULT_MAX_CAPSULE_SIZE,
            max_connections: constants::DEFAULT_CLIENT_MAX_CONNECTIONS,
            max_datagram_size: constants::DEFAULT_MAX_DATAGRAM_SIZE,
            max_event_history_size: constants::DEFAULT_MAX_EVENT_HISTORY_SIZE,
            max_event_listeners: constants::DEFAULT_MAX_EVENT_LISTENERS,
            max_event_queue_size: constants::DEFAULT_MAX_EVENT_QUEUE_SIZE,
            max_message_size: constants::DEFAULT_MAX_MESSAGE_SIZE,
            max_pending_events_per_session: constants::DEFAULT_MAX_PENDING_EVENTS_PER_SESSION,
            max_sessions: constants::DEFAULT_CLIENT_MAX_SESSIONS,
            max_stream_read_buffer: constants::DEFAULT_MAX_STREAM_READ_BUFFER,
            max_stream_write_buffer: constants::DEFAULT_MAX_STREAM_WRITE_BUFFER,
            max_total_pending_events: constants::DEFAULT_MAX_TOTAL_PENDING_EVENTS,
            pending_event_ttl: Duration::from_secs_f64(constants::DEFAULT_PENDING_EVENT_TTL),
            read_timeout: Some(Duration::from_secs_f64(constants::DEFAULT_READ_TIMEOUT)),
            resource_cleanup_interval: Duration::from_secs_f64(
                constants::DEFAULT_RESOURCE_CLEANUP_INTERVAL,
            ),
            stream_creation_timeout: Duration::from_secs_f64(
                constants::DEFAULT_STREAM_CREATION_TIMEOUT,
            ),
            write_timeout: Some(Duration::from_secs_f64(constants::DEFAULT_WRITE_TIMEOUT)),
        }
    }
}

/// WebTransport client configuration options.
#[derive(Clone, Debug)]
pub struct RustClientConfig {
    /// Custom CA certificate bundle path.
    pub ca_certs: Option<PathBuf>,

    /// Client certificate chain file path.
    pub certfile: Option<PathBuf>,

    /// Connection establishment timeout.
    pub connect_timeout: Duration,

    /// Session establishment custom headers.
    pub headers: Vec<(String, String)>,

    /// Client private key file path.
    pub keyfile: Option<PathBuf>,

    /// Maximum connection retry attempt count.
    pub max_connection_retries: u64,

    /// Maximum retry delay.
    pub max_retry_delay: Duration,

    /// Retry backoff multiplier.
    pub retry_backoff: f64,

    /// Initial retry delay.
    pub retry_delay: Duration,

    /// Core transport configuration.
    pub transport: TransportConfig,

    /// User agent string.
    pub user_agent: Option<String>,

    /// Server certificate verification flag.
    pub verify_server_certificate: bool,
}

impl Default for RustClientConfig {
    fn default() -> Self {
        Self {
            ca_certs: None,
            certfile: None,
            connect_timeout: Duration::from_secs_f64(constants::DEFAULT_CONNECT_TIMEOUT),
            headers: Vec::new(),
            keyfile: None,
            max_connection_retries: constants::DEFAULT_MAX_CONNECTION_RETRIES,
            max_retry_delay: Duration::from_secs_f64(constants::DEFAULT_MAX_RETRY_DELAY),
            retry_backoff: constants::DEFAULT_RETRY_BACKOFF,
            retry_delay: Duration::from_secs_f64(constants::DEFAULT_RETRY_DELAY),
            transport: TransportConfig::default(),
            user_agent: None,
            verify_server_certificate: true,
        }
    }
}

/// WebTransport server configuration options.
#[derive(Clone, Debug)]
pub struct RustServerConfig {
    /// Server bind address.
    pub bind_host: String,

    /// Server bind port.
    pub bind_port: u16,

    /// Server certificate chain file path.
    pub certfile: PathBuf,

    /// Server private key file path.
    pub keyfile: PathBuf,

    /// Client certificate authentication requirement flag.
    pub require_client_auth: bool,

    /// Core transport configuration.
    pub transport: TransportConfig,
}

#[cfg(test)]
mod tests;
