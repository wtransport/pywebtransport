//! Protocol constants and configuration defaults.

/// HTTP/3 ALPN protocol identifier.
pub const ALPN_H3: &str = "h3";

/// User-Agent header key.
pub const USER_AGENT_HEADER: &str = "user-agent";

/// Default WebTransport network port.
pub const WEBTRANSPORT_DEFAULT_PORT: u16 = 443;

/// Default WebTransport URI scheme.
pub const WEBTRANSPORT_SCHEME: &str = "https";

/// Bidirectional stream ID mask.
pub const BIDIRECTIONAL_STREAM: u64 = 0x0;

/// WebTransport session closure capsule type.
pub const CLOSE_WEBTRANSPORT_SESSION_TYPE: u64 = 0x2843;

/// WebTransport session drain capsule type.
pub const DRAIN_WEBTRANSPORT_SESSION_TYPE: u64 = 0x78AE;

/// HTTP/3 frame type: `CANCEL_PUSH`.
pub const H3_FRAME_TYPE_CANCEL_PUSH: u64 = 0x3;

/// HTTP/3 frame type: `DATA`.
pub const H3_FRAME_TYPE_DATA: u64 = 0x0;

/// HTTP/3 frame type: `GOAWAY`.
pub const H3_FRAME_TYPE_GOAWAY: u64 = 0x7;

/// HTTP/3 frame type: `HEADERS`.
pub const H3_FRAME_TYPE_HEADERS: u64 = 0x1;

/// HTTP/3 frame type: `MAX_PUSH_ID`.
pub const H3_FRAME_TYPE_MAX_PUSH_ID: u64 = 0xD;

/// HTTP/3 frame type: `PUSH_PROMISE`.
pub const H3_FRAME_TYPE_PUSH_PROMISE: u64 = 0x5;

/// HTTP/3 frame type: `SETTINGS`.
pub const H3_FRAME_TYPE_SETTINGS: u64 = 0x4;

/// HTTP/3 frame type: `WEBTRANSPORT_STREAM`.
pub const H3_FRAME_TYPE_WEBTRANSPORT_STREAM: u64 = 0x41;

/// HTTP/3 stream type: `CONTROL`.
pub const H3_STREAM_TYPE_CONTROL: u64 = 0x0;

/// HTTP/3 stream type: `PUSH`.
pub const H3_STREAM_TYPE_PUSH: u64 = 0x1;

/// HTTP/3 stream type: `QPACK_DECODER`.
pub const H3_STREAM_TYPE_QPACK_DECODER: u64 = 0x3;

/// HTTP/3 stream type: `QPACK_ENCODER`.
pub const H3_STREAM_TYPE_QPACK_ENCODER: u64 = 0x2;

/// HTTP/3 stream type: `WEBTRANSPORT`.
pub const H3_STREAM_TYPE_WEBTRANSPORT: u64 = 0x54;

/// Maximum close reason phrase length.
pub const MAX_CLOSE_REASON_BYTES: usize = 1024;

/// Maximum UDP datagram size.
pub const MAX_DATAGRAM_SIZE: u64 = 65535;

/// Protocol-defined maximum concurrent stream limit.
pub const MAX_PROTOCOL_STREAMS_LIMIT: u64 = 1 << 60;

/// Maximum valid stream identifier.
pub const MAX_STREAM_ID: u64 = (1 << 62) - 1;

/// QPACK maximum number of blocked streams.
pub const QPACK_DECODER_MAX_BLOCKED_STREAMS: u64 = 16;

/// QPACK maximum dynamic table capacity.
pub const QPACK_DECODER_MAX_TABLE_CAPACITY: u64 = 4096;

/// HTTP/3 Setting: `ENABLE_CONNECT_PROTOCOL`.
pub const SETTINGS_ENABLE_CONNECT_PROTOCOL: u64 = 0x8;

/// HTTP/3 Setting: `H3_DATAGRAM`.
pub const SETTINGS_H3_DATAGRAM: u64 = 0x33;

/// HTTP/3 Setting: `QPACK_BLOCKED_STREAMS`.
pub const SETTINGS_QPACK_BLOCKED_STREAMS: u64 = 0x7;

/// HTTP/3 Setting: `QPACK_MAX_TABLE_CAPACITY`.
pub const SETTINGS_QPACK_MAX_TABLE_CAPACITY: u64 = 0x1;

/// HTTP/3 Setting: `WEBTRANSPORT_INITIAL_MAX_DATA`.
pub const SETTINGS_WT_INITIAL_MAX_DATA: u64 = 0x2B61;

/// HTTP/3 Setting: `WEBTRANSPORT_INITIAL_MAX_STREAMS_BIDI`.
pub const SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI: u64 = 0x2B65;

/// HTTP/3 Setting: `WEBTRANSPORT_INITIAL_MAX_STREAMS_UNI`.
pub const SETTINGS_WT_INITIAL_MAX_STREAMS_UNI: u64 = 0x2B64;

/// Unidirectional stream ID mask.
pub const UNIDIRECTIONAL_STREAM: u64 = 0x2;

/// WebTransport frame type: `DATA_BLOCKED`.
pub const WT_DATA_BLOCKED_TYPE: u64 = 0x190B_4D41;

/// WebTransport frame type: `MAX_DATA`.
pub const WT_MAX_DATA_TYPE: u64 = 0x190B_4D3D;

/// WebTransport frame type: `MAX_STREAM_DATA`.
pub const WT_MAX_STREAM_DATA_TYPE: u64 = 0x190B_4D3E;

/// WebTransport frame type: `MAX_STREAMS_BIDI`.
pub const WT_MAX_STREAMS_BIDI_TYPE: u64 = 0x190B_4D3F;

/// WebTransport frame type: `MAX_STREAMS_UNI`.
pub const WT_MAX_STREAMS_UNI_TYPE: u64 = 0x190B_4D40;

/// WebTransport frame type: `STREAM_DATA_BLOCKED`.
pub const WT_STREAM_DATA_BLOCKED_TYPE: u64 = 0x190B_4D42;

/// WebTransport frame type: `STREAMS_BLOCKED_BIDI`.
pub const WT_STREAMS_BLOCKED_BIDI_TYPE: u64 = 0x190B_4D43;

/// WebTransport frame type: `STREAMS_BLOCKED_UNI`.
pub const WT_STREAMS_BLOCKED_UNI_TYPE: u64 = 0x190B_4D44;

/// Default ALPN protocol identifier list.
pub const DEFAULT_ALPN_PROTOCOLS: &[&str] = &[ALPN_H3];

/// Default server bind address.
pub const DEFAULT_BIND_HOST: &str = "::";

/// Default client concurrent connection limit.
pub const DEFAULT_CLIENT_MAX_CONNECTIONS: u64 = 100;

/// Default client concurrent session limit.
pub const DEFAULT_CLIENT_MAX_SESSIONS: u64 = 100;

/// Default connection closure timeout.
pub const DEFAULT_CLOSE_TIMEOUT: f64 = 5.0;

/// Default congestion control algorithm.
pub const DEFAULT_CONGESTION_CONTROL_ALGORITHM: &str = "cubic";

/// Default connection establishment timeout.
pub const DEFAULT_CONNECT_TIMEOUT: f64 = 30.0;

/// Default connection idle timeout.
pub const DEFAULT_CONNECTION_IDLE_TIMEOUT: f64 = 60.0;

/// Default development server port.
pub const DEFAULT_DEV_PORT: u16 = 4433;

/// Flow control window auto-scaling enablement flag.
pub const DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE: bool = true;

/// Default flow control window size.
pub const DEFAULT_FLOW_CONTROL_WINDOW_SIZE: u64 = 1024 * 1024;

/// Default initial maximum data limit.
pub const DEFAULT_INITIAL_MAX_DATA: u64 = 10 * 1024 * 1024;

/// Default initial maximum bidirectional streams.
pub const DEFAULT_INITIAL_MAX_STREAMS_BIDI: u64 = 100;

/// Default initial maximum unidirectional streams.
pub const DEFAULT_INITIAL_MAX_STREAMS_UNI: u64 = 100;

/// Keep-alive packet enablement flag.
pub const DEFAULT_KEEP_ALIVE: bool = true;

/// Default logging level.
pub const DEFAULT_LOG_LEVEL: &str = "INFO";

/// Default maximum H3 capsule size.
pub const DEFAULT_MAX_CAPSULE_SIZE: u64 = 65536;

/// Default maximum connection retry attempt limit.
pub const DEFAULT_MAX_CONNECTION_RETRIES: u64 = 3;

/// Default maximum UDP datagram size.
pub const DEFAULT_MAX_DATAGRAM_SIZE: u64 = 1350;

/// Default event history buffer capacity (0 disables).
pub const DEFAULT_MAX_EVENT_HISTORY_SIZE: u64 = 0;

/// Default maximum event listener count.
pub const DEFAULT_MAX_EVENT_LISTENERS: u64 = 100;

/// Default event queue capacity.
pub const DEFAULT_MAX_EVENT_QUEUE_SIZE: u64 = 1000;

/// Default maximum WebTransport message size.
pub const DEFAULT_MAX_MESSAGE_SIZE: u64 = 1024 * 1024;

/// Default maximum pending events per session.
pub const DEFAULT_MAX_PENDING_EVENTS_PER_SESSION: u64 = 100;

/// Default maximum retry delay.
pub const DEFAULT_MAX_RETRY_DELAY: f64 = 30.0;

/// Default stream read buffer capacity.
pub const DEFAULT_MAX_STREAM_READ_BUFFER: u64 = 2 * 1024 * 1024;

/// Default stream write buffer capacity.
pub const DEFAULT_MAX_STREAM_WRITE_BUFFER: u64 = 2 * 1024 * 1024;

/// Default maximum total pending events across all sessions.
pub const DEFAULT_MAX_TOTAL_PENDING_EVENTS: u64 = 1000;

/// Default pending event time-to-live (TTL).
pub const DEFAULT_PENDING_EVENT_TTL: f64 = 5.0;

/// Default stream read operation timeout.
pub const DEFAULT_READ_TIMEOUT: f64 = 60.0;

/// Default resource cleanup interval.
pub const DEFAULT_RESOURCE_CLEANUP_INTERVAL: f64 = 15.0;

/// Default retry backoff multiplier.
pub const DEFAULT_RETRY_BACKOFF: f64 = 2.0;

/// Default initial retry delay.
pub const DEFAULT_RETRY_DELAY: f64 = 1.0;

/// Default server concurrent connection limit.
pub const DEFAULT_SERVER_MAX_CONNECTIONS: u64 = 3000;

/// Default server concurrent session limit.
pub const DEFAULT_SERVER_MAX_SESSIONS: u64 = 10000;

/// Default stream creation timeout.
pub const DEFAULT_STREAM_CREATION_TIMEOUT: f64 = 10.0;

/// Default stream write operation timeout.
pub const DEFAULT_WRITE_TIMEOUT: f64 = 30.0;

/// Supported congestion control algorithm list.
pub const SUPPORTED_CONGESTION_CONTROL_ALGORITHMS: &[&str] = &["reno", "cubic"];

/// Error: AEAD limit reached.
pub const ERR_AEAD_LIMIT_REACHED: u64 = 0xF;

/// Error: Application authentication failed.
pub const ERR_APP_AUTHENTICATION_FAILED: u64 = 0x1001;

/// Error: Application connection timeout.
pub const ERR_APP_CONNECTION_TIMEOUT: u64 = 0x1000;

/// Error: Application invalid request.
pub const ERR_APP_INVALID_REQUEST: u64 = 0x1004;

/// Error: Application permission denied.
pub const ERR_APP_PERMISSION_DENIED: u64 = 0x1002;

/// Error: Application resource exhausted.
pub const ERR_APP_RESOURCE_EXHAUSTED: u64 = 0x1003;

/// Error: Application service unavailable.
pub const ERR_APP_SERVICE_UNAVAILABLE: u64 = 0x1005;

/// Error: Application error.
pub const ERR_APPLICATION_ERROR: u64 = 0xC;

/// Error: Connection ID limit exceeded.
pub const ERR_CONNECTION_ID_LIMIT_ERROR: u64 = 0x9;

/// Error: Connection refused.
pub const ERR_CONNECTION_REFUSED: u64 = 0x2;

/// Error: Crypto buffer exceeded.
pub const ERR_CRYPTO_BUFFER_EXCEEDED: u64 = 0xD;

/// Error: Final size error.
pub const ERR_FINAL_SIZE_ERROR: u64 = 0x6;

/// Error: Flow control error.
pub const ERR_FLOW_CONTROL_ERROR: u64 = 0x3;

/// Error: Frame encoding error.
pub const ERR_FRAME_ENCODING_ERROR: u64 = 0x7;

/// Error: H3 closed critical stream.
pub const ERR_H3_CLOSED_CRITICAL_STREAM: u64 = 0x104;

/// Error: H3 connect error.
pub const ERR_H3_CONNECT_ERROR: u64 = 0x10F;

/// Error: H3 datagram error.
pub const ERR_H3_DATAGRAM_ERROR: u64 = 0x33;

/// Error: H3 excessive load.
pub const ERR_H3_EXCESSIVE_LOAD: u64 = 0x107;

/// Error: H3 frame error.
pub const ERR_H3_FRAME_ERROR: u64 = 0x106;

/// Error: H3 frame unexpected.
pub const ERR_H3_FRAME_UNEXPECTED: u64 = 0x105;

/// Error: H3 general protocol error.
pub const ERR_H3_GENERAL_PROTOCOL_ERROR: u64 = 0x101;

/// Error: H3 ID error.
pub const ERR_H3_ID_ERROR: u64 = 0x108;

/// Error: H3 internal error.
pub const ERR_H3_INTERNAL_ERROR: u64 = 0x102;

/// Error: H3 message error.
pub const ERR_H3_MESSAGE_ERROR: u64 = 0x10E;

/// Error: H3 missing settings.
pub const ERR_H3_MISSING_SETTINGS: u64 = 0x10A;

/// Error: H3 no error.
pub const ERR_H3_NO_ERROR: u64 = 0x100;

/// Error: H3 request cancelled.
pub const ERR_H3_REQUEST_CANCELLED: u64 = 0x10C;

/// Error: H3 request incomplete.
pub const ERR_H3_REQUEST_INCOMPLETE: u64 = 0x10D;

/// Error: H3 request rejected.
pub const ERR_H3_REQUEST_REJECTED: u64 = 0x10B;

/// Error: H3 settings error.
pub const ERR_H3_SETTINGS_ERROR: u64 = 0x109;

/// Error: H3 stream creation error.
pub const ERR_H3_STREAM_CREATION_ERROR: u64 = 0x103;

/// Error: H3 version fallback.
pub const ERR_H3_VERSION_FALLBACK: u64 = 0x110;

/// Error: Internal error.
pub const ERR_INTERNAL_ERROR: u64 = 0x1;

/// Error: Invalid token.
pub const ERR_INVALID_TOKEN: u64 = 0xB;

/// Error: Key update error.
pub const ERR_KEY_UPDATE_ERROR: u64 = 0xE;

/// Error: Library internal connection state error.
pub const ERR_LIB_CONNECTION_STATE_ERROR: u64 = 0x1100_0001;

/// Error: Library internal generic error.
pub const ERR_LIB_INTERNAL_ERROR: u64 = 0x1000_0001;

/// Error: Library internal session state error.
pub const ERR_LIB_SESSION_STATE_ERROR: u64 = 0x1200_0001;

/// Error: Library internal stream state error.
pub const ERR_LIB_STREAM_STATE_ERROR: u64 = 0x1300_0001;

/// Error: No error.
pub const ERR_NO_ERROR: u64 = 0x0;

/// Error: No viable path.
pub const ERR_NO_VIABLE_PATH: u64 = 0x10;

/// Error: Protocol violation.
pub const ERR_PROTOCOL_VIOLATION: u64 = 0xA;

/// Error: QPACK decoder stream error.
pub const ERR_QPACK_DECODER_STREAM_ERROR: u64 = 0x202;

/// Error: QPACK decompression failed.
pub const ERR_QPACK_DECOMPRESSION_FAILED: u64 = 0x200;

/// Error: QPACK encoder stream error.
pub const ERR_QPACK_ENCODER_STREAM_ERROR: u64 = 0x201;

/// Error: Stream limit error.
pub const ERR_STREAM_LIMIT_ERROR: u64 = 0x4;

/// Error: Stream state error.
pub const ERR_STREAM_STATE_ERROR: u64 = 0x5;

/// Error: Transport parameter error.
pub const ERR_TRANSPORT_PARAMETER_ERROR: u64 = 0x8;

/// Error: WebTransport application error range start.
pub const ERR_WT_APPLICATION_ERROR_FIRST: u64 = 0x52E4_A40F_A8DB;

/// Error: WebTransport application error range end.
pub const ERR_WT_APPLICATION_ERROR_LAST: u64 = 0x52E5_AC98_3162;

/// Error: WebTransport buffered stream rejected.
pub const ERR_WT_BUFFERED_STREAM_REJECTED: u64 = 0x3994_BD84;

/// Error: WebTransport flow control error.
pub const ERR_WT_FLOW_CONTROL_ERROR: u64 = 0x045D_4487;

/// Error: WebTransport session gone.
pub const ERR_WT_SESSION_GONE: u64 = 0x170D_7B68;

#[cfg(test)]
mod tests;
