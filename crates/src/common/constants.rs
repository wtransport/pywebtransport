//! Protocol constants and configuration defaults.

// Default ALPN protocol identifier list.
pub(crate) const DEFAULT_ALPN_PROTOCOLS: &[&str] = &["h3"];
// Default server bind address.
pub(crate) const DEFAULT_BIND_HOST: &str = "::";
// Default connection closure timeout.
pub(crate) const DEFAULT_CLOSE_TIMEOUT: f64 = 5.0;
// Default congestion control algorithm.
pub(crate) const DEFAULT_CONGESTION_CONTROL_ALGORITHM: &str = "cubic";
// Default delay between concurrent connection attempts.
pub(crate) const DEFAULT_CONNECTION_ATTEMPT_DELAY: f64 = 0.250;
// Default connection idle timeout.
pub(crate) const DEFAULT_CONNECTION_IDLE_TIMEOUT: f64 = 60.0;
// Default connection establishment timeout.
pub(crate) const DEFAULT_CONNECT_TIMEOUT: f64 = 30.0;
// Default development server port.
pub(crate) const DEFAULT_DEV_PORT: u16 = 4433;
// Default event history buffer capacity (0 disables).
pub(crate) const DEFAULT_EVENT_HISTORY_CAPACITY: u64 = 0;
// Default event queue capacity.
pub(crate) const DEFAULT_EVENT_QUEUE_CAPACITY: u64 = 100;
// Default flow control window.
pub(crate) const DEFAULT_FLOW_CONTROL_WINDOW: u64 = 4 * 1024 * 1024;
// Default initial maximum data limit.
pub(crate) const DEFAULT_INITIAL_MAX_DATA: u64 = 4 * 1024 * 1024;
// Default initial maximum bidirectional streams.
pub(crate) const DEFAULT_INITIAL_MAX_STREAMS_BIDI: u64 = 10;
// Default initial maximum unidirectional streams.
pub(crate) const DEFAULT_INITIAL_MAX_STREAMS_UNI: u64 = 10;
// Default keep-alive interval.
pub(crate) const DEFAULT_KEEP_ALIVE_INTERVAL: f64 = 30.0;
// Default logging level.
pub(crate) const DEFAULT_LOG_LEVEL: &str = "INFO";
// Default maximum H3 capsule size.
pub(crate) const DEFAULT_MAX_CAPSULE_SIZE: u64 = 65536;
// Default maximum connections.
pub(crate) const DEFAULT_MAX_CONNECTIONS: u64 = 100;
// Default maximum UDP datagram size.
pub(crate) const DEFAULT_MAX_DATAGRAM_SIZE: u64 = 1350;
// Default maximum event listeners.
pub(crate) const DEFAULT_MAX_EVENT_LISTENERS: u64 = 10;
// Default maximum HTTP/3 field section size.
pub(crate) const DEFAULT_MAX_FIELD_SECTION_SIZE: u64 = 65536;
// Default maximum session pending events.
pub(crate) const DEFAULT_MAX_SESSION_PENDING_EVENTS: u64 = 100;
// Default maximum sessions.
pub(crate) const DEFAULT_MAX_SESSIONS: u64 = 10;
// Default maximum stream read buffer size.
pub(crate) const DEFAULT_MAX_STREAM_READ_BUFFER_SIZE: u64 = 1024 * 1024;
// Default maximum stream write buffer size.
pub(crate) const DEFAULT_MAX_STREAM_WRITE_BUFFER_SIZE: u64 = 1024 * 1024;
// Default maximum total pending events.
pub(crate) const DEFAULT_MAX_TOTAL_PENDING_EVENTS: u64 = 1000;
// Default pending event time-to-live (TTL).
pub(crate) const DEFAULT_PENDING_EVENT_TTL: f64 = 5.0;
// Default QUIC maximum concurrent bidirectional streams.
pub(crate) const DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS: u64 = 100;
// Default QUIC maximum concurrent unidirectional streams.
pub(crate) const DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS: u64 = 100;
// Default QUIC connection receive window.
pub(crate) const DEFAULT_QUIC_RECEIVE_WINDOW: u64 = 16 * 1024 * 1024;
// Default QUIC connection send window.
pub(crate) const DEFAULT_QUIC_SEND_WINDOW: u64 = 16 * 1024 * 1024;
// Default QUIC stream receive window.
pub(crate) const DEFAULT_QUIC_STREAM_RECEIVE_WINDOW: u64 = 1024 * 1024;
// Default stream read operation timeout.
pub(crate) const DEFAULT_READ_TIMEOUT: f64 = 60.0;
// Default resource cleanup interval.
pub(crate) const DEFAULT_RESOURCE_CLEANUP_INTERVAL: f64 = 15.0;
// Default stream creation timeout.
pub(crate) const DEFAULT_STREAM_CREATION_TIMEOUT: f64 = 10.0;
// Default stream write operation timeout.
pub(crate) const DEFAULT_WRITE_TIMEOUT: f64 = 30.0;
// Application error: `APP_AUTHENTICATION_FAILED`.
pub(crate) const ERR_APP_AUTHENTICATION_FAILED: u64 = 0x1004;
// Application error: `APP_CANCELLED`.
pub(crate) const ERR_APP_CANCELLED: u64 = 0x1000;
// Application error: `APP_CONNECTION_TIMEOUT`.
pub(crate) const ERR_APP_CONNECTION_TIMEOUT: u64 = 0x1002;
// Application error: `APP_GENERIC_ERROR`.
pub(crate) const ERR_APP_GENERIC_ERROR: u64 = 0x1001;
// Application error: `APP_INVALID_REQUEST`.
pub(crate) const ERR_APP_INVALID_REQUEST: u64 = 0x1007;
// Application error: `APP_NO_ERROR`.
pub(crate) const ERR_APP_NO_ERROR: u64 = 0x0;
// Application error: `APP_OPERATION_TIMEOUT`.
pub(crate) const ERR_APP_OPERATION_TIMEOUT: u64 = 0x1003;
// Application error: `APP_PERMISSION_DENIED`.
pub(crate) const ERR_APP_PERMISSION_DENIED: u64 = 0x1005;
// Application error: `APP_RESOURCE_EXHAUSTED`.
pub(crate) const ERR_APP_RESOURCE_EXHAUSTED: u64 = 0x1006;
// Application error: `APP_SERVICE_UNAVAILABLE`.
pub(crate) const ERR_APP_SERVICE_UNAVAILABLE: u64 = 0x1008;
// HTTP/3 error: `H3_CLOSED_CRITICAL_STREAM`.
pub(crate) const ERR_H3_CLOSED_CRITICAL_STREAM: u64 = 0x0104;
// HTTP/3 error: `H3_CONNECT_ERROR`.
pub(crate) const ERR_H3_CONNECT_ERROR: u64 = 0x010F;
// HTTP/3 error: `H3_DATAGRAM_ERROR`.
pub(crate) const ERR_H3_DATAGRAM_ERROR: u64 = 0x0033;
// HTTP/3 error: `H3_EXCESSIVE_LOAD`.
pub(crate) const ERR_H3_EXCESSIVE_LOAD: u64 = 0x0107;
// HTTP/3 error: `H3_FRAME_ERROR`.
pub(crate) const ERR_H3_FRAME_ERROR: u64 = 0x0106;
// HTTP/3 error: `H3_FRAME_UNEXPECTED`.
pub(crate) const ERR_H3_FRAME_UNEXPECTED: u64 = 0x0105;
// HTTP/3 error: `H3_GENERAL_PROTOCOL_ERROR`.
pub(crate) const ERR_H3_GENERAL_PROTOCOL_ERROR: u64 = 0x0101;
// HTTP/3 error: `H3_ID_ERROR`.
pub(crate) const ERR_H3_ID_ERROR: u64 = 0x0108;
// HTTP/3 error: `H3_INTERNAL_ERROR`.
pub(crate) const ERR_H3_INTERNAL_ERROR: u64 = 0x0102;
// HTTP/3 error: `H3_MESSAGE_ERROR`.
pub(crate) const ERR_H3_MESSAGE_ERROR: u64 = 0x010E;
// HTTP/3 error: `H3_MISSING_SETTINGS`.
pub(crate) const ERR_H3_MISSING_SETTINGS: u64 = 0x010A;
// HTTP/3 error: `H3_NO_ERROR`.
pub(crate) const ERR_H3_NO_ERROR: u64 = 0x0100;
// HTTP/3 error: `H3_REQUEST_CANCELLED`.
pub(crate) const ERR_H3_REQUEST_CANCELLED: u64 = 0x010C;
// HTTP/3 error: `H3_REQUEST_INCOMPLETE`.
pub(crate) const ERR_H3_REQUEST_INCOMPLETE: u64 = 0x010D;
// HTTP/3 error: `H3_REQUEST_REJECTED`.
pub(crate) const ERR_H3_REQUEST_REJECTED: u64 = 0x010B;
// HTTP/3 error: `H3_SETTINGS_ERROR`.
pub(crate) const ERR_H3_SETTINGS_ERROR: u64 = 0x0109;
// HTTP/3 error: `H3_STREAM_CREATION_ERROR`.
pub(crate) const ERR_H3_STREAM_CREATION_ERROR: u64 = 0x0103;
// HTTP/3 error: `H3_VERSION_FALLBACK`.
pub(crate) const ERR_H3_VERSION_FALLBACK: u64 = 0x0110;
// Library error: `LIB_CONNECTION_STATE_ERROR`.
pub(crate) const ERR_LIB_CONNECTION_STATE_ERROR: u64 = 0x1100_0001;
// Library error: `LIB_INTERNAL_ERROR`.
pub(crate) const ERR_LIB_INTERNAL_ERROR: u64 = 0x1000_0001;
// Library error: `LIB_SESSION_STATE_ERROR`.
pub(crate) const ERR_LIB_SESSION_STATE_ERROR: u64 = 0x1200_0001;
// Library error: `LIB_STREAM_STATE_ERROR`.
pub(crate) const ERR_LIB_STREAM_STATE_ERROR: u64 = 0x1300_0001;
// QPACK error: `QPACK_DECODER_STREAM_ERROR`.
pub(crate) const ERR_QPACK_DECODER_STREAM_ERROR: u64 = 0x0202;
// QPACK error: `QPACK_DECOMPRESSION_FAILED`.
pub(crate) const ERR_QPACK_DECOMPRESSION_FAILED: u64 = 0x0200;
// QPACK error: `QPACK_ENCODER_STREAM_ERROR`.
pub(crate) const ERR_QPACK_ENCODER_STREAM_ERROR: u64 = 0x0201;
// QUIC error: `AEAD_LIMIT_REACHED`.
pub(crate) const ERR_QUIC_AEAD_LIMIT_REACHED: u64 = 0x0F;
// QUIC error: `APPLICATION_ERROR`.
pub(crate) const ERR_QUIC_APPLICATION_ERROR: u64 = 0x0C;
// QUIC error: `CONNECTION_ID_LIMIT_ERROR`.
pub(crate) const ERR_QUIC_CONNECTION_ID_LIMIT_ERROR: u64 = 0x09;
// QUIC error: `CONNECTION_REFUSED`.
pub(crate) const ERR_QUIC_CONNECTION_REFUSED: u64 = 0x02;
// QUIC error: `CRYPTO_BUFFER_EXCEEDED`.
pub(crate) const ERR_QUIC_CRYPTO_BUFFER_EXCEEDED: u64 = 0x0D;
// QUIC error: `FINAL_SIZE_ERROR`.
pub(crate) const ERR_QUIC_FINAL_SIZE_ERROR: u64 = 0x06;
// QUIC error: `FLOW_CONTROL_ERROR`.
pub(crate) const ERR_QUIC_FLOW_CONTROL_ERROR: u64 = 0x03;
// QUIC error: `FRAME_ENCODING_ERROR`.
pub(crate) const ERR_QUIC_FRAME_ENCODING_ERROR: u64 = 0x07;
// QUIC error: `INTERNAL_ERROR`.
pub(crate) const ERR_QUIC_INTERNAL_ERROR: u64 = 0x01;
// QUIC error: `INVALID_TOKEN`.
pub(crate) const ERR_QUIC_INVALID_TOKEN: u64 = 0x0B;
// QUIC error: `KEY_UPDATE_ERROR`.
pub(crate) const ERR_QUIC_KEY_UPDATE_ERROR: u64 = 0x0E;
// QUIC error: `NO_ERROR`.
pub(crate) const ERR_QUIC_NO_ERROR: u64 = 0x00;
// QUIC error: `NO_VIABLE_PATH`.
pub(crate) const ERR_QUIC_NO_VIABLE_PATH: u64 = 0x10;
// QUIC error: `PROTOCOL_VIOLATION`.
pub(crate) const ERR_QUIC_PROTOCOL_VIOLATION: u64 = 0x0A;
// QUIC error: `STREAM_LIMIT_ERROR`.
pub(crate) const ERR_QUIC_STREAM_LIMIT_ERROR: u64 = 0x04;
// QUIC error: `STREAM_STATE_ERROR`.
pub(crate) const ERR_QUIC_STREAM_STATE_ERROR: u64 = 0x05;
// QUIC error: `TRANSPORT_PARAMETER_ERROR`.
pub(crate) const ERR_QUIC_TRANSPORT_PARAMETER_ERROR: u64 = 0x08;
// WebTransport error: `WT_ALPN_ERROR`.
pub(crate) const ERR_WT_ALPN_ERROR: u64 = 0x0817_B3DD;
// WebTransport error: application error range start.
pub(crate) const ERR_WT_APPLICATION_ERROR_FIRST: u64 = 0x52E4_A40F_A8DB;
// WebTransport error: application error range end.
pub(crate) const ERR_WT_APPLICATION_ERROR_LAST: u64 = 0x52E5_AC98_3162;
// WebTransport error: `WT_BUFFERED_STREAM_REJECTED`.
pub(crate) const ERR_WT_BUFFERED_STREAM_REJECTED: u64 = 0x3994_BD84;
// WebTransport error: `WT_FLOW_CONTROL_ERROR`.
pub(crate) const ERR_WT_FLOW_CONTROL_ERROR: u64 = 0x045D_4487;
// WebTransport error: `WT_REQUIREMENTS_NOT_MET`.
pub(crate) const ERR_WT_REQUIREMENTS_NOT_MET: u64 = 0x212C_0D48;
// WebTransport error: `WT_SESSION_GONE`.
pub(crate) const ERR_WT_SESSION_GONE: u64 = 0x170D_7B68;
// WebTransport error: stream buffer capacity exceeded.
pub(crate) const ERR_WT_STREAM_BUFFER_EXCEEDED: u64 = ERR_WT_APPLICATION_ERROR_FIRST + 1;
// HTTP/3 frame type: `CANCEL_PUSH`.
pub(crate) const H3_FRAME_TYPE_CANCEL_PUSH: u64 = 0x03;
// HTTP/3 frame type: `DATA`.
pub(crate) const H3_FRAME_TYPE_DATA: u64 = 0x00;
// HTTP/3 frame type: `GOAWAY`.
pub(crate) const H3_FRAME_TYPE_GOAWAY: u64 = 0x07;
// HTTP/3 frame type: `HEADERS`.
pub(crate) const H3_FRAME_TYPE_HEADERS: u64 = 0x01;
// HTTP/3 frame type: `MAX_PUSH_ID`.
pub(crate) const H3_FRAME_TYPE_MAX_PUSH_ID: u64 = 0x0D;
// HTTP/3 frame type: `PUSH_PROMISE`.
pub(crate) const H3_FRAME_TYPE_PUSH_PROMISE: u64 = 0x05;
// HTTP/3 frame type: `SETTINGS`.
pub(crate) const H3_FRAME_TYPE_SETTINGS: u64 = 0x04;
// HTTP/3 frame type: `WT_STREAM`.
pub(crate) const H3_FRAME_TYPE_WT_STREAM: u64 = 0x41;
// HTTP/3 minimum reserved unidirectional stream count.
pub(crate) const H3_MIN_UNI_STREAM_COUNT: u64 = 3;
// HTTP/3 stream type: `CONTROL`.
pub(crate) const H3_STREAM_TYPE_CONTROL: u64 = 0x00;
// HTTP/3 stream type: `PUSH`.
pub(crate) const H3_STREAM_TYPE_PUSH: u64 = 0x01;
// HTTP/3 stream type: `QPACK_DECODER`.
pub(crate) const H3_STREAM_TYPE_QPACK_DECODER: u64 = 0x03;
// HTTP/3 stream type: `QPACK_ENCODER`.
pub(crate) const H3_STREAM_TYPE_QPACK_ENCODER: u64 = 0x02;
// HTTP/3 stream type: `WEBTRANSPORT`.
pub(crate) const H3_STREAM_TYPE_WEBTRANSPORT: u64 = 0x54;
// QUIC variable-length integer limit.
pub(crate) const QUIC_VARINT_LIMIT: u64 = (1 << 62) - 1;
// QUIC stream direction identifier mask.
pub(crate) const QUIC_STREAM_DIRECTION_MASK: u64 = 0x02;
// QUIC stream initiator identifier mask.
pub(crate) const QUIC_STREAM_INITIATOR_MASK: u64 = 0x01;
// HTTP/3 Setting: `ENABLE_CONNECT_PROTOCOL`.
pub(crate) const SETTINGS_ENABLE_CONNECT_PROTOCOL: u64 = 0x08;
// HTTP/3 Setting: `H3_DATAGRAM`.
pub(crate) const SETTINGS_H3_DATAGRAM: u64 = 0x33;
// HTTP/3 Setting: `MAX_FIELD_SECTION_SIZE`.
pub(crate) const SETTINGS_MAX_FIELD_SECTION_SIZE: u64 = 0x06;
// HTTP/3 Setting: `QPACK_BLOCKED_STREAMS`.
pub(crate) const SETTINGS_QPACK_BLOCKED_STREAMS: u64 = 0x07;
// HTTP/3 Setting: `QPACK_MAX_TABLE_CAPACITY`.
pub(crate) const SETTINGS_QPACK_MAX_TABLE_CAPACITY: u64 = 0x01;
// HTTP/3 Setting: `WEBTRANSPORT_ENABLED`.
pub(crate) const SETTINGS_WT_ENABLED: u64 = 0x2C7C_F000;
// HTTP/3 Setting: `WEBTRANSPORT_INITIAL_MAX_DATA`.
pub(crate) const SETTINGS_WT_INITIAL_MAX_DATA: u64 = 0x2B61;
// HTTP/3 Setting: `WEBTRANSPORT_INITIAL_MAX_STREAMS_BIDI`.
pub(crate) const SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI: u64 = 0x2B65;
// HTTP/3 Setting: `WEBTRANSPORT_INITIAL_MAX_STREAMS_UNI`.
pub(crate) const SETTINGS_WT_INITIAL_MAX_STREAMS_UNI: u64 = 0x2B64;
// UDP maximum datagram size payload limit.
pub(crate) const UDP_MAX_DATAGRAM_SIZE: u64 = 65535;
// UDP transmit batch allocation capacity.
pub(crate) const UDP_TRANSMIT_BATCH_CAPACITY: usize = 64;
// WebTransport header: `wt-available-protocols`.
pub(crate) const WT_AVAILABLE_PROTOCOLS: &[u8] = b"wt-available-protocols";
// WebTransport capsule type: `CLOSE_SESSION`.
pub(crate) const WT_CAPSULE_TYPE_CLOSE_SESSION: u64 = 0x2843;
// WebTransport capsule type: `DATA_BLOCKED`.
pub(crate) const WT_CAPSULE_TYPE_DATA_BLOCKED: u64 = 0x190B_4D41;
// WebTransport capsule type: `DRAIN_SESSION`.
pub(crate) const WT_CAPSULE_TYPE_DRAIN_SESSION: u64 = 0x78AE;
// WebTransport capsule type: `MAX_DATA`.
pub(crate) const WT_CAPSULE_TYPE_MAX_DATA: u64 = 0x190B_4D3D;
// WebTransport capsule type: `MAX_STREAMS_BIDI`.
pub(crate) const WT_CAPSULE_TYPE_MAX_STREAMS_BIDI: u64 = 0x190B_4D3F;
// WebTransport capsule type: `MAX_STREAMS_UNI`.
pub(crate) const WT_CAPSULE_TYPE_MAX_STREAMS_UNI: u64 = 0x190B_4D40;
// WebTransport capsule type: `MAX_STREAM_DATA`.
pub(crate) const WT_CAPSULE_TYPE_MAX_STREAM_DATA: u64 = 0x190B_4D3E;
// WebTransport capsule type: `STREAMS_BLOCKED_BIDI`.
pub(crate) const WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI: u64 = 0x190B_4D43;
// WebTransport capsule type: `STREAMS_BLOCKED_UNI`.
pub(crate) const WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI: u64 = 0x190B_4D44;
// WebTransport capsule type: `STREAM_DATA_BLOCKED`.
pub(crate) const WT_CAPSULE_TYPE_STREAM_DATA_BLOCKED: u64 = 0x190B_4D42;
// WebTransport TLS exporter label: `EXPORTER-WebTransport`.
pub(crate) const WT_EXPORTER_LABEL: &str = "EXPORTER-WebTransport";
// WebTransport maximum close reason size.
pub(crate) const WT_MAX_CLOSE_REASON_SIZE: u64 = 1024;
// WebTransport header: `wt-protocol`.
pub(crate) const WT_PROTOCOL: &[u8] = b"wt-protocol";
// WebTransport session bidirectional control stream count.
pub(crate) const WT_SESSION_CONTROL_BIDI_STREAM_COUNT: u64 = 1;
// WebTransport protocol-defined maximum concurrent streams limit.
pub(crate) const WT_STREAMS_LIMIT: u64 = 1 << 60;
// WebTransport over HTTP/3 upgrade token.
pub(crate) const WT_UPGRADE_TOKEN: &[u8] = b"webtransport-h3";

#[cfg(test)]
mod tests;
