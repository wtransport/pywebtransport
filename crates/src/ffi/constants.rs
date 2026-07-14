//! FFI bindings for protocol constants and configuration defaults.

use pyo3::prelude::*;

use crate::common::constants;

// Protocol constant and default configuration registration.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("DEFAULT_ALPN_PROTOCOLS", constants::DEFAULT_ALPN_PROTOCOLS)?;
    m.add("DEFAULT_BIND_HOST", constants::DEFAULT_BIND_HOST)?;
    m.add("DEFAULT_CLOSE_TIMEOUT", constants::DEFAULT_CLOSE_TIMEOUT)?;
    m.add(
        "DEFAULT_CONGESTION_CONTROL_ALGORITHM",
        constants::DEFAULT_CONGESTION_CONTROL_ALGORITHM,
    )?;
    m.add(
        "DEFAULT_CONNECTION_ATTEMPT_DELAY",
        constants::DEFAULT_CONNECTION_ATTEMPT_DELAY,
    )?;
    m.add(
        "DEFAULT_CONNECTION_IDLE_TIMEOUT",
        constants::DEFAULT_CONNECTION_IDLE_TIMEOUT,
    )?;
    m.add(
        "DEFAULT_CONNECT_TIMEOUT",
        constants::DEFAULT_CONNECT_TIMEOUT,
    )?;
    m.add("DEFAULT_DEV_PORT", constants::DEFAULT_DEV_PORT)?;
    m.add(
        "DEFAULT_EVENT_HISTORY_CAPACITY",
        constants::DEFAULT_EVENT_HISTORY_CAPACITY,
    )?;
    m.add(
        "DEFAULT_EVENT_QUEUE_CAPACITY",
        constants::DEFAULT_EVENT_QUEUE_CAPACITY,
    )?;
    m.add(
        "DEFAULT_FLOW_CONTROL_WINDOW",
        constants::DEFAULT_FLOW_CONTROL_WINDOW,
    )?;
    m.add(
        "DEFAULT_INITIAL_MAX_DATA",
        constants::DEFAULT_INITIAL_MAX_DATA,
    )?;
    m.add(
        "DEFAULT_INITIAL_MAX_STREAMS_BIDI",
        constants::DEFAULT_INITIAL_MAX_STREAMS_BIDI,
    )?;
    m.add(
        "DEFAULT_INITIAL_MAX_STREAMS_UNI",
        constants::DEFAULT_INITIAL_MAX_STREAMS_UNI,
    )?;
    m.add(
        "DEFAULT_KEEP_ALIVE_INTERVAL",
        constants::DEFAULT_KEEP_ALIVE_INTERVAL,
    )?;
    m.add("DEFAULT_LOG_LEVEL", constants::DEFAULT_LOG_LEVEL)?;
    m.add(
        "DEFAULT_MAX_CAPSULE_SIZE",
        constants::DEFAULT_MAX_CAPSULE_SIZE,
    )?;
    m.add(
        "DEFAULT_MAX_CONNECTIONS",
        constants::DEFAULT_MAX_CONNECTIONS,
    )?;
    m.add(
        "DEFAULT_MAX_DATAGRAM_SIZE",
        constants::DEFAULT_MAX_DATAGRAM_SIZE,
    )?;
    m.add(
        "DEFAULT_MAX_EVENT_LISTENERS",
        constants::DEFAULT_MAX_EVENT_LISTENERS,
    )?;
    m.add(
        "DEFAULT_MAX_FIELD_SECTION_SIZE",
        constants::DEFAULT_MAX_FIELD_SECTION_SIZE,
    )?;
    m.add(
        "DEFAULT_MAX_PENDING_CAPSULES",
        constants::DEFAULT_MAX_PENDING_CAPSULES,
    )?;
    m.add(
        "DEFAULT_MAX_PENDING_DATAGRAMS",
        constants::DEFAULT_MAX_PENDING_DATAGRAMS,
    )?;
    m.add(
        "DEFAULT_MAX_PENDING_STREAMS",
        constants::DEFAULT_MAX_PENDING_STREAMS,
    )?;
    m.add(
        "DEFAULT_MAX_SESSION_PENDING_EVENTS",
        constants::DEFAULT_MAX_SESSION_PENDING_EVENTS,
    )?;
    m.add("DEFAULT_MAX_SESSIONS", constants::DEFAULT_MAX_SESSIONS)?;
    m.add(
        "DEFAULT_MAX_STREAM_READ_BUFFER_SIZE",
        constants::DEFAULT_MAX_STREAM_READ_BUFFER_SIZE,
    )?;
    m.add(
        "DEFAULT_MAX_STREAM_WRITE_BUFFER_SIZE",
        constants::DEFAULT_MAX_STREAM_WRITE_BUFFER_SIZE,
    )?;
    m.add(
        "DEFAULT_MAX_TOTAL_PENDING_EVENTS",
        constants::DEFAULT_MAX_TOTAL_PENDING_EVENTS,
    )?;
    m.add(
        "DEFAULT_PENDING_EVENT_TTL",
        constants::DEFAULT_PENDING_EVENT_TTL,
    )?;
    m.add(
        "DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS",
        constants::DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS,
    )?;
    m.add(
        "DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS",
        constants::DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS,
    )?;
    m.add(
        "DEFAULT_QUIC_RECEIVE_WINDOW",
        constants::DEFAULT_QUIC_RECEIVE_WINDOW,
    )?;
    m.add(
        "DEFAULT_QUIC_SEND_WINDOW",
        constants::DEFAULT_QUIC_SEND_WINDOW,
    )?;
    m.add(
        "DEFAULT_QUIC_STREAM_RECEIVE_WINDOW",
        constants::DEFAULT_QUIC_STREAM_RECEIVE_WINDOW,
    )?;
    m.add("DEFAULT_READ_TIMEOUT", constants::DEFAULT_READ_TIMEOUT)?;
    m.add(
        "DEFAULT_RESOURCE_CLEANUP_INTERVAL",
        constants::DEFAULT_RESOURCE_CLEANUP_INTERVAL,
    )?;
    m.add(
        "DEFAULT_STREAM_CREATION_TIMEOUT",
        constants::DEFAULT_STREAM_CREATION_TIMEOUT,
    )?;
    m.add("DEFAULT_WRITE_TIMEOUT", constants::DEFAULT_WRITE_TIMEOUT)?;
    m.add(
        "ERR_APP_AUTHENTICATION_FAILED",
        constants::ERR_APP_AUTHENTICATION_FAILED,
    )?;
    m.add("ERR_APP_CANCELLED", constants::ERR_APP_CANCELLED)?;
    m.add(
        "ERR_APP_CONNECTION_TIMEOUT",
        constants::ERR_APP_CONNECTION_TIMEOUT,
    )?;
    m.add("ERR_APP_GENERIC_ERROR", constants::ERR_APP_GENERIC_ERROR)?;
    m.add(
        "ERR_APP_INVALID_REQUEST",
        constants::ERR_APP_INVALID_REQUEST,
    )?;
    m.add("ERR_APP_NO_ERROR", constants::ERR_APP_NO_ERROR)?;
    m.add(
        "ERR_APP_OPERATION_TIMEOUT",
        constants::ERR_APP_OPERATION_TIMEOUT,
    )?;
    m.add(
        "ERR_APP_PERMISSION_DENIED",
        constants::ERR_APP_PERMISSION_DENIED,
    )?;
    m.add(
        "ERR_APP_RESOURCE_EXHAUSTED",
        constants::ERR_APP_RESOURCE_EXHAUSTED,
    )?;
    m.add(
        "ERR_APP_SERVICE_UNAVAILABLE",
        constants::ERR_APP_SERVICE_UNAVAILABLE,
    )?;
    m.add(
        "ERR_H3_CLOSED_CRITICAL_STREAM",
        constants::ERR_H3_CLOSED_CRITICAL_STREAM,
    )?;
    m.add("ERR_H3_CONNECT_ERROR", constants::ERR_H3_CONNECT_ERROR)?;
    m.add("ERR_H3_DATAGRAM_ERROR", constants::ERR_H3_DATAGRAM_ERROR)?;
    m.add("ERR_H3_EXCESSIVE_LOAD", constants::ERR_H3_EXCESSIVE_LOAD)?;
    m.add("ERR_H3_FRAME_ERROR", constants::ERR_H3_FRAME_ERROR)?;
    m.add(
        "ERR_H3_FRAME_UNEXPECTED",
        constants::ERR_H3_FRAME_UNEXPECTED,
    )?;
    m.add(
        "ERR_H3_GENERAL_PROTOCOL_ERROR",
        constants::ERR_H3_GENERAL_PROTOCOL_ERROR,
    )?;
    m.add("ERR_H3_ID_ERROR", constants::ERR_H3_ID_ERROR)?;
    m.add("ERR_H3_INTERNAL_ERROR", constants::ERR_H3_INTERNAL_ERROR)?;
    m.add("ERR_H3_MESSAGE_ERROR", constants::ERR_H3_MESSAGE_ERROR)?;
    m.add(
        "ERR_H3_MISSING_SETTINGS",
        constants::ERR_H3_MISSING_SETTINGS,
    )?;
    m.add("ERR_H3_NO_ERROR", constants::ERR_H3_NO_ERROR)?;
    m.add(
        "ERR_H3_REQUEST_CANCELLED",
        constants::ERR_H3_REQUEST_CANCELLED,
    )?;
    m.add(
        "ERR_H3_REQUEST_INCOMPLETE",
        constants::ERR_H3_REQUEST_INCOMPLETE,
    )?;
    m.add(
        "ERR_H3_REQUEST_REJECTED",
        constants::ERR_H3_REQUEST_REJECTED,
    )?;
    m.add("ERR_H3_SETTINGS_ERROR", constants::ERR_H3_SETTINGS_ERROR)?;
    m.add(
        "ERR_H3_STREAM_CREATION_ERROR",
        constants::ERR_H3_STREAM_CREATION_ERROR,
    )?;
    m.add(
        "ERR_H3_VERSION_FALLBACK",
        constants::ERR_H3_VERSION_FALLBACK,
    )?;
    m.add(
        "ERR_LIB_CONNECTION_STATE_ERROR",
        constants::ERR_LIB_CONNECTION_STATE_ERROR,
    )?;
    m.add("ERR_LIB_INTERNAL_ERROR", constants::ERR_LIB_INTERNAL_ERROR)?;
    m.add(
        "ERR_LIB_SESSION_STATE_ERROR",
        constants::ERR_LIB_SESSION_STATE_ERROR,
    )?;
    m.add(
        "ERR_LIB_STREAM_STATE_ERROR",
        constants::ERR_LIB_STREAM_STATE_ERROR,
    )?;
    m.add(
        "ERR_QPACK_DECODER_STREAM_ERROR",
        constants::ERR_QPACK_DECODER_STREAM_ERROR,
    )?;
    m.add(
        "ERR_QPACK_DECOMPRESSION_FAILED",
        constants::ERR_QPACK_DECOMPRESSION_FAILED,
    )?;
    m.add(
        "ERR_QPACK_ENCODER_STREAM_ERROR",
        constants::ERR_QPACK_ENCODER_STREAM_ERROR,
    )?;
    m.add(
        "ERR_QUIC_AEAD_LIMIT_REACHED",
        constants::ERR_QUIC_AEAD_LIMIT_REACHED,
    )?;
    m.add(
        "ERR_QUIC_APPLICATION_ERROR",
        constants::ERR_QUIC_APPLICATION_ERROR,
    )?;
    m.add(
        "ERR_QUIC_CONNECTION_ID_LIMIT_ERROR",
        constants::ERR_QUIC_CONNECTION_ID_LIMIT_ERROR,
    )?;
    m.add(
        "ERR_QUIC_CONNECTION_REFUSED",
        constants::ERR_QUIC_CONNECTION_REFUSED,
    )?;
    m.add(
        "ERR_QUIC_CRYPTO_BUFFER_EXCEEDED",
        constants::ERR_QUIC_CRYPTO_BUFFER_EXCEEDED,
    )?;
    m.add(
        "ERR_QUIC_FINAL_SIZE_ERROR",
        constants::ERR_QUIC_FINAL_SIZE_ERROR,
    )?;
    m.add(
        "ERR_QUIC_FLOW_CONTROL_ERROR",
        constants::ERR_QUIC_FLOW_CONTROL_ERROR,
    )?;
    m.add(
        "ERR_QUIC_FRAME_ENCODING_ERROR",
        constants::ERR_QUIC_FRAME_ENCODING_ERROR,
    )?;
    m.add(
        "ERR_QUIC_INTERNAL_ERROR",
        constants::ERR_QUIC_INTERNAL_ERROR,
    )?;
    m.add("ERR_QUIC_INVALID_TOKEN", constants::ERR_QUIC_INVALID_TOKEN)?;
    m.add(
        "ERR_QUIC_KEY_UPDATE_ERROR",
        constants::ERR_QUIC_KEY_UPDATE_ERROR,
    )?;
    m.add("ERR_QUIC_NO_ERROR", constants::ERR_QUIC_NO_ERROR)?;
    m.add(
        "ERR_QUIC_NO_VIABLE_PATH",
        constants::ERR_QUIC_NO_VIABLE_PATH,
    )?;
    m.add(
        "ERR_QUIC_PROTOCOL_VIOLATION",
        constants::ERR_QUIC_PROTOCOL_VIOLATION,
    )?;
    m.add(
        "ERR_QUIC_STREAM_LIMIT_ERROR",
        constants::ERR_QUIC_STREAM_LIMIT_ERROR,
    )?;
    m.add(
        "ERR_QUIC_STREAM_STATE_ERROR",
        constants::ERR_QUIC_STREAM_STATE_ERROR,
    )?;
    m.add(
        "ERR_QUIC_TRANSPORT_PARAMETER_ERROR",
        constants::ERR_QUIC_TRANSPORT_PARAMETER_ERROR,
    )?;
    m.add("ERR_WT_ALPN_ERROR", constants::ERR_WT_ALPN_ERROR)?;
    m.add(
        "ERR_WT_APPLICATION_ERROR_FIRST",
        constants::ERR_WT_APPLICATION_ERROR_FIRST,
    )?;
    m.add(
        "ERR_WT_APPLICATION_ERROR_LAST",
        constants::ERR_WT_APPLICATION_ERROR_LAST,
    )?;
    m.add(
        "ERR_WT_BUFFERED_STREAM_REJECTED",
        constants::ERR_WT_BUFFERED_STREAM_REJECTED,
    )?;
    m.add(
        "ERR_WT_FLOW_CONTROL_ERROR",
        constants::ERR_WT_FLOW_CONTROL_ERROR,
    )?;
    m.add(
        "ERR_WT_REQUIREMENTS_NOT_MET",
        constants::ERR_WT_REQUIREMENTS_NOT_MET,
    )?;
    m.add("ERR_WT_SESSION_GONE", constants::ERR_WT_SESSION_GONE)?;
    m.add(
        "ERR_WT_STREAM_BUFFER_EXCEEDED",
        constants::ERR_WT_STREAM_BUFFER_EXCEEDED,
    )?;
    m.add(
        "H3_MIN_UNI_STREAM_COUNT",
        constants::H3_MIN_UNI_STREAM_COUNT,
    )?;
    m.add("QUIC_VARINT_LIMIT", constants::QUIC_VARINT_LIMIT)?;
    m.add("UDP_MAX_DATAGRAM_SIZE", constants::UDP_MAX_DATAGRAM_SIZE)?;
    m.add(
        "WT_SESSION_CONTROL_BIDI_STREAM_COUNT",
        constants::WT_SESSION_CONTROL_BIDI_STREAM_COUNT,
    )?;
    m.add("WT_STREAMS_LIMIT", constants::WT_STREAMS_LIMIT)?;

    Ok(())
}
