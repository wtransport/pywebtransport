//! Unit tests for the `crate::common::constants` module.

use std::collections::HashSet;

use rstest::rstest;

use super::*;

#[test]
fn test_alpn_list_integrity() {
    let protocols = DEFAULT_ALPN_PROTOCOLS;

    assert!(protocols.contains(&"h3"));
    assert!(!protocols.is_empty());
}

#[rstest]
#[case(ERR_APP_AUTHENTICATION_FAILED, 0x1004)]
#[case(ERR_APP_CANCELLED, 0x1000)]
#[case(ERR_APP_CONNECTION_TIMEOUT, 0x1002)]
#[case(ERR_APP_GENERIC_ERROR, 0x1001)]
#[case(ERR_APP_INVALID_REQUEST, 0x1007)]
#[case(ERR_APP_NO_ERROR, 0x0)]
#[case(ERR_APP_OPERATION_TIMEOUT, 0x1003)]
#[case(ERR_APP_PERMISSION_DENIED, 0x1005)]
#[case(ERR_APP_RESOURCE_EXHAUSTED, 0x1006)]
#[case(ERR_APP_SERVICE_UNAVAILABLE, 0x1008)]
fn test_app_error_codes_match_spec(#[case] error_code: u64, #[case] expected: u64) {
    assert_eq!(error_code, expected);
}

#[rstest]
#[case(DEFAULT_EVENT_QUEUE_CAPACITY)]
#[case(DEFAULT_MAX_CONNECTIONS)]
#[case(DEFAULT_MAX_EVENT_LISTENERS)]
#[case(DEFAULT_MAX_SESSIONS)]
#[case(DEFAULT_MAX_TRANSPORT_STREAMS)]
fn test_concurrency_limits_are_sane(#[case] limit: u64) {
    assert!(limit >= 1);
}

#[rstest]
#[case(ERR_H3_CLOSED_CRITICAL_STREAM, 0x104)]
#[case(ERR_H3_CONNECT_ERROR, 0x10F)]
#[case(ERR_H3_DATAGRAM_ERROR, 0x33)]
#[case(ERR_H3_EXCESSIVE_LOAD, 0x107)]
#[case(ERR_H3_FRAME_ERROR, 0x106)]
#[case(ERR_H3_FRAME_UNEXPECTED, 0x105)]
#[case(ERR_H3_GENERAL_PROTOCOL_ERROR, 0x101)]
#[case(ERR_H3_ID_ERROR, 0x108)]
#[case(ERR_H3_INTERNAL_ERROR, 0x102)]
#[case(ERR_H3_MESSAGE_ERROR, 0x10E)]
#[case(ERR_H3_MISSING_SETTINGS, 0x10A)]
#[case(ERR_H3_NO_ERROR, 0x100)]
#[case(ERR_H3_REQUEST_CANCELLED, 0x10C)]
#[case(ERR_H3_REQUEST_INCOMPLETE, 0x10D)]
#[case(ERR_H3_REQUEST_REJECTED, 0x10B)]
#[case(ERR_H3_SETTINGS_ERROR, 0x109)]
#[case(ERR_H3_STREAM_CREATION_ERROR, 0x103)]
#[case(ERR_H3_VERSION_FALLBACK, 0x110)]
fn test_http3_error_codes_match_spec(#[case] error_code: u64, #[case] expected: u64) {
    assert_eq!(error_code, expected);
}

#[rstest]
#[case(H3_FRAME_TYPE_CANCEL_PUSH, 0x3)]
#[case(H3_FRAME_TYPE_DATA, 0x0)]
#[case(H3_FRAME_TYPE_GOAWAY, 0x7)]
#[case(H3_FRAME_TYPE_HEADERS, 0x1)]
#[case(H3_FRAME_TYPE_MAX_PUSH_ID, 0xD)]
#[case(H3_FRAME_TYPE_PUSH_PROMISE, 0x5)]
#[case(H3_FRAME_TYPE_SETTINGS, 0x4)]
#[case(H3_FRAME_TYPE_WT_STREAM, 0x41)]
fn test_http3_frame_types_match_spec(#[case] frame_type: u64, #[case] expected_value: u64) {
    assert_eq!(frame_type, expected_value);
}

#[rstest]
#[case(SETTINGS_ENABLE_CONNECT_PROTOCOL, 0x8)]
#[case(SETTINGS_H3_DATAGRAM, 0x33)]
#[case(SETTINGS_MAX_FIELD_SECTION_SIZE, 0x06)]
#[case(SETTINGS_QPACK_BLOCKED_STREAMS, 0x7)]
#[case(SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0x1)]
#[case(SETTINGS_WT_ENABLED, 0x2C7C_F000)]
#[case(SETTINGS_WT_INITIAL_MAX_DATA, 0x2B61)]
#[case(SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, 0x2B65)]
#[case(SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, 0x2B64)]
fn test_http3_settings_identifiers_match_spec(
    #[case] setting_id: u64,
    #[case] expected_value: u64,
) {
    assert_eq!(setting_id, expected_value);
}

#[rstest]
#[case(H3_STREAM_TYPE_CONTROL, 0x0)]
#[case(H3_STREAM_TYPE_PUSH, 0x1)]
#[case(H3_STREAM_TYPE_QPACK_DECODER, 0x3)]
#[case(H3_STREAM_TYPE_QPACK_ENCODER, 0x2)]
#[case(H3_STREAM_TYPE_WEBTRANSPORT, 0x54)]
fn test_http3_stream_types_match_spec(#[case] stream_type: u64, #[case] expected_value: u64) {
    assert_eq!(stream_type, expected_value);
}

#[rstest]
#[case(H3_MIN_UNI_STREAM_COUNT, 3)]
#[case(WT_SESSION_CONTROL_BIDI_STREAM_COUNT, 1)]
fn test_infrastructure_stream_counts_match_spec(
    #[case] stream_count: u64,
    #[case] expected_value: u64,
) {
    assert_eq!(stream_count, expected_value);
}

#[test]
fn test_max_datagram_size_consistency() {
    const { assert!(DEFAULT_MAX_DATAGRAM_SIZE <= UDP_MAX_DATAGRAM_SIZE) };
    assert_eq!(UDP_MAX_DATAGRAM_SIZE, 65535);
}

#[test]
fn test_max_protocol_streams_limit() {
    let calculation = 1u64 << 60;
    assert_eq!(WT_STREAMS_LIMIT, calculation);
}

#[test]
fn test_max_stream_id_limit() {
    let calculation = (1u64 << 62) - 1;
    assert_eq!(QUIC_MAX_STREAM_ID, calculation);
}

#[test]
fn test_protocol_identification_values_are_valid() {
    assert_eq!(WT_UPGRADE_TOKEN, b"webtransport-h3");
}

#[rstest]
#[case(ERR_QUIC_AEAD_LIMIT_REACHED, 0xF)]
#[case(ERR_QUIC_APPLICATION_ERROR, 0xC)]
#[case(ERR_QUIC_CONNECTION_ID_LIMIT_ERROR, 0x9)]
#[case(ERR_QUIC_CONNECTION_REFUSED, 0x2)]
#[case(ERR_QUIC_CRYPTO_BUFFER_EXCEEDED, 0xD)]
#[case(ERR_QUIC_FINAL_SIZE_ERROR, 0x6)]
#[case(ERR_QUIC_FLOW_CONTROL_ERROR, 0x3)]
#[case(ERR_QUIC_FRAME_ENCODING_ERROR, 0x7)]
#[case(ERR_QUIC_INTERNAL_ERROR, 0x1)]
#[case(ERR_QUIC_INVALID_TOKEN, 0xB)]
#[case(ERR_QUIC_KEY_UPDATE_ERROR, 0xE)]
#[case(ERR_QUIC_NO_ERROR, 0x0)]
#[case(ERR_QUIC_NO_VIABLE_PATH, 0x10)]
#[case(ERR_QUIC_PROTOCOL_VIOLATION, 0xA)]
#[case(ERR_QUIC_STREAM_LIMIT_ERROR, 0x4)]
#[case(ERR_QUIC_STREAM_STATE_ERROR, 0x5)]
#[case(ERR_QUIC_TRANSPORT_PARAMETER_ERROR, 0x8)]
fn test_quic_transport_error_codes(#[case] error_code: u64, #[case] expected: u64) {
    assert_eq!(error_code, expected);
}

#[rstest]
#[case(DEFAULT_FLOW_CONTROL_WINDOW)]
#[case(DEFAULT_INITIAL_MAX_DATA)]
#[case(DEFAULT_MAX_CAPSULE_SIZE)]
#[case(DEFAULT_MAX_DATAGRAM_SIZE)]
#[case(DEFAULT_MAX_FIELD_SECTION_SIZE)]
#[case(DEFAULT_MAX_STREAM_READ_BUFFER_SIZE)]
#[case(DEFAULT_MAX_STREAM_WRITE_BUFFER_SIZE)]
fn test_size_configuration_defaults_are_nonzero(#[case] size_value: u64) {
    assert!(size_value > 0);
}

#[rstest]
#[case(QUIC_STREAM_DIRECTION_MASK, 0x2)]
#[case(QUIC_STREAM_INITIATOR_MASK, 0x1)]
fn test_stream_direction_masks(#[case] mask: u64, #[case] expected: u64) {
    assert_eq!(mask, expected);
}

#[rstest]
#[case(DEFAULT_CLOSE_TIMEOUT)]
#[case(DEFAULT_CONNECT_TIMEOUT)]
#[case(DEFAULT_CONNECTION_ATTEMPT_DELAY)]
#[case(DEFAULT_CONNECTION_IDLE_TIMEOUT)]
#[case(DEFAULT_KEEP_ALIVE_INTERVAL)]
#[case(DEFAULT_PENDING_EVENT_TTL)]
#[case(DEFAULT_READ_TIMEOUT)]
#[case(DEFAULT_RESOURCE_CLEANUP_INTERVAL)]
#[case(DEFAULT_STREAM_CREATION_TIMEOUT)]
#[case(DEFAULT_WRITE_TIMEOUT)]
fn test_time_configuration_defaults_are_positive(#[case] timeout_value: f64) {
    assert!(timeout_value > 0.0);
    assert!(timeout_value.is_finite());
}

#[test]
fn test_webtransport_application_error_range() {
    assert_eq!(ERR_WT_APPLICATION_ERROR_FIRST, 0x52E4_A40F_A8DB);
    assert_eq!(ERR_WT_APPLICATION_ERROR_LAST, 0x52E5_AC98_3162);
    const { assert!(ERR_WT_APPLICATION_ERROR_FIRST < ERR_WT_APPLICATION_ERROR_LAST) };
}

#[test]
fn test_webtransport_capsule_types() {
    assert_eq!(WT_CAPSULE_TYPE_CLOSE_SESSION, 0x2843);
    assert_eq!(WT_CAPSULE_TYPE_DRAIN_SESSION, 0x78AE);
    assert_ne!(WT_CAPSULE_TYPE_CLOSE_SESSION, WT_CAPSULE_TYPE_DRAIN_SESSION);
}

#[rstest]
#[case(ERR_WT_ALPN_ERROR, 0x0817_B3DD)]
#[case(ERR_WT_BUFFERED_STREAM_REJECTED, 0x3994_BD84)]
#[case(ERR_WT_FLOW_CONTROL_ERROR, 0x045D_4487)]
#[case(ERR_WT_REQUIREMENTS_NOT_MET, 0x212C_0D48)]
#[case(ERR_WT_SESSION_GONE, 0x170D_7B68)]
#[case(ERR_WT_STREAM_BUFFER_EXCEEDED, 0x52E4_A40F_A8DC)]
fn test_webtransport_error_codes_match_spec(#[case] error_code: u64, #[case] expected: u64) {
    assert_eq!(error_code, expected);
}

#[test]
fn test_webtransport_frame_types_distinctness() {
    let frames = vec![
        WT_CAPSULE_TYPE_DATA_BLOCKED,
        WT_CAPSULE_TYPE_MAX_DATA,
        WT_CAPSULE_TYPE_MAX_STREAM_DATA,
        WT_CAPSULE_TYPE_MAX_STREAMS_BIDI,
        WT_CAPSULE_TYPE_MAX_STREAMS_UNI,
        WT_CAPSULE_TYPE_STREAM_DATA_BLOCKED,
        WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
        WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI,
    ];
    let total_count = frames.len();
    let unique_set: HashSet<u64> = frames.into_iter().collect();

    assert_eq!(
        unique_set.len(),
        total_count,
        "Duplicate WebTransport frame types detected"
    );
}

#[rstest]
#[case(WT_CAPSULE_TYPE_DATA_BLOCKED, 0x190B_4D41)]
#[case(WT_CAPSULE_TYPE_MAX_DATA, 0x190B_4D3D)]
#[case(WT_CAPSULE_TYPE_MAX_STREAM_DATA, 0x190B_4D3E)]
#[case(WT_CAPSULE_TYPE_MAX_STREAMS_BIDI, 0x190B_4D3F)]
#[case(WT_CAPSULE_TYPE_MAX_STREAMS_UNI, 0x190B_4D40)]
#[case(WT_CAPSULE_TYPE_STREAM_DATA_BLOCKED, 0x190B_4D42)]
#[case(WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI, 0x190B_4D43)]
#[case(WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI, 0x190B_4D44)]
fn test_webtransport_frame_types_match_spec(#[case] frame_type: u64, #[case] expected_value: u64) {
    assert_eq!(frame_type, expected_value);
}

#[test]
fn test_webtransport_headers_match_spec() {
    assert_eq!(WT_AVAILABLE_PROTOCOLS, b"wt-available-protocols");
    assert_eq!(WT_PROTOCOL, b"wt-protocol");
}
