//! Unit tests for the `crate::common::constants` module.

use std::collections::HashSet;

use rstest::rstest;

use super::*;

#[test]
fn test_alpn_list_integrity() {
    let expected_protocol = ALPN_H3;

    let protocols = DEFAULT_ALPN_PROTOCOLS;

    assert!(protocols.contains(&expected_protocol));
    assert!(!protocols.is_empty());
}

#[rstest]
#[case(DEFAULT_CLIENT_MAX_CONNECTIONS)]
#[case(DEFAULT_CLIENT_MAX_SESSIONS)]
#[case(DEFAULT_MAX_EVENT_LISTENERS)]
#[case(DEFAULT_MAX_EVENT_QUEUE_SIZE)]
#[case(DEFAULT_SERVER_MAX_CONNECTIONS)]
#[case(DEFAULT_SERVER_MAX_SESSIONS)]
#[test]
fn test_concurrency_limits_are_sane(#[case] limit: u64) {
    let min_limit = 1;

    let value = limit;

    assert!(value >= min_limit);
}

#[test]
fn test_congestion_control_algorithms_list() {
    let default_algo = DEFAULT_CONGESTION_CONTROL_ALGORITHM;
    let supported_algos = SUPPORTED_CONGESTION_CONTROL_ALGORITHMS;

    let is_default_supported = supported_algos.contains(&default_algo);

    assert!(is_default_supported);
    assert!(supported_algos.len() >= 2);
    assert!(supported_algos.contains(&"cubic"));
    assert!(supported_algos.contains(&"reno"));
}

#[rstest]
#[case(ERR_H3_NO_ERROR, 0x100)]
#[case(ERR_H3_GENERAL_PROTOCOL_ERROR, 0x101)]
#[case(ERR_H3_INTERNAL_ERROR, 0x102)]
#[case(ERR_H3_STREAM_CREATION_ERROR, 0x103)]
#[case(ERR_H3_CLOSED_CRITICAL_STREAM, 0x104)]
#[case(ERR_H3_FRAME_UNEXPECTED, 0x105)]
#[case(ERR_H3_FRAME_ERROR, 0x106)]
#[case(ERR_H3_EXCESSIVE_LOAD, 0x107)]
#[case(ERR_H3_ID_ERROR, 0x108)]
#[case(ERR_H3_SETTINGS_ERROR, 0x109)]
#[case(ERR_H3_MISSING_SETTINGS, 0x10A)]
#[case(ERR_H3_REQUEST_REJECTED, 0x10B)]
#[case(ERR_H3_REQUEST_CANCELLED, 0x10C)]
#[case(ERR_H3_REQUEST_INCOMPLETE, 0x10D)]
#[case(ERR_H3_MESSAGE_ERROR, 0x10E)]
#[case(ERR_H3_CONNECT_ERROR, 0x10F)]
#[case(ERR_H3_VERSION_FALLBACK, 0x110)]
#[test]
fn test_http3_error_codes_match_spec(#[case] error_code: u64, #[case] expected: u64) {
    let actual = error_code;

    assert_eq!(actual, expected);
}

#[rstest]
#[case(H3_FRAME_TYPE_DATA, 0x0)]
#[case(H3_FRAME_TYPE_HEADERS, 0x1)]
#[case(H3_FRAME_TYPE_CANCEL_PUSH, 0x3)]
#[case(H3_FRAME_TYPE_SETTINGS, 0x4)]
#[case(H3_FRAME_TYPE_PUSH_PROMISE, 0x5)]
#[case(H3_FRAME_TYPE_GOAWAY, 0x7)]
#[case(H3_FRAME_TYPE_MAX_PUSH_ID, 0xD)]
#[case(H3_FRAME_TYPE_WEBTRANSPORT_STREAM, 0x41)]
#[test]
fn test_http3_frame_types_match_spec(#[case] frame_type: u64, #[case] expected_value: u64) {
    let actual = frame_type;

    assert_eq!(actual, expected_value);
}

#[rstest]
#[case(SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0x1)]
#[case(SETTINGS_QPACK_BLOCKED_STREAMS, 0x7)]
#[case(SETTINGS_ENABLE_CONNECT_PROTOCOL, 0x8)]
#[case(SETTINGS_H3_DATAGRAM, 0x33)]
#[case(SETTINGS_WT_INITIAL_MAX_DATA, 0x2B61)]
#[case(SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, 0x2B64)]
#[case(SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, 0x2B65)]
#[test]
fn test_http3_settings_identifiers_match_spec(
    #[case] setting_id: u64,
    #[case] expected_value: u64,
) {
    let actual = setting_id;

    assert_eq!(actual, expected_value);
}

#[rstest]
#[case(H3_STREAM_TYPE_CONTROL, 0x0)]
#[case(H3_STREAM_TYPE_PUSH, 0x1)]
#[case(H3_STREAM_TYPE_QPACK_ENCODER, 0x2)]
#[case(H3_STREAM_TYPE_QPACK_DECODER, 0x3)]
#[case(H3_STREAM_TYPE_WEBTRANSPORT, 0x54)]
#[test]
fn test_http3_stream_types_match_spec(#[case] stream_type: u64, #[case] expected_value: u64) {
    let actual = stream_type;

    assert_eq!(actual, expected_value);
}

#[test]
fn test_max_datagram_size_consistency() {
    let absolute_max = MAX_DATAGRAM_SIZE;
    let default_max = DEFAULT_MAX_DATAGRAM_SIZE;

    let is_consistent = default_max <= absolute_max;

    assert!(is_consistent);
    assert_eq!(absolute_max, 65535);
}

#[test]
fn test_max_protocol_streams_limit() {
    let limit = MAX_PROTOCOL_STREAMS_LIMIT;

    let calculation = 1u64 << 60;

    assert_eq!(limit, calculation);
}

#[test]
fn test_max_stream_id_limit() {
    let max_id = MAX_STREAM_ID;

    let calculation = (1u64 << 62) - 1;

    assert_eq!(max_id, calculation);
}

#[test]
fn test_protocol_identification_values_are_valid() {
    let expected_alpn = "h3";
    let expected_scheme = "https";
    let expected_user_agent_key = "user-agent";

    let alpn = ALPN_H3;
    let scheme = WEBTRANSPORT_SCHEME;
    let ua_key = USER_AGENT_HEADER;

    assert_eq!(alpn, expected_alpn);
    assert_eq!(scheme, expected_scheme);
    assert_eq!(ua_key, expected_user_agent_key);
}

#[test]
fn test_qpack_decoder_constants_integrity() {
    assert_eq!(QPACK_DECODER_MAX_TABLE_CAPACITY, 4096);
    assert_eq!(QPACK_DECODER_MAX_BLOCKED_STREAMS, 16);
}

#[test]
fn test_qpack_decoder_constants_safe_convergence() {
    let u32_limit = u64::from(u32::MAX);
    assert!(QPACK_DECODER_MAX_TABLE_CAPACITY <= u32_limit);
    assert!(QPACK_DECODER_MAX_BLOCKED_STREAMS <= u32_limit);
}

#[rstest]
#[case(ERR_NO_ERROR, 0x0)]
#[case(ERR_INTERNAL_ERROR, 0x1)]
#[case(ERR_CONNECTION_REFUSED, 0x2)]
#[case(ERR_FLOW_CONTROL_ERROR, 0x3)]
#[case(ERR_STREAM_LIMIT_ERROR, 0x4)]
#[case(ERR_STREAM_STATE_ERROR, 0x5)]
#[case(ERR_FINAL_SIZE_ERROR, 0x6)]
#[case(ERR_FRAME_ENCODING_ERROR, 0x7)]
#[case(ERR_TRANSPORT_PARAMETER_ERROR, 0x8)]
#[case(ERR_CONNECTION_ID_LIMIT_ERROR, 0x9)]
#[case(ERR_PROTOCOL_VIOLATION, 0xA)]
#[case(ERR_INVALID_TOKEN, 0xB)]
#[case(ERR_APPLICATION_ERROR, 0xC)]
#[case(ERR_CRYPTO_BUFFER_EXCEEDED, 0xD)]
#[case(ERR_KEY_UPDATE_ERROR, 0xE)]
#[case(ERR_AEAD_LIMIT_REACHED, 0xF)]
#[case(ERR_NO_VIABLE_PATH, 0x10)]
#[test]
fn test_quic_transport_error_codes(#[case] error_code: u64, #[case] expected: u64) {
    let actual = error_code;

    assert_eq!(actual, expected);
}

#[rstest]
#[case(DEFAULT_FLOW_CONTROL_WINDOW_SIZE)]
#[case(DEFAULT_INITIAL_MAX_DATA)]
#[case(DEFAULT_MAX_CAPSULE_SIZE)]
#[case(DEFAULT_MAX_DATAGRAM_SIZE)]
#[case(DEFAULT_MAX_MESSAGE_SIZE)]
#[case(DEFAULT_MAX_STREAM_READ_BUFFER)]
#[case(DEFAULT_MAX_STREAM_WRITE_BUFFER)]
#[test]
fn test_size_configuration_defaults_are_nonzero(#[case] size_value: u64) {
    let min_size = 0;

    let value = size_value;

    assert!(value > min_size);
}

#[rstest]
#[case(BIDIRECTIONAL_STREAM, 0x0)]
#[case(UNIDIRECTIONAL_STREAM, 0x2)]
#[test]
fn test_stream_direction_masks(#[case] mask: u64, #[case] expected: u64) {
    let value = mask;

    assert_eq!(value, expected);
}

#[rstest]
#[case(DEFAULT_CLOSE_TIMEOUT)]
#[case(DEFAULT_CONNECT_TIMEOUT)]
#[case(DEFAULT_CONNECTION_IDLE_TIMEOUT)]
#[case(DEFAULT_MAX_RETRY_DELAY)]
#[case(DEFAULT_PENDING_EVENT_TTL)]
#[case(DEFAULT_READ_TIMEOUT)]
#[case(DEFAULT_RESOURCE_CLEANUP_INTERVAL)]
#[case(DEFAULT_RETRY_BACKOFF)]
#[case(DEFAULT_RETRY_DELAY)]
#[case(DEFAULT_STREAM_CREATION_TIMEOUT)]
#[case(DEFAULT_WRITE_TIMEOUT)]
#[test]
fn test_time_configuration_defaults_are_positive(#[case] timeout_value: f64) {
    let min_timeout = 0.0;

    let value = timeout_value;

    assert!(value > min_timeout);
    assert!(value.is_finite());
}

#[test]
fn test_webtransport_application_error_range() {
    let start = ERR_WT_APPLICATION_ERROR_FIRST;
    let end = ERR_WT_APPLICATION_ERROR_LAST;

    let range_start = 0x52E4_A40F_A8DB;
    let range_end = 0x52E5_AC98_3162;

    assert_eq!(start, range_start);
    assert_eq!(end, range_end);
    assert!(start < end);
}

#[test]
fn test_webtransport_capsule_types() {
    let close_type = CLOSE_WEBTRANSPORT_SESSION_TYPE;
    let drain_type = DRAIN_WEBTRANSPORT_SESSION_TYPE;

    assert_eq!(close_type, 0x2843);
    assert_eq!(drain_type, 0x78AE);
    assert_ne!(close_type, drain_type);
}

#[rstest]
#[case(ERR_WT_FLOW_CONTROL_ERROR, 0x045D_4487)]
#[case(ERR_WT_SESSION_GONE, 0x170D_7B68)]
#[case(ERR_WT_BUFFERED_STREAM_REJECTED, 0x3994_BD84)]
#[test]
fn test_webtransport_error_codes_match_spec(#[case] error_code: u64, #[case] expected: u64) {
    let actual = error_code;

    assert_eq!(actual, expected);
}

#[test]
fn test_webtransport_frame_types_distinctness() {
    let frames = vec![
        WT_DATA_BLOCKED_TYPE,
        WT_MAX_DATA_TYPE,
        WT_MAX_STREAM_DATA_TYPE,
        WT_MAX_STREAMS_BIDI_TYPE,
        WT_MAX_STREAMS_UNI_TYPE,
        WT_STREAM_DATA_BLOCKED_TYPE,
        WT_STREAMS_BLOCKED_BIDI_TYPE,
        WT_STREAMS_BLOCKED_UNI_TYPE,
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
#[case(WT_MAX_DATA_TYPE, 0x190B_4D3D)]
#[case(WT_MAX_STREAM_DATA_TYPE, 0x190B_4D3E)]
#[case(WT_MAX_STREAMS_BIDI_TYPE, 0x190B_4D3F)]
#[case(WT_MAX_STREAMS_UNI_TYPE, 0x190B_4D40)]
#[case(WT_DATA_BLOCKED_TYPE, 0x190B_4D41)]
#[case(WT_STREAM_DATA_BLOCKED_TYPE, 0x190B_4D42)]
#[case(WT_STREAMS_BLOCKED_BIDI_TYPE, 0x190B_4D43)]
#[case(WT_STREAMS_BLOCKED_UNI_TYPE, 0x190B_4D44)]
#[test]
fn test_webtransport_frame_types_match_spec(#[case] frame_type: u64, #[case] expected_value: u64) {
    let actual = frame_type;

    assert_eq!(actual, expected_value);
}
