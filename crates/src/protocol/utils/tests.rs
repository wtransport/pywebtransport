//! Unit tests for the `crate::protocol::utils` module.

use std::borrow::Cow;
use std::io::Cursor;

use bytes::{Buf, Bytes, BytesMut};
use rstest::rstest;

use super::*;
use crate::common::constants::{
    ERR_H3_FRAME_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_WT_APPLICATION_ERROR_FIRST, QUIC_MAX_STREAM_ID,
};
use crate::common::types::{ErrorCode, StreamDirection};

fn get_reserved_http_code() -> u64 {
    let mut candidate = ERR_WT_APPLICATION_ERROR_FIRST;
    loop {
        if candidate >= 0x21 && (candidate - 0x21).is_multiple_of(0x1F) {
            return candidate;
        }
        candidate += 1;
    }
}

#[rstest]
#[case(0, true, true)]
#[case(1, true, true)]
#[case(2, false, true)]
#[case(2, true, false)]
#[case(3, false, false)]
#[case(3, true, true)]
fn test_can_receive_on_stream_permission_check(
    #[case] stream_id: u64,
    #[case] is_client: bool,
    #[case] expected: bool,
) {
    let result = can_receive_on_stream(stream_id, is_client);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, true, true)]
#[case(1, true, true)]
#[case(2, false, false)]
#[case(2, true, true)]
#[case(3, false, true)]
#[case(3, true, false)]
fn test_can_send_on_stream_permission_check(
    #[case] stream_id: u64,
    #[case] is_client: bool,
    #[case] expected: bool,
) {
    let result = can_send_on_stream(stream_id, is_client);

    assert_eq!(result, expected);
}

#[test]
fn test_encode_wt_protocol_list_empty() {
    let protocols: Vec<String> = vec![];
    let result = encode_wt_protocol_list(&protocols);
    assert_eq!(result, Ok(Bytes::new()));
}

#[test]
fn test_encode_wt_protocol_list_escapes() {
    let protocols = vec!["h3".to_owned(), "my\\proto\"".to_owned()];
    let result = encode_wt_protocol_list(&protocols);
    assert_eq!(
        result,
        Ok(Bytes::from_static(b"\"h3\", \"my\\\\proto\\\"\""))
    );
}

#[test]
fn test_encode_wt_protocol_list_invalid() {
    let protocols = vec!["invalid\n\r".to_owned()];
    let result = encode_wt_protocol_list(&protocols);
    assert_eq!(result, Err(Cow::Borrowed("wt_protocol validate invalid")));
}

#[test]
fn test_encode_wt_protocol_list_multiple() {
    let protocols = vec!["p1".to_owned(), "p2".to_owned(), "p3".to_owned()];
    let result = encode_wt_protocol_list(&protocols);
    assert_eq!(result, Ok(Bytes::from_static(b"\"p1\", \"p2\", \"p3\"")));
}

#[test]
fn test_find_header_case_insensitive_match() {
    let headers = vec![
        (Bytes::from("Content-Type"), Bytes::from("application/json")),
        (Bytes::from("server"), Bytes::from("rust")),
    ];

    let result_exact = find_header(&headers, "Content-Type");
    let result_case = find_header(&headers, "content-type");
    let result_missing = find_header(&headers, "missing-header");

    assert_eq!(result_exact, Some(Bytes::from("application/json")));
    assert_eq!(result_case, Some(Bytes::from("application/json")));
    assert!(result_missing.is_none());
}

#[test]
fn test_find_header_str_decoding() {
    let headers = vec![
        (Bytes::from("valid-utf8"), Bytes::from("hello")),
        (Bytes::from("invalid-utf8"), Bytes::from(vec![0xFF, 0xFE])),
    ];

    let result_valid = find_header_str(&headers, "valid-utf8");
    let result_invalid = find_header_str(&headers, "invalid-utf8");

    assert_eq!(result_valid, Some("hello".to_owned()));
    assert!(result_invalid.is_none());
}

#[test]
fn test_find_header_str_missing_key() {
    let headers = vec![(Bytes::from("key"), Bytes::from("val"))];

    let result = find_header_str(&headers, "missing");

    assert_eq!(result, None);
}

#[rstest]
#[case(ERR_WT_APPLICATION_ERROR_FIRST - 1, None)]
#[case(ERR_WT_APPLICATION_ERROR_FIRST, Some(0))]
#[case(ERR_WT_APPLICATION_ERROR_FIRST + 1, Some(1))]
#[case(ERR_WT_APPLICATION_ERROR_FIRST + 31, Some(30))]
fn test_http_to_wt_error_mapping_logic(
    #[case] http_code: u64,
    #[case] expected: Option<ErrorCode>,
) {
    let result = http_to_wt_error(http_code);

    assert_eq!(result, expected);
}

#[test]
fn test_http_to_wt_error_mapping_reserved_code() {
    let reserved = get_reserved_http_code();

    if reserved > ERR_WT_APPLICATION_ERROR_FIRST {
        let result = http_to_wt_error(reserved);
        assert_eq!(result, None);
    }
}

#[rstest]
#[case(0, true)]
#[case(1, true)]
#[case(2, false)]
#[case(3, false)]
fn test_is_bidirectional_stream_logic(#[case] stream_id: u64, #[case] expected: bool) {
    let result = is_bidirectional_stream(stream_id);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, true)]
#[case(1, false)]
#[case(2, true)]
#[case(3, false)]
fn test_is_client_initiated_stream_private_logic(#[case] stream_id: u64, #[case] expected: bool) {
    let result = is_client_initiated_stream(stream_id);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, false, true)]
#[case(0, true, false)]
#[case(1, false, false)]
#[case(1, true, true)]
fn test_is_peer_initiated_stream_context_check(
    #[case] stream_id: u64,
    #[case] is_client: bool,
    #[case] expected: bool,
) {
    let result = is_peer_initiated_stream(stream_id, is_client);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, true)]
#[case(1, false)]
#[case(2, false)]
#[case(3, false)]
fn test_is_request_response_stream_logic(#[case] stream_id: u64, #[case] expected: bool) {
    let result = is_request_response_stream(stream_id);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, false)]
#[case(1, true)]
#[case(2, false)]
#[case(3, true)]
fn test_is_server_initiated_stream_private_logic(#[case] stream_id: u64, #[case] expected: bool) {
    let result = is_server_initiated_stream(stream_id);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, false)]
#[case(1, false)]
#[case(2, true)]
#[case(3, true)]
fn test_is_unidirectional_stream_logic(#[case] stream_id: u64, #[case] expected: bool) {
    let result = is_unidirectional_stream(stream_id);

    assert_eq!(result, expected);
}

#[test]
fn test_merge_headers_updates_correctly() {
    let base = vec![
        (Bytes::from("a"), Bytes::from("1")),
        (Bytes::from("b"), Bytes::from("2")),
    ];
    let update = vec![(Bytes::from("c"), Bytes::from("3"))];

    let result = merge_headers(&base, &update);

    assert_eq!(result.len(), 3);
    assert_eq!(result.first(), Some(&(Bytes::from("a"), Bytes::from("1"))));
    assert_eq!(result.get(2), Some(&(Bytes::from("c"), Bytes::from("3"))));
}

#[rstest]
#[case(100, 10, 100, true, true, Some(110))]
#[case(100, 50, 100, false, false, None)]
#[case(100, 50, 100, true, false, None)]
#[case(100, 60, 100, true, false, Some(160))]
#[case(u64::MAX - 10, 20, 10, true, true, None)]
fn test_next_data_limit_calculation(
    #[case] current: u64,
    #[case] consumed: u64,
    #[case] window: u64,
    #[case] auto_scale: bool,
    #[case] force: bool,
    #[case] expected: Option<u64>,
) {
    let result = next_data_limit(current, consumed, window, auto_scale, force);

    assert_eq!(result, expected);
}

#[rstest]
#[case(10, 2, 10, true, true, Some(12))]
#[case(10, 5, 10, false, false, None)]
#[case(10, 6, 10, true, false, Some(16))]
#[case(20, 5, 10, true, true, None)]
fn test_next_stream_limit_calculation(
    #[case] current: u64,
    #[case] closed: u64,
    #[case] window: u64,
    #[case] auto_scale: bool,
    #[case] force: bool,
    #[case] expected: Option<u64>,
) {
    let result = next_stream_limit(current, closed, window, auto_scale, force);

    assert_eq!(result, expected);
}

#[test]
fn test_parse_wt_protocol_list_invalid() {
    let unclosed = b"\"h3";
    let bad_escape = b"\"h3\\\"";
    let invalid_char = b"\"\x01\"";
    let no_quotes = b"h3";

    assert_eq!(parse_wt_protocol_list(unclosed), None);
    assert_eq!(parse_wt_protocol_list(bad_escape), None);
    assert_eq!(parse_wt_protocol_list(invalid_char), None);
    assert_eq!(parse_wt_protocol_list(no_quotes), None);
}

#[test]
fn test_parse_wt_protocol_list_valid() {
    let single = b"\"h3\"";
    let multiple = b"\"h3\", \"p1\"";
    let multiple_spaces = b"  \"h3\"  , \t \"p1\"  ";
    let escaped = b"\"my\\\\proto\\\"\"";

    assert_eq!(parse_wt_protocol_list(single), Some(vec!["h3".to_owned()]));
    assert_eq!(
        parse_wt_protocol_list(multiple),
        Some(vec!["h3".to_owned(), "p1".to_owned()])
    );
    assert_eq!(
        parse_wt_protocol_list(multiple_spaces),
        Some(vec!["h3".to_owned(), "p1".to_owned()])
    );
    assert_eq!(
        parse_wt_protocol_list(escaped),
        Some(vec!["my\\proto\"".to_owned()])
    );
}

#[test]
fn test_parse_wt_protocol_string_logic() {
    assert_eq!(parse_wt_protocol_string(b"\"h3\""), Some("h3".to_owned()));
    assert_eq!(parse_wt_protocol_string(b"\"h3\", \"p1\""), None);
    assert_eq!(parse_wt_protocol_string(b"invalid"), None);
}

#[test]
fn test_read_varint_eof_errors() {
    let empty = &[];
    let partial = &[0x40];

    let res_empty = read_varint(&mut Cursor::new(empty));
    let res_partial = read_varint(&mut Cursor::new(partial));

    assert_eq!(res_empty, Err(ERR_H3_FRAME_ERROR));
    assert_eq!(res_partial, Err(ERR_H3_FRAME_ERROR));
}

#[rstest]
#[case(&[0x00], 0)]
#[case(&[0x40, 0x25], 37)]
#[case(&[0x9d, 0x7f, 0x3e, 0x7d], 494_878_333)]
#[case(&[0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c], 151_288_809_941_952_652)]
fn test_read_varint_valid_decoding(#[case] input: &[u8], #[case] expected: u64) {
    let mut cursor = Cursor::new(input);

    let result = read_varint(&mut cursor);

    assert_eq!(result, Ok(expected));
    assert_eq!(cursor.remaining(), 0);
}

#[test]
#[should_panic(expected = "quic_stream validate exceeded")]
fn test_stream_dir_from_id_panic_on_invalid() {
    let _ = stream_dir_from_id(QUIC_MAX_STREAM_ID + 1, true);
}

#[rstest]
#[case(0, true, StreamDirection::Bidirectional)]
#[case(2, true, StreamDirection::SendOnly)]
#[case(3, true, StreamDirection::ReceiveOnly)]
fn test_stream_dir_from_id_resolution(
    #[case] stream_id: u64,
    #[case] is_client: bool,
    #[case] expected: StreamDirection,
) {
    let result = stream_dir_from_id(stream_id, is_client);

    assert!(matches!(result, _x if result == expected));
}

#[test]
fn test_write_varint_too_large() {
    let mut buf = BytesMut::new();
    let val = 0xC000_0000_0000_0000 + 1;

    let result = write_varint(&mut buf, val);

    assert_eq!(result, Err(ERR_LIB_INTERNAL_ERROR));
}

#[rstest]
#[case(63, 1)]
#[case(16383, 2)]
#[case(1_073_741_823, 4)]
#[case(4_611_686_018_427_387_903, 8)]
fn test_write_varint_valid_encoding(#[case] val: u64, #[case] expected_len: usize) {
    let mut buf = BytesMut::new();

    let result = write_varint(&mut buf, val);
    let slice: &[u8] = &buf;
    let mut cursor = Cursor::new(slice);
    let decoded = read_varint(&mut cursor);

    assert_eq!(result, Ok(()));
    assert_eq!(buf.len(), expected_len);
    assert_eq!(decoded, Ok(val));
}

#[test]
fn test_wt_to_http_error_mapping_overflow() {
    let huge_code = u64::MAX;

    let result = wt_to_http_error(huge_code);

    assert!(result.is_none());
}

#[rstest]
#[case(0, Some(ERR_WT_APPLICATION_ERROR_FIRST))]
#[case(1, Some(ERR_WT_APPLICATION_ERROR_FIRST + 1))]
#[case(30, Some(ERR_WT_APPLICATION_ERROR_FIRST + 31))]
fn test_wt_to_http_error_mapping_valid(#[case] wt_code: ErrorCode, #[case] expected: Option<u64>) {
    let result = wt_to_http_error(wt_code);

    assert_eq!(result, expected);
}
