//! Unit tests for the `crate::protocol::utils` module.

use std::borrow::Cow;
use std::io::Cursor;

use bytes::{Buf, Bytes, BytesMut};
use rstest::rstest;

use super::*;
use crate::common::constants::{
    ERR_H3_FRAME_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_WT_APPLICATION_ERROR_FIRST,
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

#[test]
fn test_combine_header_absent_returns_none() {
    let headers = vec![(Bytes::from("server"), Bytes::from("rust"))];

    let result = combine_header(&headers, b"wt-protocol");

    assert!(result.is_none());
}

#[test]
fn test_combine_header_multiple_lines_joins_in_order() {
    let headers = vec![
        (Bytes::from("wt-available-protocols"), Bytes::from("\"h3\"")),
        (Bytes::from("server"), Bytes::from("rust")),
        (Bytes::from("wt-available-protocols"), Bytes::from("\"p1\"")),
        (Bytes::from("wt-available-protocols"), Bytes::from("\"p2\"")),
    ];

    let result = combine_header(&headers, b"wt-available-protocols");

    assert!(matches!(
        result,
        Some(Cow::Owned(value)) if value == b"\"h3\", \"p1\", \"p2\""
    ));
}

#[test]
fn test_combine_header_single_line_borrows_value() {
    let headers = vec![
        (Bytes::from("wt-protocol"), Bytes::from("\"h3\"")),
        (Bytes::from("server"), Bytes::from("rust")),
    ];

    let result = combine_header(&headers, b"wt-protocol");

    assert!(matches!(result, Some(Cow::Borrowed(b"\"h3\""))));
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
fn test_find_header_exact_match() {
    let headers = vec![
        (Bytes::from("content-type"), Bytes::from("application/json")),
        (Bytes::from("server"), Bytes::from("rust")),
        (Bytes::from("server"), Bytes::from("proxy")),
    ];

    let result_exact = find_header(&headers, b"server");
    let result_case = find_header(&headers, b"Content-Type");
    let result_missing = find_header(&headers, b"missing-header");

    assert_eq!(result_exact, Some(&Bytes::from("rust")));
    assert!(result_case.is_none());
    assert!(result_missing.is_none());
}

#[rstest]
#[case(ERR_WT_APPLICATION_ERROR_FIRST - 1, None)]
#[case(ERR_WT_APPLICATION_ERROR_FIRST, Some(0))]
#[case(ERR_WT_APPLICATION_ERROR_FIRST + 1, Some(1))]
#[case(ERR_WT_APPLICATION_ERROR_FIRST + 31, Some(30))]
fn test_http_code_to_webtransport_code_mapping_logic(
    #[case] http_code: ErrorCode,
    #[case] expected: Option<u32>,
) {
    let result = http_code_to_webtransport_code(http_code);

    assert_eq!(result, expected);
}

#[test]
fn test_http_code_to_webtransport_code_mapping_reserved_code() {
    let reserved = get_reserved_http_code();

    if reserved > ERR_WT_APPLICATION_ERROR_FIRST {
        let result = http_code_to_webtransport_code(reserved);
        assert_eq!(result, None);
    }
}

#[rstest]
#[case(b"connection", b"close", true, true)]
#[case(b"keep-alive", b"timeout=5", false, true)]
#[case(b"proxy-connection", b"keep-alive", true, true)]
#[case(b"te", b"Trailers", true, false)]
#[case(b"te", b"gzip", true, true)]
#[case(b"te", b"trailers", false, true)]
#[case(b"te", b"trailers", true, false)]
#[case(b"transfer-encoding", b"chunked", false, true)]
#[case(b"upgrade", b"websocket", true, true)]
#[case(b"user-agent", b"pywebtransport", true, false)]
fn test_is_connection_specific_header_classification(
    #[case] key: &[u8],
    #[case] value: &[u8],
    #[case] is_request: bool,
    #[case] expected: bool,
) {
    let result = is_connection_specific_header(key, value, is_request);

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
#[case(0, false)]
#[case(1, false)]
#[case(2, true)]
#[case(3, true)]
fn test_is_unidirectional_stream_logic(#[case] stream_id: u64, #[case] expected: bool) {
    let result = is_unidirectional_stream(stream_id);

    assert_eq!(result, expected);
}

#[rstest]
#[case(b"", b"value", false)]
#[case(b":", b"value", false)]
#[case(b":path", b"/", true)]
#[case(b"Content-Type", b"text/plain", false)]
#[case(b"bad name", b"value", false)]
#[case(b"bad:name", b"value", false)]
#[case(b"content-type", b"text/plain", true)]
#[case(b"x-delete", b"\x7f", false)]
#[case(b"x-empty", b"", true)]
#[case(b"x-inner-whitespace", b"a b\tc", true)]
#[case(b"x-leading", b" value", false)]
#[case(b"x-line-break", b"line\r\nbreak", false)]
#[case(b"x-obs-text", b"caf\xe9", true)]
#[case(b"x-trailing", b"value\t", false)]
fn test_is_valid_header_syntax(#[case] key: &[u8], #[case] value: &[u8], #[case] expected: bool) {
    let result = is_valid_header(key, value);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, true)]
#[case(1, false)]
#[case(2, false)]
#[case(3, false)]
#[case(4, true)]
fn test_is_valid_session_id_logic(#[case] session_id: u64, #[case] expected: bool) {
    let result = is_valid_session_id(session_id);

    assert_eq!(result, expected);
}

#[test]
fn test_is_valid_user_headers_checks_every_line() {
    let headers = vec![
        (Bytes::from("user-agent"), Bytes::from("pywebtransport")),
        (Bytes::from("content-length"), Bytes::from("0")),
    ];

    let result = is_valid_user_headers(&headers, false);

    assert!(!result);
}

#[rstest]
#[case(b":authority", b"example.com", true, false)]
#[case(b"Retry-After", b"120", false, false)]
#[case(b"connection", b"close", false, false)]
#[case(b"content-length", b"0", false, false)]
#[case(b"host", b"example.com", false, true)]
#[case(b"host", b"example.com", true, false)]
#[case(b"user-agent", b"pywebtransport", true, true)]
#[case(b"wt-available-protocols", b"\"h3\"", true, false)]
#[case(b"wt-protocol", b"\"h3\"", false, false)]
fn test_is_valid_user_headers_classification(
    #[case] key: &[u8],
    #[case] value: &[u8],
    #[case] is_request: bool,
    #[case] expected: bool,
) {
    let headers = vec![(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))];

    let result = is_valid_user_headers(&headers, is_request);

    assert_eq!(result, expected);
}

#[rstest]
#[case::boolean(b"\"h3\";a=?1, \"p1\"")]
#[case::byte_sequence_double_padding(b"\"h3\";a=:YQ==:, \"p1\"")]
#[case::byte_sequence_empty(b"\"h3\";a=::, \"p1\"")]
#[case::byte_sequence_unpadded(b"\"h3\";a=:YWI:, \"p1\"")]
#[case::date(b"\"h3\";a=@1659578233, \"p1\"")]
#[case::decimal(b"\"h3\";a=-1.5, \"p1\"")]
#[case::display_string(b"\"h3\";a=%\"caf%c3%a9 ok\", \"p1\"")]
#[case::integer(b"\"h3\";a=42, \"p1\"")]
#[case::keys_only(b"\"h3\"; a;*b, \"p1\"")]
#[case::string(b"\"h3\";a=\"v\", \"p1\"")]
#[case::token(b"\"h3\";a=Tok/x:y, \"p1\"")]
fn test_parse_wt_protocol_list_ignores_parameters(#[case] header: &[u8]) {
    let result = parse_wt_protocol_list(header);

    assert_eq!(result, Some(vec!["h3".to_owned(), "p1".to_owned()]));
}

#[test]
fn test_parse_wt_protocol_list_invalid() {
    let unclosed = b"\"h3";
    let bad_escape = b"\"h3\\\"";
    let trailing_backslash = b"\"h3\\";
    let invalid_char = b"\"\x01\"";
    let no_quotes = b"h3";

    assert_eq!(parse_wt_protocol_list(unclosed), None);
    assert_eq!(parse_wt_protocol_list(bad_escape), None);
    assert_eq!(parse_wt_protocol_list(trailing_backslash), None);
    assert_eq!(parse_wt_protocol_list(invalid_char), None);
    assert_eq!(parse_wt_protocol_list(no_quotes), None);
}

#[test]
fn test_parse_wt_protocol_list_invalid_escape_target() {
    let bad_escape_target = b"\"h3\\n\"";

    assert_eq!(parse_wt_protocol_list(bad_escape_target), None);
}

#[rstest]
#[case::boolean_invalid(b"\"h3\";a=?2")]
#[case::byte_sequence_excess_padding(b"\"h3\";a=:YQ===:")]
#[case::byte_sequence_inner_padding(b"\"h3\";a=:Y=I=:")]
#[case::byte_sequence_invalid_length(b"\"h3\";a=:Y:")]
#[case::byte_sequence_misaligned_padding(b"\"h3\";a=:YW=:")]
#[case::byte_sequence_unclosed(b"\"h3\";a=:YWI")]
#[case::date_decimal(b"\"h3\";a=@1.5")]
#[case::date_invalid(b"\"h3\";a=@x")]
#[case::decimal_fraction_too_long(b"\"h3\";a=1.1234")]
#[case::decimal_integer_too_long(b"\"h3\";a=1234567890123.1")]
#[case::decimal_without_fraction(b"\"h3\";a=1.")]
#[case::display_string_invalid_character(b"\"h3\";a=%\"\x01\"")]
#[case::display_string_invalid_utf8(b"\"h3\";a=%\"%c3\"")]
#[case::display_string_truncated_escape(b"\"h3\";a=%\"%c")]
#[case::display_string_unclosed(b"\"h3\";a=%\"abc")]
#[case::display_string_unquoted(b"\"h3\";a=%abc")]
#[case::display_string_uppercase_hex(b"\"h3\";a=%\"%C3%A9\"")]
#[case::integer_too_long(b"\"h3\";a=1234567890123456")]
#[case::key_invalid(b"\"h3\";A=1")]
#[case::number_without_digits(b"\"h3\";a=-")]
#[case::string_unclosed(b"\"h3\";a=\"v")]
#[case::value_missing(b"\"h3\";a=")]
#[case::value_unknown(b"\"h3\";a=(")]
fn test_parse_wt_protocol_list_invalid_parameters(#[case] header: &[u8]) {
    let result = parse_wt_protocol_list(header);

    assert_eq!(result, None);
}

#[rstest]
#[case::empty(b"")]
#[case::leading_tab(b"\t\"h3\"")]
#[case::missing_comma(b"\"h3\" \"p1\"")]
#[case::token_member(b"\"h3\", p1")]
#[case::trailing_comma(b"\"h3\", ")]
fn test_parse_wt_protocol_list_invalid_structure(#[case] header: &[u8]) {
    let result = parse_wt_protocol_list(header);

    assert_eq!(result, None);
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
    assert_eq!(
        parse_wt_protocol_string(b"  \"h3\";q=1  "),
        Some("h3".to_owned())
    );
    assert_eq!(parse_wt_protocol_string(b"\"h3\"\t"), None);
}

#[test]
fn test_read_varint_eof_errors() {
    let empty = &[];
    let partial = &[0x40];

    let result_empty = read_varint(&mut Cursor::new(empty));
    let result_partial = read_varint(&mut Cursor::new(partial));

    assert_eq!(result_empty, Err(ERR_H3_FRAME_ERROR));
    assert_eq!(result_partial, Err(ERR_H3_FRAME_ERROR));
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

#[rstest]
#[case(0, true, StreamDirection::Bidirectional)]
#[case(2, true, StreamDirection::SendOnly)]
#[case(3, true, StreamDirection::ReceiveOnly)]
fn test_stream_direction_from_id_resolution(
    #[case] stream_id: u64,
    #[case] is_client: bool,
    #[case] expected: StreamDirection,
) {
    let result = stream_direction_from_id(stream_id, is_client);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, 1)]
#[case(63, 1)]
#[case(64, 2)]
#[case(16383, 2)]
#[case(16384, 4)]
#[case(1_073_741_823, 4)]
#[case(1_073_741_824, 8)]
#[case(4_611_686_018_427_387_903, 8)]
#[case(u64::MAX, 8)]
fn test_varint_size_calculation(#[case] value: u64, #[case] expected: usize) {
    let result = varint_size(value);

    assert_eq!(result, expected);
}

#[rstest]
#[case(0, ERR_WT_APPLICATION_ERROR_FIRST)]
#[case(1, ERR_WT_APPLICATION_ERROR_FIRST + 1)]
#[case(30, ERR_WT_APPLICATION_ERROR_FIRST + 31)]
fn test_webtransport_code_to_http_code_mapping_valid(
    #[case] application_error_code: u32,
    #[case] expected: ErrorCode,
) {
    let result = webtransport_code_to_http_code(application_error_code);

    assert_eq!(result, expected);
}

#[test]
fn test_write_varint_too_large() {
    let mut buffer = BytesMut::new();
    let value = 0xC000_0000_0000_0000 + 1;

    let result = write_varint(&mut buffer, value);

    assert_eq!(result, Err(ERR_LIB_INTERNAL_ERROR));
}

#[rstest]
#[case(63, 1)]
#[case(16383, 2)]
#[case(1_073_741_823, 4)]
#[case(4_611_686_018_427_387_903, 8)]
fn test_write_varint_valid_encoding(#[case] value: u64, #[case] expected_len: usize) {
    let mut buffer = BytesMut::new();

    let result = write_varint(&mut buffer, value);
    let mut cursor = Cursor::new(buffer.as_ref());
    let decoded = read_varint(&mut cursor);

    assert_eq!(result, Ok(()));
    assert_eq!(buffer.len(), expected_len);
    assert_eq!(decoded, Ok(value));
}
