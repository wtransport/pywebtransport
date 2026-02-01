//! Unit tests for the `crate::protocol::utils` module.

use std::fmt;
use std::io::Cursor;

use bytes::{Buf, Bytes, BytesMut};
use rstest::rstest;
use serde::Serialize;
use serde::ser::{SerializeSeq, Serializer};
use serde_json::to_string;

use super::*;
use crate::common::constants::{
    ERR_H3_FRAME_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_WT_APPLICATION_ERROR_FIRST, MAX_STREAM_ID,
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

#[derive(Debug)]
struct MockError;

impl fmt::Display for MockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MockError")
    }
}

impl std::error::Error for MockError {}

impl serde::ser::Error for MockError {
    fn custom<T: fmt::Display>(_msg: T) -> Self {
        MockError
    }
}

struct MockFailSerializer;

impl Serializer for MockFailSerializer {
    type Ok = ();
    type Error = MockError;
    type SerializeSeq = Self;
    type SerializeTuple = serde::ser::Impossible<(), MockError>;
    type SerializeTupleStruct = serde::ser::Impossible<(), MockError>;
    type SerializeTupleVariant = serde::ser::Impossible<(), MockError>;
    type SerializeMap = serde::ser::Impossible<(), MockError>;
    type SerializeStruct = serde::ser::Impossible<(), MockError>;
    type SerializeStructVariant = serde::ser::Impossible<(), MockError>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(MockError)
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(MockError)
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(MockError)
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(MockError)
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(MockError)
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(MockError)
    }
}

impl SerializeSeq for MockFailSerializer {
    type Ok = ();
    type Error = MockError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, _value: &T) -> Result<(), Self::Error> {
        Err(MockError)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(MockError)
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
        assert_eq!(result, None, "Reserved code {reserved} should be rejected");
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
fn test_serialize_headers_failure() {
    let headers = vec![(Bytes::from("key"), Bytes::from("val"))];

    let res = serialize_headers(&headers, MockFailSerializer);

    assert!(res.is_err());
}

#[test]
fn test_serialize_headers_integration() {
    struct Wrapper<'a>(&'a Vec<(Bytes, Bytes)>);

    impl Serialize for Wrapper<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serialize_headers(self.0, serializer)
        }
    }

    let headers = vec![(Bytes::from("key"), Bytes::from("val"))];
    let wrapper = Wrapper(&headers);

    let json_res = to_string(&wrapper).map_err(|e| e.to_string());

    assert_eq!(json_res, Ok(r#"[["key","val"]]"#.to_owned()));
}

#[test]
#[should_panic(expected = "Invalid stream ID encountered in debug path")]
fn test_stream_dir_from_id_panic_on_invalid() {
    let _ = stream_dir_from_id(MAX_STREAM_ID + 1, true);
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
fn test_validate_control_stream_id_rules() {
    let valid = 0;
    let invalid = 2;

    let res_valid = validate_control_stream_id(valid);
    let res_invalid = validate_control_stream_id(invalid);

    assert_eq!(res_valid, Ok(()));
    assert!(res_invalid.is_err());
}

#[test]
fn test_validate_stream_id_bounds() {
    let valid = MAX_STREAM_ID;
    let invalid = MAX_STREAM_ID + 1;

    let res_valid = validate_stream_id(valid);
    let res_invalid = validate_stream_id(invalid);

    assert_eq!(res_valid, Ok(()));
    assert!(res_invalid.is_err());
}

#[test]
fn test_validate_unidirectional_stream_id_rules() {
    let valid = 2;
    let invalid = 0;

    let res_valid = validate_unidirectional_stream_id(valid, "Test");
    let res_invalid = validate_unidirectional_stream_id(invalid, "Test");

    assert_eq!(res_valid, Ok(()));
    assert!(res_invalid.is_err());
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
