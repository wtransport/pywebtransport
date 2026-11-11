//! Unit tests for the `crate::protocol::qpack` module.

use bytes::Bytes;
use rstest::rstest;

use super::*;
use crate::common::types::Headers;

fn create_dummy_headers(count: usize) -> Headers {
    (0..count)
        .map(|index| {
            (
                Bytes::from(format!("x-header-name-{index}")),
                Bytes::from(format!("x-header-value-{index}")),
            )
        })
        .collect()
}

#[test]
fn test_decode_field_section_size_limit() {
    let mut encoder = Encoder::new();
    let mut decoder = Decoder::new();
    let stream_id = 10;
    let headers = create_dummy_headers(3);

    let Ok(header_block) = encoder.encode(stream_id, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let result_at_limit = decoder.decode(stream_id, &header_block, 189);
    let result_exceeded = decoder.decode(stream_id, &header_block, 188);

    assert!(matches!(result_at_limit, Ok(decoded_headers) if decoded_headers == headers));
    assert!(matches!(
        result_exceeded,
        Err(QpackError::FieldSectionTooLarge)
    ));
}

#[test]
fn test_decode_field_too_long_failure() {
    let mut decoder = Decoder::new();
    let mut header_block = vec![0x00, 0x00, 0x21, b'a', 0xFF, 0xE7, 0xD4, 0x02];
    header_block.resize(header_block.len() + 43_750, 0x00);

    let result = decoder.decode(10, &header_block, u64::MAX);

    assert!(matches!(result, Err(QpackError::HeaderTooLong)));
}

#[test]
fn test_decode_malformed_data() {
    let mut decoder = Decoder::new();
    let malformed_data = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF];

    let result = decoder.decode(10, &malformed_data, u64::MAX);

    assert!(matches!(result, Err(QpackError::DecoderError)));
}

#[test]
fn test_decoder_feed_encoder_instructions() {
    let mut decoder = Decoder::new();

    let result = decoder.feed_encoder(&[0x20]);

    assert!(matches!(result, Ok(())));
}

#[test]
fn test_decoder_feed_malformed_encoder_instructions() {
    let mut decoder = Decoder::new();
    let malformed_data = vec![0x80, 0xFF, 0xFF, 0xFF, 0xFF];

    let result = decoder.feed_encoder(&malformed_data);

    assert!(matches!(result, Err(QpackError::DecoderError)));
}

#[test]
fn test_encode_large_batch_triggers_buffer_resize() {
    let mut encoder = Encoder::new();
    let headers = create_dummy_headers(5000);
    let stream_id = 10;

    let result = encoder.encode(stream_id, &headers);

    let Ok(header_block) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert!(!header_block.is_empty());
}

#[test]
fn test_encoder_feed_decoder_instructions() {
    let mut encoder = Encoder::new();

    let result = encoder.feed_decoder(&[0x4A]);

    assert!(matches!(result, Ok(())));
}

#[test]
fn test_encoder_feed_malformed_decoder_instructions() {
    let mut encoder = Encoder::new();
    let malformed_data = vec![0x00, 0x01];

    let result = encoder.feed_decoder(&malformed_data);

    assert!(matches!(result, Err(QpackError::EncoderError)));
}

#[rstest]
#[case(10, u16::MAX as usize + 1)]
#[case(u16::MAX as usize + 1, 10)]
fn test_header_creation_too_long(#[case] name_length: usize, #[case] value_length: usize) {
    let name = vec![b'n'; name_length];
    let value = vec![b'v'; value_length];

    let result = Header::new(name, value);

    assert!(matches!(result, Err(QpackError::HeaderTooLong)));
}

#[rstest]
#[case(10, 10)]
#[case(10, u16::MAX as usize)]
#[case(u16::MAX as usize, 10)]
fn test_header_creation_valid_lengths(#[case] name_length: usize, #[case] value_length: usize) {
    let name = vec![b'n'; name_length];
    let value = vec![b'v'; value_length];

    let result = Header::new(name, value);

    let Ok(_) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
}

#[test]
fn test_header_lsxpack_conversion_integrity() {
    let name = b"x-test-name";
    let value = b"x-test-value";

    let Ok(expected_name_len) = u16::try_from(name.len()) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(expected_val_len) = u16::try_from(value.len()) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(expected_val_offset) = i32::try_from(name.len()) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let Ok(mut header) = Header::new(name, value) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let lsxpack_header = header.create_lsxpack_header();

    assert_eq!(lsxpack_header.name_len, expected_name_len);
    assert_eq!(lsxpack_header.val_len, expected_val_len);
    assert_eq!(lsxpack_header.name_offset, 0);
    assert_eq!(lsxpack_header.val_offset, expected_val_offset);
}

#[test]
fn test_round_trip_simple_flow() {
    let mut encoder = Encoder::new();
    let mut decoder = Decoder::new();
    let stream_id = 10;
    let headers: Headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":path"), Bytes::from("/index.html")),
    ];

    let Ok(header_block) = encoder.encode(stream_id, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let result = decoder.decode(stream_id, &header_block, u64::MAX);

    let Ok(decoded_headers) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert_eq!(decoded_headers, headers);
}
