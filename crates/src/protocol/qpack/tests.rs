//! Unit tests for the `crate::protocol::qpack` module.

use bytes::Bytes;
use rstest::rstest;

use super::*;
use crate::common::types::Headers;

fn create_dummy_headers(count: usize) -> Headers {
    (0..count)
        .map(|i| {
            (
                Bytes::from(format!("x-header-name-{i}")),
                Bytes::from(format!("x-header-value-{i}")),
            )
        })
        .collect()
}

#[test]
fn test_abandon_header_block_active_stream() {
    let mut encoder = Encoder::new(4096, 100);
    let mut decoder = Decoder::new(4096, 100);
    let stream_id = 42;
    let headers: Headers = vec![(Bytes::from("x-dynamic-abandon"), Bytes::from("value"))];

    let Ok(settings) = encoder.apply_settings(4096, 100) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(_) = decoder.feed_encoder(&settings) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let Ok((_, _)) = encoder.encode(99, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok((hb2, _)) = encoder.encode(stream_id, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let Ok((_, status)) = decoder.decode_header(stream_id, Bytes::from(hb2)) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert!(matches!(status, DecodeStatus::Blocked));
    assert!(decoder.pending_blocks.contains_key(&stream_id));

    let _cancel_instructions = decoder.abandon_header_block(stream_id);

    assert!(!decoder.pending_blocks.contains_key(&stream_id));
}

#[test]
fn test_abandon_header_block_non_existent() {
    let mut decoder = Decoder::new(4096, 100);

    let instructions = decoder.abandon_header_block(999);

    assert!(instructions.is_empty());
}

#[test]
fn test_decode_header_malformed_data() {
    let mut decoder = Decoder::new(4096, 100);
    let stream_id = 10;
    let malformed_data = Bytes::from(vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

    let result = decoder.decode_header(stream_id, malformed_data);

    assert!(matches!(result, Err(QpackError::DecoderError)));
}

#[test]
fn test_decoder_blocking_and_resumption_logic() {
    let mut encoder = Encoder::new(4096, 100);
    let mut decoder = Decoder::new(4096, 100);
    let stream_id = 10;
    let headers: Headers = vec![(Bytes::from("x-dynamic"), Bytes::from("value-1"))];

    let Ok(settings) = encoder.apply_settings(4096, 100) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(_) = decoder.feed_encoder(&settings) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let Ok((_, enc_data_1)) = encoder.encode(99, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok((hb2, enc_data_2)) = encoder.encode(stream_id, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let Ok((_, status)) = decoder.decode_header(stream_id, Bytes::from(hb2)) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert!(matches!(status, DecodeStatus::Blocked));
    assert!(decoder.pending_blocks.contains_key(&stream_id));

    let Ok(_) = decoder.feed_encoder(&enc_data_1) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(_) = decoder.feed_encoder(&enc_data_2) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let result = decoder.resume_header(stream_id);

    let Ok((_, resume_res)) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert!(matches!(resume_res, Some(ref h) if h == &headers));
    assert!(!decoder.pending_blocks.contains_key(&stream_id));
}

#[test]
fn test_decoder_feed_malformed_encoder_instructions() {
    let mut decoder = Decoder::new(4096, 100);
    let malformed_data = vec![0x80, 0xFF, 0xFF, 0xFF, 0xFF];

    let result = decoder.feed_encoder(&malformed_data);

    assert!(matches!(result, Err(QpackError::DecoderError)));
}

#[test]
fn test_decoder_pending_block_capacity_limit_dos_protection() {
    let Ok(capacity) = u32::try_from(DECODER_PENDING_BLOCK_CAPACITY) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let blocked_streams = capacity + 100;
    let mut encoder = Encoder::new(4096, blocked_streams);
    let mut decoder = Decoder::new(4096, blocked_streams);
    let headers: Headers = vec![(Bytes::from("x-dynamic-dos"), Bytes::from("value"))];

    let Ok(settings) = encoder.apply_settings(4096, u64::from(blocked_streams)) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(_) = decoder.feed_encoder(&settings) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let Ok((_, _)) = encoder.encode(99, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok((hb2, _)) = encoder.encode(1, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    for stream_id in 2..=(DECODER_PENDING_BLOCK_CAPACITY as u64 + 1) {
        let Ok((_, status)) = decoder.decode_header(stream_id, Bytes::from(hb2.clone())) else {
            assert_eq!("ok", "err");
            unreachable!()
        };
        assert!(matches!(status, DecodeStatus::Blocked));
    }

    let exceed_stream_id = DECODER_PENDING_BLOCK_CAPACITY as u64 + 2;

    let result = decoder.decode_header(exceed_stream_id, Bytes::from(hb2));

    assert!(matches!(result, Err(QpackError::DecoderError)));
}

#[test]
fn test_encode_large_batch_triggers_buffer_resize() {
    let mut encoder = Encoder::new(65536, 100);
    let headers = create_dummy_headers(5000);
    let stream_id = 10;

    let result = encoder.encode(stream_id, &headers);

    let Ok((header_block, _)) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert!(!header_block.is_empty());
}

#[test]
fn test_encoder_apply_settings_high_limits() {
    let mut encoder = Encoder::new(65536, 100);
    let max_table = 100_000;
    let blocked_streams = 200;

    let result = encoder.apply_settings(max_table, blocked_streams);

    let Ok(data) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert!(!data.is_empty());
}

#[test]
fn test_encoder_feed_decoder_noop_safely() {
    let mut encoder = Encoder::new(4096, 100);
    let dummy_data = vec![0x00, 0x01];

    encoder.feed_decoder(&dummy_data);
}

#[test]
fn test_encoder_initialization_and_settings() {
    let mut encoder = Encoder::new(4096, 100);
    let max_table = 4096;
    let blocked_streams = 100;

    let result = encoder.apply_settings(max_table, blocked_streams);

    let Ok(settings_data) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert!(!settings_data.is_empty());
}

#[rstest]
#[case(10, u16::MAX as usize + 1)]
#[case(u16::MAX as usize + 1, 10)]
fn test_header_creation_too_long(#[case] name_len: usize, #[case] value_len: usize) {
    let name = vec![b'n'; name_len];
    let value = vec![b'v'; value_len];

    let result = Header::new(name, value);

    assert!(matches!(result, Err(QpackError::HeaderTooLong)));
}

#[rstest]
#[case(10, 10)]
#[case(10, u16::MAX as usize)]
#[case(u16::MAX as usize, 10)]
fn test_header_creation_valid_lengths(#[case] name_len: usize, #[case] value_len: usize) {
    let name = vec![b'n'; name_len];
    let value = vec![b'v'; value_len];

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

    let lsx = header.create_lsxpack_header();

    assert_eq!(lsx.name_len, expected_name_len);
    assert_eq!(lsx.val_len, expected_val_len);
    assert_eq!(lsx.name_offset, 0);
    assert_eq!(lsx.val_offset, expected_val_offset);
}

#[test]
fn test_resume_header_non_existent_stream() {
    let mut decoder = Decoder::new(4096, 100);
    let stream_id = 999;

    let result = decoder.resume_header(stream_id);

    let Ok((_, h_opt)) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    assert!(h_opt.is_none());
}

#[test]
fn test_round_trip_simple_flow() {
    let mut encoder = Encoder::new(4096, 100);
    let mut decoder = Decoder::new(4096, 100);
    let stream_id = 10;
    let headers: Headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":path"), Bytes::from("/index.html")),
    ];

    let Ok((header_block, enc_data)) = encoder.encode(stream_id, &headers) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(_) = decoder.feed_encoder(&enc_data) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let result = decoder.decode_header(stream_id, Bytes::from(header_block));

    let Ok((dec_instructions, status)) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    encoder.feed_decoder(&dec_instructions);

    assert!(matches!(status, DecodeStatus::Complete(ref h) if h == &headers));
}
