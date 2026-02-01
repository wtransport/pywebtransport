use bytes::Bytes;
use rstest::rstest;

use super::*;

fn create_dummy_headers(count: usize) -> Vec<(Bytes, Bytes)> {
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
fn test_decoder_blocking_and_resumption_logic() {
    let mut encoder = Encoder::new();
    let mut decoder = Decoder::new(4096, 4096);
    let stream_id_1 = 4;
    let stream_id_2 = 8;

    let headers_1 = vec![(Bytes::from("x-dynamic"), Bytes::from("value-1"))];
    let Ok((_, enc_data_1)) = encoder.encode(stream_id_1, &headers_1) else {
        unreachable!("Encoding stream 1 failed");
    };

    let _unused_res = match decoder.feed_encoder(&enc_data_1) {
        Ok(v) => v,
        Err(ref e) => unreachable!("Feed encoder stream 1 failed: {e:?}"),
    };

    let headers_2 = vec![(Bytes::from("x-dynamic"), Bytes::from("value-1"))];
    let Ok((header_block_2, enc_data_2)) = encoder.encode(stream_id_2, &headers_2) else {
        unreachable!("Encoding stream 2 failed");
    };

    let result = decoder.decode_header(stream_id_2, Bytes::from(header_block_2));
    let Ok((_, status)) = result else {
        unreachable!("Decoding stream 2 failed");
    };

    match status {
        DecodeStatus::Blocked => {
            assert!(decoder.pending_blocks.contains_key(&stream_id_2));

            let Ok(unblocked) = decoder.feed_encoder(&enc_data_2) else {
                unreachable!("Feed encoder stream 2 failed");
            };

            assert!(unblocked.contains(&stream_id_2));

            let Ok(resume_res) = decoder.resume_header(stream_id_2) else {
                unreachable!("Resume header failed");
            };

            assert!(matches!(resume_res, Some(ref h) if h == &headers_2));
            assert!(!decoder.pending_blocks.contains_key(&stream_id_2));
        }
        DecodeStatus::Complete(h) => {
            assert_eq!(h, headers_2);
        }
    }
}

#[test]
fn test_decoder_feed_malformed_encoder_instructions() {
    let mut decoder = Decoder::new(4096, 4096);
    let malformed_data = vec![0x80, 0xFF, 0xFF, 0xFF, 0xFF];

    let res = decoder.feed_encoder(&malformed_data);

    match res {
        Err(_) => (),
        Ok(_) => unreachable!("Expected error for malformed instructions"),
    }
}

#[test]
fn test_encode_large_batch_triggers_buffer_resize() {
    let mut encoder = Encoder::new();
    let headers = create_dummy_headers(500);
    let stream_id = 2;

    let Ok((header_block, _)) = encoder.encode(stream_id, &headers) else {
        unreachable!("Encoding failed");
    };

    assert!(!header_block.is_empty());
}

#[test]
fn test_encoder_apply_settings_limit_exceeded() {
    let mut encoder = Encoder::new();
    let max_table = 4096;
    let blocked_streams = u64::from(ENCODER_MAX_BLOCKED_STREAMS_LIMIT) + 1;

    let res = encoder.apply_settings(max_table, blocked_streams);

    assert!(matches!(res, Err(QpackError::EncoderError)));
}

#[test]
fn test_encoder_feed_decoder_noop_safely() {
    let mut encoder = Encoder::new();
    let dummy_data = vec![0x00, 0x01];

    encoder.feed_decoder(&dummy_data);
}

#[test]
fn test_encoder_initialization_and_settings() {
    let mut encoder = Encoder::new();
    let max_table = 4096;
    let blocked_streams = 16;

    let res = encoder.apply_settings(max_table, blocked_streams);
    let Ok(settings_data) = res else {
        unreachable!("Apply settings failed");
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

    let header_res = Header::new(name, value);
    if let Err(ref e) = header_res {
        unreachable!("Header creation failed: {e:?}");
    }
}

#[test]
fn test_header_lsxpack_conversion_integrity() {
    let name = b"x-test-name";
    let value = b"x-test-value";
    let Ok(mut header) = Header::new(name, value) else {
        unreachable!("Header creation failed");
    };

    let lsx = header.create_lsxpack_header();

    let Ok(expected_name_len) = u16::try_from(name.len()) else {
        unreachable!("Name too long");
    };
    let Ok(expected_val_len) = u16::try_from(value.len()) else {
        unreachable!("Value too long");
    };
    let Ok(expected_val_offset) = i32::try_from(name.len()) else {
        unreachable!("Offset overflow");
    };

    assert_eq!(lsx.name_len, expected_name_len);
    assert_eq!(lsx.val_len, expected_val_len);
    assert_eq!(lsx.name_offset, 0);
    assert_eq!(lsx.val_offset, expected_val_offset);
}

#[test]
fn test_resume_header_non_existent_stream() {
    let mut decoder = Decoder::new(4096, 4096);
    let stream_id = 999;

    let result = decoder.resume_header(stream_id);
    let Ok(h_opt) = result else {
        unreachable!("Resume header check failed");
    };

    assert!(h_opt.is_none());
}

#[test]
fn test_round_trip_simple_flow() {
    let mut encoder = Encoder::new();
    let mut decoder = Decoder::new(4096, 4096);
    let stream_id = 1;
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":path"), Bytes::from("/index.html")),
    ];

    let Ok((header_block, enc_data)) = encoder.encode(stream_id, &headers) else {
        unreachable!("Encoding failed");
    };

    let _unused_unblocked = match decoder.feed_encoder(&enc_data) {
        Ok(v) => v,
        Err(ref e) => unreachable!("Feed encoder failed: {e:?}"),
    };

    let Ok((dec_instructions, status)) =
        decoder.decode_header(stream_id, Bytes::from(header_block))
    else {
        unreachable!("Decoding failed");
    };

    encoder.feed_decoder(&dec_instructions);

    assert!(matches!(status, DecodeStatus::Complete(ref h) if h == &headers));
}
