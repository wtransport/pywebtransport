//! Unit tests for the `crate::protocol::h3` module.

use std::ptr;

use bytes::{BufMut, Bytes, BytesMut};
use rstest::rstest;

use super::*;
use crate::common::constants::{
    ERR_H3_CLOSED_CRITICAL_STREAM, ERR_H3_DATAGRAM_ERROR, ERR_H3_FRAME_ERROR,
    ERR_H3_FRAME_UNEXPECTED, ERR_H3_GENERAL_PROTOCOL_ERROR, ERR_H3_ID_ERROR, ERR_H3_INTERNAL_ERROR,
    ERR_H3_MESSAGE_ERROR, ERR_H3_MISSING_SETTINGS, ERR_H3_SETTINGS_ERROR,
    ERR_H3_STREAM_CREATION_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_QPACK_DECODER_STREAM_ERROR,
    ERR_QPACK_DECOMPRESSION_FAILED, ERR_QPACK_ENCODER_STREAM_ERROR, ERR_WT_REQUIREMENTS_NOT_MET,
    ERR_WT_SESSION_GONE, H3_FRAME_TYPE_DATA, H3_FRAME_TYPE_HEADERS, H3_FRAME_TYPE_SETTINGS,
    H3_FRAME_TYPE_WT_STREAM, WT_UPGRADE_TOKEN,
};
use crate::common::error::WebTransportError;
use crate::common::types::{Headers, StreamId};
use crate::protocol::connection::{Connection, ConnectionParameters};
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::qpack::{Decoder, Encoder};
use crate::protocol::utils;

fn create_blocked_h3() -> H3 {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    h3.decoder = Decoder::new(4096, 100);
    h3.settings_received = true;
    h3.bind_local_streams(3, 7, 11);

    let mut encoder = Encoder::new(4096, 100);
    let headers = vec![(Bytes::from("x-dynamic"), Bytes::from("value"))];
    let settings_instructions = match encoder.apply_settings(4096, 100) {
        Ok(instructions) => instructions,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };
    if let Err(e) = encoder.encode(4, &headers) {
        assert_eq!(format!("{e:?}"), "");
    }
    let header_block = match encoder.encode(0, &headers) {
        Ok((header_block, _)) => header_block,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    for data in [vec![0x02], settings_instructions] {
        h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id: 2,
                data: Bytes::from(data),
                end_stream: false,
            },
            mock.as_connection(),
        );
    }
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id: 0 },
        mock.as_connection(),
    );
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 0,
            data: create_headers_frame(&header_block),
            end_stream: false,
        },
        mock.as_connection(),
    );

    h3
}

fn create_h3(is_client: bool) -> H3 {
    let parameters = H3Parameters {
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        max_capsule_size: 65536,
        max_field_section_size: 65536,
    };

    H3::new(is_client, parameters)
}

fn create_headers_frame(header_block: &[u8]) -> Bytes {
    let header = match encode_frame_header(H3_FRAME_TYPE_HEADERS, header_block.len()) {
        Ok(header) => header,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    let mut frame = BytesMut::with_capacity(header.len() + header_block.len());
    frame.put(header);
    frame.put_slice(header_block);
    frame.freeze()
}

fn encode_header_block(headers: &Headers) -> Vec<u8> {
    let mut encoder = Encoder::new(0, 0);

    match encoder.encode(0, headers) {
        Ok((header_block, _)) => header_block,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    }
}

#[repr(C, align(8))]
struct MockConnectionLayout {
    _padding: [u8; 1024],
}

impl MockConnectionLayout {
    fn as_connection(&self) -> &Connection {
        unsafe { &*ptr::from_ref(self).cast::<Connection>() }
    }
}

fn valid_request_headers() -> Headers {
    vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
        (Bytes::from(":path"), Bytes::from("/")),
    ]
}

fn valid_settings_frame() -> Bytes {
    let settings = H3Settings {
        wt_enabled: Some(1),
        max_field_section_size: Some(65536),
        ..Default::default()
    };

    let payload = match encode_settings(&settings) {
        Ok(payload) => payload,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    let header = match encode_frame_header(H3_FRAME_TYPE_SETTINGS, payload.len()) {
        Ok(header) => header,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    let mut frame = BytesMut::with_capacity(header.len() + payload.len());
    frame.put(header);
    frame.put(payload);
    frame.freeze()
}

#[test]
fn test_bind_connect_stream_registers_stream() {
    let mut h3 = create_h3(true);

    let effects = h3.bind_connect_stream(0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
    assert!(
        h3.streams
            .get(&0)
            .is_some_and(|stream| stream.is_connect_stream && stream.is_request_stream)
    );
}

#[test]
fn test_bind_local_streams_invalid_id_failure() {
    let mut h3 = create_h3(true);

    let effects = h3.bind_local_streams(0, 6, 10);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_INTERNAL_ERROR,
            reason: Some(reason),
        }] if reason == "h3_stream encode failed"
    ));
    assert_eq!(h3.local_control_stream_id(), None);
}

#[test]
fn test_bind_local_streams_settings_encode_failure() {
    let mut h3 = H3::new(
        true,
        H3Parameters {
            initial_max_data: 4 * 1024 * 1024,
            initial_max_streams_bidi: 10,
            initial_max_streams_uni: 10,
            max_capsule_size: 65536,
            max_field_section_size: u64::MAX,
        },
    );

    let effects = h3.bind_local_streams(2, 6, 10);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_INTERNAL_ERROR,
            reason: Some(reason),
        }] if reason == "h3_settings encode failed"
    ));
    assert_eq!(h3.local_control_stream_id(), None);
}

#[test]
fn test_bind_local_streams_success() {
    let mut h3 = create_h3(true);

    let effects = h3.bind_local_streams(2, 6, 10);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                stream_id: 2,
                data: control_data,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: 6,
                data: encoder_data,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: 10,
                data: decoder_data,
                end_stream: false,
            },
        ] if control_data.starts_with(b"\x00\x04")
            && encoder_data.as_ref() == b"\x02"
            && decoder_data.as_ref() == b"\x03"
    ));
    assert_eq!(h3.local_control_stream_id(), Some(2));
}

#[test]
fn test_cleanup_stream_removes_h3_stream() {
    let mut h3 = create_h3(true);
    let stream_id = 4;
    let stream = H3Stream {
        ended: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);
    h3.cleanup_stream(stream_id);
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_client_rejects_server_initiated_non_webtransport_bidirectional_stream() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 1;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_STREAM_CREATION_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_encode_capsule_bidirectional_success() {
    let stream_id = 0;
    let capsule_type = 0x1234;
    let data = Bytes::from("capsule_payload");
    let result = H3::encode_capsule(stream_id, capsule_type, data.clone());
    match result {
        Ok(parts) => assert_eq!(parts.first().and_then(|part| part.first()), Some(&0x00)),
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_encode_capsule_invalid_type_failure() {
    let result = H3::encode_capsule(0, u64::MAX, Bytes::new());

    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), _))
    ));
}

#[test]
fn test_encode_capsule_unidirectional_failure() {
    let stream_id = 2;
    let result = H3::encode_capsule(stream_id, 1, Bytes::new());
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(
            Some(ERR_H3_STREAM_CREATION_ERROR),
            _
        ))
    ));
}

#[test]
fn test_encode_connect_request_success() {
    let mut h3 = create_h3(true);

    let result = h3.encode_connect_request(0, "localhost".to_owned(), "/".to_owned(), &Vec::new());

    assert!(matches!(
        result.as_deref(),
        Ok([
            Effect::SendQuicData {
                stream_id: 0,
                end_stream: false,
                ..
            },
            Effect::SendQuicData {
                stream_id: 0,
                end_stream: false,
                ..
            }
        ])
    ));
}

#[test]
fn test_encode_datagram_success() {
    let stream_id = 0;
    let data = Bytes::from("datagram");
    let result = H3::encode_datagram(stream_id, data);
    match result {
        Ok(parts) => {
            assert_eq!(parts.len(), 2);
            if let Some(header) = parts.first() {
                assert_eq!(header.as_ref(), &[0x00]);
            } else {
                assert_eq!(parts.len(), 0);
            }
        }
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_encode_datagram_wrong_stream_type_failure() {
    let stream_id = 2;
    let result = H3::encode_datagram(stream_id, Bytes::from("d"));
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(
            Some(ERR_H3_STREAM_CREATION_ERROR),
            _
        ))
    ));
}

#[test]
fn test_encode_goaway_invalid_identifier_failure() {
    let result = H3::encode_goaway(u64::MAX);

    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), _))
    ));
}

#[test]
fn test_encode_goaway_success() {
    let result = H3::encode_goaway(100);
    match result {
        Ok(bytes) => assert_eq!(bytes.first(), Some(&0x07)),
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_encode_headers_dynamic_table_emits_encoder_instructions() {
    let mut h3 = create_h3(true);
    h3.encoder = Encoder::new(4096, 100);
    h3.bind_local_streams(2, 6, 10);
    if let Err(e) = h3.encoder.apply_settings(4096, 100) {
        assert_eq!(format!("{e:?}"), "");
    }
    let headers = vec![(Bytes::from("x-dynamic"), Bytes::from("value"))];
    if let Err(e) = h3.encode_headers(0, &headers, false) {
        assert_eq!(format!("{e:?}"), "");
    }

    let result = h3.encode_headers(4, &headers, false);

    assert!(matches!(
        result.as_deref(),
        Ok([
            Effect::SendQuicData {
                stream_id: 6,
                data: encoder_instructions,
                end_stream: false,
            },
            Effect::SendQuicData { stream_id: 4, .. },
            Effect::SendQuicData {
                stream_id: 4,
                end_stream: false,
                ..
            }
        ]) if !encoder_instructions.is_empty()
    ));
}

#[test]
fn test_encode_headers_field_too_long_failure() {
    let mut h3 = create_h3(true);
    let headers = vec![(Bytes::from("x-large"), Bytes::from(vec![b'a'; 70000]))];

    let result = h3.encode_headers(0, &headers, false);

    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(
            Some(ERR_QPACK_ENCODER_STREAM_ERROR),
            _
        ))
    ));
}

#[test]
fn test_encode_headers_success() {
    let mut h3 = create_h3(true);
    let stream_id = 0;
    let headers = valid_request_headers();

    let result = h3.encode_headers(stream_id, &headers, true);

    match result {
        Ok(effects) => {
            assert!(
                effects
                    .iter()
                    .any(|effect| matches!(effect, Effect::SendQuicData { .. }))
            );
        }
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[rstest]
#[case::unknown_identifier(u64::MAX, 1)]
#[case::unknown_value(0x21, u64::MAX)]
fn test_encode_settings_invalid_unknown_failure(#[case] identifier: u64, #[case] value: u64) {
    let settings = H3Settings {
        unknown: vec![(identifier, value)],
        ..Default::default()
    };

    let result = encode_settings(&settings);

    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), _))
    ));
}

#[test]
fn test_encode_stream_header_bidirectional() {
    let effects = H3::encode_stream_header(4, 0, false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            stream_id: 4,
            data,
            end_stream: false,
        }] if data.as_ref() == [0x40, 0x41, 0x00]
    ));
}

#[test]
fn test_encode_stream_header_invalid_session_id() {
    let effects = H3::encode_stream_header(4, u64::MAX, false);

    assert!(effects.is_empty());
}

#[test]
fn test_encode_stream_header_unidirectional() {
    let effects = H3::encode_stream_header(6, 0, true);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            stream_id: 6,
            data,
            end_stream: false,
        }] if data.as_ref() == [0x40, 0x54, 0x00]
    ));
}

#[test]
fn test_fail_local_streams_closes_connection() {
    let effects = H3::fail_local_streams();

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_GENERAL_PROTOCOL_ERROR,
            reason: Some(reason),
        }] if reason == "h3_stream create failed"
    ));
}

#[test]
fn test_handle_transport_event_datagram_malformed() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let data = Bytes::new();

    let event = ProtocolEvent::TransportDatagramReceived { data };

    let (_, effects) = h3.handle_transport_event(&event, mock.as_connection());

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_DATAGRAM_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_handle_transport_event_datagram_success() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let mut data = BytesMut::new();
    data.extend_from_slice(&[0x00]);
    data.extend_from_slice(b"payload");

    let event = ProtocolEvent::TransportDatagramReceived {
        data: data.freeze(),
    };

    let (events, _) = h3.handle_transport_event(&event, mock.as_connection());

    assert_eq!(events.len(), 1);
    if let Some(ProtocolEvent::H3DatagramReceived { stream_id, data }) = events.first() {
        assert_eq!(*stream_id, 0);
        assert_eq!(data.as_ref(), b"payload");
    } else {
        assert_eq!(events.len(), 0);
    }
}

#[test]
fn test_handle_transport_event_ignores_unrelated_event() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportHandshakeCompleted,
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(effects.is_empty());
}

#[test]
fn test_handle_transport_event_reset_critical_stream_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportResetStreamReceived {
            stream_id: 3,
            error_code: 0,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_CLOSED_CRITICAL_STREAM,
            ..
        }]
    ));
}

#[test]
fn test_handle_transport_event_reset_stream_cleans_up() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id: 4 },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportResetStreamReceived {
            stream_id: 4,
            error_code: 0,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&4));
}

#[test]
fn test_handle_transport_event_stop_sending_critical_stream_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    h3.bind_local_streams(2, 6, 10);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStopSendingReceived {
            stream_id: 2,
            error_code: 0,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_CLOSED_CRITICAL_STREAM,
            ..
        }]
    ));
}

#[test]
fn test_handle_transport_event_stop_sending_ignored() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id: 4 },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStopSendingReceived {
            stream_id: 4,
            error_code: 0,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(effects.is_empty());
    assert!(h3.streams.contains_key(&4));
}

#[test]
fn test_handle_transport_event_stream_opened() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id: 4 },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 4,
            max_size: 1,
        }]
    ));
    assert!(h3.streams.contains_key(&4));
}

#[test]
fn test_new_valid_config_success() {
    let parameters = H3Parameters {
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        max_capsule_size: 65536,
        max_field_section_size: 65536,
    };
    let _h3 = H3::new(true, parameters);
}

#[rstest]
#[case::incomplete_length(&[0x00, 0x40])]
#[case::incomplete_type(&[0x40])]
fn test_parse_capsules_incomplete_header_buffered(#[case] data: &'static [u8]) {
    let mut h3 = create_h3(true);
    let mut stream = H3Stream {
        headers_processed: true,
        is_connect_stream: true,
        ..Default::default()
    };
    stream.capsule_buffer.extend_from_slice(data);

    let events = match h3.parse_capsules(0, &mut stream) {
        Ok(events) => events,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert!(events.is_empty());
    assert_eq!(stream.capsule_buffer.as_ref(), data);
}

#[test]
fn test_parse_settings_all_named_and_unknown_fields() {
    let settings = H3Settings {
        enable_connect_protocol: Some(1),
        h3_datagram: Some(1),
        qpack_blocked_streams: Some(16),
        wt_enabled: Some(1),
        wt_initial_max_data: Some(4 * 1024 * 1024),
        wt_initial_max_streams_bidi: Some(10),
        wt_initial_max_streams_uni: Some(10),
        unknown: vec![(0x1234, 42)],
        ..Default::default()
    };

    let Ok(payload) = encode_settings(&settings) else {
        return;
    };

    let parsed = match parse_settings(&payload) {
        Ok(parsed) => parsed,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert_eq!(parsed.enable_connect_protocol, Some(1));
    assert_eq!(parsed.h3_datagram, Some(1));
    assert_eq!(parsed.qpack_blocked_streams, Some(16));
    assert_eq!(parsed.wt_initial_max_data, Some(4 * 1024 * 1024));
    assert_eq!(parsed.wt_initial_max_streams_bidi, Some(10));
    assert_eq!(parsed.wt_initial_max_streams_uni, Some(10));
    assert_eq!(parsed.unknown, vec![(0x1234, 42)]);
}

#[test]
fn test_parse_settings_duplicate_id_failure() {
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(&[0x01, 0x01]);
    buffer.extend_from_slice(&[0x01, 0x02]);
    let result = parse_settings(&buffer.freeze());
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_duplicate_unknown_id_failure() {
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(&[0x40, 0x30, 0x01]);
    buffer.extend_from_slice(&[0x40, 0x30, 0x02]);

    let result = parse_settings(&buffer.freeze());

    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_entries_limit_exceeded() {
    let mut buffer = BytesMut::new();
    for identifier in 0x40u64..=0x80u64 {
        buffer.extend_from_slice(&[0x40, identifier.try_into().unwrap_or(0)]);
        buffer.extend_from_slice(&[0x01]);
    }

    let result = parse_settings(&buffer.freeze());

    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_reserved_id_failure() {
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(&[0x02, 0x00]);
    let result = parse_settings(&buffer.freeze());
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_truncated_id_failure() {
    let buffer = Bytes::from_static(&[0x40]);

    let result = parse_settings(&buffer);

    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_truncated_value_failure() {
    let buffer = Bytes::from_static(&[0x01, 0x40]);

    let result = parse_settings(&buffer);

    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_unassigned_id_ignored() {
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(&[0x00, 0x01]);

    let parsed = match parse_settings(&buffer.freeze()) {
        Ok(settings) => settings,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert_eq!(parsed.unknown, vec![(0x00, 0x01)]);
}

#[test]
fn test_parse_settings_valid() {
    let settings = H3Settings {
        qpack_max_table_capacity: Some(100),
        max_field_section_size: Some(65536),
        ..Default::default()
    };

    let Ok(payload) = encode_settings(&settings) else {
        return;
    };

    let parsed = parse_settings(&payload);
    match parsed {
        Ok(parsed_settings) => {
            assert_eq!(parsed_settings.qpack_max_table_capacity, Some(100));
            assert_eq!(parsed_settings.max_field_section_size, Some(65536));
        }
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_recv_capsule_too_large() {
    let mut h3 = create_h3(true);
    let stream_id = 0;

    let mut stream = H3Stream {
        headers_processed: true,
        is_connect_stream: true,
        ..Default::default()
    };

    let mut data = BytesMut::new();
    data.extend_from_slice(&[0x00]);
    data.extend_from_slice(&[0x80, 0x01, 0x00, 0x01]);

    stream.capsule_buffer.extend_from_slice(&data);

    let result = h3.parse_capsules(stream_id, &mut stream);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_recv_close_control_stream_error() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::new(),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_CLOSED_CRITICAL_STREAM,
            ..
        }]
    ));
}

#[test]
fn test_recv_close_qpack_stream_error() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 11;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x03]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::new(),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_CLOSED_CRITICAL_STREAM,
            ..
        }]
    ));
}

#[test]
fn test_recv_connect_stream_capsule_fragmented() {
    let mut h3 = create_h3(true);
    let stream_id = 0;

    let mut stream = H3Stream {
        headers_processed: true,
        is_connect_stream: true,
        ..Default::default()
    };

    let mut chunk1 = BytesMut::new();
    chunk1.extend_from_slice(&[0x00]);
    chunk1.extend_from_slice(&[0x03]);

    stream.capsule_buffer.extend_from_slice(&chunk1);

    let events1 = match h3.parse_capsules(stream_id, &mut stream) {
        Ok(events) => events,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };
    assert!(events1.is_empty());

    let mut chunk2 = BytesMut::new();
    chunk2.extend_from_slice(b"ABC");

    stream.capsule_buffer.extend_from_slice(&chunk2);

    let events2 = match h3.parse_capsules(stream_id, &mut stream) {
        Ok(events) => events,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert_eq!(events2.len(), 1);
    if let Some(ProtocolEvent::H3CapsuleReceived { capsule_value, .. }) = events2.first() {
        assert_eq!(capsule_value.as_ref(), b"ABC");
    } else {
        assert_eq!(events2.len(), 0);
    }
}

#[test]
fn test_recv_connect_stream_capsule_split_across_data_chunks() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let stream = H3Stream {
        headers_processed: true,
        is_connect_stream: true,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00, 0x05, 0x00, 0x03, b'A']),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(events.is_empty());

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(b"BC"),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3CapsuleReceived {
            capsule_type: 0x00,
            capsule_value,
            ..
        }] if capsule_value.as_ref() == b"ABC"
    ));
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_connect_stream_capsules() {
    let mut h3 = create_h3(false);
    let stream_id = 0;
    h3.settings_received = true;

    let mut stream = H3Stream {
        headers_processed: true,
        is_connect_stream: true,
        ..Default::default()
    };

    let mut buffer = BytesMut::new();
    buffer.put_u8(0x00);
    buffer.put_u8(0x03);
    buffer.put_slice(b"ABC");

    stream.capsule_buffer.extend_from_slice(&buffer);

    let events = match h3.parse_capsules(stream_id, &mut stream) {
        Ok(events) => events,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert!(!events.is_empty());
    if let Some(ProtocolEvent::H3CapsuleReceived {
        capsule_type,
        capsule_value,
        ..
    }) = events.first()
    {
        assert_eq!(*capsule_type, 0x00);
        assert_eq!(capsule_value.as_ref(), b"ABC");
    } else {
        assert_eq!(events.len(), 0);
    }
}

#[test]
fn test_recv_connect_stream_capsules_via_data_frame() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let stream = H3Stream {
        headers_processed: true,
        is_connect_stream: true,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);

    let mut data = BytesMut::new();
    data.put_u8(0x00);
    data.put_u8(0x05);
    data.put_u8(0x00);
    data.put_u8(0x03);
    data.put_slice(b"ABC");

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3CapsuleReceived {
            stream_id: 0,
            capsule_type: 0x00,
            capsule_value,
        }] if capsule_value.as_ref() == b"ABC"
    ));
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_connect_stream_data_before_headers_ignored() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00, 0x02, 0x00, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.capsule_buffer.is_empty())
    );
}

#[test]
fn test_recv_control_frame_fragmented() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let valid_frame = valid_settings_frame();
    let split_at = valid_frame.len() / 2;

    let chunk1 = valid_frame.slice(0..split_at);
    let chunk2 = valid_frame.slice(split_at..);

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: chunk1,
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(events.is_empty());

    let (events_final, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: chunk2,
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(!events_final.is_empty());
}

#[test]
fn test_recv_control_frame_incomplete_type_buffered() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x40]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 3,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_control_frame_too_large() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let mut buffer = BytesMut::new();
    buffer.put_u8(0x04);
    buffer.put_u8(0xFF);
    buffer.put_slice(&[0xFF; 7]);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: buffer.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert_eq!(*error_code, ERR_H3_FRAME_ERROR);
}

#[test]
fn test_recv_control_goaway_success() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: valid_settings_frame(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x07, 0x01, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ProtocolEvent::H3GoawayReceived))
    );
}

#[test]
fn test_recv_control_headers_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: valid_settings_frame(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x01, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_UNEXPECTED,
            ..
        }]
    ));
}

#[test]
fn test_recv_control_settings_emits_encoder_instructions() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.encoder = Encoder::new(4096, 100);
    h3.bind_local_streams(2, 6, 10);

    let settings = H3Settings {
        qpack_max_table_capacity: Some(4096),
        wt_enabled: Some(1),
        ..Default::default()
    };
    let payload = match encode_settings(&settings) {
        Ok(payload) => payload,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };
    let header = match encode_frame_header(H3_FRAME_TYPE_SETTINGS, payload.len()) {
        Ok(header) => header,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };
    let mut frame = BytesMut::new();
    frame.put(header);
    frame.put(payload);

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: frame.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3SettingsReceived { .. }]
    ));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                stream_id: 6,
                data: encoder_instructions,
                end_stream: false,
            },
            Effect::ReceiveQuicData {
                stream_id: 3,
                max_size: u64::MAX,
            }
        ] if !encoder_instructions.is_empty()
    ));
}

#[test]
fn test_recv_control_settings_twice_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: valid_settings_frame(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: valid_settings_frame(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_UNEXPECTED,
            ..
        }]
    ));
}

#[test]
fn test_recv_control_unknown_frame_ignored() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: valid_settings_frame(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x21, 0x01, 0xFF]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 3,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_control_wt_stream_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: valid_settings_frame(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let mut data = BytesMut::new();
    assert!(matches!(
        utils::write_varint(&mut data, H3_FRAME_TYPE_WT_STREAM),
        Ok(())
    ));
    assert!(matches!(utils::write_varint(&mut data, 0), Ok(())));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_data_on_non_connect_stream_ignored() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let stream = H3Stream {
        headers_processed: true,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);

    let mut data = BytesMut::new();
    data.extend_from_slice(&[0x00]);
    data.extend_from_slice(&[0x03]);
    data.extend_from_slice(b"ign");

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_double_headers_error() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let stream = H3Stream {
        headers_processed: true,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);
    h3.settings_received = true;

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x01, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 0,
                error_code: ERR_H3_FRAME_UNEXPECTED,
            },
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_H3_FRAME_UNEXPECTED,
            }
        ]
    ));
    assert!(!h3.streams.contains_key(&stream_id));
}

#[rstest]
#[case::connect_stream(true)]
#[case::request_stream(false)]
fn test_recv_empty_data_frame_ignored(#[case] is_connect_stream: bool) {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let stream = H3Stream {
        headers_processed: true,
        is_connect_stream,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_headers_dynamic_table_fails_with_zero_capacity() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;

    let mut data = BytesMut::new();
    data.put_u8(0x01);
    assert!(matches!(utils::write_varint(&mut data, 3), Ok(())));
    data.extend_from_slice(&[0x02, 0x00, 0x80]);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert_eq!(*error_code, ERR_QPACK_DECOMPRESSION_FAILED);
}

#[test]
fn test_recv_malformed_control_frame() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x04]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 3,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_request_data_connect_headers_marks_connect_stream() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id },
        mock.as_connection(),
    );
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("CONNECT")),
        (
            Bytes::from(":protocol"),
            Bytes::from_static(WT_UPGRADE_TOKEN),
        ),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
        (Bytes::from(":path"), Bytes::from("/")),
    ];

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: create_headers_frame(&encode_header_block(&headers)),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3HeadersReceived {
            stream_id: 0,
            headers: received_headers,
        }] if *received_headers == headers
    ));
    assert!(effects.is_empty());
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.is_connect_stream && stream.headers_processed)
    );
}

#[test]
fn test_recv_request_data_connect_stream_fin_closes() {
    let mut h3 = create_h3(true);
    h3.bind_connect_stream(0);

    let (events, effects) = match h3.recv_request_data(0, &[], true) {
        Ok(result) => result,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3ConnectStreamClosed { stream_id: 0 }]
    ));
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&0));
}

#[test]
fn test_recv_request_data_control_frame_too_large() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let mut data = BytesMut::new();
    data.put_u8(0x21);
    assert!(matches!(utils::write_varint(&mut data, 5000), Ok(())));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_request_data_empty_stream_fin_cleanup() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 4;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::new(),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_recv_request_data_fin_cleans_up_request_stream() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 4;

    let stream = H3Stream {
        headers_processed: true,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00, 0x01, 0xFF]),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_recv_request_data_headers_blocked_on_dynamic_table() {
    let h3 = create_blocked_h3();

    assert!(
        h3.streams
            .get(&0)
            .is_some_and(|stream| stream.blocked && !stream.headers_processed)
    );
    assert!(h3.resume_request_stream(0).is_empty());
}

#[test]
fn test_recv_request_data_headers_dynamic_table_acknowledged() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    h3.decoder = Decoder::new(4096, 100);
    h3.settings_received = true;
    h3.bind_local_streams(3, 7, 11);

    let mut encoder = Encoder::new(4096, 100);
    let mut headers = valid_request_headers();
    headers.push((Bytes::from("x-dynamic"), Bytes::from("value")));
    let settings_instructions = match encoder.apply_settings(4096, 100) {
        Ok(instructions) => instructions,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };
    if let Err(e) = encoder.encode(4, &headers) {
        assert_eq!(format!("{e:?}"), "");
    }
    let (header_block, insert_instructions) = match encoder.encode(0, &headers) {
        Ok(result) => result,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    for data in [vec![0x02], settings_instructions, insert_instructions] {
        h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id: 2,
                data: Bytes::from(data),
                end_stream: false,
            },
            mock.as_connection(),
        );
    }
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id: 0 },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 0,
            data: create_headers_frame(&header_block),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3HeadersReceived {
            stream_id: 0,
            headers: received_headers,
        }] if *received_headers == headers
    ));
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            stream_id: 11,
            data: decoder_instructions,
            end_stream: false,
        }] if !decoder_instructions.is_empty()
    ));
}

#[test]
fn test_recv_request_data_headers_frame_flow() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;

    let headers = valid_request_headers();
    let mut sender_h3 = create_h3(true);
    if let Err(e) = sender_h3.set_local_stream_ids(2, 6, 10) {
        assert_eq!(format!("{e:?}"), "");
    }

    let Ok(encoded_effects) = sender_h3.encode_headers(stream_id, &headers, false) else {
        return;
    };

    let mut encoder_data = BytesMut::new();
    let mut frame_data = BytesMut::new();

    for effect in encoded_effects {
        if let Effect::SendQuicData {
            stream_id: effect_stream_id,
            data,
            ..
        } = effect
        {
            if effect_stream_id == 6 {
                encoder_data.extend_from_slice(&data);
            } else if effect_stream_id == stream_id {
                frame_data.extend_from_slice(&data);
            }
        }
    }

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 6,
            data: Bytes::from(vec![0x02]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 6,
            data: encoder_data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: frame_data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3HeadersReceived {
            stream_id: 0,
            headers: received_headers,
        }] if *received_headers == headers
    ));
    assert!(effects.is_empty());
}

#[test]
fn test_recv_request_data_headers_recovery_still_blocked() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let stream = H3Stream {
        headers_processed: false,
        blocked: true,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(b""),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(effects.is_empty());
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.blocked)
    );
}

#[test]
fn test_recv_request_data_headers_too_large() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;
    h3.bind_connect_stream(stream_id);

    let mut data = BytesMut::new();
    data.put_u8(0x01);
    assert!(matches!(utils::write_varint(&mut data, 65537), Ok(())));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 0,
                error_code: ERR_H3_MESSAGE_ERROR,
            },
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_H3_MESSAGE_ERROR,
            }
        ]
    ));
}

#[test]
fn test_recv_request_data_incomplete_frame_buffered() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id },
        mock.as_connection(),
    );

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.is_request_stream && stream.buffer.as_ref() == [0x00])
    );
}

#[test]
fn test_recv_request_data_partial_headers_frame_buffered() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x01, 0x0A, 0x00, 0x00, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.frame_type == Some(0x01) && stream.buffer.len() == 3)
    );
}

#[test]
fn test_recv_request_data_push_promise_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;
    h3.bind_connect_stream(stream_id);

    let mut data = BytesMut::new();
    data.put_u8(0x05);
    data.put_u8(0x00);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_UNEXPECTED,
            ..
        }]
    ));
}

#[test]
fn test_recv_request_data_unknown_frame_ignored() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let data = Bytes::from(vec![0x21, 0x01, 0xFF]);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data,
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_request_data_webtransport_stream_header_parsed() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 4;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamOpened { stream_id },
        mock.as_connection(),
    );

    for byte in [0x40, 0x41] {
        let (events, effects) = h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data: Bytes::from(vec![byte]),
                end_stream: false,
            },
            mock.as_connection(),
        );

        assert!(events.is_empty());
        assert!(matches!(
            effects.as_slice(),
            [Effect::ReceiveQuicData {
                stream_id: 4,
                max_size: 1,
            }]
        ));
    }

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3WebTransportStreamOpened {
            session_id: 0,
            stream_id: 4,
        }]
    ));
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_recv_request_data_webtransport_stream_invalid_session_fails() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 4;

    for byte in [0x40, 0x41] {
        h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data: Bytes::from(vec![byte]),
                end_stream: false,
            },
            mock.as_connection(),
        );
    }

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x02]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_ID_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_request_data_wt_stream_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x40, 0x41, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_request_settings_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x04, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_UNEXPECTED,
            ..
        }]
    ));
}

#[test]
fn test_recv_response_headers_on_connect_stream() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);
    let headers = vec![(Bytes::from(":status"), Bytes::from("200"))];

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: create_headers_frame(&encode_header_block(&headers)),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3HeadersReceived {
            stream_id: 0,
            headers: received_headers,
        }] if *received_headers == headers
    ));
    assert!(effects.is_empty());
}

#[test]
fn test_recv_unidirectional_stream_cancel_push_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    h3.settings_received = true;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let mut data = BytesMut::new();
    data.put_u8(0x03);
    data.put_u8(0x00);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_ID_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_data_control_data_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: valid_settings_frame(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let mut data = BytesMut::new();
    assert!(matches!(
        utils::write_varint(&mut data, H3_FRAME_TYPE_DATA),
        Ok(())
    ));
    assert!(matches!(utils::write_varint(&mut data, 0), Ok(())));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_UNEXPECTED,
            ..
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_data_control_missing_settings_failure() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let frame = Bytes::from(vec![0x07, 0x01, 0x00]);
    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: frame,
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_MISSING_SETTINGS,
            ..
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_data_control_settings_success() {
    let h3 = create_h3(true);
    assert_eq!(h3.local_control_stream_id(), None);

    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let settings_frame = valid_settings_frame();
    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: settings_frame,
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(h3.settings_received);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ProtocolEvent::H3SettingsReceived { .. }))
    );
}

#[test]
fn test_recv_unidirectional_stream_data_identifies_control_stream() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;

    let data = Bytes::from(vec![0x00]);
    let event = ProtocolEvent::TransportStreamDataReceived {
        stream_id,
        data,
        end_stream: false,
    };

    let (_, effects) = h3.handle_transport_event(&event, mock.as_connection());

    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 3,
            max_size: u64::MAX,
        }]
    ));
    assert_eq!(h3.peer_control_stream_id, Some(stream_id));
}

#[test]
fn test_recv_unidirectional_stream_data_webtransport_stream_invalid_session_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    for byte in [0x40, 0x54] {
        h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data: Bytes::from(vec![byte]),
                end_stream: false,
            },
            mock.as_connection(),
        );
    }

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x02]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert_eq!(*error_code, ERR_H3_ID_ERROR);
}

#[test]
fn test_recv_unidirectional_stream_duplicate_control_failure() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 7,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_STREAM_CREATION_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_empty_fin_cleanup() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::new(),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_recv_unidirectional_stream_id_reuse_failure() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(!h3.streams.contains_key(&stream_id));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x02]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_STREAM_CREATION_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_max_push_id_frame_ignored() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    h3.settings_received = true;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 2,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let mut data = BytesMut::new();
    data.put_u8(0x0D);
    data.put_u8(0x01);
    data.put_u8(0x00);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 2,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 2,
            max_size: u64::MAX,
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_push_rejected() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 7,
            data: Bytes::from(vec![0x01]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_ID_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_qpack_decoder_feed() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 11;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x03]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let event = ProtocolEvent::TransportStreamDataReceived {
        stream_id,
        data: Bytes::from_static(b"instruction"),
        end_stream: false,
    };
    let (_, effects) = h3.handle_transport_event(&event, mock.as_connection());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 11,
            max_size: u64::MAX,
        }]
    ));
    assert_eq!(h3.peer_decoder_stream_id, Some(stream_id));
}

#[test]
fn test_recv_unidirectional_stream_qpack_decoder_malformed_instruction_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 11;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x03]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_QPACK_DECODER_STREAM_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_qpack_encoder_malformed_instruction_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 11;
    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x02]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00, 0x01, 0x02]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_QPACK_ENCODER_STREAM_ERROR,
            ..
        }]
    ));
}

#[test]
fn test_recv_unidirectional_stream_unknown_type_rejected() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;
    let data = Bytes::from(vec![0x1F]);
    let event = ProtocolEvent::TransportStreamDataReceived {
        stream_id,
        data,
        end_stream: false,
    };
    let (_, effects) = h3.handle_transport_event(&event, mock.as_connection());
    assert!(matches!(
        effects.as_slice(),
        [Effect::StopQuicStream {
            stream_id: 7,
            error_code: ERR_H3_STREAM_CREATION_ERROR,
        }]
    ));
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_recv_webtransport_stream_preamble_fragmented() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    for byte in [0x40, 0x54, 0x40] {
        let (events, effects) = h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data: Bytes::from(vec![byte]),
                end_stream: false,
            },
            mock.as_connection(),
        );

        assert!(events.is_empty());
        assert!(matches!(
            effects.as_slice(),
            [Effect::ReceiveQuicData {
                stream_id: 7,
                max_size: 1,
            }]
        ));
    }

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x04]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3WebTransportStreamOpened {
            session_id: 4,
            stream_id: 7,
        }]
    ));
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_recv_webtransport_unidirectional_stream_missing_session_id_buffer() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    for byte in [0x40, 0x54] {
        h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data: Bytes::from(vec![byte]),
                end_stream: false,
            },
            mock.as_connection(),
        );
    }

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 7,
            max_size: 1,
        }]
    ));
}

#[test]
fn test_recv_webtransport_unidirectional_stream_missing_session_id_fin_cleanup() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;
    for byte in [0x40, 0x54] {
        h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data: Bytes::from(vec![byte]),
                end_stream: false,
            },
            mock.as_connection(),
        );
    }

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::new(),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&stream_id));
}

#[rstest]
#[case::active(false, false, true)]
#[case::blocked(true, false, false)]
#[case::ended(false, true, false)]
fn test_resume_request_stream(
    #[case] blocked: bool,
    #[case] ended: bool,
    #[case] is_resumed: bool,
) {
    let mut h3 = create_h3(false);
    let stream = H3Stream {
        blocked,
        ended,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(0, stream);

    let effects = h3.resume_request_stream(0);

    assert_eq!(
        matches!(
            effects.as_slice(),
            [Effect::ReceiveQuicData {
                stream_id: 0,
                max_size: u64::MAX,
            }]
        ),
        is_resumed
    );
    assert_eq!(effects.is_empty(), !is_resumed);
}

#[test]
fn test_resume_request_stream_not_found() {
    let h3 = create_h3(false);

    let effects = h3.resume_request_stream(0);

    assert!(effects.is_empty());
}

#[test]
fn test_set_local_stream_ids_duplicate_failure() {
    let mut h3 = create_h3(true);
    let result = h3.set_local_stream_ids(2, 2, 6);
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), _))
    ));
}

#[rstest]
#[case::control(0, 6, 10)]
#[case::decoder(2, 6, 8)]
#[case::encoder(2, 4, 10)]
fn test_set_local_stream_ids_invalid_id_failure(
    #[case] control_stream_id: StreamId,
    #[case] encoder_stream_id: StreamId,
    #[case] decoder_stream_id: StreamId,
) {
    let mut h3 = create_h3(true);
    let result = h3.set_local_stream_ids(control_stream_id, encoder_stream_id, decoder_stream_id);
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_ID_ERROR), _))
    ));
    assert_eq!(h3.local_control_stream_id(), None);
}

#[test]
fn test_set_local_stream_ids_reinitialization_failure() {
    let mut h3 = create_h3(true);
    assert!(matches!(h3.set_local_stream_ids(2, 6, 10), Ok(())));
    let result = h3.set_local_stream_ids(14, 18, 22);
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), _))
    ));
}

#[test]
fn test_set_local_stream_ids_success() {
    let mut h3 = create_h3(true);
    let result = h3.set_local_stream_ids(2, 6, 10);
    if let Err(e) = result {
        assert_eq!(format!("{e:?}"), "");
    }
    assert_eq!(h3.local_control_stream_id(), Some(2));
}

#[test]
fn test_stop_request_stream_cancels_blocked_header_block() {
    let mut h3 = create_blocked_h3();

    let effects = h3.stop_request_stream(0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_WT_SESSION_GONE,
            },
            Effect::SendQuicData {
                stream_id: 11,
                data: cancel_instructions,
                end_stream: false,
            }
        ] if !cancel_instructions.is_empty()
    ));
    assert!(!h3.streams.contains_key(&0));
}

#[test]
fn test_stop_request_stream_not_found() {
    let mut h3 = create_h3(false);

    let effects = h3.stop_request_stream(0);

    assert!(effects.is_empty());
}

#[test]
fn test_stop_request_stream_success() {
    let mut h3 = create_h3(false);
    let stream = H3Stream {
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(0, stream);

    let effects = h3.stop_request_stream(0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::StopQuicStream {
            stream_id: 0,
            error_code: ERR_WT_SESSION_GONE,
        }]
    ));
    assert!(!h3.streams.contains_key(&0));
}

#[rstest]
#[case::authority_only(Some("localhost"), &[], true)]
#[case::empty_authority(Some(""), &[], false)]
#[case::empty_host(None, &[""], false)]
#[case::host_only(None, &["localhost"], true)]
#[case::matching_host(Some("localhost"), &["localhost"], true)]
#[case::mismatched_host(Some("localhost"), &["example.com"], false)]
#[case::missing_authority_and_host(None, &[], false)]
#[case::repeated_host(Some("localhost"), &["localhost", "localhost"], false)]
fn test_validate_request_headers_authority_and_host(
    #[case] authority: Option<&'static str>,
    #[case] hosts: &[&'static str],
    #[case] is_valid: bool,
) {
    let mut headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":path"), Bytes::from("/")),
    ];
    if let Some(authority) = authority {
        headers.push((Bytes::from(":authority"), Bytes::from(authority)));
    }
    headers.extend(
        hosts
            .iter()
            .map(|&host| (Bytes::from("host"), Bytes::from(host))),
    );
    let result = validate_request_headers(0, &headers);
    assert_eq!(result.is_ok(), is_valid);
}

#[test]
fn test_validate_request_headers_duplicate_pseudo() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":method"), Bytes::from("POST")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_empty_path_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
        (Bytes::from(":path"), Bytes::from("")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_extended_connect_missing_path_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("CONNECT")),
        (Bytes::from(":protocol"), Bytes::from("webtransport-h3")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_extended_connect_success() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("CONNECT")),
        (Bytes::from(":protocol"), Bytes::from("webtransport-h3")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
        (Bytes::from(":path"), Bytes::from("/")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(result, Ok(())));
}

#[rstest]
#[case::connection_specific(b"connection", b"close")]
#[case::invalid_value(b"custom", b"line\nbreak")]
#[case::te_not_trailers(b"te", b"gzip")]
#[case::uppercase_name(b"Custom", b"value")]
fn test_validate_request_headers_invalid_field_failure(
    #[case] key: &'static [u8],
    #[case] value: &'static [u8],
) {
    let mut headers = valid_request_headers();
    headers.push((Bytes::from_static(key), Bytes::from_static(value)));
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_missing_method_failure() {
    let headers = vec![
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
        (Bytes::from(":path"), Bytes::from("/")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_missing_pseudo_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("GET"))];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_non_http_scheme_without_authority_success() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":scheme"), Bytes::from("urn")),
        (Bytes::from(":path"), Bytes::from("example")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(result, Ok(())));
}

#[test]
fn test_validate_request_headers_plain_connect_empty_authority_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("CONNECT")),
        (Bytes::from(":authority"), Bytes::from("")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_plain_connect_missing_authority_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("CONNECT"))];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_plain_connect_success() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("CONNECT")),
        (Bytes::from(":authority"), Bytes::from("localhost:443")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(result, Ok(())));
}

#[test]
fn test_validate_request_headers_plain_connect_with_scheme_and_path_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("CONNECT")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost:443")),
        (Bytes::from(":path"), Bytes::from("/")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_protocol_without_connect_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":protocol"), Bytes::from("webtransport-h3")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
        (Bytes::from(":path"), Bytes::from("/")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_pseudo_after_regular_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from("custom"), Bytes::from("value")),
        (Bytes::from(":scheme"), Bytes::from("https")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_te_trailers_success() {
    let mut headers = valid_request_headers();
    headers.push((Bytes::from("te"), Bytes::from("trailers")));
    let result = validate_request_headers(0, &headers);
    assert!(matches!(result, Ok(())));
}

#[test]
fn test_validate_request_headers_unknown_pseudo() {
    let headers = vec![(Bytes::from(":unknown"), Bytes::from("value"))];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_duplicate_status() {
    let headers = vec![
        (Bytes::from(":status"), Bytes::from("200")),
        (Bytes::from(":status"), Bytes::from("404")),
    ];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[rstest]
#[case::invalid_value(b"server", b" nginx")]
#[case::te_in_response(b"te", b"trailers")]
#[case::uppercase_name(b"Server", b"nginx")]
fn test_validate_response_headers_invalid_field_failure(
    #[case] key: &'static [u8],
    #[case] value: &'static [u8],
) {
    let headers = vec![
        (Bytes::from(":status"), Bytes::from("200")),
        (Bytes::from_static(key), Bytes::from_static(value)),
    ];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_invalid_pseudo_name_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("GET"))];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_missing_status() {
    let headers = vec![(Bytes::from("server"), Bytes::from("nginx"))];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_pseudo_after_regular_failure() {
    let headers = vec![
        (Bytes::from(":status"), Bytes::from("200")),
        (Bytes::from("server"), Bytes::from("nginx")),
        (Bytes::from(":status"), Bytes::from("200")),
    ];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(
        result,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_valid_success() {
    let headers = vec![
        (Bytes::from(":status"), Bytes::from("200")),
        (Bytes::from("server"), Bytes::from("nginx")),
    ];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(result, Ok(())));
}

#[test]
fn test_validate_settings_client_rejects_invalid_wt_enabled() {
    let parameters = ConnectionParameters {
        early_event_ttl: 5.0,
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        max_connection_pending_events: 1000,
        max_pending_capsules: 20,
        max_pending_datagrams: 100,
        max_pending_streams: 10,
        max_session_pending_events: 100,
        max_sessions: 10,
        max_write_buffer_size: 1024 * 1024,
        session_receive_window: 4 * 1024 * 1024,
    };
    let connection = Connection::new(42, None, true, parameters);

    let settings = H3Settings {
        enable_connect_protocol: Some(1),
        wt_enabled: Some(2),
        ..Default::default()
    };

    let result = validate_settings(&settings, &connection);
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_validate_settings_datagram_not_supported() {
    let settings = H3Settings {
        enable_connect_protocol: Some(1),
        h3_datagram: Some(1),
        wt_enabled: Some(1),
        ..Default::default()
    };
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let result = validate_settings(&settings, mock.as_connection());
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_validate_settings_enable_connect_protocol_invalid() {
    let settings = H3Settings {
        enable_connect_protocol: Some(0),
        wt_enabled: Some(1),
        ..Default::default()
    };
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let result = validate_settings(&settings, mock.as_connection());
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_validate_settings_missing_wt_enabled() {
    let map = H3Settings::default();
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let result = validate_settings(&map, mock.as_connection());
    assert!(matches!(
        result,
        Err(WebTransportError::Protocol(
            Some(ERR_WT_REQUIREMENTS_NOT_MET),
            _
        ))
    ));
}
