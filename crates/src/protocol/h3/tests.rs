//! Unit tests for the `crate::protocol::h3` module.

use std::ptr;

use bytes::{BufMut, Bytes, BytesMut};
use rstest::rstest;

use super::*;
use crate::common::constants::{
    ERR_H3_CLOSED_CRITICAL_STREAM, ERR_H3_DATAGRAM_ERROR, ERR_H3_EXCESSIVE_LOAD,
    ERR_H3_FRAME_ERROR, ERR_H3_FRAME_UNEXPECTED, ERR_H3_GENERAL_PROTOCOL_ERROR, ERR_H3_ID_ERROR,
    ERR_H3_INTERNAL_ERROR, ERR_H3_MESSAGE_ERROR, ERR_H3_MISSING_SETTINGS, ERR_H3_REQUEST_CANCELLED,
    ERR_H3_SETTINGS_ERROR, ERR_H3_STREAM_CREATION_ERROR, ERR_QPACK_DECODER_STREAM_ERROR,
    ERR_QPACK_DECOMPRESSION_FAILED, ERR_QPACK_ENCODER_STREAM_ERROR, ERR_WT_REQUIREMENTS_NOT_MET,
    ERR_WT_SESSION_GONE, H3_FRAME_TYPE_DATA, H3_FRAME_TYPE_HEADERS,
    H3_FRAME_TYPE_RESERVED_PRIORITY, H3_FRAME_TYPE_SETTINGS, H3_FRAME_TYPE_WT_STREAM,
    WT_UPGRADE_TOKEN,
};
use crate::common::types::{Headers, StreamId};
use crate::protocol::connection::{Connection, ConnectionParameters};
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::qpack::Encoder;
use crate::protocol::utils;

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
    let Some(header) = encode_frame_header(H3_FRAME_TYPE_HEADERS, header_block.len()) else {
        assert_eq!("some", "none");
        unreachable!()
    };

    let mut frame = BytesMut::with_capacity(header.len() + header_block.len());
    frame.put(header);
    frame.put_slice(header_block);
    frame.freeze()
}

fn encode_header_block(headers: &Headers) -> Vec<u8> {
    let mut encoder = Encoder::new();

    match encoder.encode(0, headers) {
        Ok(header_block) => header_block,
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

    let Some(payload) = encode_settings(&settings) else {
        assert_eq!("some", "none");
        unreachable!()
    };

    let Some(header) = encode_frame_header(H3_FRAME_TYPE_SETTINGS, payload.len()) else {
        assert_eq!("some", "none");
        unreachable!()
    };

    let mut frame = BytesMut::with_capacity(header.len() + payload.len());
    frame.put(header);
    frame.put(payload);
    frame.freeze()
}

#[test]
fn test_abort_request_stream_resets_and_stops() {
    let effects = H3::abort_request_stream(0);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_CANCELLED,
            },
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_CANCELLED,
            }
        ]
    ));
}

#[test]
fn test_bind_connect_stream_after_goaway_rejected() {
    let mut h3 = create_h3(true);
    h3.peer_goaway_identifier = Some(0);

    let (events, effects) = h3.bind_connect_stream(0);

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3ConnectStreamRejected { stream_id: 0 }]
    ));
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&0));
}

#[rstest]
#[case::below_goaway(Some(4))]
#[case::no_goaway(None)]
fn test_bind_connect_stream_registers_stream(#[case] peer_goaway_identifier: Option<u64>) {
    let mut h3 = create_h3(true);
    h3.peer_goaway_identifier = peer_goaway_identifier;

    let (events, effects) = h3.bind_connect_stream(0);

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: 1,
        }]
    ));
    assert!(
        h3.streams
            .get(&0)
            .is_some_and(|stream| stream.is_connect_stream && stream.is_request_stream)
    );
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
    assert_eq!(h3.local_control_stream_id, None);
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
    assert_eq!(h3.local_control_stream_id, Some(2));
}

#[test]
fn test_cleanup_stream_removes_h3_stream() {
    let mut h3 = create_h3(true);
    let stream_id = 4;
    let stream = H3Stream::default();
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
fn test_encode_capsule_invalid_type_failure() {
    let effects = H3::encode_capsule(0, u64::MAX, Bytes::new(), false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_INTERNAL_ERROR,
            reason: Some(reason),
        }] if reason == "wt_capsule encode failed"
    ));
}

#[test]
fn test_encode_capsule_success() {
    let effects = H3::encode_capsule(0, 0x1234, Bytes::from("capsule_payload"), true);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                stream_id: 0,
                data: frame_header,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: 0,
                data: capsule_header,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: 0,
                data: capsule_value,
                end_stream: true,
            },
        ] if frame_header.as_ref() == [0x00, 0x12]
            && capsule_header.as_ref() == [0x52, 0x34, 0x0F]
            && capsule_value.as_ref() == b"capsule_payload"
    ));
}

#[test]
fn test_encode_datagram_success() {
    let data = Bytes::from("datagram");

    let effects = H3::encode_datagram(4, data.clone());

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicDatagram { header, payload }]
            if header.as_ref() == [0x01] && *payload == data
    ));
}

#[test]
fn test_encode_goaway_invalid_identifier_failure() {
    let mut h3 = create_h3(true);
    h3.bind_local_streams(2, 6, 10);

    let effects = h3.encode_goaway(u64::MAX);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_INTERNAL_ERROR,
            reason: Some(reason),
        }] if reason == "h3_goaway encode failed"
    ));
}

#[test]
fn test_encode_goaway_no_control_stream() {
    let h3 = create_h3(true);

    let effects = h3.encode_goaway(0);

    assert!(effects.is_empty());
}

#[test]
fn test_encode_goaway_success() {
    let mut h3 = create_h3(true);
    h3.bind_local_streams(2, 6, 10);

    let effects = h3.encode_goaway(100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            stream_id: 2,
            data,
            end_stream: false,
        }] if data.as_ref() == [0x07, 0x02, 0x40, 0x64]
    ));
}

#[test]
fn test_encode_headers_field_too_long_failure() {
    let mut h3 = create_h3(true);
    let headers = vec![(Bytes::from("x-large"), Bytes::from(vec![b'a'; 70000]))];

    let effects = h3.encode_headers(0, &headers, false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_INTERNAL_ERROR,
            reason: Some(reason),
        }] if reason == "qpack_encoder encode failed"
    ));
}

#[test]
fn test_encode_headers_success() {
    let mut h3 = create_h3(true);
    let headers = valid_request_headers();

    let effects = h3.encode_headers(0, &headers, true);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                stream_id: 0,
                data: frame_header,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: 0,
                end_stream: true,
                ..
            },
        ] if frame_header.first() == Some(&0x01)
    ));
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

    assert!(result.is_none());
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

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_INTERNAL_ERROR,
            reason: Some(reason),
        }] if reason == "wt_stream encode failed"
    ));
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

#[rstest]
#[case::limit_exceeded(&[0xD0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])]
#[case::varint_maximum(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])]
fn test_handle_transport_event_datagram_session_id_exceeded(#[case] data: &'static [u8]) {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let event = ProtocolEvent::TransportDatagramReceived {
        data: Bytes::from_static(data),
    };

    let (events, effects) = h3.handle_transport_event(&event, mock.as_connection());

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_DATAGRAM_ERROR,
            reason: Some(reason),
        }] if reason == "wt_datagram validate exceeded"
    ));
}

#[rstest]
#[case::first_session(&[0x01], 4)]
#[case::last_session(&[0xCF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 4_611_686_018_427_387_900)]
fn test_handle_transport_event_datagram_success(
    #[case] header: &'static [u8],
    #[case] expected_session_id: u64,
) {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let mut data = BytesMut::new();
    data.extend_from_slice(header);
    data.extend_from_slice(b"payload");

    let event = ProtocolEvent::TransportDatagramReceived {
        data: data.freeze(),
    };

    let (events, _) = h3.handle_transport_event(&event, mock.as_connection());

    assert_eq!(events.len(), 1);
    if let Some(ProtocolEvent::H3DatagramReceived { session_id, data }) = events.first() {
        assert_eq!(*session_id, expected_session_id);
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
    let mut events = Vec::new();

    let result = h3.parse_capsules(0, &mut stream, &mut events);

    assert!(matches!(result, Ok(())));
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

    let Some(payload) = encode_settings(&settings) else {
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
        Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            ..
        })
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
        Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            ..
        })
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
        Err(H3Error::Connection {
            error_code: ERR_H3_EXCESSIVE_LOAD,
            ..
        })
    ));
}

#[test]
fn test_parse_settings_reserved_id_failure() {
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(&[0x02, 0x00]);
    let result = parse_settings(&buffer.freeze());
    assert!(matches!(
        result,
        Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            ..
        })
    ));
}

#[test]
fn test_parse_settings_truncated_id_failure() {
    let buffer = Bytes::from_static(&[0x40]);

    let result = parse_settings(&buffer);

    assert!(matches!(
        result,
        Err(H3Error::Connection {
            error_code: ERR_H3_FRAME_ERROR,
            ..
        })
    ));
}

#[test]
fn test_parse_settings_truncated_value_failure() {
    let buffer = Bytes::from_static(&[0x01, 0x40]);

    let result = parse_settings(&buffer);

    assert!(matches!(
        result,
        Err(H3Error::Connection {
            error_code: ERR_H3_FRAME_ERROR,
            ..
        })
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

    let Some(payload) = encode_settings(&settings) else {
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

#[rstest]
#[case::missing(&[])]
#[case::trailing(&[0x00, 0x00])]
fn test_parse_varint_payload_invalid_layout_failure(#[case] data: &'static [u8]) {
    let result = parse_varint_payload(data);

    assert!(matches!(
        result,
        Err(H3Error::Connection {
            error_code: ERR_H3_FRAME_ERROR,
            ..
        })
    ));
}

#[test]
fn test_receive_capsule_too_large() {
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
    let mut events = Vec::new();

    let result = h3.parse_capsules(stream_id, &mut stream, &mut events);
    assert!(matches!(
        result,
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
    ));
    assert!(events.is_empty());
}

#[test]
fn test_receive_close_control_stream_error() {
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
fn test_receive_close_qpack_stream_error() {
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
fn test_receive_connect_stream_capsule_fragmented() {
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
    let mut events = Vec::new();

    let result1 = h3.parse_capsules(stream_id, &mut stream, &mut events);
    assert!(matches!(result1, Ok(())));
    assert!(events.is_empty());

    let mut chunk2 = BytesMut::new();
    chunk2.extend_from_slice(b"ABC");

    stream.capsule_buffer.extend_from_slice(&chunk2);

    let result2 = h3.parse_capsules(stream_id, &mut stream, &mut events);

    assert!(matches!(result2, Ok(())));
    assert_eq!(events.len(), 1);
    if let Some(ProtocolEvent::H3CapsuleReceived { capsule_value, .. }) = events.first() {
        assert_eq!(capsule_value.as_ref(), b"ABC");
    } else {
        assert_eq!(events.len(), 0);
    }
}

#[test]
fn test_receive_connect_stream_capsule_split_across_data_chunks() {
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
fn test_receive_connect_stream_capsules() {
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
    let mut events = Vec::new();

    let result = h3.parse_capsules(stream_id, &mut stream, &mut events);

    assert!(matches!(result, Ok(())));
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
fn test_receive_connect_stream_capsules_via_data_frame() {
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
fn test_receive_connect_stream_data_before_headers_fails() {
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
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_UNEXPECTED,
            ..
        }]
    ));
}

#[test]
fn test_receive_control_frame_fragmented() {
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
fn test_receive_control_frame_incomplete_type_buffered() {
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
fn test_receive_control_goaway_rejects_pending_connect_streams() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;
    h3.bind_connect_stream(0);
    h3.bind_connect_stream(4);
    h3.bind_connect_stream(8);
    h3.streams.insert(
        12,
        H3Stream {
            headers_processed: true,
            is_connect_stream: true,
            is_request_stream: true,
            ..Default::default()
        },
    );
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
            data: Bytes::from(vec![0x07, 0x01, 0x08, 0x07, 0x01, 0x04]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [
            ProtocolEvent::H3GoawayReceived,
            ProtocolEvent::H3ConnectStreamRejected { stream_id: 8 },
            ProtocolEvent::H3GoawayReceived,
            ProtocolEvent::H3ConnectStreamRejected { stream_id: 4 }
        ]
    ));
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 3,
            max_size: u64::MAX,
        }]
    ));
    assert!(h3.streams.contains_key(&0));
    assert!(!h3.streams.contains_key(&4));
    assert!(!h3.streams.contains_key(&8));
    assert!(h3.streams.contains_key(&12));
    assert_eq!(h3.peer_goaway_identifier, Some(4));
}

#[test]
fn test_receive_control_goaway_success() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 2;
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
            data: Bytes::from(vec![0x07, 0x01, 0x01]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3GoawayReceived]
    ));
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 2,
            max_size: u64::MAX,
        }]
    ));
    assert_eq!(h3.peer_goaway_identifier, Some(1));
}

#[rstest]
#[case::bidirectional_server(&[0x07, 0x01, 0x01], ERR_H3_ID_ERROR, 0)]
#[case::increased(&[0x07, 0x01, 0x00, 0x07, 0x01, 0x04], ERR_H3_ID_ERROR, 1)]
#[case::trailing(&[0x07, 0x02, 0x00, 0x00], ERR_H3_FRAME_ERROR, 0)]
#[case::unidirectional(&[0x07, 0x01, 0x02], ERR_H3_ID_ERROR, 0)]
fn test_receive_control_goaway_validation_fails(
    #[case] data: &'static [u8],
    #[case] expected_error_code: u64,
    #[case] expected_event_count: usize,
) {
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
            data: Bytes::from_static(data),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert_eq!(events.len(), expected_event_count);
    assert!(
        events
            .iter()
            .all(|event| matches!(event, ProtocolEvent::H3GoawayReceived))
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection { error_code, .. }] if *error_code == expected_error_code
    ));
}

#[test]
fn test_receive_control_headers_fails() {
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
            data: Bytes::from(vec![0x01, 0x80, 0x01, 0x00, 0x00]),
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
fn test_receive_control_large_unknown_frame_streamed() {
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
    data.put_u8(0x21);
    assert!(matches!(utils::write_varint(&mut data, 5000), Some(())));
    data.put_slice(&[0xFF; 3]);

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
            stream_id: 3,
            max_size: u64::MAX,
        }]
    ));
    assert!(
        h3.streams.get(&stream_id).is_some_and(
            |stream| stream.partial_frame == Some((0x21, 4997)) && stream.buffer.is_empty()
        )
    );
}

#[test]
fn test_receive_control_reserved_frame_fails() {
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
        utils::write_varint(&mut data, H3_FRAME_TYPE_RESERVED_PRIORITY),
        Some(())
    ));
    assert!(matches!(utils::write_varint(&mut data, 0), Some(())));

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
fn test_receive_control_settings_too_large() {
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
    buffer.put_slice(&[0x44, 0x01]);

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
    assert_eq!(*error_code, ERR_H3_EXCESSIVE_LOAD);
}

#[test]
fn test_receive_control_settings_twice_fails() {
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
fn test_receive_control_unknown_frame_ignored() {
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
fn test_receive_control_wt_stream_frame_fails() {
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
        Some(())
    ));

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
fn test_receive_data_on_non_connect_stream_ignored() {
    let mut h3 = create_h3(false);
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
fn test_receive_double_headers_error() {
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
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_UNEXPECTED,
            ..
        }]
    ));
    assert!(!h3.streams.contains_key(&stream_id));
}

#[rstest]
#[case::connect_stream(true)]
#[case::request_stream(false)]
fn test_receive_empty_data_frame_ignored(#[case] is_connect_stream: bool) {
    let mut h3 = create_h3(false);
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
fn test_receive_headers_dynamic_table_fails_with_zero_capacity() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;

    let mut data = BytesMut::new();
    data.put_u8(0x01);
    assert!(matches!(utils::write_varint(&mut data, 3), Some(())));
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
fn test_receive_headers_field_section_too_large_fails() {
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
    let headers = vec![(Bytes::from("accept"), Bytes::from("*/*")); 1599];

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: create_headers_frame(&encode_header_block(&headers)),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
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
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_receive_headers_field_too_long_fails() {
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
    let mut header_block = vec![0x00, 0x00, 0x21, b'a', 0xFF, 0xE7, 0xD4, 0x02];
    header_block.resize(header_block.len() + 43_750, 0x00);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: create_headers_frame(&header_block),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 0,
                error_code: ERR_QPACK_DECOMPRESSION_FAILED,
            },
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_QPACK_DECOMPRESSION_FAILED,
            }
        ]
    ));
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_receive_malformed_control_frame() {
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

#[rstest]
#[case::canonical(WT_UPGRADE_TOKEN)]
#[case::mixed_case(b"WebTransport-H3")]
fn test_receive_request_data_connect_headers_marks_connect_stream(#[case] protocol: &'static [u8]) {
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
        (Bytes::from(":protocol"), Bytes::from_static(protocol)),
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
fn test_receive_request_data_connect_stream_fin_closes() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream = H3Stream {
        headers_processed: true,
        is_connect_stream: true,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(0, stream);
    let mut events = Vec::new();

    let effects = match h3.receive_request_data(0, &[], &mut events, true, mock.as_connection()) {
        Ok(effects) => effects,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3ConnectStreamClosed {
            stream_id: 0,
            error_code: None,
        }]
    ));
    assert!(effects.is_empty());
    assert!(!h3.streams.contains_key(&0));
}

#[rstest]
#[case::missing_response(&[], false)]
#[case::truncated_capsule(&[0x00, 0x02, 0x00, 0x05], true)]
fn test_receive_request_data_connect_stream_incomplete_fin_fails(
    #[case] data: &'static [u8],
    #[case] headers_processed: bool,
) {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let stream = H3Stream {
        headers_processed,
        is_connect_stream: true,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(data),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        events.as_slice(),
        [ProtocolEvent::H3ConnectStreamClosed {
            stream_id: 0,
            error_code: Some(ERR_H3_MESSAGE_ERROR),
        }]
    ));
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
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_receive_request_data_empty_stream_fin_cleanup() {
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
fn test_receive_request_data_fin_cleans_up_request_stream() {
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
fn test_receive_request_data_headers_frame_flow() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;

    let headers = valid_request_headers();
    let mut sender_h3 = create_h3(true);
    sender_h3.bind_local_streams(2, 6, 10);

    let encoded_effects = sender_h3.encode_headers(stream_id, &headers, false);

    let mut frame_data = BytesMut::new();

    for effect in encoded_effects {
        if let Effect::SendQuicData { data, .. } = effect {
            frame_data.extend_from_slice(&data);
        }
    }

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
fn test_receive_request_data_headers_too_large() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;
    h3.bind_connect_stream(stream_id);

    let mut data = BytesMut::new();
    data.put_u8(0x01);
    assert!(matches!(utils::write_varint(&mut data, 65537), Some(())));

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
fn test_receive_request_data_incomplete_frame_buffered() {
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
            data: Bytes::from(vec![0x01]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: 1,
        }]
    ));
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.is_request_stream && stream.buffer.as_ref() == [0x01])
    );
}

#[test]
fn test_receive_request_data_large_unknown_frame_streamed() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let mut data = BytesMut::new();
    data.put_u8(0x21);
    assert!(matches!(utils::write_varint(&mut data, 5000), Some(())));
    data.put_slice(&[0xFF; 3]);

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
            max_size: 4997,
        }]
    ));
    assert!(
        h3.streams.get(&stream_id).is_some_and(
            |stream| stream.partial_frame == Some((0x21, 4997)) && stream.buffer.is_empty()
        )
    );
}

#[test]
fn test_receive_request_data_partial_headers_frame_buffered() {
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
            max_size: 7,
        }]
    ));
    assert!(
        h3.streams.get(&stream_id).is_some_and(
            |stream| stream.partial_frame == Some((0x01, 10)) && stream.buffer.len() == 3
        )
    );
}

#[rstest]
#[case::missing_push_id(&[0x05, 0x00], ERR_H3_FRAME_ERROR)]
#[case::truncated_push_id(&[0x05, 0x01, 0x40, 0x00], ERR_H3_FRAME_ERROR)]
#[case::unadvertised_push_id(&[0x05, 0x01, 0x00], ERR_H3_ID_ERROR)]
fn test_receive_request_data_push_promise_frame_fails(
    #[case] data: &'static [u8],
    #[case] expected_error_code: u64,
) {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(data),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection { error_code, .. }] if *error_code == expected_error_code
    ));
}

#[test]
fn test_receive_request_data_push_promise_frame_partial_push_id_buffered() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x05, 0x02, 0x40]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: 1,
        }]
    ));
    assert!(h3.streams.get(&stream_id).is_some_and(|stream| {
        stream.partial_frame.is_none() && stream.buffer.as_ref() == [0x05, 0x02, 0x40]
    }));

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
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_ID_ERROR,
            ..
        }]
    ));
}

#[rstest]
#[case::client_after_response(true, true)]
#[case::server(false, false)]
fn test_receive_request_data_push_promise_frame_unexpected_fails(
    #[case] headers_processed: bool,
    #[case] is_client: bool,
) {
    let mut h3 = create_h3(is_client);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let stream = H3Stream {
        headers_processed,
        is_connect_stream: headers_processed,
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(stream_id, stream);

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x05, 0x01, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_UNEXPECTED,
            ..
        }]
    ));
}

#[rstest]
#[case::partial_frame(&[0x00, 0x05, 0x00, 0x03, b'A'])]
#[case::partial_header(&[0x40])]
fn test_receive_request_data_truncated_frame_fin_fails(#[case] data: &'static [u8]) {
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

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(data),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection {
            error_code: ERR_H3_FRAME_ERROR,
            ..
        }]
    ));
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_receive_request_data_unknown_frame_ignored() {
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
            max_size: 1,
        }]
    ));
}

#[test]
fn test_receive_request_data_webtransport_stream_header_parsed() {
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
fn test_receive_request_data_webtransport_stream_invalid_session_fails() {
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
fn test_receive_request_data_wt_stream_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x40, 0x41]),
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
fn test_receive_request_data_zero_length_frame_before_headers() {
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
    let headers = valid_request_headers();
    let mut data = BytesMut::new();
    data.put_slice(&[0x21, 0x00]);
    data.put(create_headers_frame(&encode_header_block(&headers)));

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
        [ProtocolEvent::H3HeadersReceived {
            stream_id: 0,
            headers: received_headers,
        }] if *received_headers == headers
    ));
    assert!(effects.is_empty());
}

#[test]
fn test_receive_request_reserved_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);

    let mut data = BytesMut::new();
    assert!(matches!(
        utils::write_varint(&mut data, H3_FRAME_TYPE_RESERVED_PRIORITY),
        Some(())
    ));
    assert!(matches!(utils::write_varint(&mut data, 5000), Some(())));

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
fn test_receive_request_settings_frame_fails() {
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
fn test_receive_response_headers_interim_before_final() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);
    let interim_headers = vec![(Bytes::from(":status"), Bytes::from("103"))];
    let headers = vec![(Bytes::from(":status"), Bytes::from("200"))];

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: create_headers_frame(&encode_header_block(&interim_headers)),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: 1,
        }]
    ));
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| !stream.headers_processed)
    );

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
fn test_receive_response_headers_non_2xx_aborts_stream() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.bind_connect_stream(stream_id);
    let headers = vec![(Bytes::from(":status"), Bytes::from("403"))];

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
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_CANCELLED,
            },
            Effect::StopQuicStream {
                stream_id: 0,
                error_code: ERR_H3_REQUEST_CANCELLED,
            }
        ]
    ));
    assert!(!h3.streams.contains_key(&stream_id));
}

#[test]
fn test_receive_response_headers_on_connect_stream() {
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
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.is_connect_stream && stream.headers_processed)
    );
}

#[test]
fn test_receive_unidirectional_stream_cancel_push_frame_fails() {
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
    data.put_u8(0x01);
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
fn test_receive_unidirectional_stream_data_control_data_frame_fails() {
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
        Some(())
    ));
    assert!(matches!(utils::write_varint(&mut data, 0), Some(())));

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

#[rstest]
#[case::data(&[0x00, 0x00])]
#[case::goaway(&[0x07, 0x01, 0x00])]
fn test_receive_unidirectional_stream_data_control_missing_settings_failure(
    #[case] frame: &'static [u8],
) {
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
            data: Bytes::from_static(frame),
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
fn test_receive_unidirectional_stream_data_control_settings_success() {
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
fn test_receive_unidirectional_stream_data_identifies_control_stream() {
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
fn test_receive_unidirectional_stream_data_webtransport_stream_invalid_session_fails() {
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
fn test_receive_unidirectional_stream_duplicate_control_failure() {
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
fn test_receive_unidirectional_stream_empty_fin_cleanup() {
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
fn test_receive_unidirectional_stream_max_push_id_frame_accepted() {
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

    let (events, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 2,
            data: Bytes::from(vec![0x0D, 0x01, 0x05, 0x0D, 0x01, 0x05]),
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
    assert_eq!(h3.peer_max_push_id, Some(5));
}

#[test]
fn test_receive_unidirectional_stream_max_push_id_frame_client_fails() {
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

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: Bytes::from(vec![0x0D, 0x01, 0x00]),
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
fn test_receive_unidirectional_stream_max_push_id_frame_reduced_fails() {
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

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 2,
            data: Bytes::from(vec![0x0D, 0x01, 0x05, 0x0D, 0x01, 0x04]),
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
fn test_receive_unidirectional_stream_max_push_id_frame_too_large() {
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

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 2,
            data: Bytes::from(vec![0x0D, 0x09]),
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

#[rstest]
#[case::client(ERR_H3_ID_ERROR, true, 7)]
#[case::server(ERR_H3_STREAM_CREATION_ERROR, false, 6)]
fn test_receive_unidirectional_stream_push_rejected(
    #[case] expected_error_code: u64,
    #[case] is_client: bool,
    #[case] stream_id: StreamId,
) {
    let mut h3 = create_h3(is_client);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x01]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::CloseQuicConnection { error_code, .. }] if *error_code == expected_error_code
    ));
}

#[test]
fn test_receive_unidirectional_stream_qpack_decoder_feed() {
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
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.buffer.is_empty())
    );
}

#[test]
fn test_receive_unidirectional_stream_qpack_decoder_malformed_instruction_fails() {
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
fn test_receive_unidirectional_stream_qpack_encoder_feed() {
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
            data: Bytes::from(vec![0x20]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 11,
            max_size: u64::MAX,
        }]
    ));
    assert_eq!(h3.peer_encoder_stream_id, Some(stream_id));
    assert!(
        h3.streams
            .get(&stream_id)
            .is_some_and(|stream| stream.buffer.is_empty())
    );
}

#[test]
fn test_receive_unidirectional_stream_qpack_encoder_malformed_instruction_fails() {
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
fn test_receive_unidirectional_stream_unknown_type_rejected() {
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
fn test_receive_webtransport_stream_preamble_fragmented() {
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
fn test_receive_webtransport_unidirectional_stream_missing_session_id_buffer() {
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
fn test_receive_webtransport_unidirectional_stream_missing_session_id_fin_cleanup() {
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

#[test]
fn test_resume_request_stream_not_found() {
    let h3 = create_h3(false);

    let effects = h3.resume_request_stream(0);

    assert!(effects.is_empty());
}

#[test]
fn test_resume_request_stream_success() {
    let mut h3 = create_h3(false);
    let stream = H3Stream {
        is_request_stream: true,
        ..Default::default()
    };
    h3.streams.insert(0, stream);

    let effects = h3.resume_request_stream(0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: u64::MAX,
        }]
    ));
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
#[case::asterisk_with_get(b"GET", b"*", false)]
#[case::asterisk_with_options(b"OPTIONS", b"*", true)]
#[case::inner_space(b"GET", b"/a b", false)]
#[case::non_ascii(b"GET", b"/caf\xe9", false)]
#[case::relative(b"GET", b"a", false)]
fn test_validate_request_headers_http_path(
    #[case] method: &'static [u8],
    #[case] path: &'static [u8],
    #[case] is_valid: bool,
) {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from_static(method)),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
        (Bytes::from(":path"), Bytes::from_static(path)),
    ];
    let result = validate_request_headers(0, &headers);
    assert_eq!(result.is_ok(), is_valid);
}

#[rstest]
#[case::http(b"http")]
#[case::uppercase_https(b"HTTPS")]
fn test_validate_request_headers_http_scheme_without_authority_failure(
    #[case] scheme: &'static [u8],
) {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":scheme"), Bytes::from_static(scheme)),
        (Bytes::from(":path"), Bytes::from("/")),
    ];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
    ));
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
    ));
}

#[test]
fn test_validate_request_headers_missing_pseudo_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("GET"))];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
    ));
}

#[test]
fn test_validate_request_headers_plain_connect_missing_authority_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("CONNECT"))];
    let result = validate_request_headers(0, &headers);
    assert!(matches!(
        result,
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
    ));
}

#[test]
fn test_validate_response_headers_invalid_pseudo_name_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("GET"))];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(
        result,
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
    ));
}

#[rstest]
#[case::non_digit(b"2xx")]
#[case::too_long(b"2000")]
#[case::too_short(b"20")]
fn test_validate_response_headers_invalid_status_failure(#[case] status: &'static [u8]) {
    let headers = vec![(Bytes::from(":status"), Bytes::from_static(status))];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(
        result,
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
    ));
}

#[test]
fn test_validate_response_headers_missing_status() {
    let headers = vec![(Bytes::from("server"), Bytes::from("nginx"))];
    let result = validate_response_headers(0, &headers);
    assert!(matches!(
        result,
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        Err(H3Error::Stream {
            stream_id: 0,
            error_code: ERR_H3_MESSAGE_ERROR,
        })
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
        max_connection_early_events: 1000,
        max_pending_capsules: 20,
        max_pending_datagrams: 100,
        max_pending_streams: 10,
        max_session_early_events: 100,
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
        Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            ..
        })
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
        Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            ..
        })
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
        Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            ..
        })
    ));
}

#[test]
fn test_validate_settings_missing_wt_enabled() {
    let settings = H3Settings::default();
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let result = validate_settings(&settings, mock.as_connection());
    assert!(matches!(
        result,
        Err(H3Error::Connection {
            error_code: ERR_WT_REQUIREMENTS_NOT_MET,
            ..
        })
    ));
}
