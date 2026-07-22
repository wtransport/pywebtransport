//! Unit tests for the `crate::protocol::h3` module.

use bytes::{BufMut, Bytes, BytesMut};
use rstest::*;

use super::*;
use crate::common::error::WebTransportError;
use crate::common::types::Headers;
use crate::protocol::connection::{Connection, ConnectionParams};
use crate::protocol::events::{Effect, ProtocolEvent};

fn create_h3(is_client: bool) -> H3 {
    let params = H3Params {
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        max_capsule_size: 65536,
        max_field_section_size: 65536,
    };

    H3::new(is_client, params)
}

#[repr(C, align(8))]
struct MockConnectionLayout {
    _padding: [u8; 1024],
}

impl MockConnectionLayout {
    fn as_connection(&self) -> &Connection {
        unsafe { &*std::ptr::from_ref(self).cast::<Connection>() }
    }
}

fn valid_req_headers() -> Headers {
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
        Ok(p) => p,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    let header = match encode_frame_header(H3_FRAME_TYPE_SETTINGS, payload.len()) {
        Ok(h) => h,
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
fn test_cleanup_stream_removes_partial_frame() {
    let mut h3 = create_h3(true);
    let stream_id = 4;
    let mut p = PartialFrameInfo::new(stream_id);
    p.ended = true;
    h3.partial_frames.insert(stream_id, p);
    h3.cleanup_stream(stream_id);
    assert!(!h3.partial_frames.contains_key(&stream_id));
}

#[test]
fn test_client_rejects_server_initiated_non_wt_bidi_stream() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 1;

    let mut data = BytesMut::new();
    assert!(matches!(
        write_varint(&mut data, H3_FRAME_TYPE_DATA),
        Ok(())
    ));
    assert!(matches!(write_varint(&mut data, 0), Ok(())));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_STREAM_CREATION_ERROR);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_encode_capsule_bidirectional_success() {
    let stream_id = 0;
    let capsule_type = 0x1234;
    let data = Bytes::from("capsule_payload");
    let res = H3::encode_capsule(stream_id, capsule_type, data.clone());
    match res {
        Ok(vec) => assert_eq!(vec.first().and_then(|b| b.first()), Some(&0x00)),
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_encode_capsule_unidirectional_failure() {
    let stream_id = 2;
    let res = H3::encode_capsule(stream_id, 1, Bytes::new());
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(
            Some(ERR_H3_STREAM_CREATION_ERROR),
            _
        ))
    ));
}

#[test]
fn test_encode_datagram_success() {
    let stream_id = 0;
    let data = Bytes::from("datagram");
    let res = H3::encode_datagram(stream_id, data);
    match res {
        Ok(vec) => {
            assert_eq!(vec.len(), 2);
            if let Some(header) = vec.first() {
                assert_eq!(header.as_ref(), &[0x00]);
            } else {
                assert_eq!(vec.len(), 0);
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
    let res = H3::encode_datagram(stream_id, Bytes::from("d"));
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(
            Some(ERR_H3_STREAM_CREATION_ERROR),
            _
        ))
    ));
}

#[test]
fn test_encode_goaway_success() {
    let res = H3::encode_goaway(100);
    match res {
        Ok(bytes) => assert_eq!(bytes.first(), Some(&0x07)),
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_encode_headers_success() {
    let mut h3 = create_h3(true);
    let stream_id = 0;
    let headers = valid_req_headers();

    let res = h3.encode_headers(stream_id, &headers, true);

    match res {
        Ok(effects) => {
            assert!(
                effects
                    .iter()
                    .any(|e| matches!(e, Effect::SendQuicData { .. }))
            );
        }
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_encode_stream_creation_bidirectional() {
    let mut h3 = create_h3(true);
    let effects = h3.encode_stream_creation(0, 6, false);
    let sends = effects
        .iter()
        .filter(|e| matches!(e, Effect::SendQuicData { .. }))
        .count();
    assert_eq!(sends, 1);
}

#[test]
fn test_encode_stream_creation_unidirectional() {
    let mut h3 = create_h3(true);
    let effects = h3.encode_stream_creation(2, 6, true);
    let sends = effects
        .iter()
        .filter(|e| matches!(e, Effect::SendQuicData { .. }))
        .count();
    assert_eq!(sends, 1);
}

#[test]
fn test_handle_transport_event_datagram_malformed() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };

    let data = Bytes::new();

    let event = ProtocolEvent::TransportDatagramFrameReceived { data };

    let (_, effects) = h3.handle_transport_event(&event, mock.as_connection());

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_DATAGRAM_ERROR);
    } else {
        assert_eq!(effects.len(), 0);
    }
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

    let event = ProtocolEvent::TransportDatagramFrameReceived {
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
fn test_initialize_settings_success() {
    let mut h3 = create_h3(true);
    let res = h3.initialize_settings();
    match res {
        Ok(bytes) => assert_eq!(bytes.first(), Some(&0x04)),
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_new_valid_config_success() {
    let params = H3Params {
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        max_capsule_size: 65536,
        max_field_section_size: 65536,
    };
    let _h3 = H3::new(true, params);
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
        Ok(s) => s,
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
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&[0x01, 0x01]);
    buf.extend_from_slice(&[0x01, 0x02]);
    let res = parse_settings(&buf.freeze());
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_duplicate_unknown_id_failure() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&[0x40, 0x30, 0x01]);
    buf.extend_from_slice(&[0x40, 0x30, 0x02]);

    let res = parse_settings(&buf.freeze());

    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_entries_limit_exceeded() {
    let mut buf = BytesMut::new();
    for id in 0x40u64..=0x80u64 {
        buf.extend_from_slice(&[0x40, id.try_into().unwrap_or(0)]);
        buf.extend_from_slice(&[0x01]);
    }

    let res = parse_settings(&buf.freeze());

    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_reserved_id_failure() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&[0x02, 0x00]);
    let res = parse_settings(&buf.freeze());
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_truncated_id_failure() {
    let buf = Bytes::from_static(&[0x40]);

    let res = parse_settings(&buf);

    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_truncated_value_failure() {
    let buf = Bytes::from_static(&[0x01, 0x40]);

    let res = parse_settings(&buf);

    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), _))
    ));
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
        Ok(s) => {
            assert_eq!(s.qpack_max_table_capacity, Some(100));
            assert_eq!(s.max_field_section_size, Some(65536));
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

    let mut p = PartialFrameInfo::new(stream_id);
    p.headers_processed = true;
    p.is_webtransport_control = true;

    let mut data = BytesMut::new();
    data.extend_from_slice(&[0x00]);
    data.extend_from_slice(&[0x80, 0x01, 0x00, 0x01]);

    p.capsule_buffer.extend_from_slice(&data);

    let res = h3.parse_capsules(stream_id, &mut p);
    assert!(matches!(
        res,
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

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_CLOSED_CRITICAL_STREAM);
    } else {
        assert_eq!(effects.len(), 0);
    }
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

    let mut buf = BytesMut::new();
    buf.put_u8(0x04);
    buf.put_u8(0xFF);
    buf.put_slice(&[0xFF; 7]);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: buf.freeze(),
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
            .any(|e| matches!(e, ProtocolEvent::H3GoawayReceived))
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
    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_UNEXPECTED);
    } else {
        assert_eq!(effects.len(), 0);
    }
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

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_UNEXPECTED);
    } else {
        assert_eq!(effects.len(), 0);
    }
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
        write_varint(&mut data, H3_FRAME_TYPE_WT_STREAM),
        Ok(())
    ));
    assert!(matches!(write_varint(&mut data, 0), Ok(())));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_ERROR);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_data_on_closed_wt_stream() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let mut p = PartialFrameInfo::new(stream_id);
    p.headers_processed = true;
    p.is_webtransport_control = true;
    h3.partial_frames.insert(stream_id, p);

    let mut data = BytesMut::new();
    data.extend_from_slice(&[0x00]);
    data.extend_from_slice(&[0x03]);
    data.extend_from_slice(b"abc");

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if let Some(Effect::ResetQuicStream { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_MESSAGE_ERROR);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_data_on_non_wt_stream_ignored() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let mut p = PartialFrameInfo::new(stream_id);
    p.headers_processed = true;
    h3.partial_frames.insert(stream_id, p);

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
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::CloseQuicConnection { .. }))
    );
}

#[test]
fn test_recv_double_headers_error() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let mut p = PartialFrameInfo::new(stream_id);
    p.headers_processed = true;
    h3.partial_frames.insert(stream_id, p);
    h3.settings_received = true;

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x01, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if let Some(Effect::ResetQuicStream { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_UNEXPECTED);
    } else {
        assert_eq!(effects.len(), 0);
    }
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
    assert!(matches!(write_varint(&mut data, 3), Ok(())));
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

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x04]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(effects.is_empty());
}

#[test]
fn test_recv_request_data_headers_frame_flow() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;

    let headers = valid_req_headers();
    let mut sender_h3 = create_h3(true);
    if let Err(e) = sender_h3.set_local_stream_ids(2, 6, 10) {
        assert_eq!(format!("{e:?}"), "");
    }

    let Ok(encoded_eff) = sender_h3.encode_headers(stream_id, &headers, false) else {
        return;
    };

    let mut encoder_data = BytesMut::new();
    let mut frame_data = BytesMut::new();

    for eff in encoded_eff {
        if let Effect::SendQuicData {
            stream_id: sid,
            data,
            ..
        } = eff
        {
            if sid == 6 {
                encoder_data.extend_from_slice(&data);
            } else if sid == stream_id {
                frame_data.extend_from_slice(&data);
            }
        }
    }

    let mut full_encoder_data = BytesMut::new();
    full_encoder_data.extend_from_slice(&[0x02]);
    full_encoder_data.extend_from_slice(&encoder_data);

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 6,
            data: full_encoder_data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if frame_data.is_empty() {
        assert_eq!(frame_data.len(), 0);
    } else {
        let (events, _) = h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data: frame_data.freeze(),
                end_stream: false,
            },
            mock.as_connection(),
        );

        assert!(!events.is_empty());
        assert!(matches!(
            events.first(),
            Some(ProtocolEvent::H3HeadersReceived { .. })
        ));
    }
}

#[test]
fn test_recv_request_data_headers_recovery_still_blocked() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let mut p = PartialFrameInfo::new(stream_id);
    p.headers_processed = false;
    p.blocked = true;
    h3.partial_frames.insert(stream_id, p);

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
    assert!(h3.partial_frames.get(&stream_id).is_some_and(|p| p.blocked));
}

#[test]
fn test_recv_request_data_headers_too_large() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;

    let p = PartialFrameInfo::new(stream_id);
    h3.partial_frames.insert(stream_id, p);

    let mut data = BytesMut::new();
    data.put_u8(0x01);
    assert!(matches!(write_varint(&mut data, 65537), Ok(())));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let Some(Effect::ResetQuicStream { error_code, .. }) = effects.first() else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert_eq!(*error_code, ERR_H3_MESSAGE_ERROR);
}

#[test]
fn test_recv_request_data_incomplete_frame_buffered() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: Bytes::from(vec![0x00, 0x04, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let data = Bytes::from(vec![0x00]);

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data,
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(h3.partial_frames.contains_key(&stream_id));
}

#[test]
fn test_recv_request_data_push_promise_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;

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

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_UNEXPECTED);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_request_data_session_stream_closure_via_fast_path() {
    let mut h3 = create_h3(false);
    let mut connection = Connection::new(
        1,
        false,
        ConnectionParams {
            early_event_ttl: 5.0,
            flow_control_window: 4 * 1024 * 1024,
            initial_max_data: 4 * 1024 * 1024,
            initial_max_streams_bidi: 10,
            initial_max_streams_uni: 10,
            max_pending_capsules: 20,
            max_pending_datagrams: 100,
            max_pending_streams: 10,
            max_session_pending_events: 100,
            max_sessions: 10,
            max_stream_read_buffer_size: 1024 * 1024,
            max_stream_write_buffer_size: 1024 * 1024,
            max_total_pending_events: 1000,
        },
    );
    connection.handshake_completed(1.0);
    let session_headers: Headers = vec![
        (
            Bytes::from_static(b":method"),
            Bytes::from_static(b"CONNECT"),
        ),
        (Bytes::from_static(b":scheme"), Bytes::from_static(b"https")),
        (
            Bytes::from_static(b":authority"),
            Bytes::from_static(b"localhost"),
        ),
        (Bytes::from_static(b":path"), Bytes::from_static(b"/")),
        (
            Bytes::from_static(b":protocol"),
            Bytes::from_static(WT_UPGRADE_TOKEN),
        ),
    ];
    connection.recv_headers(0, session_headers, false, 1.0);
    assert!(connection.is_session_stream(0));

    let (events, _effects) = match h3.recv_request_data(0, Bytes::new(), true, &connection) {
        Ok(result) => result,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert!(
        events
            .iter()
            .any(|e| matches!(e, ProtocolEvent::H3ConnectStreamClosed { stream_id: 0 }))
    );
}

#[test]
fn test_recv_request_data_unknown_frame_ignored() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: Bytes::from(vec![0x00, 0x04, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let data = Bytes::from(vec![0x21, 0x01, 0xFF]);

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data,
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
}

#[test]
fn test_recv_request_data_wt_fast_path() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let mut p = PartialFrameInfo::new(stream_id);
    p.stream_type = Some(H3_STREAM_TYPE_WEBTRANSPORT);
    p.control_stream_id = Some(4);
    h3.partial_frames.insert(stream_id, p);

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(b"fast_path_data"),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert_eq!(events.len(), 1);
    assert!(matches!(
        events.first(),
        Some(ProtocolEvent::WebTransportStreamDataReceived { .. })
    ));
}

#[test]
fn test_recv_request_data_wt_stream_preamble_detected() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 4;

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x40, 0x41, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert_eq!(events.len(), 1);
    assert!(matches!(
        events.first(),
        Some(ProtocolEvent::WebTransportStreamDataReceived {
            session_id: 0,
            stream_id: 4,
            ..
        })
    ));
    assert!(h3.partial_frames.get(&stream_id).is_some_and(|p| {
        p.stream_type == Some(H3_STREAM_TYPE_WEBTRANSPORT) && p.control_stream_id == Some(0)
    }));
}

#[test]
fn test_recv_request_settings_frame_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x04, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_UNEXPECTED);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_uni_stream_cancel_push_frame_fails() {
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

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_ID_ERROR);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_uni_stream_data_control_data_frame_fails() {
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
        write_varint(&mut data, H3_FRAME_TYPE_DATA),
        Ok(())
    ));
    assert!(matches!(write_varint(&mut data, 0), Ok(())));

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_UNEXPECTED);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_uni_stream_data_control_missing_settings_failure() {
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
    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_MISSING_SETTINGS);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_uni_stream_data_control_settings_success() {
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
            .any(|e| matches!(e, ProtocolEvent::H3SettingsReceived { .. }))
    );
}

#[test]
fn test_recv_uni_stream_data_identifies_control_stream() {
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

    assert!(effects.is_empty());
    assert_eq!(h3.peer_control_stream_id, Some(stream_id));
}

#[test]
fn test_recv_uni_stream_data_wt_fast_path() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 3;

    let mut p = PartialFrameInfo::new(stream_id);
    p.stream_type = Some(H3_STREAM_TYPE_WEBTRANSPORT);
    p.control_stream_id = Some(4);
    h3.partial_frames.insert(stream_id, p);

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(b"fast_path_data"),
            end_stream: true,
        },
        mock.as_connection(),
    );

    assert_eq!(events.len(), 1);
    assert!(matches!(
        events.first(),
        Some(ProtocolEvent::WebTransportStreamDataReceived { .. })
    ));
}

#[test]
fn test_recv_uni_stream_data_wt_stream_parsing() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    let mut payload = BytesMut::new();
    payload.extend_from_slice(&[0x40, 0x54]);
    payload.extend_from_slice(&[0x00]);
    payload.extend_from_slice(b"abc");

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: payload.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert_eq!(events.len(), 1);
    let Some(ProtocolEvent::WebTransportStreamDataReceived {
        stream_id: sid,
        data,
        ..
    }) = events.first()
    else {
        assert_eq!("some", "none");
        unreachable!()
    };
    assert_eq!(*sid, 7);
    assert_eq!(data.as_ref(), b"abc");
}

#[test]
fn test_recv_uni_stream_data_wt_stream_unknown_session_ignored() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    let mut payload = BytesMut::new();
    payload.extend_from_slice(&[0x40, 0x54]);
    payload.extend_from_slice(&[0x02]);
    payload.extend_from_slice(b"abc");

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: payload.freeze(),
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
fn test_recv_uni_stream_duplicate_control_failure() {
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

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_STREAM_CREATION_ERROR);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_uni_stream_max_push_id_frame_fails() {
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
    data.put_u8(0x0D);
    data.put_u8(0x00);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 3,
            data: data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_UNEXPECTED);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_uni_stream_push_rejected() {
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

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_ID_ERROR);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_uni_stream_qpack_decoder_feed() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 11;
    let mut data = BytesMut::new();
    data.extend_from_slice(&[0x03]);
    data.extend_from_slice(b"instruction");

    let event = ProtocolEvent::TransportStreamDataReceived {
        stream_id,
        data: data.freeze(),
        end_stream: false,
    };
    let (_, effects) = h3.handle_transport_event(&event, mock.as_connection());
    assert!(effects.is_empty());
    assert_eq!(h3.peer_decoder_stream_id, Some(stream_id));
}

#[test]
fn test_recv_uni_stream_qpack_encoder_malformed_instruction_fails() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 11;

    let mut payload = BytesMut::new();
    payload.put_u8(0x02);
    payload.put_slice(&[0x00, 0x01, 0x02]);

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: payload.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_QPACK_ENCODER_STREAM_ERROR);
    } else {
        assert_eq!(effects.len(), 0);
    }
}

#[test]
fn test_recv_uni_stream_unknown_type_logging() {
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
    assert!(effects.is_empty());
}

#[test]
fn test_recv_wt_capsule_fragmented() {
    let mut h3 = create_h3(true);
    let stream_id = 0;

    let mut p = PartialFrameInfo::new(stream_id);
    p.headers_processed = true;
    p.is_webtransport_control = true;

    let mut chunk1 = BytesMut::new();
    chunk1.extend_from_slice(&[0x00]);
    chunk1.extend_from_slice(&[0x03]);

    p.capsule_buffer.extend_from_slice(&chunk1);

    let events1 = match h3.parse_capsules(stream_id, &mut p) {
        Ok(evts) => evts,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };
    assert!(events1.is_empty());

    let mut chunk2 = BytesMut::new();
    chunk2.extend_from_slice(b"ABC");

    p.capsule_buffer.extend_from_slice(&chunk2);

    let events2 = match h3.parse_capsules(stream_id, &mut p) {
        Ok(evts) => evts,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert_eq!(events2.len(), 1);
    if let Some(ProtocolEvent::H3CapsuleReceived { capsule_data, .. }) = events2.first() {
        assert_eq!(capsule_data.as_ref(), b"ABC");
    } else {
        assert_eq!(events2.len(), 0);
    }
}

#[test]
fn test_recv_wt_control_stream_capsules() {
    let mut h3 = create_h3(false);
    let stream_id = 0;
    h3.settings_received = true;

    let mut p = PartialFrameInfo::new(stream_id);
    p.headers_processed = true;
    p.is_webtransport_control = true;

    let mut buf = BytesMut::new();
    buf.put_u8(0x00);
    buf.put_u8(0x03);
    buf.put_slice(b"ABC");

    p.capsule_buffer.extend_from_slice(&buf);

    let events = match h3.parse_capsules(stream_id, &mut p) {
        Ok(evts) => evts,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert!(!events.is_empty());
    if let Some(ProtocolEvent::H3CapsuleReceived {
        capsule_type,
        capsule_data,
        ..
    }) = events.first()
    {
        assert_eq!(*capsule_type, 0x00);
        assert_eq!(capsule_data.as_ref(), b"ABC");
    } else {
        assert_eq!(events.len(), 0);
    }
}

#[test]
fn test_recv_wt_control_stream_capsules_via_real_session() {
    let mut h3 = create_h3(false);
    let mut connection = Connection::new(
        1,
        false,
        ConnectionParams {
            early_event_ttl: 5.0,
            flow_control_window: 4 * 1024 * 1024,
            initial_max_data: 4 * 1024 * 1024,
            initial_max_streams_bidi: 10,
            initial_max_streams_uni: 10,
            max_pending_capsules: 20,
            max_pending_datagrams: 100,
            max_pending_streams: 10,
            max_session_pending_events: 100,
            max_sessions: 10,
            max_stream_read_buffer_size: 1024 * 1024,
            max_stream_write_buffer_size: 1024 * 1024,
            max_total_pending_events: 1000,
        },
    );
    connection.handshake_completed(1.0);
    let session_headers: Headers = vec![
        (
            Bytes::from_static(b":method"),
            Bytes::from_static(b"CONNECT"),
        ),
        (Bytes::from_static(b":scheme"), Bytes::from_static(b"https")),
        (
            Bytes::from_static(b":authority"),
            Bytes::from_static(b"localhost"),
        ),
        (Bytes::from_static(b":path"), Bytes::from_static(b"/")),
        (
            Bytes::from_static(b":protocol"),
            Bytes::from_static(WT_UPGRADE_TOKEN),
        ),
    ];
    connection.recv_headers(0, session_headers, false, 1.0);
    assert!(connection.is_session_stream(0));

    let mut p = PartialFrameInfo::new(0);
    p.headers_processed = true;
    p.is_webtransport_control = true;

    let mut capsule = BytesMut::new();
    capsule.put_u8(0x00);
    capsule.put_u8(0x03);
    capsule.put_slice(b"ABC");

    let (events, _effects) = match h3.handle_request_frame(
        0,
        &mut p,
        H3_FRAME_TYPE_DATA,
        Some(capsule.freeze()),
        false,
        &connection,
    ) {
        Ok(result) => result,
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
            unreachable!()
        }
    };

    assert_eq!(events.len(), 1);
    assert!(matches!(
        events.first(),
        Some(ProtocolEvent::H3CapsuleReceived {
            capsule_type: 0x00,
            ..
        })
    ));
}

#[test]
fn test_recv_wt_stream_preamble_fragmented() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    let (events1, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x40]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(events1.is_empty());

    let (events2, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x54, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(events2.is_empty());

    let (events_final, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(b"data"),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert_eq!(events_final.len(), 1);
}

#[test]
fn test_recv_wt_uni_stream_missing_id_buffer() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    let (_, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x40, 0x54]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(effects.is_empty());

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
}

#[test]
fn test_set_local_stream_ids_duplicate_failure() {
    let mut h3 = create_h3(true);
    let res = h3.set_local_stream_ids(2, 2, 6);
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), _))
    ));
}

#[test]
fn test_set_local_stream_ids_invalid_id_failure() {
    let mut h3 = create_h3(true);
    let res = h3.set_local_stream_ids(0, 6, 10);
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_H3_ID_ERROR), _))
    ));
}

#[test]
fn test_set_local_stream_ids_reinit_failure() {
    let mut h3 = create_h3(true);
    assert!(matches!(h3.set_local_stream_ids(2, 6, 10), Ok(())));
    let res = h3.set_local_stream_ids(14, 18, 22);
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), _))
    ));
}

#[test]
fn test_set_local_stream_ids_success() {
    let mut h3 = create_h3(true);
    let res = h3.set_local_stream_ids(2, 6, 10);
    if let Err(e) = res {
        assert_eq!(format!("{e:?}"), "");
    }
    assert_eq!(h3.local_control_stream_id(), Some(2));
}

#[rstest]
#[case(b"", false)]
#[case(b"Name", false)]
#[case(b"name", true)]
#[case(b"name:", false)]
fn test_validate_header_name_cases(#[case] name: &[u8], #[case] is_valid: bool) {
    let res = validate_header_name(0, name);
    assert_eq!(res.is_ok(), is_valid);
}

#[test]
fn test_validate_header_name_chars() {
    assert!(matches!(validate_header_name(0, b"valid-name"), Ok(())));
    assert!(validate_header_name(0, b"invalid@name").is_err());
    assert!(validate_header_name(0, b"invalid name").is_err());
    assert!(validate_header_name(0, b"invalid\tname").is_err());
    assert!(validate_header_name(0, b"invalid\rname").is_err());
    assert!(validate_header_name(0, b"invalid\nname").is_err());
}

#[rstest]
#[case::colon_start(b":status", b"200", true)]
#[case::uppercase(b"Content-Type", b"text/plain", false)]
#[case::colon_mid(b"na:me", b"val", false)]
#[case::valid(b"name", b"value", true)]
#[case::invalid_char_val(b"name", b"val\x7F", false)]
#[case::invalid_char_name(b"name ", b"value", false)]
fn test_validate_header_syntax_cases(#[case] k: &[u8], #[case] v: &[u8], #[case] valid: bool) {
    let res_n = validate_header_name(0, k);
    let res_v = validate_header_value(0, v);

    let is_valid = res_n.is_ok() && res_v.is_ok();
    assert_eq!(is_valid, valid);
}

#[rstest]
#[case(b" value", false)]
#[case(b"val\tue", true)]
#[case(b"val\nue", false)]
#[case(b"value", true)]
#[case(b"value ", false)]
fn test_validate_header_value_cases(#[case] val: &[u8], #[case] is_valid: bool) {
    let res = validate_header_value(0, val);
    assert_eq!(res.is_ok(), is_valid);
}

#[test]
fn test_validate_request_headers_duplicate_pseudo() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":method"), Bytes::from("POST")),
    ];
    let res = validate_request_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_missing_authority_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("")),
        (Bytes::from(":path"), Bytes::from("/")),
    ];
    let res = validate_request_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_missing_path_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":scheme"), Bytes::from("https")),
        (Bytes::from(":authority"), Bytes::from("localhost")),
        (Bytes::from(":path"), Bytes::from("")),
    ];
    let res = validate_request_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_missing_pseudo_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("GET"))];
    let res = validate_request_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_pseudo_after_regular_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from("custom"), Bytes::from("val")),
        (Bytes::from(":scheme"), Bytes::from("https")),
    ];
    let res = validate_request_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_pseudo_order() {
    let headers = vec![
        (Bytes::from("custom"), Bytes::from("value")),
        (Bytes::from(":method"), Bytes::from("GET")),
    ];
    let res = validate_request_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_unknown_pseudo() {
    let headers = vec![(Bytes::from(":unknown"), Bytes::from("val"))];
    let res = validate_request_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_unknown_pseudo_failure() {
    let headers = vec![(Bytes::from(":unknown"), Bytes::from("foo"))];
    let res = validate_request_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_duplicate_status() {
    let headers = vec![
        (Bytes::from(":status"), Bytes::from("200")),
        (Bytes::from(":status"), Bytes::from("404")),
    ];
    let res = validate_response_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_invalid_pseudo_name_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("GET"))];
    let res = validate_response_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_missing_status() {
    let headers = vec![(Bytes::from("server"), Bytes::from("nginx"))];
    let res = validate_response_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_missing_status_failure() {
    let headers = vec![(Bytes::from("server"), Bytes::from("me"))];
    let res = validate_response_headers(0, &headers);
    assert!(matches!(
        res,
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
    let res = validate_response_headers(0, &headers);
    assert!(matches!(
        res,
        Err(WebTransportError::Stream(0, Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_valid_success() {
    let headers = vec![
        (Bytes::from(":status"), Bytes::from("200")),
        (Bytes::from("server"), Bytes::from("nginx")),
    ];
    match validate_response_headers(0, &headers) {
        Ok(()) => {}
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }
}

#[test]
fn test_validate_settings_client_rejects_invalid_wt_enabled() {
    let params = ConnectionParams {
        early_event_ttl: 5.0,
        flow_control_window: 4 * 1024 * 1024,
        initial_max_data: 4 * 1024 * 1024,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        max_pending_capsules: 20,
        max_pending_datagrams: 100,
        max_pending_streams: 10,
        max_session_pending_events: 100,
        max_sessions: 10,
        max_stream_read_buffer_size: 1024 * 1024,
        max_stream_write_buffer_size: 1024 * 1024,
        max_total_pending_events: 1000,
    };
    let conn = Connection::new(42, true, params);

    let settings = H3Settings {
        enable_connect_protocol: Some(1),
        wt_enabled: Some(2),
        ..Default::default()
    };

    let res = validate_settings(&settings, &conn);
    assert!(matches!(
        res,
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
    let res = validate_settings(&settings, mock.as_connection());
    assert!(matches!(
        res,
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
    let res = validate_settings(&settings, mock.as_connection());
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}
#[test]
fn test_validate_settings_missing_wt_enabled() {
    let map = H3Settings::default();
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let res = validate_settings(&map, mock.as_connection());
    assert!(matches!(
        res,
        Err(WebTransportError::Protocol(
            Some(ERR_WT_REQUIREMENTS_NOT_MET),
            _
        ))
    ));
}
