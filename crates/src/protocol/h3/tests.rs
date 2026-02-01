//! Unit tests for the `crate::protocol::h3` module.

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use rstest::*;

use super::*;
use crate::common::error::WebTransportError;
use crate::common::types::Headers;
use crate::protocol::connection::Connection;
use crate::protocol::events::{Effect, ProtocolEvent};

fn create_h3(is_client: bool) -> H3 {
    match H3::new(is_client, 10000, 10, 10, 2048) {
        Ok(h3) => h3,
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "H3 initialization failed");
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

#[test]
fn test_cleanup_stream_removes_partial_frame() {
    let mut h3 = create_h3(true);
    let stream_id = 4;
    h3.ensure_partial_frame(stream_id).ended = true;
    h3.cleanup_stream(stream_id);
    assert!(!h3.partial_frames.contains_key(&stream_id));
}

#[test]
fn test_encode_capsule_bidirectional_success() {
    let stream_id = 0;
    let capsule_type = 0x1234;
    let data = Bytes::from("capsule_payload");
    let res = H3::encode_capsule(stream_id, capsule_type, data.clone());
    match res {
        Ok(bytes) => assert_eq!(bytes.first(), Some(&0x00)),
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Failed to encode capsule");
        }
    }
}

#[test]
fn test_encode_capsule_unidirectional_failure() {
    let stream_id = 2;
    let res = H3::encode_capsule(stream_id, 1, Bytes::new());
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_STREAM_CREATION_ERROR), _))
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
                assert_eq!(vec.len(), 0, "Encoded datagram vector is empty");
            }
        }
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Failed to encode datagram");
        }
    }
}

#[test]
fn test_encode_datagram_wrong_stream_type_failure() {
    let stream_id = 2;
    let res = H3::encode_datagram(stream_id, Bytes::from("d"));
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_STREAM_CREATION_ERROR), _))
    ));
}

#[test]
fn test_encode_goaway_success() {
    let res = H3::encode_goaway(100);
    match res {
        Ok(bytes) => assert_eq!(bytes.first(), Some(&0x07)),
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Failed to encode GOAWAY");
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
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Failed to encode headers");
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
    assert_eq!(sends, 2);
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
        assert_eq!(effects.len(), 0, "Expected CloseQuicConnection effect");
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
    if let Some(ProtocolEvent::DatagramReceived { stream_id, data }) = events.first() {
        assert_eq!(*stream_id, 0);
        assert_eq!(data.as_ref(), b"payload");
    } else {
        assert_eq!(events.len(), 0, "Unexpected event type or empty events");
    }
}

#[test]
fn test_initialize_settings_success() {
    let mut h3 = create_h3(true);
    let res = h3.initialize_settings();
    match res {
        Ok(bytes) => assert_eq!(bytes.first(), Some(&0x04)),
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Failed to init settings");
        }
    }
}

#[test]
fn test_new_valid_config_success() {
    let res = H3::new(true, 100, 5, 5, 1000);
    if let Err(e) = res {
        let msg = format!("{e:?}");
        assert_eq!(msg, "", "H3 initialization failed");
    }
}

#[test]
fn test_parse_settings_duplicate_id_failure() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&[0x01, 0x01]);
    buf.extend_from_slice(&[0x01, 0x02]);
    let res = parse_settings(&buf.freeze());
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_reserved_id_failure() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&[0x02, 0x00]);
    let res = parse_settings(&buf.freeze());
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_SETTINGS_ERROR), _))
    ));
}

#[test]
fn test_parse_settings_valid() {
    let mut map = HashMap::new();
    map.insert(SETTINGS_QPACK_MAX_TABLE_CAPACITY, 100);

    let Ok(payload) = encode_settings(&map) else {
        return;
    };

    let parsed = parse_settings(&payload);
    match parsed {
        Ok(settings) => {
            assert_eq!(settings.get(&SETTINGS_QPACK_MAX_TABLE_CAPACITY), Some(&100));
        }
        Err(e) => {
            let msg = format!("{e:?}");
            assert_eq!(msg, "", "Parse settings failed");
        }
    }
}

#[test]
fn test_recv_capsule_too_large() {
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
    data.extend_from_slice(&[0x05]);

    data.extend_from_slice(&[0x00]);
    data.extend_from_slice(&[0x80, 0x00, 0x20, 0x00]);

    let event = ProtocolEvent::TransportStreamDataReceived {
        stream_id,
        data: data.freeze(),
        end_stream: false,
    };

    let (_, effects) = h3.handle_transport_event(&event, mock.as_connection());

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_EXCESSIVE_LOAD);
    } else {
        assert_eq!(effects.len(), 0, "Expected CloseQuicConnection effect");
    }
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
        assert_eq!(effects.len(), 0, "Expected error on closing control stream");
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

    let chunk1 = Bytes::from(vec![0x04, 0x02]);
    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: chunk1,
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(events.is_empty());

    let chunk2 = Bytes::from(vec![0x01, 0x01]);
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

    assert!(effects.is_empty());
    assert!(h3.partial_frames.contains_key(&stream_id));
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
            data: Bytes::from(vec![0x04, 0x00]),
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
            .any(|e| matches!(e, ProtocolEvent::GoawayReceived))
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
            data: Bytes::from(vec![0x04, 0x00]),
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
        assert_eq!(effects.len(), 0, "Expected error on headers in control");
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
            data: Bytes::from(vec![0x04, 0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

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
        assert_eq!(effects.len(), 0, "Expected error on duplicate settings");
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

    if let Some(Effect::CloseQuicConnection { error_code, .. }) = effects.first() {
        assert_eq!(*error_code, ERR_H3_FRAME_UNEXPECTED);
    } else {
        assert_eq!(effects.len(), 0, "Expected error on double headers");
    }
}

#[test]
fn test_recv_headers_blocked_by_qpack_then_unblocked() {
    let mut server = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    server.settings_received = true;

    let mut client = create_h3(true);
    if let Err(e) = client.set_local_stream_ids(2, 6, 10) {
        let msg = format!("{e:?}");
        assert_eq!(msg, "", "Failed to set client stream IDs");
    }

    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (
            Bytes::from(":path"),
            Bytes::from("/dynamic/path/to/force/indexing"),
        ),
        (
            Bytes::from("custom-header"),
            Bytes::from("custom-value-that-should-be-indexed"),
        ),
    ];

    let Ok(effects) = client.encode_headers(stream_id, &headers, false) else {
        return;
    };

    let mut encoder_data = BytesMut::new();
    let mut frame_data = BytesMut::new();

    for eff in effects {
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

    let mut preamble = BytesMut::new();
    preamble.put_u8(0x02);

    server.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 6,
            data: preamble.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    server.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: frame_data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    server.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id: 6,
            data: encoder_data.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );
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
        let msg = format!("{e:?}");
        assert_eq!(msg, "", "Set stream IDs failed");
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
        assert_eq!(frame_data.len(), 0, "No frame data generated");
    } else {
        let (events, _) = h3.handle_transport_event(
            &ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data: frame_data.freeze(),
                end_stream: false,
            },
            mock.as_connection(),
        );

        assert!(!events.is_empty(), "No events generated");
        assert!(matches!(
            events.first(),
            Some(ProtocolEvent::HeadersReceived { .. })
        ));
    }
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
        assert_eq!(effects.len(), 0, "Expected CloseQuicConnection effect");
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

    let settings_frame = Bytes::from(vec![0x04, 0x00]);
    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: settings_frame,
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(h3.is_settings_received());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ProtocolEvent::SettingsReceived { .. }))
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

    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::LogH3Frame { event, .. } if event == "stream_type_set"))
    );
}

#[test]
fn test_recv_uni_stream_data_wt_stream_parsing() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    let mut payload = BytesMut::new();
    payload.extend_from_slice(&[0x54]);
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

    assert_eq!(events.len(), 0);
}

#[test]
fn test_recv_uni_stream_data_wt_stream_unknown_session_ignored() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    let mut payload = BytesMut::new();
    payload.extend_from_slice(&[0x54]);
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

    assert_eq!(events.len(), 0);
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
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::LogH3Frame { event, .. } if event == "stream_type_set"))
    );
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
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::LogH3Frame { .. }))
    );
}

#[test]
fn test_recv_wt_capsule_fragmented() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;

    let mut p = PartialFrameInfo::new(stream_id);
    p.headers_processed = true;
    p.is_webtransport_control = true;
    h3.partial_frames.insert(stream_id, p);

    let mut chunk1 = BytesMut::new();
    chunk1.extend_from_slice(&[0x00]);
    chunk1.extend_from_slice(&[0x05]);
    chunk1.extend_from_slice(&[0x00]);

    let (_, effects1) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: chunk1.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(
        !effects1
            .iter()
            .any(|e| matches!(e, Effect::CloseQuicConnection { .. }))
    );

    let mut chunk2 = BytesMut::new();
    chunk2.extend_from_slice(&[0x03]);
    chunk2.extend_from_slice(b"ABC");

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: chunk2.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert_eq!(events.len(), 1);
    if let Some(ProtocolEvent::CapsuleReceived { capsule_data, .. }) = events.first() {
        assert_eq!(capsule_data.as_ref(), b"ABC");
    }
}

#[test]
fn test_recv_wt_control_stream_capsules() {
    let mut h3 = create_h3(false);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 0;
    h3.settings_received = true;

    let p = h3.ensure_partial_frame(stream_id);
    p.headers_processed = true;
    p.is_webtransport_control = true;

    let mut buf = BytesMut::new();
    buf.put_u8(0x00);
    buf.put_u8(0x05);
    buf.put_u8(0x00);
    buf.put_u8(0x03);
    buf.put_slice(b"ABC");

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: buf.freeze(),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(!events.is_empty());
    if let Some(ProtocolEvent::CapsuleReceived {
        capsule_type,
        capsule_data,
        ..
    }) = events.first()
    {
        assert_eq!(*capsule_type, 0x00);
        assert_eq!(capsule_data.as_ref(), b"ABC");
    } else {
        assert_eq!(events.len(), 0, "Expected CapsuleReceived event");
    }
}

#[test]
fn test_recv_wt_stream_preamble_fragmented() {
    let mut h3 = create_h3(true);
    let mock = MockConnectionLayout {
        _padding: [0; 1024],
    };
    let stream_id = 7;

    h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x54]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(events.is_empty());

    let (events_final, effects) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from_static(b"data"),
            end_stream: false,
        },
        mock.as_connection(),
    );

    if events_final.is_empty() {
        assert!(effects.is_empty());
    } else {
        assert_eq!(events_final.len(), 1);
    }
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
            data: Bytes::from(vec![0x54]),
            end_stream: false,
        },
        mock.as_connection(),
    );
    assert!(effects.is_empty());

    let (events, _) = h3.handle_transport_event(
        &ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data: Bytes::from(vec![0x00]),
            end_stream: false,
        },
        mock.as_connection(),
    );

    assert!(events.is_empty());
}

#[test]
fn test_set_local_stream_ids_invalid_id_failure() {
    let mut h3 = create_h3(true);
    let res = h3.set_local_stream_ids(0, 6, 10);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_ID_ERROR), _))
    ));
}

#[test]
fn test_set_local_stream_ids_success() {
    let mut h3 = create_h3(true);
    let res = h3.set_local_stream_ids(2, 6, 10);
    if let Err(e) = res {
        let msg = format!("{e:?}");
        assert_eq!(msg, "", "Failed to set local stream IDs");
    }
    assert_eq!(h3.local_control_stream_id(), Some(2));
}

#[rstest]
#[case(b"", false)]
#[case(b"Name", false)]
#[case(b"name", true)]
#[case(b"name:", false)]
fn test_validate_header_name_cases(#[case] name: &[u8], #[case] is_valid: bool) {
    let res = validate_header_name(name);
    assert_eq!(res.is_ok(), is_valid);
}

#[test]
fn test_validate_header_name_chars() {
    assert!(matches!(validate_header_name(b"valid-name"), Ok(())));
    assert!(validate_header_name(b"invalid@name").is_err());
    assert!(validate_header_name(b"invalid name").is_err());
    assert!(validate_header_name(b"invalid\tname").is_err());
    assert!(validate_header_name(b"invalid\rname").is_err());
    assert!(validate_header_name(b"invalid\nname").is_err());
}

#[rstest]
#[case::colon_start(b":status", b"200", true)]
#[case::uppercase(b"Content-Type", b"text/plain", false)]
#[case::colon_mid(b"na:me", b"val", false)]
#[case::valid(b"name", b"value", true)]
#[case::invalid_char_val(b"name", b"val\x7F", false)]
#[case::invalid_char_name(b"name ", b"value", false)]
fn test_validate_header_syntax_cases(#[case] k: &[u8], #[case] v: &[u8], #[case] valid: bool) {
    let res_n = validate_header_name(k);
    let res_v = validate_header_value(v);

    let is_valid = res_n.is_ok() && res_v.is_ok();
    assert_eq!(
        is_valid,
        valid,
        "Failed for {:?} : {:?}",
        String::from_utf8_lossy(k),
        String::from_utf8_lossy(v)
    );
}

#[rstest]
#[case(b" value", false)]
#[case(b"val\tue", true)]
#[case(b"val\nue", false)]
#[case(b"value", true)]
#[case(b"value ", false)]
fn test_validate_header_value_cases(#[case] val: &[u8], #[case] is_valid: bool) {
    let res = validate_header_value(val);
    assert_eq!(res.is_ok(), is_valid);
}

#[test]
fn test_validate_request_headers_duplicate_pseudo() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from(":method"), Bytes::from("POST")),
    ];
    let res = validate_request_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_missing_pseudo_failure() {
    let headers = vec![(Bytes::from(":method"), Bytes::from("GET"))];
    let res = validate_request_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_pseudo_after_regular_failure() {
    let headers = vec![
        (Bytes::from(":method"), Bytes::from("GET")),
        (Bytes::from("custom"), Bytes::from("val")),
        (Bytes::from(":scheme"), Bytes::from("https")),
    ];
    let res = validate_request_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_pseudo_order() {
    let headers = vec![
        (Bytes::from("custom"), Bytes::from("value")),
        (Bytes::from(":method"), Bytes::from("GET")),
    ];
    let res = validate_request_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_unknown_pseudo() {
    let headers = vec![(Bytes::from(":unknown"), Bytes::from("val"))];
    let res = validate_request_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_request_headers_unknown_pseudo_failure() {
    let headers = vec![(Bytes::from(":unknown"), Bytes::from("foo"))];
    let res = validate_request_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_duplicate_status() {
    let headers = vec![
        (Bytes::from(":status"), Bytes::from("200")),
        (Bytes::from(":status"), Bytes::from("404")),
    ];
    let res = validate_response_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_missing_status() {
    let headers = vec![(Bytes::from("server"), Bytes::from("nginx"))];
    let res = validate_response_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}

#[test]
fn test_validate_response_headers_missing_status_failure() {
    let headers = vec![(Bytes::from("server"), Bytes::from("me"))];
    let res = validate_response_headers(&headers);
    assert!(matches!(
        res,
        Err(WebTransportError::H3(Some(ERR_H3_MESSAGE_ERROR), _))
    ));
}
