//! Unit tests for the `crate::protocol::stream` module.

use bytes::Bytes;
use rstest::*;

use super::*;
use crate::common::constants::{
    ERR_LIB_STREAM_STATE_ERROR, ERR_WT_APPLICATION_ERROR_FIRST, ERR_WT_FLOW_CONTROL_ERROR,
    WT_DATA_BLOCKED_TYPE,
};
use crate::common::types::{EventType, StreamDirection, StreamState};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::utils::wt_to_http_error;

const MAX_READ_BUF: u64 = 1024;
const MAX_WRITE_BUF: u64 = 1024;

#[fixture]
fn fixture_stream() -> Stream {
    Stream::new(
        0,
        0,
        StreamDirection::Bidirectional,
        0.0,
        MAX_READ_BUF,
        MAX_WRITE_BUF,
    )
}

#[rstest]
fn test_diagnose_snapshot_generation_success(fixture_stream: Stream) {
    let stream = fixture_stream;
    let req_id = 1;

    let effects = stream.diagnose(req_id);

    assert_eq!(effects.len(), 1);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::Diagnostics(_),
            ..
        }]
    ));

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::Diagnostics(json),
            ..
        },
    ] = effects.as_slice()
    {
        assert!(json.contains("\"stream_id\":0"));
        assert!(json.contains("\"state\":\"open\""));
    }
}

#[rstest]
fn test_diagnose_with_close_reason(mut fixture_stream: Stream) {
    fixture_stream.close_reason = Some("Application Error".to_owned());

    let effects = fixture_stream.diagnose(1);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::Diagnostics(_),
            ..
        }]
    ));

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::Diagnostics(json),
            ..
        },
    ] = effects.as_slice()
    {
        assert!(json.contains("\"close_reason\":\"Application Error\""));
    }
}

#[rstest]
fn test_flush_writes_full_drain_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let data = Bytes::from_static(b"buffered");
    fixture_stream.write(req_id, data, true, 0, 1000);

    let (effects, sent) = fixture_stream.flush_writes(100, 1000);

    assert_eq!(sent, 8);
    assert_eq!(fixture_stream.write_buffer_size, 0);
    assert_eq!(fixture_stream.state, StreamState::HalfClosedLocal);
    assert_eq!(effects.len(), 2);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                end_stream: true,
                ..
            },
            Effect::NotifyRequestDone {
                result: RequestResult::None,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_flush_writes_on_half_closed_remote_completes_stream_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::HalfClosedRemote;
    fixture_stream.write(1, Bytes::from_static(b"data"), true, 0, 1000);

    let (effects, _) = fixture_stream.flush_writes(100, 1000);

    assert_eq!(fixture_stream.state, StreamState::Closed);
    assert!(effects.len() >= 3);
    assert!(matches!(
        effects.last(),
        Some(Effect::EmitStreamEvent {
            event_type: EventType::StreamClosed,
            ..
        })
    ));
}

#[rstest]
fn test_flush_writes_partial_drain_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let data = Bytes::from_static(b"long data");
    fixture_stream.write(req_id, data, false, 0, 1000);

    let (effects, sent) = fixture_stream.flush_writes(4, 1000);

    assert_eq!(sent, 4);
    assert_eq!(fixture_stream.write_buffer_size, 5);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                end_stream: false,
                ..
            },
            Effect::SendH3Capsule { .. }
        ]
    ));
}

#[rstest]
fn test_flush_writes_varint_error(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"blocked"), false, 0, 1000);

    let (effects, _) = fixture_stream.flush_writes(0, u64::MAX);

    assert!(effects.is_empty());
}

#[rstest]
fn test_new_stream_initialization_success(fixture_stream: Stream) {
    let stream = fixture_stream;

    assert_eq!(stream.id, 0);
    assert_eq!(stream.state, StreamState::Open);
    assert_eq!(stream.read_buffer_size, 0);
    assert_eq!(stream.write_buffer_size, 0);
    assert_eq!(stream.max_read_buffer_size, MAX_READ_BUF);
}

#[rstest]
fn test_read_all_buffered_data_using_zero_size_success(mut fixture_stream: Stream) {
    fixture_stream.unread(Bytes::from_static(b"buffered"));

    let req_id = 1;
    let (effects, consumed) = fixture_stream.read(req_id, 0);

    assert_eq!(consumed, 8);
    assert_eq!(fixture_stream.read_buffer_size, 0);

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::ReadData(d),
            ..
        },
    ] = effects.as_slice()
    {
        assert_eq!(d.len(), 8);
    }
}

#[rstest]
fn test_read_immediate_from_buffer_success(mut fixture_stream: Stream) {
    fixture_stream.unread(Bytes::from_static(b"buffered"));

    let req_id = 1;
    let (effects, consumed) = fixture_stream.read(req_id, 100);

    assert_eq!(consumed, 8);
    assert_eq!(fixture_stream.read_buffer_size, 0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::ReadData(_),
            ..
        }]
    ));
}

#[rstest]
fn test_read_on_closed_stream_returns_eof_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::Closed;
    fixture_stream.close_code = Some(0);

    let req_id = 1;
    let (effects, consumed) = fixture_stream.read(req_id, 100);

    assert_eq!(consumed, 0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::ReadData(_),
            ..
        }]
    ));

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::ReadData(d),
            ..
        },
    ] = effects.as_slice()
    {
        assert!(d.is_empty());
    }
}

#[rstest]
fn test_read_on_closed_stream_with_error_fails(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::Closed;
    fixture_stream.close_code = Some(500);

    let (effects, consumed) = fixture_stream.read(1, 100);

    assert_eq!(consumed, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_read_on_half_closed_remote_returns_eof_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::HalfClosedRemote;

    let (effects, consumed) = fixture_stream.read(1, 100);

    assert_eq!(consumed, 0);
    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::ReadData(d),
            ..
        },
    ] = effects.as_slice()
    {
        assert!(d.is_empty());
    } else {
        assert!(matches!(
            effects.as_slice(),
            [Effect::NotifyRequestDone {
                result: RequestResult::ReadData(d),
                ..
            }] if d.is_empty()
        ));
    }
}

#[rstest]
fn test_read_on_reset_received_stream_fails_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::ResetReceived;

    let (effects, _) = fixture_stream.read(1, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));

    if let [Effect::NotifyRequestFailed { error_code, .. }] = effects.as_slice() {
        assert_eq!(*error_code, Some(ERR_LIB_STREAM_STATE_ERROR));
    }
}

#[rstest]
fn test_read_queues_request_when_empty_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let (effects, consumed) = fixture_stream.read(req_id, 100);

    assert_eq!(consumed, 0);
    assert_eq!(fixture_stream.pending_read_requests.len(), 1);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_data_fills_buffer_success(mut fixture_stream: Stream) {
    let data = Bytes::from_static(b"incoming");
    let len = data.len() as u64;

    let (effects, consumed) = fixture_stream.recv_data(data, false, 1.0);

    assert_eq!(consumed, 0);
    assert_eq!(fixture_stream.read_buffer_size, len);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_data_fin_on_reset_sent_transitions_to_closed(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::ResetSent;

    let (effects, _) = fixture_stream.recv_data(Bytes::new(), true, 1.0);

    assert_eq!(fixture_stream.state, StreamState::Closed);
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitStreamEvent {
            event_type: EventType::StreamClosed,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_data_fin_transitions_half_closed_local_to_closed_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::HalfClosedLocal;
    let data = Bytes::new();

    let (effects, _) = fixture_stream.recv_data(data, true, 1.0);

    assert_eq!(fixture_stream.state, StreamState::Closed);
    assert!(fixture_stream.closed_at.is_some());

    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitStreamEvent {
            event_type: EventType::StreamClosed,
            ..
        }]
    ));
}

#[rstest]
fn test_recv_data_fin_transitions_open_to_half_closed_remote_success(mut fixture_stream: Stream) {
    let data = Bytes::new();
    let (effects, _) = fixture_stream.recv_data(data, true, 1.0);

    assert_eq!(fixture_stream.state, StreamState::HalfClosedRemote);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_data_fin_with_buffered_data_keeps_stream_active_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::HalfClosedLocal;
    let data = Bytes::from_static(b"final data");

    let (effects, _) = fixture_stream.recv_data(data, true, 1.0);

    assert_eq!(fixture_stream.state, StreamState::HalfClosedRemote);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_data_fulfills_pending_read_success(mut fixture_stream: Stream) {
    let req_id = 1;
    fixture_stream.read(req_id, 10);

    let data = Bytes::from_static(b"incoming");

    let (effects, consumed) = fixture_stream.recv_data(data.clone(), false, 1.0);

    assert_eq!(consumed, 8);
    assert_eq!(fixture_stream.read_buffer_size, 0);
    assert_eq!(effects.len(), 1);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::ReadData(_),
            ..
        }]
    ));

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::ReadData(d),
            ..
        },
    ] = effects.as_slice()
    {
        assert_eq!(d, &data);
    }
}

#[rstest]
fn test_recv_data_on_closed_stream_ignored(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::Closed;

    let (effects, consumed) = fixture_stream.recv_data(Bytes::from_static(b"ignore"), false, 1.0);

    assert_eq!(consumed, 0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_data_overflow_error_success(mut fixture_stream: Stream) {
    let size = usize::try_from(MAX_READ_BUF + 1).unwrap_or(usize::MAX);
    let large_data = Bytes::from(vec![0u8; size]);

    let (effects, _) = fixture_stream.recv_data(large_data, false, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::StopQuicStream { .. }]
    ));

    if let [Effect::StopQuicStream { error_code, .. }] = effects.as_slice() {
        assert_eq!(*error_code, ERR_WT_FLOW_CONTROL_ERROR);
    }
}

#[rstest]
fn test_recv_reset_on_closed_stream_idempotency_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::Closed;
    let effects = fixture_stream.recv_reset(0x100, 1.0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_reset_remote_success(mut fixture_stream: Stream) {
    fixture_stream.read(1, 100);

    let effects = fixture_stream.recv_reset(0x100, 1.0);

    assert_eq!(fixture_stream.state, StreamState::Closed);
    assert!(fixture_stream.close_code.is_some());
    assert!(effects.len() >= 2);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }, ..]
    ));

    let last = effects.last();
    assert!(matches!(
        last,
        Some(Effect::EmitStreamEvent {
            event_type: EventType::StreamClosed,
            ..
        })
    ));
}

#[rstest]
fn test_recv_reset_unknown_error_code_success(mut fixture_stream: Stream) {
    let unknown_code = 0x1234_5678;
    let effects = fixture_stream.recv_reset(unknown_code, 1.0);

    assert_eq!(fixture_stream.state, StreamState::Closed);
    assert_eq!(fixture_stream.close_code, Some(unknown_code));

    assert!(matches!(
        effects.last(),
        Some(Effect::EmitStreamEvent {
            event_type: EventType::StreamClosed,
            ..
        })
    ));
}

#[rstest]
fn test_recv_reset_with_reserved_error_code(mut fixture_stream: Stream) {
    let effects = fixture_stream.recv_reset(ERR_WT_APPLICATION_ERROR_FIRST, 1.0);
    assert!(!effects.is_empty());
}

#[rstest]
fn test_reset_local_command_on_closed_stream_idempotency_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::Closed;
    let effects = fixture_stream.reset(1, 404, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::None,
            ..
        }]
    ));
}

#[rstest]
fn test_reset_local_command_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let error_code = 404;

    let effects = fixture_stream.reset(req_id, error_code, 1.0);

    assert_eq!(fixture_stream.state, StreamState::ResetSent);
    assert_eq!(fixture_stream.close_code, Some(404));

    assert!(matches!(
        effects.as_slice(),
        [Effect::ResetQuicStream { .. }, ..]
    ));

    if let [
        Effect::ResetQuicStream {
            error_code: code, ..
        },
        ..,
    ] = effects.as_slice()
    {
        assert_eq!(*code, wt_to_http_error(404).unwrap_or(404));
    }
}

#[rstest]
fn test_reset_on_half_closed_remote_transitions_to_closed_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::HalfClosedRemote;

    let effects = fixture_stream.reset(1, 404, 1.0);

    assert_eq!(fixture_stream.state, StreamState::Closed);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream { .. },
            Effect::EmitStreamEvent {
                event_type: EventType::StreamClosed,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_stop_local_command_on_closed_stream_idempotency_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::Closed;
    let effects = fixture_stream.stop(1, 500, 1.0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::None,
            ..
        }]
    ));
}

#[rstest]
fn test_stop_local_command_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let error_code = 500;

    let effects = fixture_stream.stop(req_id, error_code, 1.0);

    assert_eq!(fixture_stream.state, StreamState::ResetReceived);

    assert!(matches!(
        effects.as_slice(),
        [Effect::StopQuicStream { .. }, ..]
    ));

    if let [
        Effect::StopQuicStream {
            error_code: code, ..
        },
        ..,
    ] = effects.as_slice()
    {
        assert_eq!(*code, wt_to_http_error(500).unwrap_or(500));
    }
}

#[rstest]
fn test_stop_on_half_closed_local_transitions_to_closed_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::HalfClosedLocal;

    let effects = fixture_stream.stop(1, 500, 1.0);

    assert_eq!(fixture_stream.state, StreamState::Closed);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::StopQuicStream { .. },
            Effect::EmitStreamEvent {
                event_type: EventType::StreamClosed,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_take_data_exact_chunk_match(mut fixture_stream: Stream) {
    let chunk = Bytes::from(vec![0u8; 50]);
    fixture_stream.unread(chunk);

    let (effects, consumed) = fixture_stream.read(1, 50);

    assert_eq!(consumed, 50);
    assert_eq!(fixture_stream.read_buffer_size, 0);

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::ReadData(d),
            ..
        },
    ] = effects.as_slice()
    {
        assert_eq!(d.len(), 50);
    }
}

#[rstest]
fn test_take_data_multi_chunk_merge_success(mut fixture_stream: Stream) {
    fixture_stream.unread(Bytes::from_static(b"world"));
    fixture_stream.unread(Bytes::from_static(b"hello "));

    let (effects, consumed) = fixture_stream.read(1, 11);

    assert_eq!(consumed, 11);
    assert_eq!(fixture_stream.read_buffer_size, 0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::ReadData(_),
            ..
        }]
    ));

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::ReadData(d),
            ..
        },
    ] = effects.as_slice()
    {
        assert_eq!(d, &Bytes::from_static(b"hello world"));
    }
}

#[rstest]
fn test_take_data_slicing_optimization_success(mut fixture_stream: Stream) {
    let chunk = Bytes::from(vec![0u8; 100]);
    fixture_stream.unread(chunk);

    let (effects, consumed) = fixture_stream.read(1, 50);

    assert_eq!(consumed, 50);
    assert_eq!(fixture_stream.read_buffer_size, 50);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::ReadData(_),
            ..
        }]
    ));

    if let [
        Effect::NotifyRequestDone {
            result: RequestResult::ReadData(d),
            ..
        },
    ] = effects.as_slice()
    {
        assert_eq!(d.len(), 50);
    }
}

#[rstest]
fn test_write_appends_to_existing_buffer_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"chunk1"), false, 0, 1000);
    assert_eq!(fixture_stream.write_buffer.len(), 1);

    let (effects, sent) = fixture_stream.write(2, Bytes::from_static(b"chunk2"), true, 0, 1000);

    assert_eq!(sent, 0);
    assert_eq!(fixture_stream.write_buffer.len(), 2);
    assert!(effects.is_empty());
}

#[rstest]
fn test_write_buffer_full_rejects_success(mut fixture_stream: Stream) {
    let size = usize::try_from(MAX_WRITE_BUF).unwrap_or(usize::MAX);
    let data = Bytes::from(vec![0u8; size]);
    fixture_stream.write(1, data, false, 0, 1000);

    let (effects, sent) = fixture_stream.write(2, Bytes::from_static(b"1"), false, 0, 1000);

    assert_eq!(sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_write_buffer_overflow_error_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let size = usize::try_from(MAX_WRITE_BUF + 1).unwrap_or(usize::MAX);
    let large_data = Bytes::from(vec![0u8; size]);

    let (effects, sent) = fixture_stream.write(req_id, large_data, false, 0, 1000);

    assert_eq!(sent, 0);
    assert_eq!(fixture_stream.write_buffer_size, 0);
    assert_eq!(effects.len(), 1);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));

    if let [Effect::NotifyRequestFailed { error_code, .. }] = effects.as_slice() {
        assert_eq!(*error_code, Some(ERR_LIB_STREAM_STATE_ERROR));
    }
}

#[rstest]
fn test_write_empty_fin_on_closed_stream_no_op_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::HalfClosedLocal;

    let (effects, _) = fixture_stream.write(1, Bytes::new(), true, 100, 1000);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::None,
            ..
        }]
    ));
}

#[rstest]
fn test_write_empty_fin_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let data = Bytes::new();

    let (effects, _) = fixture_stream.write(req_id, data, true, 0, 1000);

    assert_eq!(fixture_stream.state, StreamState::HalfClosedLocal);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                end_stream: true,
                ..
            },
            Effect::NotifyRequestDone { .. }
        ]
    ));
}

#[rstest]
fn test_write_fin_state_transition_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let data = Bytes::new();

    let (effects, _) = fixture_stream.write(req_id, data, true, 100, 1000);

    assert_eq!(fixture_stream.state, StreamState::HalfClosedLocal);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                end_stream: true,
                ..
            },
            ..
        ]
    ));
}

#[rstest]
fn test_write_full_credit_immediate_send_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let data = Bytes::from_static(b"hello");
    let len = data.len() as u64;
    let credit = 100;

    let (effects, sent) = fixture_stream.write(req_id, data.clone(), false, credit, 1000);

    assert_eq!(sent, len);
    assert_eq!(fixture_stream.bytes_sent, len);
    assert_eq!(fixture_stream.write_buffer_size, 0);
    assert_eq!(effects.len(), 2);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::SendQuicData {
                end_stream: false,
                ..
            },
            Effect::NotifyRequestDone {
                result: RequestResult::None,
                ..
            }
        ]
    ));
}

#[rstest]
fn test_write_no_credit_full_buffering_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let data = Bytes::from_static(b"blocked");
    let len = data.len() as u64;
    let credit = 0;

    let (effects, sent) = fixture_stream.write(req_id, data, false, credit, 1000);

    assert_eq!(sent, 0);
    assert_eq!(fixture_stream.write_buffer_size, len);
    assert_eq!(effects.len(), 1);

    assert!(matches!(effects.as_slice(), [Effect::SendH3Capsule { .. }]));
}

#[rstest]
fn test_write_on_closed_stream_fails_success(mut fixture_stream: Stream) {
    fixture_stream.state = StreamState::Closed;

    let (effects, _) = fixture_stream.write(1, Bytes::from_static(b"data"), false, 100, 1000);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed { .. }]
    ));

    if let [Effect::NotifyRequestFailed { error_code, .. }] = effects.as_slice() {
        assert_eq!(*error_code, Some(ERR_LIB_STREAM_STATE_ERROR));
    }
}

#[rstest]
fn test_write_partial_credit_buffering_success(mut fixture_stream: Stream) {
    let req_id = 1;
    let data = Bytes::from_static(b"hello world");
    let credit = 5;

    let (effects, sent) = fixture_stream.write(req_id, data, false, credit, 1000);

    assert_eq!(sent, 5);
    assert_eq!(fixture_stream.bytes_sent, 5);
    assert_eq!(fixture_stream.write_buffer_size, 6);
    assert_eq!(effects.len(), 2);

    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData { .. }, Effect::SendH3Capsule { .. }]
    ));

    if let [_, Effect::SendH3Capsule { capsule_type, .. }] = effects.as_slice() {
        assert_eq!(*capsule_type, WT_DATA_BLOCKED_TYPE);
    }
}

#[rstest]
fn test_write_varint_error(mut fixture_stream: Stream) {
    let (effects, _) = fixture_stream.write(1, Bytes::from_static(b"data"), false, 0, u64::MAX);

    assert!(effects.is_empty());
}
