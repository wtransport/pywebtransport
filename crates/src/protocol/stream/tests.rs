//! Unit tests for the `crate::protocol::stream` module.

use bytes::Bytes;
use rstest::{fixture, rstest};

use super::*;
use crate::common::constants::{ERR_LIB_STREAM_STATE_ERROR, ERR_WT_APPLICATION_ERROR_FIRST};
use crate::common::types::{
    ErrorSource, EventType, StreamDirection, StreamReceiveState, StreamSendState,
};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::utils;

fn create_stream(direction: StreamDirection) -> Stream {
    Stream::new(
        0,
        0,
        direction,
        false,
        StreamParameters {
            max_write_buffer_size: 1024 * 1024,
        },
        0.0,
    )
}

#[fixture]
fn fixture_stream() -> Stream {
    create_stream(StreamDirection::Bidirectional)
}

#[rstest]
fn test_abort_bidirectional_success(mut fixture_stream: Stream) {
    let effects = fixture_stream.abort(500);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: 500,
                ..
            },
            Effect::StopQuicStream {
                error_code: 500,
                ..
            }
        ]
    ));
    assert_eq!(fixture_stream.send_state, Some(StreamSendState::ResetSent));
    assert_eq!(fixture_stream.send_error_code, Some(500));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::ResetRead)
    );
    assert_eq!(fixture_stream.receive_error_code, Some(500));
    assert!(fixture_stream.is_closed());
}

#[rstest]
fn test_abort_fails_pending_requests_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"data"), false, 0);
    fixture_stream.read(2, 100);

    let effects = fixture_stream.abort(500);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream { .. },
            Effect::NotifyRequestFailed {
                request_id: 1,
                source: ErrorSource::Stream,
                error_code: Some(500),
                ..
            },
            Effect::StopQuicStream { .. },
            Effect::NotifyRequestFailed {
                request_id: 2,
                source: ErrorSource::Stream,
                error_code: Some(500),
                ..
            }
        ]
    ));
    assert_eq!(fixture_stream.write_buffer_size, 0);
    assert!(fixture_stream.write_buffer.is_empty());
    assert!(fixture_stream.pending_read_requests.is_empty());
}

#[rstest]
fn test_abort_on_terminated_stream_idempotency_success(mut fixture_stream: Stream) {
    fixture_stream.abort(500);

    let effects = fixture_stream.abort(600);

    assert!(effects.is_empty());
    assert_eq!(fixture_stream.send_error_code, Some(500));
    assert_eq!(fixture_stream.receive_error_code, Some(500));
}

#[rstest]
fn test_abort_receive_only_success() {
    let mut stream = create_stream(StreamDirection::ReceiveOnly);

    let effects = stream.abort(500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::StopQuicStream {
            error_code: 500,
            ..
        }]
    ));
    assert!(stream.is_closed());
}

#[rstest]
fn test_abort_send_only_success() {
    let mut stream = create_stream(StreamDirection::SendOnly);

    let effects = stream.abort(500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ResetQuicStream {
            error_code: 500,
            ..
        }]
    ));
    assert!(stream.is_closed());
}

#[rstest]
fn test_complete_write_continues_partial_request_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"long data"), false, 4);

    let (effects, sent) = fixture_stream.complete_write(100);

    assert_eq!(sent, 5);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            data,
            end_stream: false,
            ..
        }] if data.as_ref() == b" data"
    ));
    assert_eq!(fixture_stream.pending_write_request, Some(1));
    assert_eq!(fixture_stream.write_buffer_size, 0);
}

#[rstest]
fn test_complete_write_flushes_next_request_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"first"), false, 100);
    fixture_stream.write(2, Bytes::from_static(b"second"), false, 100);

    let (effects, sent) = fixture_stream.complete_write(100);

    assert_eq!(sent, 6);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestDone {
                request_id: 1,
                result: RequestResult::None,
            },
            Effect::SendQuicData { data, .. }
        ] if data.as_ref() == b"second"
    ));
    assert_eq!(fixture_stream.pending_write_request, Some(2));
}

#[rstest]
fn test_complete_write_without_pending_request_no_op_success(mut fixture_stream: Stream) {
    let (effects, sent) = fixture_stream.complete_write(100);

    assert_eq!(sent, 0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_diagnose_snapshot_generation_success(mut fixture_stream: Stream) {
    fixture_stream.reset(1, 404);

    let effects = fixture_stream.diagnose(2);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 2,
            result: RequestResult::StreamDiagnostics(diagnostics),
        }] if diagnostics.stream_id == 0
            && diagnostics.session_id == 0
            && diagnostics.direction == StreamDirection::Bidirectional
            && !diagnostics.is_peer_initiated
            && diagnostics.receive_error_code.is_none()
            && diagnostics.receive_state == Some(StreamReceiveState::Receive)
            && diagnostics.send_error_code == Some(404)
            && diagnostics.send_state == Some(StreamSendState::ResetSent)
    ));
}

#[rstest]
fn test_flush_writes_empty_buffer_no_op_success(mut fixture_stream: Stream) {
    let (effects, sent) = fixture_stream.flush_writes(100);

    assert_eq!(sent, 0);
    assert!(effects.is_empty());
}

#[rstest]
fn test_flush_writes_empty_fin_without_credit_success(mut fixture_stream: Stream) {
    fixture_stream
        .write_buffer
        .push_back((Bytes::new(), 1, true));

    let (effects, sent) = fixture_stream.flush_writes(0);

    assert_eq!(sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            end_stream: true,
            ..
        }]
    ));
    assert_eq!(fixture_stream.send_state, Some(StreamSendState::DataSent));
}

#[rstest]
fn test_flush_writes_without_credit_keeps_buffer_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"data"), false, 0);

    let (effects, sent) = fixture_stream.flush_writes(0);

    assert_eq!(sent, 0);
    assert!(effects.is_empty());
    assert_eq!(fixture_stream.write_buffer.len(), 1);
    assert_eq!(fixture_stream.write_buffer_size, 4);
    assert!(fixture_stream.pending_write_request.is_none());
}

#[rstest]
fn test_handle_data_acknowledged_before_fin_no_op_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"data"), false, 100);

    fixture_stream.handle_data_acknowledged();

    assert_eq!(fixture_stream.send_state, Some(StreamSendState::Ready));
}

#[rstest]
fn test_handle_data_acknowledged_completes_sending_part_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"data"), true, 100);

    fixture_stream.handle_data_acknowledged();

    assert_eq!(
        fixture_stream.send_state,
        Some(StreamSendState::DataReceived)
    );
}

#[rstest]
fn test_is_blocked_predicate_success(mut fixture_stream: Stream) {
    assert!(!fixture_stream.is_blocked());

    fixture_stream.write(1, Bytes::from_static(b"data"), false, 0);
    assert!(fixture_stream.is_blocked());

    fixture_stream.flush_writes(100);
    assert!(!fixture_stream.is_blocked());
}

#[rstest]
#[case(None, Some(StreamSendState::DataReceived), true)]
#[case(Some(StreamReceiveState::DataRead), None, true)]
#[case(
    Some(StreamReceiveState::DataRead),
    Some(StreamSendState::DataReceived),
    true
)]
#[case(
    Some(StreamReceiveState::DataReceived),
    Some(StreamSendState::ResetSent),
    false
)]
#[case(
    Some(StreamReceiveState::ResetRead),
    Some(StreamSendState::DataSent),
    false
)]
#[case(
    Some(StreamReceiveState::ResetRead),
    Some(StreamSendState::ResetSent),
    true
)]
fn test_is_closed_predicate_success(
    mut fixture_stream: Stream,
    #[case] receive_state: Option<StreamReceiveState>,
    #[case] send_state: Option<StreamSendState>,
    #[case] expected: bool,
) {
    fixture_stream.receive_state = receive_state;
    fixture_stream.send_state = send_state;

    assert_eq!(fixture_stream.is_closed(), expected);
}

#[rstest]
#[case(
    StreamDirection::Bidirectional,
    Some(StreamReceiveState::Receive),
    Some(StreamSendState::Ready)
)]
#[case(StreamDirection::ReceiveOnly, Some(StreamReceiveState::Receive), None)]
#[case(StreamDirection::SendOnly, None, Some(StreamSendState::Ready))]
fn test_new_stream_initialization_success(
    #[case] direction: StreamDirection,
    #[case] receive_state: Option<StreamReceiveState>,
    #[case] send_state: Option<StreamSendState>,
) {
    let stream = create_stream(direction);

    assert_eq!(stream.id, 0);
    assert_eq!(stream.session_id, 0);
    assert_eq!(stream.direction, direction);
    assert_eq!(stream.receive_state, receive_state);
    assert_eq!(stream.send_state, send_state);
    assert_eq!(stream.max_write_buffer_size, 1024 * 1024);
    assert!(!stream.is_peer_initiated);
    assert!(!stream.is_closed());
}

#[rstest]
fn test_read_after_fin_consumed_returns_eof_success(mut fixture_stream: Stream) {
    fixture_stream.receive_state = Some(StreamReceiveState::DataRead);

    let effects = fixture_stream.read(1, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::ReadData(read_data),
        }] if read_data.is_empty()
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::DataRead)
    );
}

#[rstest]
fn test_read_after_fin_received_consumes_eof_success(mut fixture_stream: Stream) {
    fixture_stream.recv_data(Bytes::new(), true);

    let effects = fixture_stream.read(1, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::ReadData(read_data),
        }] if read_data.is_empty()
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::DataRead)
    );
}

#[rstest]
fn test_read_after_reset_fails_with_peer_error_code(mut fixture_stream: Stream) {
    fixture_stream.recv_reset(utils::webtransport_code_to_http_code(0x100));

    let effects = fixture_stream.read(1, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 1,
            source: ErrorSource::Stream,
            error_code: Some(0x100),
            ..
        }]
    ));
}

#[rstest]
fn test_read_queues_request_while_another_is_pending_success(mut fixture_stream: Stream) {
    fixture_stream.read(1, 100);

    let effects = fixture_stream.read(2, 50);

    assert!(effects.is_empty());
    assert_eq!(
        fixture_stream.pending_read_requests,
        VecDeque::from([(1, 100), (2, 50)])
    );
}

#[rstest]
fn test_read_requests_quic_data_success(mut fixture_stream: Stream) {
    let effects = fixture_stream.read(1, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::ReceiveQuicData {
            stream_id: 0,
            max_size: 100,
        }]
    ));
    assert_eq!(fixture_stream.pending_read_requests.len(), 1);
}

#[rstest]
fn test_read_send_only_fails() {
    let mut stream = create_stream(StreamDirection::SendOnly);

    let effects = stream.read(1, 100);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 1,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_read_zero_size_after_fin_received_keeps_eof_success(mut fixture_stream: Stream) {
    fixture_stream.recv_data(Bytes::new(), true);

    let effects = fixture_stream.read(1, 0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            result: RequestResult::ReadData(read_data),
            ..
        }] if read_data.is_empty()
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::DataReceived)
    );
}

#[rstest]
fn test_read_zero_size_returns_empty_success(mut fixture_stream: Stream) {
    let effects = fixture_stream.read(1, 0);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::ReadData(read_data),
        }] if read_data.is_empty()
    ));
    assert!(fixture_stream.pending_read_requests.is_empty());
}

#[rstest]
fn test_recv_data_after_fin_ignored(mut fixture_stream: Stream) {
    fixture_stream.recv_data(Bytes::new(), true);

    let effects = fixture_stream.recv_data(Bytes::from_static(b"ignore"), false);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_data_delivers_to_pending_read_success(mut fixture_stream: Stream) {
    fixture_stream.read(1, 100);

    let effects = fixture_stream.recv_data(Bytes::from_static(b"incoming"), false);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::ReadData(read_data),
        }] if read_data.as_ref() == b"incoming"
    ));
    assert!(fixture_stream.pending_read_requests.is_empty());
}

#[rstest]
fn test_recv_data_fin_with_data_drains_remaining_pending_reads_success(mut fixture_stream: Stream) {
    fixture_stream.read(1, 100);
    fixture_stream.read(2, 100);

    let effects = fixture_stream.recv_data(Bytes::from_static(b"final"), true);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestDone {
                request_id: 1,
                result: RequestResult::ReadData(first_read_data),
            },
            Effect::NotifyRequestDone {
                request_id: 2,
                result: RequestResult::ReadData(second_read_data),
            }
        ] if first_read_data.as_ref() == b"final" && second_read_data.is_empty()
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::DataRead)
    );
    assert!(fixture_stream.pending_read_requests.is_empty());
}

#[rstest]
fn test_recv_data_fin_with_data_keeps_eof_for_next_read_success(mut fixture_stream: Stream) {
    fixture_stream.read(1, 100);

    let effects = fixture_stream.recv_data(Bytes::from_static(b"final"), true);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::ReadData(read_data),
        }] if read_data.as_ref() == b"final"
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::DataReceived)
    );
}

#[rstest]
fn test_recv_data_fin_without_data_delivers_eof_success(mut fixture_stream: Stream) {
    fixture_stream.read(1, 100);

    let effects = fixture_stream.recv_data(Bytes::new(), true);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::ReadData(read_data),
        }] if read_data.is_empty()
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::DataRead)
    );
}

#[rstest]
fn test_recv_data_fin_without_pending_read_success(mut fixture_stream: Stream) {
    let effects = fixture_stream.recv_data(Bytes::new(), true);

    assert!(effects.is_empty());
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::DataReceived)
    );
}

#[rstest]
fn test_recv_data_requests_data_for_next_pending_read_success(mut fixture_stream: Stream) {
    fixture_stream.read(1, 100);
    fixture_stream.read(2, 50);

    let effects = fixture_stream.recv_data(Bytes::from_static(b"incoming"), false);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::NotifyRequestDone { request_id: 1, .. },
            Effect::ReceiveQuicData {
                stream_id: 0,
                max_size: 50,
            }
        ]
    ));
    assert_eq!(
        fixture_stream.pending_read_requests,
        VecDeque::from([(2, 50)])
    );
}

#[rstest]
fn test_recv_data_without_pending_read_no_op_success(mut fixture_stream: Stream) {
    let effects = fixture_stream.recv_data(Bytes::from_static(b"incoming"), false);

    assert!(effects.is_empty());
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::Receive)
    );
}

#[rstest]
fn test_recv_reset_after_fin_consumed_ignored(mut fixture_stream: Stream) {
    fixture_stream.receive_state = Some(StreamReceiveState::DataRead);

    let effects = fixture_stream.recv_reset(0x100);

    assert!(effects.is_empty());
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::DataRead)
    );
}

#[rstest]
fn test_recv_reset_fails_pending_reads_success(mut fixture_stream: Stream) {
    fixture_stream.read(1, 100);
    fixture_stream.read(2, 100);

    let effects = fixture_stream.recv_reset(utils::webtransport_code_to_http_code(0x100));

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitStreamEvent {
                event_type: EventType::StreamResetReceived,
                error_code: Some(0x100),
                ..
            },
            Effect::NotifyRequestFailed {
                request_id: 1,
                source: ErrorSource::Stream,
                error_code: Some(0x100),
                ..
            },
            Effect::NotifyRequestFailed {
                request_id: 2,
                source: ErrorSource::Stream,
                error_code: Some(0x100),
                ..
            }
        ]
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::ResetRead)
    );
    assert_eq!(fixture_stream.receive_error_code, Some(0x100));
}

#[rstest]
fn test_recv_reset_on_reset_read_idempotency_success(mut fixture_stream: Stream) {
    fixture_stream.recv_reset(utils::webtransport_code_to_http_code(0x100));

    let effects = fixture_stream.recv_reset(utils::webtransport_code_to_http_code(0x200));

    assert!(effects.is_empty());
    assert_eq!(fixture_stream.receive_error_code, Some(0x100));
}

#[rstest]
fn test_recv_reset_send_only_ignored() {
    let mut stream = create_stream(StreamDirection::SendOnly);

    let effects = stream.recv_reset(0x100);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_reset_unknown_error_code_success(mut fixture_stream: Stream) {
    let effects = fixture_stream.recv_reset(0x1234_5678);

    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitStreamEvent {
            event_type: EventType::StreamResetReceived,
            error_code: None,
            ..
        }]
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::ResetRead)
    );
    assert_eq!(fixture_stream.receive_error_code, None);
}

#[rstest]
fn test_recv_stop_sending_after_data_acknowledged_ignored(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::new(), true, 0);
    fixture_stream.handle_data_acknowledged();

    let effects = fixture_stream.recv_stop_sending(0x100);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stop_sending_fails_pending_write_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"data"), true, 100);
    let network_code = utils::webtransport_code_to_http_code(0x100);

    let effects = fixture_stream.recv_stop_sending(network_code);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitStreamEvent {
                event_type: EventType::StreamStopSendingReceived,
                error_code: Some(0x100),
                ..
            },
            Effect::ResetQuicStream { error_code, .. },
            Effect::NotifyRequestFailed {
                request_id: 1,
                source: ErrorSource::Stream,
                error_code: Some(0x100),
                ..
            }
        ] if *error_code == network_code
    ));
    assert!(fixture_stream.pending_write_request.is_none());
}

#[rstest]
fn test_recv_stop_sending_on_reset_sent_idempotency_success(mut fixture_stream: Stream) {
    fixture_stream.reset(1, 404);

    let effects = fixture_stream.recv_stop_sending(0x100);

    assert!(effects.is_empty());
    assert_eq!(fixture_stream.send_error_code, Some(404));
}

#[rstest]
fn test_recv_stop_sending_receive_only_ignored() {
    let mut stream = create_stream(StreamDirection::ReceiveOnly);

    let effects = stream.recv_stop_sending(0x100);

    assert!(effects.is_empty());
}

#[rstest]
fn test_recv_stop_sending_success(mut fixture_stream: Stream) {
    let network_code = utils::webtransport_code_to_http_code(0x100);

    let effects = fixture_stream.recv_stop_sending(network_code);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::EmitStreamEvent {
                event_type: EventType::StreamStopSendingReceived,
                error_code: Some(0x100),
                ..
            },
            Effect::ResetQuicStream { error_code, .. }
        ] if *error_code == network_code
    ));
    assert_eq!(fixture_stream.send_state, Some(StreamSendState::ResetSent));
    assert_eq!(fixture_stream.send_error_code, Some(0x100));
}

#[rstest]
fn test_reset_fails_buffered_and_pending_writes_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"first"), false, 100);
    fixture_stream.write(2, Bytes::from_static(b"second"), false, 100);

    let effects = fixture_stream.reset(3, 404);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream { .. },
            Effect::NotifyRequestFailed {
                request_id: 1,
                source: ErrorSource::Stream,
                error_code: Some(404),
                ..
            },
            Effect::NotifyRequestFailed {
                request_id: 2,
                source: ErrorSource::Stream,
                error_code: Some(404),
                ..
            },
            Effect::NotifyRequestDone {
                request_id: 3,
                result: RequestResult::None,
            }
        ]
    ));
    assert_eq!(fixture_stream.write_buffer_size, 0);
    assert!(fixture_stream.write_buffer.is_empty());
}

#[rstest]
fn test_reset_fails_partially_sent_write_once_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"long data"), false, 4);

    let effects = fixture_stream.reset(2, 404);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream { .. },
            Effect::NotifyRequestFailed { request_id: 1, .. },
            Effect::NotifyRequestDone { request_id: 2, .. }
        ]
    ));
}

#[rstest]
fn test_reset_local_command_success(mut fixture_stream: Stream) {
    let effects = fixture_stream.reset(1, 404);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream { error_code, .. },
            Effect::NotifyRequestDone {
                request_id: 1,
                result: RequestResult::None,
            }
        ] if *error_code == utils::webtransport_code_to_http_code(404)
    ));
    assert_eq!(fixture_stream.send_state, Some(StreamSendState::ResetSent));
    assert_eq!(fixture_stream.send_error_code, Some(404));
}

#[rstest]
fn test_reset_local_command_with_invalid_code_fallbacks_to_first(mut fixture_stream: Stream) {
    let effects = fixture_stream.reset(1, u64::MAX);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ResetQuicStream {
                error_code: ERR_WT_APPLICATION_ERROR_FIRST,
                ..
            },
            Effect::NotifyRequestDone { request_id: 1, .. }
        ]
    ));
    assert_eq!(fixture_stream.send_error_code, Some(u64::MAX));
}

#[rstest]
fn test_reset_on_reset_sent_idempotency_success(mut fixture_stream: Stream) {
    fixture_stream.reset(1, 404);

    let effects = fixture_stream.reset(2, 500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 2,
            result: RequestResult::None,
        }]
    ));
    assert_eq!(fixture_stream.send_error_code, Some(404));
}

#[rstest]
fn test_reset_receive_only_fails() {
    let mut stream = create_stream(StreamDirection::ReceiveOnly);

    let effects = stream.reset(1, 404);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 1,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_stop_after_fin_received_terminates_without_frame_success(mut fixture_stream: Stream) {
    fixture_stream.recv_data(Bytes::new(), true);

    let effects = fixture_stream.stop(1, 500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 1,
            result: RequestResult::None,
        }]
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::ResetRead)
    );
    assert_eq!(fixture_stream.receive_error_code, Some(500));
}

#[rstest]
fn test_stop_local_command_success(mut fixture_stream: Stream) {
    fixture_stream.read(9, 100);

    let effects = fixture_stream.stop(1, 500);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::StopQuicStream { error_code, .. },
            Effect::NotifyRequestFailed {
                request_id: 9,
                source: ErrorSource::Stream,
                error_code: Some(500),
                ..
            },
            Effect::NotifyRequestDone {
                request_id: 1,
                result: RequestResult::None,
            }
        ] if *error_code == utils::webtransport_code_to_http_code(500)
    ));
    assert_eq!(
        fixture_stream.receive_state,
        Some(StreamReceiveState::ResetRead)
    );
    assert!(fixture_stream.pending_read_requests.is_empty());
}

#[rstest]
fn test_stop_local_command_with_invalid_code_fallbacks_to_first(mut fixture_stream: Stream) {
    let effects = fixture_stream.stop(1, u64::MAX);

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::StopQuicStream {
                error_code: ERR_WT_APPLICATION_ERROR_FIRST,
                ..
            },
            Effect::NotifyRequestDone { request_id: 1, .. }
        ]
    ));
    assert_eq!(fixture_stream.receive_error_code, Some(u64::MAX));
}

#[rstest]
fn test_stop_on_reset_read_idempotency_success(mut fixture_stream: Stream) {
    fixture_stream.stop(1, 500);

    let effects = fixture_stream.stop(2, 600);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 2,
            result: RequestResult::None,
        }]
    ));
    assert_eq!(fixture_stream.receive_error_code, Some(500));
}

#[rstest]
fn test_stop_send_only_fails() {
    let mut stream = create_stream(StreamDirection::SendOnly);

    let effects = stream.stop(1, 500);

    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 1,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_write_after_fin_buffered_fails(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"data"), true, 0);

    let (effects, sent) = fixture_stream.write(2, Bytes::from_static(b"more"), false, 100);

    assert_eq!(sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 2,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_write_buffer_overflow_error_success(mut fixture_stream: Stream) {
    let size = usize::try_from(1024 * 1024 + 1).unwrap_or(usize::MAX);
    let large_data = Bytes::from(vec![0u8; size]);

    let (effects, sent) = fixture_stream.write(1, large_data, false, 0);

    assert_eq!(sent, 0);
    assert_eq!(fixture_stream.write_buffer_size, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 1,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_write_empty_fin_after_fin_sent_no_op_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::new(), true, 0);

    let (effects, sent) = fixture_stream.write(2, Bytes::new(), true, 100);

    assert_eq!(sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestDone {
            request_id: 2,
            result: RequestResult::None,
        }]
    ));
}

#[rstest]
fn test_write_empty_fin_success(mut fixture_stream: Stream) {
    let (effects, sent) = fixture_stream.write(1, Bytes::new(), true, 0);

    assert_eq!(sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            data,
            end_stream: true,
            ..
        }] if data.is_empty()
    ));
    assert_eq!(fixture_stream.send_state, Some(StreamSendState::DataSent));
    assert_eq!(fixture_stream.pending_write_request, Some(1));
}

#[rstest]
fn test_write_full_credit_immediate_send_success(mut fixture_stream: Stream) {
    let data = Bytes::from_static(b"hello");

    let (effects, sent) = fixture_stream.write(1, data.clone(), false, 100);

    assert_eq!(sent, 5);
    assert_eq!(fixture_stream.write_buffer_size, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            stream_id: 0,
            data: sent_data,
            end_stream: false,
        }] if sent_data == &data
    ));
    assert_eq!(fixture_stream.pending_write_request, Some(1));
}

#[rstest]
fn test_write_no_credit_full_buffering_success(mut fixture_stream: Stream) {
    let (effects, sent) = fixture_stream.write(1, Bytes::from_static(b"blocked"), false, 0);

    assert_eq!(sent, 0);
    assert_eq!(fixture_stream.write_buffer_size, 7);
    assert!(effects.is_empty());
}

#[rstest]
fn test_write_on_reset_sent_fails(mut fixture_stream: Stream) {
    fixture_stream.reset(1, 404);

    let (effects, sent) = fixture_stream.write(2, Bytes::from_static(b"data"), false, 100);

    assert_eq!(sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 2,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_write_partial_credit_buffering_success(mut fixture_stream: Stream) {
    let (effects, sent) = fixture_stream.write(1, Bytes::from_static(b"hello world"), true, 5);

    assert_eq!(sent, 5);
    assert_eq!(fixture_stream.write_buffer_size, 6);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendQuicData {
            data,
            end_stream: false,
            ..
        }] if data.as_ref() == b"hello"
    ));
    assert_eq!(fixture_stream.send_state, Some(StreamSendState::Ready));
}

#[rstest]
fn test_write_receive_only_fails() {
    let mut stream = create_stream(StreamDirection::ReceiveOnly);

    let (effects, sent) = stream.write(1, Bytes::from_static(b"data"), false, 100);

    assert_eq!(sent, 0);
    assert!(matches!(
        effects.as_slice(),
        [Effect::NotifyRequestFailed {
            request_id: 1,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            ..
        }]
    ));
}

#[rstest]
fn test_write_while_write_pending_buffers_success(mut fixture_stream: Stream) {
    fixture_stream.write(1, Bytes::from_static(b"first"), false, 100);

    let (effects, sent) = fixture_stream.write(2, Bytes::from_static(b"second"), false, 100);

    assert_eq!(sent, 0);
    assert!(effects.is_empty());
    assert_eq!(fixture_stream.write_buffer_size, 6);
    assert_eq!(fixture_stream.pending_write_request, Some(1));
}
