//! Stream state machine.

use std::collections::VecDeque;

use bytes::Bytes;
use tracing::debug;

use crate::common::constants::{ERR_LIB_STREAM_STATE_ERROR, ERR_WT_APPLICATION_ERROR_FIRST};
use crate::common::types::{
    ErrorCode, ErrorSource, EventType, RequestId, SessionId, StreamDirection, StreamId,
    StreamReceiveState, StreamSendState,
};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::utils;

#[derive(Clone, Debug)]
pub(crate) struct StreamDiagnostics {
    pub(crate) created_at: f64,
    pub(crate) direction: StreamDirection,
    pub(crate) is_peer_initiated: bool,
    pub(crate) receive_error_code: Option<ErrorCode>,
    pub(crate) receive_state: Option<StreamReceiveState>,
    pub(crate) send_error_code: Option<ErrorCode>,
    pub(crate) send_state: Option<StreamSendState>,
    pub(crate) session_id: SessionId,
    pub(crate) stream_id: StreamId,
}

#[derive(Debug)]
pub(super) struct Stream {
    created_at: f64,
    direction: StreamDirection,
    id: StreamId,
    is_peer_initiated: bool,
    max_write_buffer_size: u64,
    pending_read_requests: VecDeque<(RequestId, u64)>,
    pending_write_request: Option<RequestId>,
    receive_error_code: Option<ErrorCode>,
    receive_state: Option<StreamReceiveState>,
    send_error_code: Option<ErrorCode>,
    send_state: Option<StreamSendState>,
    session_id: SessionId,
    write_buffer: VecDeque<(Bytes, RequestId, bool)>,
    write_buffer_size: u64,
}

impl Stream {
    pub(super) fn new(
        id: StreamId,
        session_id: SessionId,
        direction: StreamDirection,
        is_peer_initiated: bool,
        parameters: StreamParameters,
        created_at: f64,
    ) -> Self {
        Self {
            created_at,
            direction,
            id,
            is_peer_initiated,
            max_write_buffer_size: parameters.max_write_buffer_size,
            pending_read_requests: VecDeque::new(),
            pending_write_request: None,
            receive_error_code: None,
            receive_state: (direction != StreamDirection::SendOnly)
                .then_some(StreamReceiveState::Receive),
            send_error_code: None,
            send_state: (direction != StreamDirection::ReceiveOnly)
                .then_some(StreamSendState::Ready),
            session_id,
            write_buffer: VecDeque::new(),
            write_buffer_size: 0,
        }
    }

    pub(super) fn abort(&mut self, error_code: ErrorCode) -> Vec<Effect> {
        debug!("wt_stream abort stream_id={} err={error_code}", self.id);
        let mut effects = self.send_reset(Some(error_code), error_code);
        effects.extend(self.send_stop(Some(error_code), error_code));

        effects
    }

    pub(super) fn complete_write(&mut self, available_credit: u64) -> (Vec<Effect>, u64) {
        let Some(request_id) = self.pending_write_request.take() else {
            return (Vec::new(), 0);
        };

        let mut effects = Vec::new();

        let is_request_complete = self
            .write_buffer
            .front()
            .is_none_or(|&(_, write_request_id, _)| write_request_id != request_id);

        if is_request_complete {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
        }

        let (flush_effects, sent) = self.flush_writes(available_credit);
        effects.extend(flush_effects);

        (effects, sent)
    }

    pub(super) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diagnostics = self.diagnostics_snapshot();

        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::StreamDiagnostics(Box::new(diagnostics)),
        }]
    }

    pub(super) fn flush_writes(&mut self, available_credit: u64) -> (Vec<Effect>, u64) {
        if self.pending_write_request.is_some() || self.send_state != Some(StreamSendState::Ready) {
            return (Vec::new(), 0);
        }

        let Some((mut data, request_id, end_stream)) = self.write_buffer.pop_front() else {
            return (Vec::new(), 0);
        };

        let data_len = data.len() as u64;

        if data_len > 0 && available_credit == 0 {
            self.write_buffer.push_front((data, request_id, end_stream));
            return (Vec::new(), 0);
        }

        let is_request_complete = data_len <= available_credit;
        if !is_request_complete {
            let sendable = usize::try_from(available_credit).unwrap_or(usize::MAX);
            let remaining_data = data.split_off(sendable);
            self.write_buffer
                .push_front((remaining_data, request_id, end_stream));
        }

        let sent = data.len() as u64;
        let is_finished = is_request_complete && end_stream;

        self.write_buffer_size -= sent;
        self.pending_write_request = Some(request_id);

        if is_finished {
            self.send_state = Some(StreamSendState::DataSent);
        }

        (
            vec![Effect::SendQuicData {
                stream_id: self.id,
                data,
                end_stream: is_finished,
            }],
            sent,
        )
    }

    pub(super) fn handle_data_acknowledged(&mut self) {
        if self.send_state == Some(StreamSendState::DataSent) {
            self.send_state = Some(StreamSendState::DataReceived);
        }
    }

    pub(super) fn is_blocked(&self) -> bool {
        self.pending_write_request.is_none() && !self.write_buffer.is_empty()
    }

    pub(super) fn is_closed(&self) -> bool {
        let is_receive_terminated = self.receive_state.is_none_or(|receive_state| {
            matches!(
                receive_state,
                StreamReceiveState::DataRead | StreamReceiveState::ResetRead
            )
        });
        let is_send_terminated = self.send_state.is_none_or(|send_state| {
            matches!(
                send_state,
                StreamSendState::DataReceived | StreamSendState::ResetSent
            )
        });

        is_receive_terminated && is_send_terminated
    }

    pub(super) fn read(&mut self, request_id: RequestId, max_size: u64) -> Vec<Effect> {
        let Some(receive_state) = self.receive_state else {
            debug!(
                "wt_stream validate invalid actual={:?} stream_id={}",
                self.direction, self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate invalid".into(),
            }];
        };

        match receive_state {
            StreamReceiveState::Receive if max_size > 0 => {
                self.pending_read_requests.push_back((request_id, max_size));

                if self.pending_read_requests.len() == 1 {
                    return vec![Effect::ReceiveQuicData {
                        stream_id: self.id,
                        max_size,
                    }];
                }

                Vec::new()
            }
            StreamReceiveState::DataRead
            | StreamReceiveState::DataReceived
            | StreamReceiveState::Receive => {
                if receive_state == StreamReceiveState::DataReceived && max_size > 0 {
                    self.receive_state = Some(StreamReceiveState::DataRead);
                }

                vec![Effect::NotifyRequestDone {
                    request_id,
                    result: RequestResult::ReadData(Bytes::new()),
                }]
            }
            StreamReceiveState::ResetRead => {
                debug!(
                    "wt_stream validate failed actual={receive_state:?} stream_id={}",
                    self.id
                );
                vec![Effect::NotifyRequestFailed {
                    request_id,
                    source: ErrorSource::Stream,
                    error_code: self.receive_error_code,
                    reason: "wt_stream validate failed".into(),
                }]
            }
        }
    }

    pub(super) fn recv_data(&mut self, data: Bytes, end_stream: bool) -> Vec<Effect> {
        if self.receive_state != Some(StreamReceiveState::Receive) {
            return Vec::new();
        }

        let mut effects = Vec::new();

        if let Some((request_id, _)) = self.pending_read_requests.pop_front() {
            let is_data_delivered = !data.is_empty();

            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::ReadData(data),
            });

            if end_stream && !is_data_delivered {
                self.receive_state = Some(StreamReceiveState::DataRead);
            }
        }

        if end_stream {
            if self.receive_state == Some(StreamReceiveState::Receive) {
                self.receive_state = Some(StreamReceiveState::DataReceived);
            }

            while let Some((request_id, _)) = self.pending_read_requests.pop_front() {
                self.receive_state = Some(StreamReceiveState::DataRead);
                effects.push(Effect::NotifyRequestDone {
                    request_id,
                    result: RequestResult::ReadData(Bytes::new()),
                });
            }
        } else if let Some(&(_, max_size)) = self.pending_read_requests.front() {
            effects.push(Effect::ReceiveQuicData {
                stream_id: self.id,
                max_size,
            });
        }

        effects
    }

    pub(super) fn recv_reset(&mut self, error_code: ErrorCode) -> Vec<Effect> {
        if matches!(
            self.receive_state,
            None | Some(StreamReceiveState::DataRead | StreamReceiveState::ResetRead)
        ) {
            return Vec::new();
        }

        debug!("wt_stream abort stream_id={} err={error_code}", self.id);
        let application_error_code =
            utils::http_code_to_webtransport_code(error_code).map(u64::from);
        self.receive_error_code = application_error_code;
        self.receive_state = Some(StreamReceiveState::ResetRead);

        let mut effects = vec![Effect::EmitStreamEvent {
            stream_id: self.id,
            event_type: EventType::StreamResetReceived,
            session_id: None,
            direction: None,
            is_peer_initiated: None,
            error_code: application_error_code,
        }];

        while let Some((request_id, _)) = self.pending_read_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: application_error_code,
                reason: "wt_stream abort".into(),
            });
        }

        effects
    }

    pub(super) fn recv_stop_sending(&mut self, error_code: ErrorCode) -> Vec<Effect> {
        if matches!(
            self.send_state,
            None | Some(StreamSendState::DataReceived | StreamSendState::ResetSent)
        ) {
            return Vec::new();
        }

        debug!("wt_stream abort stream_id={} err={error_code}", self.id);
        let application_error_code =
            utils::http_code_to_webtransport_code(error_code).map(u64::from);

        let mut effects = vec![Effect::EmitStreamEvent {
            stream_id: self.id,
            event_type: EventType::StreamStopSendingReceived,
            session_id: None,
            direction: None,
            is_peer_initiated: None,
            error_code: application_error_code,
        }];
        effects.extend(self.send_reset(application_error_code, error_code));

        effects
    }

    pub(super) fn reset(&mut self, request_id: RequestId, error_code: ErrorCode) -> Vec<Effect> {
        let Some(send_state) = self.send_state else {
            debug!(
                "wt_stream validate invalid actual={:?} stream_id={}",
                self.direction, self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate invalid".into(),
            }];
        };

        let mut effects = Vec::new();

        if !matches!(
            send_state,
            StreamSendState::DataReceived | StreamSendState::ResetSent
        ) {
            debug!(
                "wt_stream abort request_id={request_id} stream_id={} err={error_code}",
                self.id
            );
            let http_error_code = u32::try_from(error_code)
                .map(utils::webtransport_code_to_http_code)
                .unwrap_or(ERR_WT_APPLICATION_ERROR_FIRST);

            effects.extend(self.send_reset(Some(error_code), http_error_code));
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    pub(super) fn stop(&mut self, request_id: RequestId, error_code: ErrorCode) -> Vec<Effect> {
        let Some(receive_state) = self.receive_state else {
            debug!(
                "wt_stream validate invalid actual={:?} stream_id={}",
                self.direction, self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate invalid".into(),
            }];
        };

        let mut effects = Vec::new();

        if !matches!(
            receive_state,
            StreamReceiveState::DataRead | StreamReceiveState::ResetRead
        ) {
            debug!(
                "wt_stream abort request_id={request_id} stream_id={} err={error_code}",
                self.id
            );
            let http_error_code = u32::try_from(error_code)
                .map(utils::webtransport_code_to_http_code)
                .unwrap_or(ERR_WT_APPLICATION_ERROR_FIRST);

            effects.extend(self.send_stop(Some(error_code), http_error_code));
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    pub(super) fn write(
        &mut self,
        request_id: RequestId,
        data: Bytes,
        end_stream: bool,
        available_credit: u64,
    ) -> (Vec<Effect>, u64) {
        let Some(send_state) = self.send_state else {
            debug!(
                "wt_stream validate invalid actual={:?} stream_id={}",
                self.direction, self.id
            );
            return (
                vec![Effect::NotifyRequestFailed {
                    request_id,
                    source: ErrorSource::Stream,
                    error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                    reason: "wt_stream validate invalid".into(),
                }],
                0,
            );
        };

        let is_finish_pending = self
            .write_buffer
            .back()
            .is_some_and(|&(_, _, end_stream)| end_stream);

        if send_state != StreamSendState::Ready || is_finish_pending {
            if data.is_empty() && end_stream {
                return (
                    vec![Effect::NotifyRequestDone {
                        request_id,
                        result: RequestResult::None,
                    }],
                    0,
                );
            }

            debug!(
                "wt_stream validate failed actual={send_state:?} stream_id={}",
                self.id
            );
            return (
                vec![Effect::NotifyRequestFailed {
                    request_id,
                    source: ErrorSource::Stream,
                    error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                    reason: "wt_stream validate failed".into(),
                }],
                0,
            );
        }

        let data_len = data.len() as u64;

        if self.write_buffer_size + data_len > self.max_write_buffer_size {
            debug!(
                "wt_stream validate exceeded actual={} limit={} stream_id={}",
                self.write_buffer_size + data_len,
                self.max_write_buffer_size,
                self.id
            );
            return (
                vec![Effect::NotifyRequestFailed {
                    request_id,
                    source: ErrorSource::Stream,
                    error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                    reason: "wt_stream validate exceeded".into(),
                }],
                0,
            );
        }

        self.write_buffer.push_back((data, request_id, end_stream));
        self.write_buffer_size += data_len;

        self.flush_writes(available_credit)
    }

    fn diagnostics_snapshot(&self) -> StreamDiagnostics {
        StreamDiagnostics {
            created_at: self.created_at,
            direction: self.direction,
            is_peer_initiated: self.is_peer_initiated,
            receive_error_code: self.receive_error_code,
            receive_state: self.receive_state,
            send_error_code: self.send_error_code,
            send_state: self.send_state,
            session_id: self.session_id,
            stream_id: self.id,
        }
    }

    fn send_reset(
        &mut self,
        error_code: Option<ErrorCode>,
        http_error_code: ErrorCode,
    ) -> Vec<Effect> {
        if matches!(
            self.send_state,
            None | Some(StreamSendState::DataReceived | StreamSendState::ResetSent)
        ) {
            return Vec::new();
        }

        self.send_error_code = error_code;
        self.send_state = Some(StreamSendState::ResetSent);
        self.write_buffer_size = 0;

        let mut effects = vec![Effect::ResetQuicStream {
            stream_id: self.id,
            error_code: http_error_code,
        }];

        if let Some(request_id) = self.pending_write_request.take()
            && self
                .write_buffer
                .front()
                .is_none_or(|&(_, write_request_id, _)| write_request_id != request_id)
        {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code,
                reason: "wt_stream abort".into(),
            });
        }

        while let Some((_, request_id, _)) = self.write_buffer.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code,
                reason: "wt_stream abort".into(),
            });
        }

        effects
    }

    fn send_stop(
        &mut self,
        error_code: Option<ErrorCode>,
        http_error_code: ErrorCode,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        match self.receive_state {
            Some(StreamReceiveState::Receive) => {
                effects.push(Effect::StopQuicStream {
                    stream_id: self.id,
                    error_code: http_error_code,
                });
            }
            Some(StreamReceiveState::DataReceived) => {}
            _ => return effects,
        }

        self.receive_error_code = error_code;
        self.receive_state = Some(StreamReceiveState::ResetRead);

        while let Some((request_id, _)) = self.pending_read_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code,
                reason: "wt_stream abort".into(),
            });
        }

        effects
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct StreamParameters {
    pub(super) max_write_buffer_size: u64,
}

#[cfg(test)]
mod tests;
