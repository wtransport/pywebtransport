//! Single stream state machine and logic entity.

use std::collections::VecDeque;

use bytes::{BufMut, Bytes};
use tracing::debug;

use crate::common::constants::{
    ERR_LIB_STREAM_STATE_ERROR, ERR_WT_APPLICATION_ERROR_FIRST, ERR_WT_STREAM_BUFFER_EXCEEDED,
};
use crate::common::types::{
    ErrorCode, ErrorSource, EventType, RequestId, SessionId, StreamDirection, StreamId, StreamState,
};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::utils::{http_to_wt_error, wt_to_http_error};

// Threshold for zero-copy slicing optimization (32KB).
const OPTIMIZED_READ_SLICE_SIZE: u64 = 32 * 1024;

// Diagnostic information snapshot for a stream.
#[derive(Clone, Debug)]
pub(crate) struct StreamDiagnostics {
    pub(crate) bytes_received: u64,
    pub(crate) bytes_sent: u64,
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
    pub(crate) created_at: f64,
    pub(crate) direction: StreamDirection,
    pub(crate) is_peer_initiated: bool,
    pub(crate) read_buffer_size: u64,
    pub(crate) session_id: SessionId,
    pub(crate) state: StreamState,
    pub(crate) stream_id: StreamId,
    pub(crate) write_buffer_size: u64,
}

// Single WebTransport stream state machine.
#[derive(Debug)]
pub(super) struct Stream {
    bytes_received: u64,
    bytes_sent: u64,
    close_code: Option<ErrorCode>,
    close_reason: Option<String>,
    closed_at: Option<f64>,
    created_at: f64,
    direction: StreamDirection,
    id: StreamId,
    is_peer_initiated: bool,
    max_read_buffer_size: u64,
    max_write_buffer_size: u64,
    pending_read_requests: VecDeque<(RequestId, u64)>,
    read_buffer: VecDeque<Bytes>,
    read_buffer_size: u64,
    session_id: SessionId,
    state: StreamState,
    write_buffer: VecDeque<(Bytes, RequestId, bool)>,
    write_buffer_size: u64,
}

impl Stream {
    // Stream entity initialization.
    pub(super) fn new(
        id: StreamId,
        session_id: SessionId,
        direction: StreamDirection,
        is_peer_initiated: bool,
        max_read_buffer_size: u64,
        max_write_buffer_size: u64,
        created_at: f64,
    ) -> Self {
        Self {
            bytes_received: 0,
            bytes_sent: 0,
            close_code: None,
            close_reason: None,
            closed_at: None,
            created_at,
            direction,
            id,
            is_peer_initiated,
            max_read_buffer_size,
            max_write_buffer_size,
            pending_read_requests: VecDeque::new(),
            read_buffer: VecDeque::new(),
            read_buffer_size: 0,
            session_id,
            state: StreamState::Open,
            write_buffer: VecDeque::new(),
            write_buffer_size: 0,
        }
    }

    // Forceful stream termination.
    pub(super) fn abort(&mut self, error_code: ErrorCode, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state == StreamState::Closed {
            return effects;
        }

        debug!("wt_stream abort stream_id={} err={error_code}", self.id);
        self.close_code = Some(error_code);
        self.closed_at = Some(now);
        self.state = StreamState::Closed;

        let can_send = matches!(
            self.direction,
            StreamDirection::Bidirectional | StreamDirection::SendOnly
        );
        let can_receive = matches!(
            self.direction,
            StreamDirection::Bidirectional | StreamDirection::ReceiveOnly
        );

        if can_send {
            effects.push(Effect::ResetQuicStream {
                stream_id: self.id,
                error_code,
            });
        }

        if can_receive {
            effects.push(Effect::StopQuicStream {
                stream_id: self.id,
                error_code,
            });
        }

        while let Some((req_id, _)) = self.pending_read_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                source: ErrorSource::Stream,
                error_code: Some(error_code),
                reason: "wt_stream abort".into(),
            });
        }

        while let Some((_, req_id, _)) = self.write_buffer.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                source: ErrorSource::Stream,
                error_code: Some(error_code),
                reason: "wt_stream abort".into(),
            });
        }

        self.read_buffer.clear();
        self.read_buffer_size = 0;
        self.write_buffer_size = 0;

        effects.push(Effect::EmitStreamEvent {
            stream_id: self.id,
            event_type: EventType::StreamClosed,
            session_id: None,
            direction: None,
            is_peer_initiated: None,
            error_code: None,
        });

        effects
    }

    // User diagnostics event handling.
    pub(super) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diag = self.diagnostics_snapshot();

        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::StreamDiagnostics(Box::new(diag)),
        }]
    }

    // Write buffer flushing with flow control.
    pub(super) fn flush_writes(&mut self, available_credit: u64, now: f64) -> (Vec<Effect>, u64) {
        let mut effects = Vec::new();
        let mut remaining_credit = available_credit;
        let mut total_sent = 0;

        while remaining_credit > 0 {
            let Some((data, request_id, end_stream)) = self.write_buffer.pop_front() else {
                break;
            };

            let data_len = data.len() as u64;
            self.write_buffer_size -= data_len;

            if data_len <= remaining_credit {
                total_sent += data_len;
                remaining_credit -= data_len;
                self.bytes_sent += data_len;

                effects.push(Effect::SendQuicData {
                    stream_id: self.id,
                    data,
                    end_stream,
                });
                effects.push(Effect::NotifyRequestDone {
                    request_id,
                    result: RequestResult::None,
                });

                if end_stream {
                    match self.state {
                        StreamState::HalfClosedRemote | StreamState::ResetReceived => {
                            self.closed_at = Some(now);
                            self.state = StreamState::Closed;
                            effects.push(Effect::EmitStreamEvent {
                                stream_id: self.id,
                                event_type: EventType::StreamClosed,
                                session_id: None,
                                direction: None,
                                is_peer_initiated: None,
                                error_code: None,
                            });
                        }
                        StreamState::Open => {
                            self.state = StreamState::HalfClosedLocal;
                        }
                        _ => {}
                    }
                }
            } else {
                let sendable = usize::try_from(remaining_credit).unwrap_or(usize::MAX);
                let data_to_send = data.slice(0..sendable);
                let remaining_data = data.slice(sendable..);
                let remaining_len = remaining_data.len() as u64;

                total_sent += remaining_credit;
                self.bytes_sent += remaining_credit;

                effects.push(Effect::SendQuicData {
                    stream_id: self.id,
                    data: data_to_send,
                    end_stream: false,
                });

                self.write_buffer
                    .push_front((remaining_data, request_id, end_stream));
                self.write_buffer_size += remaining_len;

                break;
            }
        }

        (effects, total_sent)
    }

    // Pending write state validation.
    pub(super) fn has_pending_writes(&self) -> bool {
        self.write_buffer_size > 0
    }

    // Terminal state predicate.
    pub(super) fn is_closed(&self) -> bool {
        self.state == StreamState::Closed
    }

    // User read request handling.
    pub(super) fn read(&mut self, request_id: RequestId, max_bytes: u64) -> (Vec<Effect>, u64) {
        let mut effects = Vec::new();

        if self.direction == StreamDirection::SendOnly {
            debug!(
                "wt_stream validate invalid actual={:?} stream_id={}",
                self.direction, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate invalid".into(),
            });
            return (effects, 0);
        }

        if self.read_buffer_size > 0 {
            let target = Self::limit_read(max_bytes, self.read_buffer_size);
            let data_to_return = self.take_data(target);
            let consumed = data_to_return.len() as u64;

            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::ReadData(data_to_return),
            });

            if self.read_buffer_size == 0 && self.state == StreamState::Closed {
                effects.push(Effect::EmitStreamEvent {
                    stream_id: self.id,
                    event_type: EventType::StreamClosed,
                    session_id: None,
                    direction: None,
                    is_peer_initiated: None,
                    error_code: self.close_code,
                });
            }

            return (effects, consumed);
        }

        if self.state == StreamState::HalfClosedRemote
            || (self.state == StreamState::Closed
                && (self.close_code.is_none() || self.close_code == Some(0)))
        {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::ReadData(Bytes::new()),
            });
            return (effects, 0);
        }

        if !matches!(
            self.state,
            StreamState::HalfClosedLocal | StreamState::Open | StreamState::ResetSent
        ) {
            let error_to_return = self.close_code.unwrap_or(ERR_LIB_STREAM_STATE_ERROR);
            debug!(
                "wt_stream validate failed actual={:?} stream_id={}",
                self.state, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(error_to_return),
                reason: "wt_stream validate failed".into(),
            });
            return (effects, 0);
        }

        self.pending_read_requests
            .push_back((request_id, max_bytes));

        (effects, 0)
    }

    // Network data reception handling.
    pub(super) fn recv_data(
        &mut self,
        data: Bytes,
        end_stream: bool,
        now: f64,
    ) -> (Vec<Effect>, u64) {
        let mut effects = Vec::new();
        let mut consumed_bytes_by_reads = 0;

        if !matches!(
            self.state,
            StreamState::HalfClosedLocal | StreamState::Open | StreamState::ResetSent
        ) {
            return (effects, 0);
        }

        if !data.is_empty() {
            let data_len = data.len() as u64;
            if self.read_buffer_size + data_len > self.max_read_buffer_size {
                debug!(
                    "wt_stream validate exceeded actual={} limit={} stream_id={}",
                    self.read_buffer_size + data_len,
                    self.max_read_buffer_size,
                    self.id
                );
                effects.push(Effect::StopQuicStream {
                    stream_id: self.id,
                    error_code: ERR_WT_STREAM_BUFFER_EXCEEDED,
                });
                return (effects, 0);
            }

            self.bytes_received += data_len;
            self.read_buffer.push_back(data);
            self.read_buffer_size += data_len;
        }

        while !self.pending_read_requests.is_empty() && self.read_buffer_size > 0 {
            if let Some((req_id, max_bytes)) = self.pending_read_requests.pop_front() {
                let target = Self::limit_read(max_bytes, self.read_buffer_size);
                let data_chunk = self.take_data(target);

                consumed_bytes_by_reads += data_chunk.len() as u64;

                effects.push(Effect::NotifyRequestDone {
                    request_id: req_id,
                    result: RequestResult::ReadData(data_chunk),
                });
            }
        }

        if end_stream {
            match self.state {
                StreamState::HalfClosedLocal | StreamState::ResetSent => {
                    self.closed_at = Some(now);
                    self.state = StreamState::Closed;
                    if self.read_buffer_size == 0 {
                        effects.push(Effect::EmitStreamEvent {
                            stream_id: self.id,
                            event_type: EventType::StreamClosed,
                            session_id: None,
                            direction: None,
                            is_peer_initiated: None,
                            error_code: None,
                        });
                    }
                }
                StreamState::Open => {
                    self.state = StreamState::HalfClosedRemote;
                }
                _ => {}
            }

            while let Some((req_id, _)) = self.pending_read_requests.pop_front() {
                effects.push(Effect::NotifyRequestDone {
                    request_id: req_id,
                    result: RequestResult::ReadData(Bytes::new()),
                });
            }
        }

        (effects, consumed_bytes_by_reads)
    }

    // Network reset reception handling.
    pub(super) fn recv_reset(&mut self, error_code: ErrorCode, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if matches!(self.state, StreamState::Closed | StreamState::ResetReceived) {
            return effects;
        }

        debug!("wt_stream abort stream_id={} err={error_code}", self.id);
        let app_error_code = http_to_wt_error(error_code).map(u64::from);

        effects.push(Effect::EmitStreamEvent {
            stream_id: self.id,
            event_type: EventType::StreamResetReceived,
            session_id: None,
            direction: None,
            is_peer_initiated: None,
            error_code: app_error_code,
        });

        while let Some((req_id, _)) = self.pending_read_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                source: ErrorSource::Stream,
                error_code: app_error_code,
                reason: "wt_stream abort".into(),
            });
        }

        match self.state {
            StreamState::HalfClosedLocal | StreamState::ResetSent => {
                self.close_code = app_error_code;
                self.closed_at = Some(now);
                self.state = StreamState::Closed;

                while let Some((_, req_id, _)) = self.write_buffer.pop_front() {
                    effects.push(Effect::NotifyRequestFailed {
                        request_id: req_id,
                        source: ErrorSource::Stream,
                        error_code: app_error_code,
                        reason: "wt_stream abort".into(),
                    });
                }
                self.write_buffer_size = 0;

                effects.push(Effect::EmitStreamEvent {
                    stream_id: self.id,
                    event_type: EventType::StreamClosed,
                    session_id: None,
                    direction: None,
                    is_peer_initiated: None,
                    error_code: None,
                });
            }
            StreamState::HalfClosedRemote | StreamState::Open => {
                self.close_code = app_error_code;
                self.state = StreamState::ResetReceived;
            }
            _ => {}
        }

        effects
    }

    // Network stop_sending reception handling.
    pub(super) fn recv_stop_sending(&mut self, error_code: ErrorCode) -> Vec<Effect> {
        let mut effects = Vec::new();

        if matches!(self.state, StreamState::Closed | StreamState::ResetSent) {
            return effects;
        }

        debug!("wt_stream abort stream_id={} err={error_code}", self.id);
        let app_error_code = http_to_wt_error(error_code).map(u64::from);

        effects.push(Effect::EmitStreamEvent {
            stream_id: self.id,
            event_type: EventType::StopSendingReceived,
            session_id: None,
            direction: None,
            is_peer_initiated: None,
            error_code: app_error_code,
        });

        effects
    }

    // User reset command handling.
    pub(super) fn reset(
        &mut self,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.direction == StreamDirection::ReceiveOnly {
            debug!(
                "wt_stream validate invalid actual={:?} stream_id={}",
                self.direction, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate invalid".into(),
            });
            return effects;
        }

        if matches!(self.state, StreamState::Closed | StreamState::ResetSent) {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
            return effects;
        }

        let previous_state = self.state;
        debug!(
            "wt_stream abort request_id={request_id} stream_id={} err={error_code}",
            self.id
        );
        self.close_code = Some(error_code);
        self.closed_at = Some(now);
        self.state = StreamState::ResetSent;

        let http_error_code = u32::try_from(error_code)
            .map(wt_to_http_error)
            .unwrap_or(ERR_WT_APPLICATION_ERROR_FIRST);

        effects.push(Effect::ResetQuicStream {
            stream_id: self.id,
            error_code: http_error_code,
        });

        while let Some((_, req_id, _)) = self.write_buffer.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                source: ErrorSource::Stream,
                error_code: Some(error_code),
                reason: "wt_stream abort".into(),
            });
        }
        self.write_buffer_size = 0;

        match previous_state {
            StreamState::HalfClosedRemote | StreamState::ResetReceived => {
                self.state = StreamState::Closed;
                effects.push(Effect::EmitStreamEvent {
                    stream_id: self.id,
                    event_type: EventType::StreamClosed,
                    session_id: None,
                    direction: None,
                    is_peer_initiated: None,
                    error_code: None,
                });
            }
            _ => {}
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    // User stop command handling.
    pub(super) fn stop(
        &mut self,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.direction == StreamDirection::SendOnly {
            debug!(
                "wt_stream validate invalid actual={:?} stream_id={}",
                self.direction, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate invalid".into(),
            });
            return effects;
        }

        if matches!(
            self.state,
            StreamState::Closed | StreamState::HalfClosedRemote | StreamState::ResetReceived
        ) {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
            return effects;
        }

        let previous_state = self.state;
        debug!(
            "wt_stream abort request_id={request_id} stream_id={} err={error_code}",
            self.id
        );
        self.close_code = Some(error_code);
        self.closed_at = Some(now);
        self.state = StreamState::ResetReceived;

        let http_error_code = u32::try_from(error_code)
            .map(wt_to_http_error)
            .unwrap_or(ERR_WT_APPLICATION_ERROR_FIRST);

        effects.push(Effect::StopQuicStream {
            stream_id: self.id,
            error_code: http_error_code,
        });

        while let Some((req_id, _)) = self.pending_read_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                source: ErrorSource::Stream,
                error_code: Some(error_code),
                reason: "wt_stream abort".into(),
            });
        }

        match previous_state {
            StreamState::HalfClosedLocal | StreamState::ResetSent => {
                self.state = StreamState::Closed;
                effects.push(Effect::EmitStreamEvent {
                    stream_id: self.id,
                    event_type: EventType::StreamClosed,
                    session_id: None,
                    direction: None,
                    is_peer_initiated: None,
                    error_code: None,
                });
            }
            _ => {}
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    // User write request handling.
    pub(super) fn write(
        &mut self,
        request_id: RequestId,
        data: Bytes,
        end_stream: bool,
        available_credit: u64,
        now: f64,
    ) -> (Vec<Effect>, u64) {
        let mut effects = Vec::new();

        if self.direction == StreamDirection::ReceiveOnly {
            debug!(
                "wt_stream validate invalid actual={:?} stream_id={}",
                self.direction, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate invalid".into(),
            });
            return (effects, 0);
        }

        if !matches!(
            self.state,
            StreamState::HalfClosedRemote | StreamState::Open | StreamState::ResetReceived
        ) {
            if data.is_empty() && end_stream {
                effects.push(Effect::NotifyRequestDone {
                    request_id,
                    result: RequestResult::None,
                });
                return (effects, 0);
            }

            debug!(
                "wt_stream validate failed actual={:?} stream_id={}",
                self.state, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate failed".into(),
            });
            return (effects, 0);
        }

        let data_len = data.len() as u64;
        let current_buffer_size = self.write_buffer_size;

        if current_buffer_size + data_len > self.max_write_buffer_size {
            debug!(
                "wt_stream validate exceeded actual={} limit={} stream_id={}",
                current_buffer_size + data_len,
                self.max_write_buffer_size,
                self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream validate exceeded".into(),
            });
            return (effects, 0);
        }

        if !self.write_buffer.is_empty() {
            self.write_buffer.push_back((data, request_id, end_stream));
            self.write_buffer_size += data_len;
            return (effects, 0);
        }

        if data_len <= available_credit {
            self.bytes_sent += data_len;
            effects.push(Effect::SendQuicData {
                stream_id: self.id,
                data,
                end_stream,
            });
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });

            if end_stream {
                match self.state {
                    StreamState::HalfClosedRemote | StreamState::ResetReceived => {
                        self.closed_at = Some(now);
                        self.state = StreamState::Closed;
                        effects.push(Effect::EmitStreamEvent {
                            stream_id: self.id,
                            event_type: EventType::StreamClosed,
                            session_id: None,
                            direction: None,
                            is_peer_initiated: None,
                            error_code: None,
                        });
                    }
                    StreamState::Open => {
                        self.state = StreamState::HalfClosedLocal;
                    }
                    _ => {}
                }
            }
            (effects, data_len)
        } else if available_credit > 0 {
            let sendable = usize::try_from(available_credit).unwrap_or(usize::MAX);
            let data_to_send = data.slice(0..sendable);
            let remaining_data = data.slice(sendable..);
            let remaining_len = remaining_data.len() as u64;

            self.bytes_sent += available_credit;
            effects.push(Effect::SendQuicData {
                stream_id: self.id,
                data: data_to_send,
                end_stream: false,
            });

            self.write_buffer
                .push_front((remaining_data, request_id, end_stream));
            self.write_buffer_size += remaining_len;

            (effects, available_credit)
        } else {
            self.write_buffer.push_back((data, request_id, end_stream));
            self.write_buffer_size += data_len;

            (effects, 0)
        }
    }

    // Internal diagnostics snapshot creation.
    fn diagnostics_snapshot(&self) -> StreamDiagnostics {
        StreamDiagnostics {
            bytes_received: self.bytes_received,
            bytes_sent: self.bytes_sent,
            close_code: self.close_code,
            close_reason: self.close_reason.clone(),
            closed_at: self.closed_at,
            created_at: self.created_at,
            direction: self.direction,
            is_peer_initiated: self.is_peer_initiated,
            read_buffer_size: self.read_buffer_size,
            session_id: self.session_id,
            state: self.state,
            stream_id: self.id,
            write_buffer_size: self.write_buffer_size,
        }
    }

    // Read size clamping calculation.
    fn limit_read(requested_bytes: u64, buffer_size: u64) -> u64 {
        if requested_bytes == 0 {
            buffer_size
        } else {
            std::cmp::min(requested_bytes, buffer_size)
        }
    }

    // Read buffer chunk extraction logic.
    fn take_data(&mut self, max_bytes: u64) -> Bytes {
        if max_bytes == 0 || self.read_buffer_size == 0 {
            return Bytes::new();
        }

        let mut chunks = Vec::new();
        let mut bytes_collected = 0;

        while bytes_collected < max_bytes {
            let Some(chunk) = self.read_buffer.pop_front() else {
                break;
            };

            let chunk_len = chunk.len() as u64;
            let needed = max_bytes - bytes_collected;

            if chunk_len <= needed {
                chunks.push(chunk);
                bytes_collected += chunk_len;
                self.read_buffer_size -= chunk_len;
            } else {
                let usize_needed = usize::try_from(needed).unwrap_or(usize::MAX);
                if chunk_len <= OPTIMIZED_READ_SLICE_SIZE || needed >= chunk_len / 2 {
                    let part = chunk.slice(0..usize_needed);
                    let remainder = chunk.slice(usize_needed..);
                    chunks.push(part);
                    self.read_buffer_size -= needed;
                    self.read_buffer.push_front(remainder);
                } else {
                    let mut isolated_buffer = bytes::BytesMut::with_capacity(usize_needed);
                    isolated_buffer.put(chunk.slice(0..usize_needed));
                    let remainder = chunk.slice(usize_needed..);
                    chunks.push(isolated_buffer.freeze());
                    self.read_buffer_size -= needed;
                    self.read_buffer.push_front(remainder);
                }
                break;
            }
        }

        if chunks.len() == 1 {
            chunks.pop().unwrap_or_default()
        } else {
            let total_len = chunks.iter().map(Bytes::len).sum();
            let mut merged = bytes::BytesMut::with_capacity(total_len);
            for c in chunks {
                merged.put(c);
            }
            merged.freeze()
        }
    }
}

#[cfg(test)]
mod tests;
