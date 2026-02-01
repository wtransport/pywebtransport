//! Single stream state machine and logic entity.

use std::collections::VecDeque;

use bytes::{BufMut, Bytes};
use serde::Serialize;
use tracing::{debug, error, warn};

use crate::common::constants::{
    ERR_LIB_STREAM_STATE_ERROR, ERR_WT_APPLICATION_ERROR_FIRST, ERR_WT_APPLICATION_ERROR_LAST,
    ERR_WT_FLOW_CONTROL_ERROR, WT_DATA_BLOCKED_TYPE,
};
use crate::common::types::{
    ErrorCode, EventType, RequestId, SessionId, StreamDirection, StreamId, StreamState,
};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::utils::{http_to_wt_error, write_varint, wt_to_http_error};

// Threshold for zero-copy slicing optimization (32KB).
const OPTIMIZED_READ_SLICE_THRESHOLD: u64 = 32 * 1024;

// Single WebTransport stream state machine.
#[derive(Debug)]
pub(crate) struct Stream {
    pub(crate) id: StreamId,
    pub(crate) session_id: SessionId,
    pub(crate) direction: StreamDirection,
    pub(crate) state: StreamState,
    pub(crate) created_at: f64,
    pub(crate) bytes_sent: u64,
    pub(crate) bytes_received: u64,
    pub(crate) read_buffer: VecDeque<Bytes>,
    pub(crate) read_buffer_size: u64,
    pub(crate) pending_read_requests: VecDeque<(RequestId, u64)>,
    pub(crate) write_buffer: VecDeque<(Bytes, RequestId, bool)>,
    pub(crate) write_buffer_size: u64,
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
    pub(crate) max_read_buffer_size: u64,
    pub(crate) max_write_buffer_size: u64,
}

impl Stream {
    // Stream entity initialization.
    pub(crate) fn new(
        id: StreamId,
        session_id: SessionId,
        direction: StreamDirection,
        created_at: f64,
        max_read_buffer_size: u64,
        max_write_buffer_size: u64,
    ) -> Self {
        Self {
            id,
            session_id,
            direction,
            state: StreamState::Open,
            created_at,
            bytes_sent: 0,
            bytes_received: 0,
            read_buffer: VecDeque::new(),
            read_buffer_size: 0,
            pending_read_requests: VecDeque::new(),
            write_buffer: VecDeque::new(),
            write_buffer_size: 0,
            close_code: None,
            close_reason: None,
            closed_at: None,
            max_read_buffer_size,
            max_write_buffer_size,
        }
    }

    // User diagnostics event handling.
    pub(crate) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diag = self.diagnostics_snapshot();
        let json_str = serde_json::to_string(&diag).unwrap_or_else(|_| "{}".to_owned());
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::Diagnostics(json_str),
        }]
    }

    // Write buffer flushing with flow control.
    pub(crate) fn flush_writes(
        &mut self,
        available_credit: u64,
        peer_max_data: u64,
    ) -> (Vec<Effect>, u64) {
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
                            self.state = StreamState::Closed;
                            effects.push(Effect::EmitStreamEvent {
                                stream_id: self.id,
                                event_type: EventType::StreamClosed,
                                direction: None,
                                session_id: None,
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

        if !self.write_buffer.is_empty() {
            let mut buf = bytes::BytesMut::with_capacity(8);
            if let Err(e) = write_varint(&mut buf, peer_max_data) {
                error!("Internal error: Failed to encode peer_max_data for DATA_BLOCKED: {e:?}");
            } else {
                effects.push(Effect::SendH3Capsule {
                    stream_id: self.session_id,
                    capsule_type: WT_DATA_BLOCKED_TYPE,
                    capsule_data: buf.freeze(),
                    end_stream: false,
                });
            }
        }

        (effects, total_sent)
    }

    // User read request handling.
    pub(crate) fn read(&mut self, request_id: RequestId, max_bytes: u64) -> (Vec<Effect>, u64) {
        let mut effects = Vec::new();

        if self.read_buffer_size > 0 {
            let target = Self::limit_read(max_bytes, self.read_buffer_size);
            let data_to_return = self.take_data(target);
            let consumed = data_to_return.len() as u64;

            debug!(
                "Stream {} read {} bytes (requested {})",
                self.id, consumed, max_bytes
            );

            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::ReadData(data_to_return),
            });

            return (effects, consumed);
        }

        if self.state == StreamState::Closed
            && (self.close_code.is_none() || self.close_code == Some(0))
        {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::ReadData(Bytes::new()),
            });
            return (effects, 0);
        }

        if matches!(self.state, StreamState::ResetReceived | StreamState::Closed) {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: format!(
                    "Stream {} receive side closed (state: {:?})",
                    self.id, self.state
                ),
            });
            return (effects, 0);
        }

        if self.state == StreamState::HalfClosedRemote {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::ReadData(Bytes::new()),
            });
            return (effects, 0);
        }

        self.pending_read_requests
            .push_back((request_id, max_bytes));
        (effects, 0)
    }

    // Network data reception handling.
    pub(crate) fn recv_data(
        &mut self,
        data: Bytes,
        end_stream: bool,
        now: f64,
    ) -> (Vec<Effect>, u64) {
        let mut effects = Vec::new();
        let mut consumed_bytes_by_reads = 0;

        if matches!(self.state, StreamState::ResetReceived | StreamState::Closed) {
            return (effects, 0);
        }

        if !data.is_empty() {
            let data_len = data.len() as u64;
            if self.read_buffer_size + data_len > self.max_read_buffer_size {
                warn!(
                    "Stream {} read buffer limit exceeded ({} + {} > {}). Sending STOP_SENDING.",
                    self.id, self.read_buffer_size, data_len, self.max_read_buffer_size
                );
                effects.push(Effect::StopQuicStream {
                    stream_id: self.id,
                    error_code: ERR_WT_FLOW_CONTROL_ERROR,
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

                debug!(
                    "Stream {} read {} bytes (requested {})",
                    self.id,
                    data_chunk.len(),
                    max_bytes
                );

                effects.push(Effect::NotifyRequestDone {
                    request_id: req_id,
                    result: RequestResult::ReadData(data_chunk),
                });
            }
        }

        if end_stream {
            match self.state {
                StreamState::HalfClosedLocal | StreamState::ResetSent => {
                    if self.read_buffer_size == 0 {
                        self.state = StreamState::Closed;
                        self.closed_at = Some(now);
                        effects.push(Effect::EmitStreamEvent {
                            stream_id: self.id,
                            event_type: EventType::StreamClosed,
                            direction: None,
                            session_id: None,
                        });
                    } else {
                        self.state = StreamState::HalfClosedRemote;
                        debug!(
                            "Stream {} rx closed, data pending read. Moving to HALF_CLOSED_REMOTE",
                            self.id
                        );
                    }
                }
                StreamState::Open => {
                    self.state = StreamState::HalfClosedRemote;
                    debug!("Stream {} receive side closed by peer (WT data)", self.id);
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
    pub(crate) fn recv_reset(&mut self, error_code: ErrorCode, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state == StreamState::Closed {
            return effects;
        }

        debug!("Stream {} reset by peer with code {}", self.id, error_code);

        let mut app_error_code = error_code;
        if (ERR_WT_APPLICATION_ERROR_FIRST..=ERR_WT_APPLICATION_ERROR_LAST).contains(&error_code) {
            match http_to_wt_error(error_code) {
                Some(code) => app_error_code = code,
                None => {
                    warn!(
                        "Received reserved H3 error code {:x} on stream {}, using as-is.",
                        error_code, self.id
                    );
                }
            }
        }

        self.closed_at = Some(now);
        self.close_code = Some(app_error_code);

        while let Some((req_id, _)) = self.pending_read_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                error_code: Some(app_error_code),
                reason: format!("Stream {} reset by peer", self.id),
            });
        }

        while let Some((_, req_id, _)) = self.write_buffer.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                error_code: Some(app_error_code),
                reason: format!("Stream {} reset by peer", self.id),
            });
        }
        self.write_buffer_size = 0;

        self.state = StreamState::Closed;
        effects.push(Effect::EmitStreamEvent {
            stream_id: self.id,
            event_type: EventType::StreamClosed,
            direction: None,
            session_id: None,
        });

        effects
    }

    // User reset command handling.
    pub(crate) fn reset(
        &mut self,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if matches!(
            self.state,
            StreamState::HalfClosedLocal | StreamState::Closed | StreamState::ResetSent
        ) {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
            return effects;
        }

        let previous_state = self.state;
        self.state = StreamState::ResetSent;
        self.closed_at = Some(now);
        self.close_code = Some(error_code);

        let http_error_code = wt_to_http_error(error_code).unwrap_or(error_code);

        effects.push(Effect::ResetQuicStream {
            stream_id: self.id,
            error_code: http_error_code,
        });

        while let Some((_, req_id, _)) = self.write_buffer.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                error_code: Some(error_code),
                reason: format!("Stream {} reset by application", self.id),
            });
        }
        self.write_buffer_size = 0;

        match previous_state {
            StreamState::HalfClosedRemote | StreamState::ResetReceived => {
                self.state = StreamState::Closed;
                effects.push(Effect::EmitStreamEvent {
                    stream_id: self.id,
                    event_type: EventType::StreamClosed,
                    direction: None,
                    session_id: None,
                });
            }
            _ => {}
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        debug!(
            "Stream {} reset locally with code {} (mapped to {:x})",
            self.id, error_code, http_error_code
        );

        effects
    }

    // User stop command handling.
    pub(crate) fn stop(
        &mut self,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if matches!(
            self.state,
            StreamState::HalfClosedRemote | StreamState::Closed | StreamState::ResetReceived
        ) {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
            return effects;
        }

        let previous_state = self.state;
        self.state = StreamState::ResetReceived;
        self.closed_at = Some(now);
        self.close_code = Some(error_code);

        let http_error_code = wt_to_http_error(error_code).unwrap_or(error_code);

        effects.push(Effect::StopQuicStream {
            stream_id: self.id,
            error_code: http_error_code,
        });

        while let Some((req_id, _)) = self.pending_read_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                error_code: Some(error_code),
                reason: format!("Stream {} stopped by application", self.id),
            });
        }

        match previous_state {
            StreamState::HalfClosedLocal | StreamState::ResetSent => {
                self.state = StreamState::Closed;
                effects.push(Effect::EmitStreamEvent {
                    stream_id: self.id,
                    event_type: EventType::StreamClosed,
                    direction: None,
                    session_id: None,
                });
            }
            _ => {}
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        debug!(
            "Stream {} receive side stopped locally with code {} (mapped to {:x})",
            self.id, error_code, http_error_code
        );

        effects
    }

    // Unread data operation handling.
    pub(crate) fn unread(&mut self, data: Bytes) -> Vec<Effect> {
        if !data.is_empty() {
            let len = data.len() as u64;
            self.read_buffer.push_front(data);
            self.read_buffer_size += len;
        }
        Vec::new()
    }

    // User write request handling.
    pub(crate) fn write(
        &mut self,
        request_id: RequestId,
        data: Bytes,
        end_stream: bool,
        available_credit: u64,
        peer_max_data: u64,
    ) -> (Vec<Effect>, u64) {
        let mut effects = Vec::new();

        if matches!(
            self.state,
            StreamState::HalfClosedLocal | StreamState::Closed | StreamState::ResetSent
        ) {
            if data.is_empty() && end_stream {
                effects.push(Effect::NotifyRequestDone {
                    request_id,
                    result: RequestResult::None,
                });
                return (effects, 0);
            }

            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: format!(
                    "Stream {} is not writable (state: {:?})",
                    self.id, self.state
                ),
            });
            return (effects, 0);
        }

        let data_len = data.len() as u64;
        let current_buffer_size = self.write_buffer_size;

        if current_buffer_size + data_len > self.max_write_buffer_size {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: format!(
                    "Stream {} write buffer full ({} + {} > {} bytes)",
                    self.id, current_buffer_size, data_len, self.max_write_buffer_size
                ),
            });
            return (effects, 0);
        }

        if !self.write_buffer.is_empty() {
            debug!("Stream {} write added to existing write buffer", self.id);
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
                        self.state = StreamState::Closed;
                        effects.push(Effect::EmitStreamEvent {
                            stream_id: self.id,
                            event_type: EventType::StreamClosed,
                            direction: None,
                            session_id: None,
                        });
                    }
                    StreamState::Open => {
                        self.state = StreamState::HalfClosedLocal;
                    }
                    _ => {}
                }
                debug!("Stream {} send side closed", self.id);
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

            debug!(
                "Stream {} partial send: sent {} bytes, buffering {} bytes",
                self.id,
                available_credit,
                remaining_data.len()
            );

            self.write_buffer
                .push_back((remaining_data, request_id, end_stream));
            self.write_buffer_size += remaining_len;

            let mut buf = bytes::BytesMut::with_capacity(8);
            if let Err(e) = write_varint(&mut buf, peer_max_data) {
                error!("Internal error: Failed to encode peer_max_data for DATA_BLOCKED: {e:?}");
            } else {
                effects.push(Effect::SendH3Capsule {
                    stream_id: self.session_id,
                    capsule_type: WT_DATA_BLOCKED_TYPE,
                    capsule_data: buf.freeze(),
                    end_stream: false,
                });
            }

            (effects, available_credit)
        } else {
            debug!(
                "Stream {} write blocked by session flow control ({} > {})",
                self.id, data_len, available_credit
            );
            self.write_buffer.push_back((data, request_id, end_stream));
            self.write_buffer_size += data_len;

            let mut buf = bytes::BytesMut::with_capacity(8);
            if let Err(e) = write_varint(&mut buf, peer_max_data) {
                error!("Internal error: Failed to encode peer_max_data for DATA_BLOCKED: {e:?}");
            } else {
                effects.push(Effect::SendH3Capsule {
                    stream_id: self.session_id,
                    capsule_type: WT_DATA_BLOCKED_TYPE,
                    capsule_data: buf.freeze(),
                    end_stream: false,
                });
            }

            (effects, 0)
        }
    }

    // Internal diagnostics snapshot creation.
    fn diagnostics_snapshot(&self) -> StreamDiagnostics {
        StreamDiagnostics {
            stream_id: self.id,
            session_id: self.session_id,
            direction: self.direction,
            state: self.state,
            created_at: self.created_at,
            bytes_sent: self.bytes_sent,
            bytes_received: self.bytes_received,
            read_buffer_size: self.read_buffer_size,
            write_buffer_size: self.write_buffer_size,
            close_code: self.close_code,
            close_reason: self.close_reason.clone(),
            closed_at: self.closed_at,
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
        let Some(head_chunk) = self.read_buffer.front() else {
            return Bytes::new();
        };

        let head_len = head_chunk.len() as u64;

        if head_len >= max_bytes
            && (head_len == max_bytes || head_len <= OPTIMIZED_READ_SLICE_THRESHOLD)
        {
            let chunk = self.read_buffer.pop_front().unwrap_or_default();
            self.read_buffer_size -= max_bytes;

            if head_len == max_bytes {
                return chunk;
            }

            let usize_max_bytes = usize::try_from(max_bytes).unwrap_or(usize::MAX);
            let result = chunk.slice(0..usize_max_bytes);
            let remainder = chunk.slice(usize_max_bytes..);
            self.read_buffer.push_front(remainder);
            return result;
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
                let part = chunk.slice(0..usize_needed);
                let remainder = chunk.slice(usize_needed..);
                chunks.push(part);
                self.read_buffer_size -= needed;
                self.read_buffer.push_front(remainder);
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

// Diagnostic information snapshot for a stream.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct StreamDiagnostics {
    pub(crate) stream_id: StreamId,
    pub(crate) session_id: SessionId,
    pub(crate) direction: StreamDirection,
    pub(crate) state: StreamState,
    pub(crate) created_at: f64,
    pub(crate) bytes_sent: u64,
    pub(crate) bytes_received: u64,
    pub(crate) read_buffer_size: u64,
    pub(crate) write_buffer_size: u64,
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
}

#[cfg(test)]
mod tests;
