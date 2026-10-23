//! Session state machine.

use std::borrow::Cow;
use std::cmp;
use std::collections::VecDeque;
use std::io::Cursor;
use std::slice;

use bytes::{BufMut, Bytes, BytesMut};
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::debug;

use crate::common::constants::{
    ERR_APP_NO_ERROR, ERR_H3_EXCESSIVE_LOAD, ERR_H3_FRAME_UNEXPECTED,
    ERR_H3_GENERAL_PROTOCOL_ERROR, ERR_H3_MESSAGE_ERROR, ERR_LIB_SESSION_STATE_ERROR,
    ERR_LIB_STREAM_STATE_ERROR, ERR_WT_BUFFERED_STREAM_REJECTED, ERR_WT_FLOW_CONTROL_ERROR,
    ERR_WT_SESSION_GONE, WT_CAPSULE_TYPE_CLOSE_SESSION, WT_CAPSULE_TYPE_DATA_BLOCKED,
    WT_CAPSULE_TYPE_DRAIN_SESSION, WT_CAPSULE_TYPE_MAX_DATA, WT_CAPSULE_TYPE_MAX_STREAM_DATA,
    WT_CAPSULE_TYPE_MAX_STREAMS_BIDI, WT_CAPSULE_TYPE_MAX_STREAMS_UNI,
    WT_CAPSULE_TYPE_STREAM_DATA_BLOCKED, WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
    WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI, WT_CLOSE_REASON_SIZE_LIMIT, WT_EXPORTER_LABEL,
    WT_PROTOCOL, WT_STREAMS_LIMIT,
};
use crate::common::types::{
    ErrorCode, ErrorSource, EventType, Headers, RequestId, SessionId, SessionState,
    StreamDirection, StreamId,
};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::stream::{Stream, StreamParameters};
use crate::protocol::utils;

#[derive(Clone, Debug)]
pub(crate) struct SessionDiagnostics {
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
    pub(crate) created_at: f64,
    pub(crate) data_consumed: u64,
    pub(crate) data_received: u64,
    pub(crate) data_sent: u64,
    pub(crate) flow_control_negotiated: bool,
    pub(crate) headers: Headers,
    pub(crate) is_client: bool,
    pub(crate) local_max_data: u64,
    pub(crate) local_max_streams_bidi: u64,
    pub(crate) local_max_streams_uni: u64,
    pub(crate) local_streams_bidi_opened: u64,
    pub(crate) local_streams_uni_opened: u64,
    pub(crate) path: String,
    pub(crate) peer_max_data: u64,
    pub(crate) peer_max_streams_bidi: u64,
    pub(crate) peer_max_streams_uni: u64,
    pub(crate) peer_streams_bidi_closed: u64,
    pub(crate) peer_streams_bidi_opened: u64,
    pub(crate) peer_streams_uni_closed: u64,
    pub(crate) peer_streams_uni_opened: u64,
    pub(crate) ready_at: Option<f64>,
    pub(crate) session_id: SessionId,
    pub(crate) state: SessionState,
    pub(crate) wt_available_protocols: Option<Vec<String>>,
    pub(crate) wt_protocol: Option<String>,
}

pub(super) struct Session {
    blocked_stream_requests_bidi: VecDeque<RequestId>,
    blocked_stream_requests_uni: VecDeque<RequestId>,
    blocked_streams: FxHashSet<StreamId>,
    blocked_streams_queue: VecDeque<StreamId>,
    close_code: Option<ErrorCode>,
    close_reason: Option<String>,
    closed_at: Option<f64>,
    created_at: f64,
    data_consumed: u64,
    data_received: u64,
    data_sent: u64,
    flow_control_negotiated: bool,
    headers: Headers,
    id: SessionId,
    initial_max_streams_bidi: u64,
    initial_max_streams_uni: u64,
    is_client: bool,
    last_data_blocked_sent: Option<u64>,
    last_streams_blocked_bidi_sent: Option<u64>,
    last_streams_blocked_uni_sent: Option<u64>,
    local_max_data: u64,
    local_max_streams_bidi: u64,
    local_max_streams_uni: u64,
    local_streams_bidi_opened: u64,
    local_streams_uni_opened: u64,
    max_pending_capsules: u64,
    max_pending_datagrams: u64,
    max_pending_streams: u64,
    max_write_buffer_size: u64,
    path: String,
    peer_max_data: u64,
    peer_max_streams_bidi: u64,
    peer_max_streams_uni: u64,
    peer_streams_bidi_closed: u64,
    peer_streams_bidi_opened: u64,
    peer_streams_uni_closed: u64,
    peer_streams_uni_opened: u64,
    pending_capsules: VecDeque<(u64, Bytes)>,
    pending_datagram_requests: VecDeque<RequestId>,
    pending_datagrams: VecDeque<Bytes>,
    pending_streams: VecDeque<StreamId>,
    ready_at: Option<f64>,
    receive_window: u64,
    state: SessionState,
    streams: FxHashMap<StreamId, Stream>,
    wt_available_protocols: Option<Vec<String>>,
    wt_protocol: Option<String>,
}

impl Session {
    pub(super) fn new(
        id: SessionId,
        path: String,
        headers: Headers,
        wt_available_protocols: Option<Vec<String>>,
        is_client: bool,
        parameters: SessionParameters,
        created_at: f64,
    ) -> Self {
        Self {
            blocked_stream_requests_bidi: VecDeque::new(),
            blocked_stream_requests_uni: VecDeque::new(),
            blocked_streams: FxHashSet::default(),
            blocked_streams_queue: VecDeque::new(),
            close_code: None,
            close_reason: None,
            closed_at: None,
            created_at,
            data_consumed: 0,
            data_received: 0,
            data_sent: 0,
            flow_control_negotiated: parameters.flow_control_negotiated,
            headers,
            id,
            initial_max_streams_bidi: parameters.initial_max_streams_bidi,
            initial_max_streams_uni: parameters.initial_max_streams_uni,
            is_client,
            last_data_blocked_sent: None,
            last_streams_blocked_bidi_sent: None,
            last_streams_blocked_uni_sent: None,
            local_max_data: parameters.initial_max_data,
            local_max_streams_bidi: parameters.initial_max_streams_bidi,
            local_max_streams_uni: parameters.initial_max_streams_uni,
            local_streams_bidi_opened: 0,
            local_streams_uni_opened: 0,
            max_pending_capsules: parameters.max_pending_capsules,
            max_pending_datagrams: parameters.max_pending_datagrams,
            max_pending_streams: parameters.max_pending_streams,
            max_write_buffer_size: parameters.max_write_buffer_size,
            path,
            peer_max_data: parameters.peer_max_data,
            peer_max_streams_bidi: parameters.peer_max_streams_bidi,
            peer_max_streams_uni: parameters.peer_max_streams_uni,
            peer_streams_bidi_closed: 0,
            peer_streams_bidi_opened: 0,
            peer_streams_uni_closed: 0,
            peer_streams_uni_opened: 0,
            pending_capsules: VecDeque::new(),
            pending_datagram_requests: VecDeque::new(),
            pending_datagrams: VecDeque::new(),
            pending_streams: VecDeque::new(),
            ready_at: None,
            receive_window: parameters.receive_window,
            state: SessionState::Connecting,
            streams: FxHashMap::default(),
            wt_available_protocols,
            wt_protocol: None,
        }
    }

    pub(super) fn abort(
        &mut self,
        error_code: ErrorCode,
        reason: Cow<'static, str>,
        now: f64,
    ) -> Vec<Effect> {
        if matches!(self.state, SessionState::Closed | SessionState::Closing) {
            return Vec::new();
        }

        debug!(
            "wt_session abort actual={:?} session_id={} err={error_code}",
            self.state, self.id
        );
        self.close_code = Some(error_code);
        self.close_reason = Some(reason.clone().into_owned());
        self.closed_at = Some(now);
        self.state = SessionState::Closed;
        self.pending_capsules.clear();
        self.pending_datagrams.clear();

        let mut effects = self.abort_streams();
        while let Some(request_id) = self.pending_datagram_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session abort".into(),
            });
        }

        effects.push(Effect::ResetQuicStream {
            stream_id: self.id,
            error_code,
        });
        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionClosed,
            path: None,
            headers: None,
            wt_available_protocols: None,
            wt_protocol: None,
            data: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            ready_at: None,
            status_code: None,
            error_code: Some(error_code),
            reason: Some(reason),
        });

        effects
    }

    pub(super) fn accept(
        &mut self,
        request_id: RequestId,
        headers: Headers,
        wt_protocol: Option<String>,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.is_client {
            debug!(
                "wt_session validate failed actual={} expected=false request_id={request_id} session_id={}",
                self.is_client, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session validate failed".into(),
            });
            return effects;
        }

        if self.state != SessionState::Connecting {
            debug!(
                "wt_session validate failed actual={:?} expected=connecting request_id={request_id} session_id={}",
                self.state, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: "wt_session validate failed".into(),
            });
            return effects;
        }

        if !utils::is_valid_user_headers(&headers, false) {
            debug!(
                "h3_headers validate invalid request_id={request_id} session_id={}",
                self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "h3_headers validate invalid".into(),
            });
            return effects;
        }

        let mut response_headers: Headers =
            vec![(Bytes::from_static(b":status"), Bytes::from("200"))];
        response_headers.extend(headers);

        if let Some(ref wt_protocol) = wt_protocol {
            let encoded = match utils::encode_wt_protocol_list(slice::from_ref(wt_protocol)) {
                Ok(encoded) => encoded,
                Err(e) => {
                    debug!(
                        "wt_protocol encode failed request_id={request_id} session_id={} err={e:?}",
                        self.id
                    );
                    effects.push(Effect::NotifyRequestFailed {
                        request_id,
                        source: ErrorSource::Session,
                        error_code: None,
                        reason: "wt_protocol encode failed".into(),
                    });
                    return effects;
                }
            };

            if !self
                .wt_available_protocols
                .as_ref()
                .is_some_and(|protocols| protocols.contains(wt_protocol))
            {
                debug!(
                    "wt_protocol validate invalid actual={wt_protocol} expected=wt_available_protocols request_id={request_id} session_id={}",
                    self.id
                );
                effects.push(Effect::NotifyRequestFailed {
                    request_id,
                    source: ErrorSource::Session,
                    error_code: None,
                    reason: "wt_protocol validate invalid".into(),
                });
                return effects;
            }

            response_headers.push((Bytes::from_static(WT_PROTOCOL), encoded));
        }

        debug!(
            "wt_session open actual={:?} request_id={request_id} session_id={}",
            self.state, self.id
        );
        self.ready_at = Some(now);
        self.wt_protocol.clone_from(&wt_protocol);
        self.state = SessionState::Connected;

        effects.push(Effect::SendH3Headers {
            stream_id: self.id,
            headers: response_headers,
            end_stream: false,
        });
        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionReady,
            path: None,
            headers: None,
            wt_available_protocols: None,
            wt_protocol,
            data: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            ready_at: Some(now),
            status_code: None,
            error_code: None,
            reason: None,
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects.extend(self.replay_pending_queues(now));

        effects
    }

    pub(super) fn bind_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        is_unidirectional: bool,
        now: f64,
    ) -> Vec<Effect> {
        let direction = if is_unidirectional {
            StreamDirection::SendOnly
        } else {
            StreamDirection::Bidirectional
        };

        if !matches!(
            self.state,
            SessionState::Connected | SessionState::Connecting | SessionState::Draining
        ) {
            debug!(
                "wt_session validate failed actual={:?} request_id={request_id} session_id={} stream_id={stream_id}",
                self.state, self.id
            );
            let mut effects = vec![Effect::ResetQuicStream {
                stream_id,
                error_code: ERR_WT_SESSION_GONE,
            }];
            if direction == StreamDirection::Bidirectional {
                effects.push(Effect::StopQuicStream {
                    stream_id,
                    error_code: ERR_WT_SESSION_GONE,
                });
            }
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: "wt_session validate failed".into(),
            });
            return effects;
        }

        let is_peer_initiated = utils::is_peer_initiated_stream(stream_id, self.is_client);

        let stream = Stream::new(
            stream_id,
            self.id,
            direction,
            is_peer_initiated,
            StreamParameters {
                max_write_buffer_size: self.max_write_buffer_size,
            },
            now,
        );

        self.streams.insert(stream_id, stream);

        vec![
            Effect::EmitStreamEvent {
                stream_id,
                event_type: EventType::StreamOpened,
                session_id: Some(self.id),
                direction: Some(direction),
                is_peer_initiated: Some(is_peer_initiated),
                error_code: None,
            },
            Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::StreamId(stream_id),
            },
        ]
    }

    pub(super) fn close(
        &mut self,
        request_id: RequestId,
        error_code: ErrorCode,
        reason: Option<Cow<'static, str>>,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if matches!(self.state, SessionState::Closed | SessionState::Closing) {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
            return effects;
        }

        debug!(
            "wt_session close actual={:?} request_id={request_id} session_id={} err={error_code}",
            self.state, self.id
        );
        self.close_code = Some(error_code);
        self.close_reason = reason.clone().map(Cow::into_owned);
        self.closed_at = Some(now);
        self.state = SessionState::Closing;
        self.pending_capsules.clear();
        self.pending_datagrams.clear();

        effects.extend(self.abort_streams());
        while let Some(request_id) = self.pending_datagram_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session abort".into(),
            });
        }

        let is_clean_close =
            error_code == ERR_APP_NO_ERROR && reason.as_deref().unwrap_or("").is_empty();

        if is_clean_close {
            effects.push(Effect::SendQuicData {
                stream_id: self.id,
                data: Bytes::new(),
                end_stream: true,
            });
        } else {
            let reason_str = reason.as_deref().unwrap_or("");
            let limit = usize::try_from(WT_CLOSE_REASON_SIZE_LIMIT).unwrap_or(usize::MAX);
            let safe_limit = reason_str.floor_char_boundary(limit);
            let truncated_reason = reason_str.as_bytes().get(..safe_limit).unwrap_or_default();

            let mut buffer = BytesMut::with_capacity(4 + truncated_reason.len());
            buffer.put_u32(u32::try_from(error_code).unwrap_or(u32::MAX));
            buffer.put_slice(truncated_reason);

            effects.push(Effect::SendH3Capsule {
                stream_id: self.id,
                capsule_type: WT_CAPSULE_TYPE_CLOSE_SESSION,
                capsule_value: buffer.freeze(),
                end_stream: true,
            });
        }

        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionClosed,
            path: None,
            headers: None,
            wt_available_protocols: None,
            wt_protocol: None,
            data: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            ready_at: None,
            status_code: None,
            error_code: Some(error_code),
            reason,
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    pub(super) fn complete_stream_write(&mut self, stream_id: StreamId) -> Vec<Effect> {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Vec::new();
        };

        let session_credit = if self.flow_control_negotiated {
            self.peer_max_data.saturating_sub(self.data_sent)
        } else {
            u64::MAX
        };

        let (mut effects, sent) = stream.complete_write(session_credit);
        let is_blocked = stream.is_blocked();

        self.data_sent += sent;

        if is_blocked && self.blocked_streams.insert(stream_id) {
            self.blocked_streams_queue.push_back(stream_id);
        }

        if let Some(effect) = self.emit_data_blocked() {
            effects.push(effect);
        }

        effects
    }

    pub(super) fn confirm(&mut self, wt_protocol: Option<String>, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state != SessionState::Connecting {
            return effects;
        }

        debug!(
            "wt_session open actual={:?} session_id={}",
            self.state, self.id
        );
        self.ready_at = Some(now);
        self.wt_protocol.clone_from(&wt_protocol);
        self.state = SessionState::Connected;

        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionReady,
            path: Some(self.path.clone()),
            headers: Some(self.headers.clone()),
            wt_available_protocols: None,
            wt_protocol,
            data: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            ready_at: Some(now),
            status_code: None,
            error_code: None,
            reason: None,
        });

        effects.extend(self.replay_pending_queues(now));

        effects
    }

    pub(super) fn create_stream(
        &mut self,
        request_id: RequestId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !matches!(
            self.state,
            SessionState::Connected | SessionState::Connecting | SessionState::Draining
        ) {
            debug!(
                "wt_session validate failed actual={:?} request_id={request_id} session_id={}",
                self.state, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: "wt_session validate failed".into(),
            });
            return effects;
        }

        let limit_exceeded = self.flow_control_negotiated
            && if is_unidirectional {
                self.local_streams_uni_opened >= self.peer_max_streams_uni
            } else {
                self.local_streams_bidi_opened >= self.peer_max_streams_bidi
            };

        if limit_exceeded {
            if is_unidirectional {
                debug!(
                    "wt_session validate exceeded actual={} limit={} request_id={request_id} session_id={}",
                    self.local_streams_uni_opened, self.peer_max_streams_uni, self.id
                );
                self.blocked_stream_requests_uni.push_back(request_id);
            } else {
                debug!(
                    "wt_session validate exceeded actual={} limit={} request_id={request_id} session_id={}",
                    self.local_streams_bidi_opened, self.peer_max_streams_bidi, self.id
                );
                self.blocked_stream_requests_bidi.push_back(request_id);
            }

            if let Some(effect) = self.emit_streams_blocked(is_unidirectional) {
                effects.push(effect);
            }
            return effects;
        }

        if is_unidirectional {
            self.local_streams_uni_opened += 1;
        } else {
            self.local_streams_bidi_opened += 1;
        }

        effects.push(Effect::CreateQuicStream {
            request_id,
            session_id: self.id,
            is_unidirectional,
        });

        effects
    }

    pub(super) fn defer_stream(
        &mut self,
        request_id: RequestId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        if !matches!(
            self.state,
            SessionState::Connected | SessionState::Connecting | SessionState::Draining
        ) {
            debug!(
                "wt_session validate failed actual={:?} request_id={request_id} session_id={}",
                self.state, self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: "wt_session validate failed".into(),
            }];
        }

        if is_unidirectional {
            self.local_streams_uni_opened -= 1;
            self.blocked_stream_requests_uni.push_back(request_id);
        } else {
            self.local_streams_bidi_opened -= 1;
            self.blocked_stream_requests_bidi.push_back(request_id);
        }

        Vec::new()
    }

    pub(super) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diagnostics = self.diagnostics_snapshot();
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::SessionDiagnostics(Box::new(diagnostics)),
        }]
    }

    pub(super) fn diagnose_stream(
        &self,
        stream_id: StreamId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        if let Some(stream) = self.streams.get(&stream_id) {
            return stream.diagnose(request_id);
        }

        debug!(
            "wt_stream resolve failed request_id={request_id} session_id={} stream_id={stream_id}",
            self.id
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            reason: "wt_stream resolve failed".into(),
        }]
    }

    pub(super) fn drain(&mut self) -> Vec<Effect> {
        if self.state != SessionState::Connected {
            return Vec::new();
        }

        debug!(
            "wt_session drain actual={:?} session_id={}",
            self.state, self.id
        );
        self.state = SessionState::Draining;

        vec![
            Effect::SendH3Capsule {
                stream_id: self.id,
                capsule_type: WT_CAPSULE_TYPE_DRAIN_SESSION,
                capsule_value: Bytes::new(),
                end_stream: false,
            },
            Effect::EmitSessionEvent {
                session_id: self.id,
                event_type: EventType::SessionDraining,
                path: None,
                headers: None,
                wt_available_protocols: None,
                wt_protocol: None,
                data: None,
                is_unidirectional: None,
                max_data: None,
                max_streams: None,
                ready_at: None,
                status_code: None,
                error_code: None,
                reason: None,
            },
        ]
    }

    pub(super) fn export_keying_material(
        &self,
        request_id: RequestId,
        label: &str,
        context: &[u8],
        length: u32,
    ) -> Vec<Effect> {
        if !matches!(self.state, SessionState::Connected | SessionState::Draining) {
            debug!(
                "wt_session validate failed actual={:?} request_id={request_id} session_id={}",
                self.state, self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: "wt_session validate failed".into(),
            }];
        }

        if label.len() > usize::from(u8::MAX) || context.len() > usize::from(u8::MAX) {
            let len = cmp::max(label.len(), context.len());
            debug!(
                "wt_session validate invalid actual={len} request_id={request_id} session_id={}",
                self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session validate invalid".into(),
            }];
        }

        let label_len = u8::try_from(label.len()).unwrap_or(u8::MAX);
        let context_len = u8::try_from(context.len()).unwrap_or(u8::MAX);

        let mut exporter_context = BytesMut::with_capacity(8 + 1 + label.len() + 1 + context.len());
        exporter_context.put_u64(self.id);
        exporter_context.put_u8(label_len);
        exporter_context.put_slice(label.as_bytes());
        exporter_context.put_u8(context_len);
        exporter_context.put_slice(context);

        vec![Effect::ExportTlsKeyingMaterial {
            request_id,
            label: WT_EXPORTER_LABEL.to_owned(),
            context: exporter_context.freeze(),
            length,
        }]
    }

    pub(super) fn flush_blocked_stream_requests(&mut self, is_unidirectional: bool) -> Vec<Effect> {
        let mut effects = Vec::new();

        let (opened, limit, blocked_stream_requests) = if is_unidirectional {
            (
                &mut self.local_streams_uni_opened,
                self.peer_max_streams_uni,
                &mut self.blocked_stream_requests_uni,
            )
        } else {
            (
                &mut self.local_streams_bidi_opened,
                self.peer_max_streams_bidi,
                &mut self.blocked_stream_requests_bidi,
            )
        };

        while (!self.flow_control_negotiated || *opened < limit)
            && let Some(request_id) = blocked_stream_requests.pop_front()
        {
            *opened += 1;
            effects.push(Effect::CreateQuicStream {
                request_id,
                session_id: self.id,
                is_unidirectional,
            });
        }

        if let Some(effect) = self.emit_streams_blocked(is_unidirectional) {
            effects.push(effect);
        }

        effects
    }

    pub(super) fn handle_stream_data_acknowledged(&mut self, stream_id: StreamId) -> Vec<Effect> {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Vec::new();
        };

        stream.handle_data_acknowledged();

        self.handle_closed(stream_id)
    }

    pub(super) fn handle_stream_opened(&mut self, stream_id: StreamId, now: f64) -> Vec<Effect> {
        let direction = utils::stream_direction_from_id(stream_id, self.is_client);

        if !matches!(
            self.state,
            SessionState::Connected | SessionState::Connecting | SessionState::Draining
        ) {
            debug!(
                "wt_session validate failed actual={:?} session_id={} stream_id={stream_id}",
                self.state, self.id
            );
            let mut effects = Vec::new();
            if direction == StreamDirection::Bidirectional {
                effects.push(Effect::ResetQuicStream {
                    stream_id,
                    error_code: ERR_WT_SESSION_GONE,
                });
            }
            effects.push(Effect::StopQuicStream {
                stream_id,
                error_code: ERR_WT_SESSION_GONE,
            });
            return effects;
        }

        match direction {
            StreamDirection::Bidirectional => {
                if self.flow_control_negotiated
                    && self.peer_streams_bidi_opened >= self.local_max_streams_bidi
                {
                    debug!(
                        "wt_session validate exceeded actual={} limit={} session_id={}",
                        self.peer_streams_bidi_opened, self.local_max_streams_bidi, self.id
                    );
                    let mut effects = vec![
                        Effect::ResetQuicStream {
                            stream_id,
                            error_code: ERR_WT_SESSION_GONE,
                        },
                        Effect::StopQuicStream {
                            stream_id,
                            error_code: ERR_WT_SESSION_GONE,
                        },
                    ];
                    effects.extend(self.abort(
                        ERR_WT_FLOW_CONTROL_ERROR,
                        "wt_session validate exceeded".into(),
                        now,
                    ));

                    return effects;
                }
                self.peer_streams_bidi_opened += 1;
            }
            StreamDirection::ReceiveOnly => {
                if self.flow_control_negotiated
                    && self.peer_streams_uni_opened >= self.local_max_streams_uni
                {
                    debug!(
                        "wt_session validate exceeded actual={} limit={} session_id={}",
                        self.peer_streams_uni_opened, self.local_max_streams_uni, self.id
                    );
                    let mut effects = vec![Effect::StopQuicStream {
                        stream_id,
                        error_code: ERR_WT_SESSION_GONE,
                    }];
                    effects.extend(self.abort(
                        ERR_WT_FLOW_CONTROL_ERROR,
                        "wt_session validate exceeded".into(),
                        now,
                    ));

                    return effects;
                }
                self.peer_streams_uni_opened += 1;
            }
            StreamDirection::SendOnly => {
                debug!(
                    "wt_stream validate invalid actual={direction:?} session_id={} stream_id={stream_id}",
                    self.id
                );
                return Vec::new();
            }
        }

        if self.state == SessionState::Connecting {
            if (self.pending_streams.len() as u64) >= self.max_pending_streams {
                debug!(
                    "wt_session validate exceeded actual={} expected=max_pending_streams session_id={}",
                    self.pending_streams.len(),
                    self.id
                );
                let mut effects = Vec::new();
                if direction == StreamDirection::Bidirectional {
                    effects.push(Effect::ResetQuicStream {
                        stream_id,
                        error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                    });
                }
                effects.push(Effect::StopQuicStream {
                    stream_id,
                    error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                });

                if direction == StreamDirection::ReceiveOnly {
                    self.peer_streams_uni_closed += 1;
                    if let Some(effect) = self.replenish_streams_credit(true, false) {
                        effects.push(effect);
                    }
                } else {
                    self.peer_streams_bidi_closed += 1;
                    if let Some(effect) = self.replenish_streams_credit(false, false) {
                        effects.push(effect);
                    }
                }

                return effects;
            }

            self.pending_streams.push_back(stream_id);
        }

        let stream = Stream::new(
            stream_id,
            self.id,
            direction,
            true,
            StreamParameters {
                max_write_buffer_size: self.max_write_buffer_size,
            },
            now,
        );

        self.streams.insert(stream_id, stream);

        if self.state == SessionState::Connecting {
            return Vec::new();
        }

        vec![Effect::EmitStreamEvent {
            stream_id,
            event_type: EventType::StreamOpened,
            session_id: Some(self.id),
            direction: Some(direction),
            is_peer_initiated: Some(true),
            error_code: None,
        }]
    }

    pub(super) fn has_stream(&self, stream_id: StreamId) -> bool {
        self.streams.contains_key(&stream_id)
    }

    pub(super) fn is_closed(&self) -> bool {
        self.state == SessionState::Closed
    }

    pub(super) fn read_datagram(&mut self, request_id: RequestId) -> Vec<Effect> {
        if !matches!(
            self.state,
            SessionState::Connected | SessionState::Connecting | SessionState::Draining
        ) {
            debug!(
                "wt_session validate failed actual={:?} request_id={request_id} session_id={}",
                self.state, self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: "wt_session validate failed".into(),
            }];
        }

        if self.state != SessionState::Connecting
            && let Some(data) = self.pending_datagrams.pop_front()
        {
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::Datagram(data),
            }];
        }

        self.pending_datagram_requests.push_back(request_id);

        Vec::new()
    }

    pub(super) fn read_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        max_bytes: u64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            debug!(
                "wt_stream resolve failed request_id={request_id} session_id={} stream_id={stream_id}",
                self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream resolve failed".into(),
            });
            return effects;
        };

        effects.extend(stream.read(request_id, max_bytes));
        effects.extend(self.handle_closed(stream_id));

        effects
    }

    pub(super) fn recv_capsule(
        &mut self,
        capsule_type: u64,
        capsule_value: &[u8],
        now: f64,
    ) -> Vec<Effect> {
        if self.state == SessionState::Connecting {
            if (self.pending_capsules.len() as u64) >= self.max_pending_capsules {
                debug!(
                    "wt_session validate exceeded actual={} expected=max_pending_capsules session_id={}",
                    self.pending_capsules.len(),
                    self.id
                );
                return self.abort(
                    ERR_H3_EXCESSIVE_LOAD,
                    "wt_session validate exceeded".into(),
                    now,
                );
            }

            self.pending_capsules
                .push_back((capsule_type, Bytes::copy_from_slice(capsule_value)));

            return Vec::new();
        }

        if self.state == SessionState::Closing {
            return Vec::new();
        }

        if self.state == SessionState::Closed {
            debug!(
                "wt_session validate failed actual={:?} session_id={}",
                self.state, self.id
            );
            return vec![Effect::ResetQuicStream {
                stream_id: self.id,
                error_code: ERR_H3_MESSAGE_ERROR,
            }];
        }

        let mut effects = Vec::new();

        if !self.flow_control_negotiated
            && matches!(
                capsule_type,
                WT_CAPSULE_TYPE_DATA_BLOCKED
                    | WT_CAPSULE_TYPE_MAX_DATA
                    | WT_CAPSULE_TYPE_MAX_STREAMS_BIDI
                    | WT_CAPSULE_TYPE_MAX_STREAMS_UNI
                    | WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI
                    | WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI
            )
        {
            debug!(
                "wt_session validate failed actual={capsule_type} session_id={}",
                self.id
            );
            return effects;
        }

        let mut cursor = Cursor::new(capsule_value);

        match capsule_type {
            WT_CAPSULE_TYPE_CLOSE_SESSION => {
                if capsule_value.len() < 4 {
                    debug!(
                        "wt_capsule validate invalid actual={} session_id={}",
                        capsule_value.len(),
                        self.id
                    );
                    return self.abort(
                        ERR_H3_MESSAGE_ERROR,
                        "wt_capsule validate invalid".into(),
                        now,
                    );
                }

                let code_bytes = capsule_value.get(..4).unwrap_or_default();
                let error_code = u64::from(u32::from_be_bytes(
                    code_bytes.try_into().unwrap_or_default(),
                ));
                let payload = capsule_value.get(4..).unwrap_or_default();

                let limit = usize::try_from(WT_CLOSE_REASON_SIZE_LIMIT).unwrap_or(usize::MAX);
                if payload.len() > limit {
                    debug!(
                        "wt_capsule validate exceeded actual={} expected=wt_close_reason_size_limit session_id={}",
                        payload.len(),
                        self.id
                    );
                    return self.abort(
                        ERR_H3_MESSAGE_ERROR,
                        "wt_capsule validate exceeded".into(),
                        now,
                    );
                }

                let Ok(reason) = str::from_utf8(payload) else {
                    debug!(
                        "wt_capsule validate invalid session_id={} size={}",
                        self.id,
                        payload.len()
                    );
                    return self.abort(
                        ERR_H3_MESSAGE_ERROR,
                        "wt_capsule validate invalid".into(),
                        now,
                    );
                };
                let reason = reason.to_owned();

                debug!(
                    "wt_session close actual={:?} session_id={} err={error_code}",
                    self.state, self.id
                );
                self.close_code = Some(error_code);
                self.close_reason = Some(reason.clone());
                self.closed_at = Some(now);
                self.state = SessionState::Closed;
                self.pending_capsules.clear();
                self.pending_datagrams.clear();

                effects.extend(self.abort_streams());
                while let Some(request_id) = self.pending_datagram_requests.pop_front() {
                    effects.push(Effect::NotifyRequestFailed {
                        request_id,
                        source: ErrorSource::Session,
                        error_code: None,
                        reason: "wt_session abort".into(),
                    });
                }

                effects.push(Effect::SendQuicData {
                    stream_id: self.id,
                    data: Bytes::new(),
                    end_stream: true,
                });
                effects.push(Effect::EmitSessionEvent {
                    session_id: self.id,
                    event_type: EventType::SessionClosed,
                    path: None,
                    headers: None,
                    wt_available_protocols: None,
                    wt_protocol: None,
                    data: None,
                    is_unidirectional: None,
                    max_data: None,
                    max_streams: None,
                    ready_at: None,
                    status_code: None,
                    error_code: Some(error_code),
                    reason: Some(reason.into()),
                });
            }
            WT_CAPSULE_TYPE_DATA_BLOCKED => {
                if let Some(credit_effect) = self.replenish_data_credit(true) {
                    effects.push(credit_effect);
                } else {
                    effects.push(Effect::EmitSessionEvent {
                        session_id: self.id,
                        event_type: EventType::SessionDataBlocked,
                        path: None,
                        headers: None,
                        wt_available_protocols: None,
                        wt_protocol: None,
                        data: None,
                        is_unidirectional: None,
                        max_data: None,
                        max_streams: None,
                        ready_at: None,
                        status_code: None,
                        error_code: None,
                        reason: None,
                    });
                }
            }
            WT_CAPSULE_TYPE_DRAIN_SESSION if self.state == SessionState::Connected => {
                debug!(
                    "wt_session drain actual={:?} session_id={}",
                    self.state, self.id
                );
                self.state = SessionState::Draining;
                effects.push(Effect::EmitSessionEvent {
                    session_id: self.id,
                    event_type: EventType::SessionDraining,
                    path: None,
                    headers: None,
                    wt_available_protocols: None,
                    wt_protocol: None,
                    data: None,
                    is_unidirectional: None,
                    max_data: None,
                    max_streams: None,
                    ready_at: None,
                    status_code: None,
                    error_code: None,
                    reason: None,
                });
            }
            WT_CAPSULE_TYPE_MAX_DATA => {
                let Ok(peer_max_data) = utils::read_varint(&mut cursor) else {
                    debug!("varint decode invalid session_id={}", self.id);
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "varint decode invalid".into(),
                        now,
                    );
                };

                if peer_max_data > self.peer_max_data {
                    self.peer_max_data = peer_max_data;
                    effects.push(Effect::EmitSessionEvent {
                        session_id: self.id,
                        event_type: EventType::SessionMaxDataUpdated,
                        path: None,
                        headers: None,
                        wt_available_protocols: None,
                        wt_protocol: None,
                        data: None,
                        is_unidirectional: None,
                        max_data: Some(peer_max_data),
                        max_streams: None,
                        ready_at: None,
                        status_code: None,
                        error_code: None,
                        reason: None,
                    });
                    effects.extend(self.flush_blocked_writes());
                } else {
                    debug!(
                        "wt_session validate failed actual={peer_max_data} session_id={}",
                        self.id
                    );
                    return self.abort(
                        ERR_WT_FLOW_CONTROL_ERROR,
                        "wt_session validate failed".into(),
                        now,
                    );
                }
            }
            WT_CAPSULE_TYPE_MAX_STREAM_DATA | WT_CAPSULE_TYPE_STREAM_DATA_BLOCKED => {
                debug!(
                    "wt_session validate failed actual={capsule_type} session_id={}",
                    self.id
                );
                return self.abort(
                    ERR_H3_FRAME_UNEXPECTED,
                    "wt_session validate failed".into(),
                    now,
                );
            }
            WT_CAPSULE_TYPE_MAX_STREAMS_BIDI => {
                let Ok(peer_max_streams_bidi) = utils::read_varint(&mut cursor) else {
                    debug!("varint decode invalid session_id={}", self.id);
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "varint decode invalid".into(),
                        now,
                    );
                };

                if peer_max_streams_bidi > WT_STREAMS_LIMIT {
                    debug!(
                        "wt_session validate exceeded actual={peer_max_streams_bidi} expected=wt_streams_limit session_id={}",
                        self.id
                    );
                    return self.abort(
                        ERR_WT_FLOW_CONTROL_ERROR,
                        "wt_session validate exceeded".into(),
                        now,
                    );
                }
                if peer_max_streams_bidi > self.peer_max_streams_bidi {
                    self.peer_max_streams_bidi = peer_max_streams_bidi;
                    effects.push(Effect::EmitSessionEvent {
                        session_id: self.id,
                        event_type: EventType::SessionMaxStreamsBidiUpdated,
                        path: None,
                        headers: None,
                        wt_available_protocols: None,
                        wt_protocol: None,
                        data: None,
                        is_unidirectional: None,
                        max_data: None,
                        max_streams: Some(peer_max_streams_bidi),
                        ready_at: None,
                        status_code: None,
                        error_code: None,
                        reason: None,
                    });

                    effects.extend(self.flush_blocked_stream_requests(false));
                } else {
                    debug!(
                        "wt_session validate failed actual={peer_max_streams_bidi} session_id={}",
                        self.id
                    );
                    return self.abort(
                        ERR_WT_FLOW_CONTROL_ERROR,
                        "wt_session validate failed".into(),
                        now,
                    );
                }
            }
            WT_CAPSULE_TYPE_MAX_STREAMS_UNI => {
                let Ok(peer_max_streams_uni) = utils::read_varint(&mut cursor) else {
                    debug!("varint decode invalid session_id={}", self.id);
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "varint decode invalid".into(),
                        now,
                    );
                };

                if peer_max_streams_uni > WT_STREAMS_LIMIT {
                    debug!(
                        "wt_session validate exceeded actual={peer_max_streams_uni} expected=wt_streams_limit session_id={}",
                        self.id
                    );
                    return self.abort(
                        ERR_WT_FLOW_CONTROL_ERROR,
                        "wt_session validate exceeded".into(),
                        now,
                    );
                }
                if peer_max_streams_uni > self.peer_max_streams_uni {
                    self.peer_max_streams_uni = peer_max_streams_uni;
                    effects.push(Effect::EmitSessionEvent {
                        session_id: self.id,
                        event_type: EventType::SessionMaxStreamsUniUpdated,
                        path: None,
                        headers: None,
                        wt_available_protocols: None,
                        wt_protocol: None,
                        data: None,
                        is_unidirectional: None,
                        max_data: None,
                        max_streams: Some(peer_max_streams_uni),
                        ready_at: None,
                        status_code: None,
                        error_code: None,
                        reason: None,
                    });

                    effects.extend(self.flush_blocked_stream_requests(true));
                } else {
                    debug!(
                        "wt_session validate failed actual={peer_max_streams_uni} session_id={}",
                        self.id
                    );
                    return self.abort(
                        ERR_WT_FLOW_CONTROL_ERROR,
                        "wt_session validate failed".into(),
                        now,
                    );
                }
            }
            WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI | WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI => {
                let is_unidirectional = capsule_type == WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI;

                let Ok(peer_reported_limit) = utils::read_varint(&mut cursor) else {
                    debug!("varint decode invalid session_id={}", self.id);
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "varint decode invalid".into(),
                        now,
                    );
                };

                if peer_reported_limit > WT_STREAMS_LIMIT {
                    debug!(
                        "wt_session validate exceeded actual={peer_reported_limit} expected=wt_streams_limit session_id={}",
                        self.id
                    );
                    return self.abort(
                        ERR_WT_FLOW_CONTROL_ERROR,
                        "wt_session validate exceeded".into(),
                        now,
                    );
                }

                if let Some(credit_effect) = self.replenish_streams_credit(is_unidirectional, true)
                {
                    effects.push(credit_effect);
                } else {
                    effects.push(Effect::EmitSessionEvent {
                        session_id: self.id,
                        event_type: EventType::SessionStreamsBlocked,
                        path: None,
                        headers: None,
                        wt_available_protocols: None,
                        wt_protocol: None,
                        data: None,
                        is_unidirectional: Some(is_unidirectional),
                        max_data: None,
                        max_streams: None,
                        ready_at: None,
                        status_code: None,
                        error_code: None,
                        reason: None,
                    });
                }
            }
            _ => {}
        }

        effects
    }

    pub(super) fn recv_connect_close(
        &mut self,
        error_code: ErrorCode,
        reason: Cow<'static, str>,
        now: f64,
    ) -> Vec<Effect> {
        if self.state == SessionState::Closed {
            return Vec::new();
        }

        if self.state == SessionState::Closing {
            debug!(
                "wt_session close actual={:?} session_id={}",
                self.state, self.id
            );
            self.state = SessionState::Closed;
            return Vec::new();
        }

        debug!(
            "wt_session close actual={:?} session_id={} err={error_code}",
            self.state, self.id
        );
        self.close_code = Some(error_code);
        self.close_reason = Some(reason.clone().into_owned());
        self.closed_at = Some(now);
        self.state = SessionState::Closed;
        self.pending_capsules.clear();
        self.pending_datagrams.clear();

        let mut effects = self.abort_streams();
        while let Some(request_id) = self.pending_datagram_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session abort".into(),
            });
        }

        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionClosed,
            path: None,
            headers: None,
            wt_available_protocols: None,
            wt_protocol: None,
            data: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            ready_at: None,
            status_code: None,
            error_code: Some(error_code),
            reason: Some(reason),
        });

        effects
    }

    pub(super) fn recv_datagram(&mut self, data: Bytes) -> Vec<Effect> {
        if !matches!(
            self.state,
            SessionState::Connected | SessionState::Connecting | SessionState::Draining
        ) {
            debug!(
                "wt_session validate failed actual={:?} session_id={}",
                self.state, self.id
            );
            return Vec::new();
        }

        if self.state != SessionState::Connecting
            && let Some(request_id) = self.pending_datagram_requests.pop_front()
        {
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::Datagram(data),
            }];
        }

        if (self.pending_datagrams.len() as u64) >= self.max_pending_datagrams {
            debug!(
                "wt_session validate exceeded actual={} expected=max_pending_datagrams session_id={}",
                self.pending_datagrams.len(),
                self.id
            );
            return Vec::new();
        }

        self.pending_datagrams.push_back(data);

        Vec::new()
    }

    pub(super) fn recv_stop_sending(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Vec<Effect> {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Vec::new();
        };
        let stream_effects = stream.recv_stop_sending(error_code);

        let mut effects = self.suppress_pending_stream_events(stream_id, stream_effects);
        effects.extend(self.handle_closed(stream_id));

        effects
    }

    pub(super) fn recv_stream_data(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
        now: f64,
    ) -> Vec<Effect> {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Vec::new();
        };

        let data_len = data.len() as u64;
        self.data_received += data_len;

        if self.flow_control_negotiated && self.data_received > self.local_max_data {
            debug!(
                "wt_session validate exceeded actual={} limit={} session_id={}",
                self.data_received, self.local_max_data, self.id
            );
            return self.abort(
                ERR_WT_FLOW_CONTROL_ERROR,
                "wt_session validate exceeded".into(),
                now,
            );
        }

        let mut effects = stream.recv_data(data, end_stream);

        self.data_consumed += data_len;
        if let Some(credit_effect) = self.replenish_data_credit(false) {
            effects.push(credit_effect);
        }

        effects.extend(self.handle_closed(stream_id));

        effects
    }

    pub(super) fn recv_stream_reset(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Vec<Effect> {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Vec::new();
        };
        let stream_effects = stream.recv_reset(error_code);

        let mut effects = self.suppress_pending_stream_events(stream_id, stream_effects);
        effects.extend(self.handle_closed(stream_id));

        effects
    }

    pub(super) fn reject(
        &mut self,
        request_id: RequestId,
        headers: Headers,
        status_code: u16,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.is_client {
            debug!(
                "wt_session validate failed actual={} expected=false request_id={request_id} session_id={}",
                self.is_client, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session validate failed".into(),
            });
            return effects;
        }

        if self.state != SessionState::Connecting {
            debug!(
                "wt_session validate failed actual={:?} expected=connecting request_id={request_id} session_id={}",
                self.state, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: "wt_session validate failed".into(),
            });
            return effects;
        }

        if !(300..=599).contains(&status_code) {
            debug!(
                "wt_session validate invalid actual={status_code} request_id={request_id} session_id={}",
                self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session validate invalid".into(),
            });
            return effects;
        }

        if !utils::is_valid_user_headers(&headers, false) {
            debug!(
                "h3_headers validate invalid request_id={request_id} session_id={}",
                self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "h3_headers validate invalid".into(),
            });
            return effects;
        }

        debug!(
            "wt_session reject actual={:?} request_id={request_id} session_id={} err={status_code}",
            self.state, self.id
        );
        self.close_reason = Some("wt_session reject".to_owned());
        self.closed_at = Some(now);
        self.state = SessionState::Closed;
        self.pending_capsules.clear();
        self.pending_datagrams.clear();

        effects.extend(self.abort_streams());
        while let Some(request_id) = self.pending_datagram_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session abort".into(),
            });
        }

        let mut response_headers: Headers = vec![(
            Bytes::from_static(b":status"),
            Bytes::from(status_code.to_string()),
        )];
        response_headers.extend(headers);

        effects.push(Effect::SendH3Headers {
            stream_id: self.id,
            headers: response_headers,
            end_stream: true,
        });
        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionClosed,
            path: None,
            headers: None,
            wt_available_protocols: None,
            wt_protocol: None,
            data: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            ready_at: None,
            status_code: Some(status_code),
            error_code: None,
            reason: Some("wt_session reject".into()),
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    pub(super) fn reset_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
    ) -> Vec<Effect> {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            debug!(
                "wt_stream resolve failed request_id={request_id} session_id={} stream_id={stream_id}",
                self.id
            );
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            }];
        };

        let mut effects = stream.reset(request_id, error_code);
        effects.extend(self.handle_closed(stream_id));

        effects
    }

    pub(super) fn send_datagram(
        &mut self,
        request_id: RequestId,
        data: Bytes,
        peer_max_datagram_frame_size: u64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !matches!(
            self.state,
            SessionState::Connected | SessionState::Connecting | SessionState::Draining
        ) {
            debug!(
                "wt_session validate failed actual={:?} request_id={request_id} session_id={}",
                self.state, self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: "wt_session validate failed".into(),
            });
            return effects;
        }

        let quarter_stream_id = self.id / 4;
        let header_size = utils::varint_size(quarter_stream_id) as u64;
        let max_allowed_payload = peer_max_datagram_frame_size.saturating_sub(header_size);

        if (data.len() as u64) > max_allowed_payload {
            debug!(
                "wt_datagram validate exceeded actual={} limit={max_allowed_payload} request_id={request_id} session_id={}",
                data.len(),
                self.id
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Datagram,
                error_code: None,
                reason: "wt_datagram validate exceeded".into(),
            });
            return effects;
        }

        effects.push(Effect::SendH3Datagram {
            stream_id: self.id,
            data,
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    pub(super) fn stop_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
    ) -> Vec<Effect> {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            debug!(
                "wt_stream resolve failed request_id={request_id} session_id={} stream_id={stream_id}",
                self.id
            );
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            }];
        };

        let mut effects = stream.stop(request_id, error_code);
        effects.extend(self.handle_closed(stream_id));

        effects
    }

    pub(super) fn write_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        data: Bytes,
        end_stream: bool,
    ) -> Vec<Effect> {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            debug!(
                "wt_stream resolve failed request_id={request_id} session_id={} stream_id={stream_id}",
                self.id
            );
            if data.is_empty() && end_stream {
                return vec![Effect::NotifyRequestDone {
                    request_id,
                    result: RequestResult::None,
                }];
            }
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Stream,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: "wt_stream resolve failed".into(),
            }];
        };

        let session_credit = if self.flow_control_negotiated {
            self.peer_max_data.saturating_sub(self.data_sent)
        } else {
            u64::MAX
        };

        let (mut effects, sent) = stream.write(request_id, data, end_stream, session_credit);
        let is_blocked = stream.is_blocked();

        self.data_sent += sent;

        if is_blocked && self.blocked_streams.insert(stream_id) {
            self.blocked_streams_queue.push_back(stream_id);
        }

        if let Some(effect) = self.emit_data_blocked() {
            effects.push(effect);
        }

        effects
    }

    pub(super) fn wt_available_protocols(&self) -> Option<&[String]> {
        self.wt_available_protocols.as_deref()
    }

    fn abort_streams(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();

        let mut stream_ids: Vec<_> = self.streams.keys().copied().collect();
        stream_ids.sort_unstable();

        for stream_id in stream_ids {
            if let Some(stream) = self.streams.get_mut(&stream_id) {
                let stream_effects = stream.abort(ERR_WT_SESSION_GONE);
                effects.extend(self.suppress_pending_stream_events(stream_id, stream_effects));
            }
            effects.extend(self.handle_closed(stream_id));
        }
        self.blocked_streams.clear();
        self.blocked_streams_queue.clear();
        self.pending_streams.clear();

        while let Some(request_id) = self.blocked_stream_requests_bidi.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session abort".into(),
            });
        }
        while let Some(request_id) = self.blocked_stream_requests_uni.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: None,
                reason: "wt_session abort".into(),
            });
        }

        effects
    }

    fn diagnostics_snapshot(&self) -> SessionDiagnostics {
        SessionDiagnostics {
            close_code: self.close_code,
            close_reason: self.close_reason.clone(),
            closed_at: self.closed_at,
            created_at: self.created_at,
            data_consumed: self.data_consumed,
            data_received: self.data_received,
            data_sent: self.data_sent,
            flow_control_negotiated: self.flow_control_negotiated,
            headers: self.headers.clone(),
            is_client: self.is_client,
            local_max_data: self.local_max_data,
            local_max_streams_bidi: self.local_max_streams_bidi,
            local_max_streams_uni: self.local_max_streams_uni,
            local_streams_bidi_opened: self.local_streams_bidi_opened,
            local_streams_uni_opened: self.local_streams_uni_opened,
            path: self.path.clone(),
            peer_max_data: self.peer_max_data,
            peer_max_streams_bidi: self.peer_max_streams_bidi,
            peer_max_streams_uni: self.peer_max_streams_uni,
            peer_streams_bidi_closed: self.peer_streams_bidi_closed,
            peer_streams_bidi_opened: self.peer_streams_bidi_opened,
            peer_streams_uni_closed: self.peer_streams_uni_closed,
            peer_streams_uni_opened: self.peer_streams_uni_opened,
            ready_at: self.ready_at,
            session_id: self.id,
            state: self.state,
            wt_available_protocols: self.wt_available_protocols.clone(),
            wt_protocol: self.wt_protocol.clone(),
        }
    }

    fn emit_data_blocked(&mut self) -> Option<Effect> {
        if !self.flow_control_negotiated {
            return None;
        }

        if self.data_sent >= self.peer_max_data
            && !self.blocked_streams.is_empty()
            && self
                .last_data_blocked_sent
                .is_none_or(|last| self.peer_max_data > last)
        {
            self.last_data_blocked_sent = Some(self.peer_max_data);
            let mut buffer = BytesMut::with_capacity(8);
            if let Err(e) = utils::write_varint(&mut buffer, self.peer_max_data) {
                debug!("varint encode failed session_id={} err={e:?}", self.id);
                return None;
            }

            return Some(Effect::SendH3Capsule {
                stream_id: self.id,
                capsule_type: WT_CAPSULE_TYPE_DATA_BLOCKED,
                capsule_value: buffer.freeze(),
                end_stream: false,
            });
        }

        None
    }

    fn emit_streams_blocked(&mut self, is_unidirectional: bool) -> Option<Effect> {
        if !self.flow_control_negotiated {
            return None;
        }

        let (opened, limit, is_blocked, last_streams_blocked_sent, capsule_type) =
            if is_unidirectional {
                (
                    self.local_streams_uni_opened,
                    self.peer_max_streams_uni,
                    !self.blocked_stream_requests_uni.is_empty(),
                    &mut self.last_streams_blocked_uni_sent,
                    WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI,
                )
            } else {
                (
                    self.local_streams_bidi_opened,
                    self.peer_max_streams_bidi,
                    !self.blocked_stream_requests_bidi.is_empty(),
                    &mut self.last_streams_blocked_bidi_sent,
                    WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
                )
            };

        if !is_blocked
            || opened < limit
            || last_streams_blocked_sent.is_some_and(|last| limit <= last)
        {
            return None;
        }

        let mut buffer = BytesMut::with_capacity(8);
        if let Err(e) = utils::write_varint(&mut buffer, limit) {
            debug!("varint encode failed session_id={} err={e:?}", self.id);
            return None;
        }
        *last_streams_blocked_sent = Some(limit);

        Some(Effect::SendH3Capsule {
            stream_id: self.id,
            capsule_type,
            capsule_value: buffer.freeze(),
            end_stream: false,
        })
    }

    fn flush_blocked_writes(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.flow_control_negotiated && self.data_sent >= self.peer_max_data {
            return effects;
        }

        let count = self.blocked_streams_queue.len();
        for _ in 0..count {
            let Some(stream_id) = self.blocked_streams_queue.pop_front() else {
                break;
            };
            self.blocked_streams.remove(&stream_id);

            let session_credit = if self.flow_control_negotiated {
                self.peer_max_data.saturating_sub(self.data_sent)
            } else {
                u64::MAX
            };

            if session_credit == 0 {
                self.blocked_streams_queue.push_front(stream_id);
                self.blocked_streams.insert(stream_id);
                break;
            }

            let Some(stream) = self.streams.get_mut(&stream_id) else {
                continue;
            };

            let (stream_effects, sent) = stream.flush_writes(session_credit);
            let is_blocked = stream.is_blocked();

            effects.extend(stream_effects);
            self.data_sent += sent;

            if is_blocked {
                self.blocked_streams_queue.push_back(stream_id);
                self.blocked_streams.insert(stream_id);
            }
        }

        if let Some(effect) = self.emit_data_blocked() {
            effects.push(effect);
        }

        effects
    }

    fn handle_closed(&mut self, stream_id: StreamId) -> Vec<Effect> {
        if !self.streams.get(&stream_id).is_some_and(Stream::is_closed) {
            return Vec::new();
        }

        self.streams.remove(&stream_id);
        self.blocked_streams.remove(&stream_id);

        let mut effects = self.suppress_pending_stream_events(
            stream_id,
            vec![Effect::EmitStreamEvent {
                stream_id,
                event_type: EventType::StreamClosed,
                session_id: None,
                direction: None,
                is_peer_initiated: None,
                error_code: None,
            }],
        );
        if utils::is_peer_initiated_stream(stream_id, self.is_client) {
            if utils::is_unidirectional_stream(stream_id) {
                self.peer_streams_uni_closed += 1;
                if let Some(effect) = self.replenish_streams_credit(true, false) {
                    effects.push(effect);
                }
            } else {
                self.peer_streams_bidi_closed += 1;
                if let Some(effect) = self.replenish_streams_credit(false, false) {
                    effects.push(effect);
                }
            }
        }

        effects
    }

    fn replay_pending_queues(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        while let Some((capsule_type, capsule_value)) = self.pending_capsules.pop_front() {
            effects.extend(self.recv_capsule(capsule_type, &capsule_value, now));

            if self.state != SessionState::Connected {
                return effects;
            }
        }

        while !self.pending_datagram_requests.is_empty()
            && let Some(data) = self.pending_datagrams.pop_front()
        {
            effects.extend(self.recv_datagram(data));
        }

        while let Some(stream_id) = self.pending_streams.pop_front() {
            if !self.streams.contains_key(&stream_id) {
                continue;
            }

            let direction = utils::stream_direction_from_id(stream_id, self.is_client);
            let is_peer_initiated = utils::is_peer_initiated_stream(stream_id, self.is_client);

            effects.push(Effect::EmitStreamEvent {
                stream_id,
                event_type: EventType::StreamOpened,
                session_id: Some(self.id),
                direction: Some(direction),
                is_peer_initiated: Some(is_peer_initiated),
                error_code: None,
            });
        }

        effects
    }

    fn replenish_data_credit(&mut self, force_send: bool) -> Option<Effect> {
        if !self.flow_control_negotiated {
            return None;
        }

        if !matches!(
            (self.state, self.is_client),
            (SessionState::Connected | SessionState::Draining, _)
                | (SessionState::Connecting, true)
        ) {
            return None;
        }

        let new_limit = utils::next_data_limit(
            self.local_max_data,
            self.data_consumed,
            self.receive_window,
            force_send,
        )?;

        let mut buffer = BytesMut::with_capacity(8);
        if let Err(e) = utils::write_varint(&mut buffer, new_limit) {
            debug!("varint encode failed session_id={} err={e:?}", self.id);
            return None;
        }

        self.local_max_data = new_limit;

        Some(Effect::SendH3Capsule {
            stream_id: self.id,
            capsule_type: WT_CAPSULE_TYPE_MAX_DATA,
            capsule_value: buffer.freeze(),
            end_stream: false,
        })
    }

    fn replenish_streams_credit(
        &mut self,
        is_unidirectional: bool,
        force_send: bool,
    ) -> Option<Effect> {
        if !self.flow_control_negotiated {
            return None;
        }

        if !matches!(
            (self.state, self.is_client),
            (SessionState::Connected | SessionState::Draining, _)
                | (SessionState::Connecting, true)
        ) {
            return None;
        }

        let (current, closed, initial, capsule_type) = if is_unidirectional {
            (
                self.local_max_streams_uni,
                self.peer_streams_uni_closed,
                self.initial_max_streams_uni,
                WT_CAPSULE_TYPE_MAX_STREAMS_UNI,
            )
        } else {
            (
                self.local_max_streams_bidi,
                self.peer_streams_bidi_closed,
                self.initial_max_streams_bidi,
                WT_CAPSULE_TYPE_MAX_STREAMS_BIDI,
            )
        };

        let new_limit = utils::next_stream_limit(current, closed, initial, force_send)?;

        let mut buffer = BytesMut::with_capacity(8);
        if let Err(e) = utils::write_varint(&mut buffer, new_limit) {
            debug!("varint encode failed session_id={} err={e:?}", self.id);
            return None;
        }

        if is_unidirectional {
            self.local_max_streams_uni = new_limit;
        } else {
            self.local_max_streams_bidi = new_limit;
        }

        Some(Effect::SendH3Capsule {
            stream_id: self.id,
            capsule_type,
            capsule_value: buffer.freeze(),
            end_stream: false,
        })
    }

    fn suppress_pending_stream_events(
        &self,
        stream_id: StreamId,
        stream_effects: Vec<Effect>,
    ) -> Vec<Effect> {
        if !self.pending_streams.contains(&stream_id) {
            return stream_effects;
        }

        stream_effects
            .into_iter()
            .filter(|effect| !matches!(effect, Effect::EmitStreamEvent { .. }))
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SessionParameters {
    pub(super) flow_control_negotiated: bool,
    pub(super) initial_max_data: u64,
    pub(super) initial_max_streams_bidi: u64,
    pub(super) initial_max_streams_uni: u64,
    pub(super) max_pending_capsules: u64,
    pub(super) max_pending_datagrams: u64,
    pub(super) max_pending_streams: u64,
    pub(super) max_write_buffer_size: u64,
    pub(super) peer_max_data: u64,
    pub(super) peer_max_streams_bidi: u64,
    pub(super) peer_max_streams_uni: u64,
    pub(super) receive_window: u64,
}

#[cfg(test)]
mod tests;
