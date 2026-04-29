//! Session-level state machine and resource aggregator.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Cursor;
use std::slice;

use bytes::{Bytes, BytesMut};
use tracing::debug;

use crate::common::constants::{
    ERR_H3_DATAGRAM_ERROR, ERR_H3_FRAME_UNEXPECTED, ERR_H3_GENERAL_PROTOCOL_ERROR,
    ERR_LIB_INTERNAL_ERROR, ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR,
    ERR_WT_FLOW_CONTROL_ERROR, WT_CAPSULE_TYPE_CLOSE_SESSION, WT_CAPSULE_TYPE_DATA_BLOCKED,
    WT_CAPSULE_TYPE_DRAIN_SESSION, WT_CAPSULE_TYPE_MAX_DATA, WT_CAPSULE_TYPE_MAX_STREAM_DATA,
    WT_CAPSULE_TYPE_MAX_STREAMS_BIDI, WT_CAPSULE_TYPE_MAX_STREAMS_UNI,
    WT_CAPSULE_TYPE_STREAM_DATA_BLOCKED, WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
    WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI, WT_MAX_CLOSE_REASON_SIZE, WT_PROTOCOL, WT_STREAMS_LIMIT,
};
use crate::common::types::{
    ErrorCode, ErrorSource, EventType, Headers, RequestId, SessionId, SessionState,
    StreamDirection, StreamId,
};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::stream::Stream;
use crate::protocol::utils::{
    encode_wt_protocol_list, is_peer_initiated_stream, is_unidirectional_stream, next_data_limit,
    next_stream_limit, read_varint, stream_dir_from_id, write_varint,
};

// Diagnostic information snapshot for a session.
#[derive(Clone, Debug)]
pub(crate) struct SessionDiagnostics {
    pub(crate) active_streams: HashSet<StreamId>,
    pub(crate) blocked_streams: HashSet<StreamId>,
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
    pub(crate) created_at: f64,
    pub(crate) datagram_bytes_received: u64,
    pub(crate) datagram_bytes_sent: u64,
    pub(crate) datagrams_received: u64,
    pub(crate) datagrams_sent: u64,
    pub(crate) flow_control_negotiated: bool,
    pub(crate) headers: Headers,
    pub(crate) is_client: bool,
    pub(crate) local_data_consumed: u64,
    pub(crate) local_data_received: u64,
    pub(crate) local_data_sent: u64,
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
    pub(crate) pending_bidi_stream_requests: VecDeque<RequestId>,
    pub(crate) pending_uni_stream_requests: VecDeque<RequestId>,
    pub(crate) ready_at: Option<f64>,
    pub(crate) session_id: SessionId,
    pub(crate) state: SessionState,
    pub(crate) wt_protocol: Option<String>,
}

// Representation of a WebTransport session.
pub(super) struct Session {
    active_streams: HashSet<StreamId>,
    blocked_streams: HashSet<StreamId>,
    blocked_streams_queue: VecDeque<StreamId>,
    close_code: Option<ErrorCode>,
    close_reason: Option<String>,
    closed_at: Option<f64>,
    created_at: f64,
    datagram_bytes_received: u64,
    datagram_bytes_sent: u64,
    datagrams_received: u64,
    datagrams_sent: u64,
    flow_control_negotiated: bool,
    flow_control_window: u64,
    flow_control_window_auto_scale_enabled: bool,
    headers: Headers,
    id: SessionId,
    initial_max_streams_bidi: u64,
    initial_max_streams_uni: u64,
    is_client: bool,
    local_data_consumed: u64,
    local_data_received: u64,
    local_data_sent: u64,
    local_max_data: u64,
    local_max_streams_bidi: u64,
    local_max_streams_uni: u64,
    local_streams_bidi_opened: u64,
    local_streams_uni_opened: u64,
    max_stream_read_buffer_size: u64,
    max_stream_write_buffer_size: u64,
    path: String,
    peer_max_data: u64,
    peer_max_streams_bidi: u64,
    peer_max_streams_uni: u64,
    peer_streams_bidi_closed: u64,
    peer_streams_bidi_opened: u64,
    peer_streams_uni_closed: u64,
    peer_streams_uni_opened: u64,
    pending_bidi_stream_requests: VecDeque<RequestId>,
    pending_uni_stream_requests: VecDeque<RequestId>,
    ready_at: Option<f64>,
    state: SessionState,
    streams: HashMap<StreamId, Stream>,
    wt_protocol: Option<String>,
}

impl Session {
    // New session entity creation.
    #[allow(
        clippy::too_many_arguments,
        reason = "Orthogonal domain primitives post-extraction."
    )]
    pub(super) fn new(
        id: SessionId,
        path: String,
        headers: Headers,
        wt_protocol: Option<String>,
        state: SessionState,
        is_client: bool,
        params: SessionParams,
        created_at: f64,
    ) -> Self {
        Self {
            active_streams: HashSet::new(),
            blocked_streams: HashSet::new(),
            blocked_streams_queue: VecDeque::new(),
            close_code: None,
            close_reason: None,
            closed_at: None,
            created_at,
            datagram_bytes_received: 0,
            datagram_bytes_sent: 0,
            datagrams_received: 0,
            datagrams_sent: 0,
            flow_control_negotiated: params.flow_control_negotiated,
            flow_control_window: params.flow_control_window,
            flow_control_window_auto_scale_enabled: params.flow_control_window_auto_scale_enabled,
            headers,
            id,
            initial_max_streams_bidi: params.initial_max_streams_bidi,
            initial_max_streams_uni: params.initial_max_streams_uni,
            is_client,
            local_data_consumed: 0,
            local_data_received: 0,
            local_data_sent: 0,
            local_max_data: params.initial_max_data,
            local_max_streams_bidi: params.initial_max_streams_bidi,
            local_max_streams_uni: params.initial_max_streams_uni,
            local_streams_bidi_opened: 0,
            local_streams_uni_opened: 0,
            max_stream_read_buffer_size: params.max_stream_read_buffer_size,
            max_stream_write_buffer_size: params.max_stream_write_buffer_size,
            path,
            peer_max_data: params.peer_max_data,
            peer_max_streams_bidi: params.peer_max_streams_bidi,
            peer_max_streams_uni: params.peer_max_streams_uni,
            peer_streams_bidi_closed: 0,
            peer_streams_bidi_opened: 0,
            peer_streams_uni_closed: 0,
            peer_streams_uni_opened: 0,
            pending_bidi_stream_requests: VecDeque::new(),
            pending_uni_stream_requests: VecDeque::new(),
            ready_at: None,
            state,
            streams: HashMap::new(),
            wt_protocol,
        }
    }

    // User session acceptance handling.
    pub(super) fn accept(
        &mut self,
        request_id: RequestId,
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

        let mut response_headers: Headers =
            vec![(Bytes::from_static(b":status"), Bytes::from("200"))];

        if let Some(ref proto) = wt_protocol {
            match encode_wt_protocol_list(slice::from_ref(proto)) {
                Ok(encoded) => {
                    response_headers.push((Bytes::from_static(WT_PROTOCOL), encoded));
                }
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
            }
        }

        debug!(
            "wt_session open actual={:?} request_id={request_id} session_id={}",
            self.state, self.id
        );
        self.state = SessionState::Connected;
        self.ready_at = Some(now);
        self.wt_protocol.clone_from(&wt_protocol);

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
            error_code: None,
            reason: None,
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    // QUIC stream binding.
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

        let is_peer_initiated = is_peer_initiated_stream(stream_id, self.is_client);

        let stream = Stream::new(
            stream_id,
            self.id,
            direction,
            is_peer_initiated,
            self.max_stream_read_buffer_size,
            self.max_stream_write_buffer_size,
            now,
        );

        self.streams.insert(stream_id, stream);
        self.active_streams.insert(stream_id);

        vec![
            Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::StreamId(stream_id),
            },
            Effect::EmitStreamEvent {
                stream_id,
                event_type: EventType::StreamOpened,
                session_id: Some(self.id),
                direction: Some(direction),
                is_peer_initiated: Some(is_peer_initiated),
                error_code: None,
            },
        ]
    }

    // User session closure handling.
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
        self.state = SessionState::Closing;
        self.closed_at = Some(now);
        self.close_code = Some(error_code);
        self.close_reason = reason.clone().map(Cow::into_owned);

        effects.extend(self.reset_all_streams(error_code, now));

        let is_clean_close = error_code == 0 && reason.as_deref().unwrap_or("").is_empty();

        if is_clean_close {
            effects.push(Effect::SendQuicData {
                stream_id: self.id,
                data: Bytes::new(),
                end_stream: true,
            });
        } else {
            let reason_str = reason.as_deref().unwrap_or("");
            let reason_bytes = reason_str.as_bytes();
            let limit = usize::try_from(WT_MAX_CLOSE_REASON_SIZE).unwrap_or(usize::MAX);
            let truncated_reason = reason_bytes.get(..limit).unwrap_or(reason_bytes);

            let mut buf = BytesMut::with_capacity(8 + truncated_reason.len());
            if let Err(e) = write_varint(&mut buf, error_code) {
                debug!(
                    "varint encode failed request_id={request_id} session_id={} err={e:?}",
                    self.id
                );
            }
            buf.extend_from_slice(truncated_reason);

            effects.push(Effect::SendH3Capsule {
                stream_id: self.id,
                capsule_type: WT_CAPSULE_TYPE_CLOSE_SESSION,
                capsule_data: buf.freeze(),
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
            error_code: Some(error_code),
            reason,
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    // User stream creation request handling.
    pub(super) fn create_stream(
        &mut self,
        request_id: RequestId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !matches!(self.state, SessionState::Connected | SessionState::Draining) {
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

        let limit_exceeded = if is_unidirectional {
            self.local_streams_uni_opened >= self.peer_max_streams_uni
        } else {
            self.local_streams_bidi_opened >= self.peer_max_streams_bidi
        };

        if limit_exceeded {
            let mut buf = BytesMut::with_capacity(8);
            if is_unidirectional {
                debug!(
                    "wt_session validate exceeded actual={} limit={} request_id={request_id} session_id={}",
                    self.local_streams_uni_opened, self.peer_max_streams_uni, self.id
                );
                self.pending_uni_stream_requests.push_back(request_id);
                if let Err(e) = write_varint(&mut buf, self.peer_max_streams_uni) {
                    debug!(
                        "varint encode failed request_id={request_id} session_id={} err={e:?}",
                        self.id
                    );
                } else {
                    effects.push(Effect::SendH3Capsule {
                        stream_id: self.id,
                        capsule_type: WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI,
                        capsule_data: buf.freeze(),
                        end_stream: false,
                    });
                }
            } else {
                debug!(
                    "wt_session validate exceeded actual={} limit={} request_id={request_id} session_id={}",
                    self.local_streams_bidi_opened, self.peer_max_streams_bidi, self.id
                );
                self.pending_bidi_stream_requests.push_back(request_id);
                if let Err(e) = write_varint(&mut buf, self.peer_max_streams_bidi) {
                    debug!(
                        "varint encode failed request_id={request_id} session_id={} err={e:?}",
                        self.id
                    );
                } else {
                    effects.push(Effect::SendH3Capsule {
                        stream_id: self.id,
                        capsule_type: WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI,
                        capsule_data: buf.freeze(),
                        end_stream: false,
                    });
                }
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

    // User session diagnostics event handling.
    pub(super) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diag = self.diagnostics_snapshot();
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::SessionDiagnostics(Box::new(diag)),
        }]
    }

    // Session draining command handling.
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
                capsule_data: Bytes::new(),
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
                error_code: None,
                reason: None,
            },
        ]
    }

    // TLS keying material export handling.
    pub(super) fn export_keying_material(
        &self,
        request_id: RequestId,
        label: String,
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

        let mut exporter_context = BytesMut::with_capacity(8 + context.len());
        exporter_context.extend_from_slice(&self.id.to_be_bytes());
        exporter_context.extend_from_slice(context);

        vec![Effect::ExportTlsKeyingMaterial {
            request_id,
            label,
            context: exporter_context.freeze(),
            length,
        }]
    }

    // Failed QUIC stream creation handling.
    pub(super) fn fail_stream(
        &mut self,
        request_id: RequestId,
        is_unidirectional: bool,
        error_code: Option<ErrorCode>,
        reason: Cow<'static, str>,
    ) -> Vec<Effect> {
        debug!(
            "wt_stream create failed request_id={request_id} session_id={}",
            self.id
        );
        if is_unidirectional {
            if self.local_streams_uni_opened > 0 {
                self.local_streams_uni_opened -= 1;
            }
        } else if self.local_streams_bidi_opened > 0 {
            self.local_streams_bidi_opened -= 1;
        }

        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code,
            reason,
        }]
    }

    // Manual data credit grant handling.
    pub(super) fn grant_data_credit(
        &mut self,
        request_id: RequestId,
        max_data: u64,
    ) -> Vec<Effect> {
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Closing | SessionState::Draining
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

        if max_data <= self.local_max_data {
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            }];
        }

        let mut buf = BytesMut::with_capacity(8);
        if let Err(e) = write_varint(&mut buf, max_data) {
            debug!(
                "varint encode failed request_id={request_id} session_id={} err={e:?}",
                self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_INTERNAL_ERROR),
                reason: "varint encode failed".into(),
            }];
        }

        self.local_max_data = max_data;

        vec![
            Effect::SendH3Capsule {
                stream_id: self.id,
                capsule_type: WT_CAPSULE_TYPE_MAX_DATA,
                capsule_data: buf.freeze(),
                end_stream: false,
            },
            Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            },
        ]
    }

    // Manual stream credit grant handling.
    pub(super) fn grant_streams_credit(
        &mut self,
        request_id: RequestId,
        is_unidirectional: bool,
        max_streams: u64,
    ) -> Vec<Effect> {
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Closing | SessionState::Draining
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

        let current = if is_unidirectional {
            self.local_max_streams_uni
        } else {
            self.local_max_streams_bidi
        };

        if max_streams <= current {
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            }];
        }

        let cap_type = if is_unidirectional {
            WT_CAPSULE_TYPE_MAX_STREAMS_UNI
        } else {
            WT_CAPSULE_TYPE_MAX_STREAMS_BIDI
        };

        let mut buf = BytesMut::with_capacity(8);
        if let Err(e) = write_varint(&mut buf, max_streams) {
            debug!(
                "varint encode failed request_id={request_id} session_id={} err={e:?}",
                self.id
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Session,
                error_code: Some(ERR_LIB_INTERNAL_ERROR),
                reason: "varint encode failed".into(),
            }];
        }

        if is_unidirectional {
            self.local_max_streams_uni = max_streams;
        } else {
            self.local_max_streams_bidi = max_streams;
        }

        vec![
            Effect::SendH3Capsule {
                stream_id: self.id,
                capsule_type: cap_type,
                capsule_data: buf.freeze(),
                end_stream: false,
            },
            Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            },
        ]
    }

    // Session closed state predicate.
    pub(super) fn is_closed(&self) -> bool {
        self.state == SessionState::Closed
    }

    // Closed streams pruning.
    pub(super) fn prune_closed_streams(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut ids_to_remove: Vec<StreamId> = self
            .streams
            .iter()
            .filter(|(_, stream)| stream.is_closed())
            .map(|(id, _)| *id)
            .collect();

        ids_to_remove.sort_unstable();

        for stream_id in ids_to_remove {
            self.streams.remove(&stream_id);
            self.active_streams.remove(&stream_id);
            self.blocked_streams.remove(&stream_id);
            effects.push(Effect::CleanupH3Stream { stream_id });
        }

        effects
    }

    // Capsule reception handling.
    pub(super) fn recv_capsule(&mut self, capsule_type: u64, data: &[u8], now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !self.flow_control_negotiated
            && matches!(
                capsule_type,
                WT_CAPSULE_TYPE_MAX_DATA
                    | WT_CAPSULE_TYPE_MAX_STREAMS_BIDI
                    | WT_CAPSULE_TYPE_MAX_STREAMS_UNI
                    | WT_CAPSULE_TYPE_DATA_BLOCKED
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

        let mut cur = Cursor::new(data);

        match capsule_type {
            WT_CAPSULE_TYPE_MAX_DATA => {
                if let Ok(new_max) = read_varint(&mut cur) {
                    if new_max > self.peer_max_data {
                        self.peer_max_data = new_max;
                        effects.push(Effect::EmitSessionEvent {
                            session_id: self.id,
                            event_type: EventType::SessionMaxDataUpdated,
                            path: None,
                            headers: None,
                            wt_available_protocols: None,
                            wt_protocol: None,
                            data: None,
                            is_unidirectional: None,
                            max_data: Some(new_max),
                            max_streams: None,
                            ready_at: None,
                            error_code: None,
                            reason: None,
                        });
                        effects.extend(self.flush_blocked_writes());
                    } else if new_max < self.peer_max_data {
                        debug!(
                            "wt_session validate failed actual={new_max} session_id={}",
                            self.id
                        );
                        return self.abort(
                            ERR_WT_FLOW_CONTROL_ERROR,
                            "wt_session validate failed".into(),
                            now,
                        );
                    }
                } else {
                    debug!("varint decode invalid session_id={}", self.id);
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "varint decode invalid".into(),
                        now,
                    );
                }
            }
            WT_CAPSULE_TYPE_MAX_STREAMS_BIDI => {
                if let Ok(new_max) = read_varint(&mut cur) {
                    if new_max > WT_STREAMS_LIMIT {
                        debug!(
                            "wt_session validate exceeded actual={new_max} expected=wt_streams_limit session_id={}",
                            self.id
                        );
                        return self.abort(
                            ERR_H3_DATAGRAM_ERROR,
                            "wt_session validate exceeded".into(),
                            now,
                        );
                    }
                    if new_max > self.peer_max_streams_bidi {
                        self.peer_max_streams_bidi = new_max;
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
                            max_streams: Some(new_max),
                            ready_at: None,
                            error_code: None,
                            reason: None,
                        });

                        while self.local_streams_bidi_opened < self.peer_max_streams_bidi
                            && !self.pending_bidi_stream_requests.is_empty()
                        {
                            if let Some(req_id) = self.pending_bidi_stream_requests.pop_front() {
                                self.local_streams_bidi_opened += 1;
                                effects.push(Effect::CreateQuicStream {
                                    request_id: req_id,
                                    session_id: self.id,
                                    is_unidirectional: false,
                                });
                            }
                        }
                    } else if new_max < self.peer_max_streams_bidi {
                        debug!(
                            "wt_session validate failed actual={new_max} session_id={}",
                            self.id
                        );
                        return self.abort(
                            ERR_WT_FLOW_CONTROL_ERROR,
                            "wt_session validate failed".into(),
                            now,
                        );
                    }
                } else {
                    debug!("varint decode invalid session_id={}", self.id);
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "varint decode invalid".into(),
                        now,
                    );
                }
            }
            WT_CAPSULE_TYPE_MAX_STREAMS_UNI => {
                if let Ok(new_max) = read_varint(&mut cur) {
                    if new_max > WT_STREAMS_LIMIT {
                        debug!(
                            "wt_session validate exceeded actual={new_max} expected=wt_streams_limit session_id={}",
                            self.id
                        );
                        return self.abort(
                            ERR_H3_DATAGRAM_ERROR,
                            "wt_session validate exceeded".into(),
                            now,
                        );
                    }
                    if new_max > self.peer_max_streams_uni {
                        self.peer_max_streams_uni = new_max;
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
                            max_streams: Some(new_max),
                            ready_at: None,
                            error_code: None,
                            reason: None,
                        });

                        while self.local_streams_uni_opened < self.peer_max_streams_uni
                            && !self.pending_uni_stream_requests.is_empty()
                        {
                            if let Some(req_id) = self.pending_uni_stream_requests.pop_front() {
                                self.local_streams_uni_opened += 1;
                                effects.push(Effect::CreateQuicStream {
                                    request_id: req_id,
                                    session_id: self.id,
                                    is_unidirectional: true,
                                });
                            }
                        }
                    } else if new_max < self.peer_max_streams_uni {
                        debug!(
                            "wt_session validate failed actual={new_max} session_id={}",
                            self.id
                        );
                        return self.abort(
                            ERR_WT_FLOW_CONTROL_ERROR,
                            "wt_session validate failed".into(),
                            now,
                        );
                    }
                } else {
                    debug!("varint decode invalid session_id={}", self.id);
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "varint decode invalid".into(),
                        now,
                    );
                }
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
                        error_code: None,
                        reason: None,
                    });
                }
            }
            WT_CAPSULE_TYPE_STREAMS_BLOCKED_BIDI | WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI => {
                let is_uni = capsule_type == WT_CAPSULE_TYPE_STREAMS_BLOCKED_UNI;
                if let Some(credit_effect) = self.replenish_streams_credit(is_uni, true) {
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
                        is_unidirectional: Some(is_uni),
                        max_data: None,
                        max_streams: None,
                        ready_at: None,
                        error_code: None,
                        reason: None,
                    });
                }
            }
            WT_CAPSULE_TYPE_CLOSE_SESSION => {
                let error_code = read_varint(&mut cur).unwrap_or_default();
                let pos = usize::try_from(cur.position()).unwrap_or(0);
                let reason_bytes = data.get(pos..).unwrap_or_default();
                let reason = String::from_utf8_lossy(reason_bytes).into_owned();

                debug!(
                    "wt_session close actual={:?} session_id={} err={error_code}",
                    self.state, self.id
                );
                self.state = SessionState::Closed;
                self.close_code = Some(error_code);
                self.close_reason = Some(reason.clone());
                self.closed_at = Some(now);

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
                    error_code: Some(error_code),
                    reason: Some(reason.into()),
                });
                effects.extend(self.reset_all_streams(error_code, now));
            }
            WT_CAPSULE_TYPE_DRAIN_SESSION => {
                if self.state == SessionState::Connected {
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
                        error_code: None,
                        reason: None,
                    });
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
            _ => {}
        }

        effects
    }

    // CONNECT stream closure reception handling.
    pub(super) fn recv_connect_close(&mut self, now: f64) -> Vec<Effect> {
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
            "wt_session close actual={:?} session_id={}",
            self.state, self.id
        );
        self.state = SessionState::Closed;
        self.closed_at = Some(now);
        self.close_code = Some(0);
        self.close_reason = Some("wt_session close".to_owned());

        let mut effects = self.reset_all_streams(0, now);

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
            error_code: Some(0),
            reason: Some("wt_session close".into()),
        });

        effects
    }

    // Datagram reception handling.
    pub(super) fn recv_datagram(&mut self, data: Bytes) -> Vec<Effect> {
        let mut effects = Vec::new();
        if matches!(self.state, SessionState::Connected | SessionState::Draining) {
            self.datagrams_received += 1;
            self.datagram_bytes_received += data.len() as u64;
            effects.push(Effect::EmitSessionEvent {
                session_id: self.id,
                event_type: EventType::DatagramReceived,
                path: None,
                headers: None,
                wt_available_protocols: None,
                wt_protocol: None,
                data: Some(data),
                is_unidirectional: None,
                max_data: None,
                max_streams: None,
                ready_at: None,
                error_code: None,
                reason: None,
            });
        } else {
            debug!(
                "wt_session validate failed actual={:?} session_id={}",
                self.state, self.id
            );
        }
        effects
    }

    // Stream data reception handling.
    pub(super) fn recv_stream_data(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if matches!(
            self.state,
            SessionState::Closed | SessionState::Closing | SessionState::Draining
        ) {
            debug!(
                "wt_session validate failed actual={:?} session_id={} stream_id={stream_id}",
                self.state, self.id
            );
            return effects;
        }

        if !self.streams.contains_key(&stream_id) {
            let direction = stream_dir_from_id(stream_id, self.is_client);
            let is_peer_initiated = is_peer_initiated_stream(stream_id, self.is_client);

            match direction {
                StreamDirection::SendOnly => {
                    debug!(
                        "wt_stream validate invalid actual={:?} session_id={} stream_id={stream_id}",
                        direction, self.id
                    );
                    return effects;
                }
                StreamDirection::ReceiveOnly => {
                    if self.peer_streams_uni_opened >= self.local_max_streams_uni {
                        debug!(
                            "wt_session validate exceeded actual={} limit={} session_id={}",
                            self.peer_streams_uni_opened, self.local_max_streams_uni, self.id
                        );
                        return self.abort(
                            ERR_WT_FLOW_CONTROL_ERROR,
                            "wt_session validate exceeded".into(),
                            now,
                        );
                    }
                    self.peer_streams_uni_opened += 1;
                }
                StreamDirection::Bidirectional => {
                    if self.peer_streams_bidi_opened >= self.local_max_streams_bidi {
                        debug!(
                            "wt_session validate exceeded actual={} limit={} session_id={}",
                            self.peer_streams_bidi_opened, self.local_max_streams_bidi, self.id
                        );
                        return self.abort(
                            ERR_WT_FLOW_CONTROL_ERROR,
                            "wt_session validate exceeded".into(),
                            now,
                        );
                    }
                    self.peer_streams_bidi_opened += 1;
                }
            }

            self.active_streams.insert(stream_id);

            let stream = Stream::new(
                stream_id,
                self.id,
                direction,
                is_peer_initiated,
                self.max_stream_read_buffer_size,
                self.max_stream_write_buffer_size,
                now,
            );
            self.streams.insert(stream_id, stream);

            effects.push(Effect::EmitStreamEvent {
                stream_id,
                event_type: EventType::StreamOpened,
                session_id: Some(self.id),
                direction: Some(direction),
                is_peer_initiated: Some(is_peer_initiated),
                error_code: None,
            });
        }

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return effects;
        };

        self.local_data_received += data.len() as u64;
        if self.local_data_received > self.local_max_data {
            debug!(
                "wt_session validate exceeded actual={} limit={} session_id={}",
                self.local_data_received, self.local_max_data, self.id
            );
            return self.abort(
                ERR_WT_FLOW_CONTROL_ERROR,
                "wt_session validate exceeded".into(),
                now,
            );
        }

        let (stream_effects, consumed) = stream.recv_data(data, end_stream, now);
        let is_closed = stream.is_closed();

        effects.extend(stream_effects);

        if consumed > 0 {
            self.local_data_consumed += consumed;
            if let Some(credit_effect) = self.replenish_data_credit(false) {
                effects.push(credit_effect);
            }
        }

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }

        effects
    }

    // Transport stream reset reception handling.
    pub(super) fn recv_stream_reset(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let is_closed = if let Some(stream) = self.streams.get_mut(&stream_id) {
            effects.extend(stream.recv_reset(error_code, now));
            stream.is_closed()
        } else {
            false
        };

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }
        effects
    }

    // Transport stream stop_sending reception handling.
    pub(super) fn recv_stop_sending(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Vec<Effect> {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            return stream.recv_stop_sending(error_code);
        }
        Vec::new()
    }

    // User session rejection handling.
    pub(super) fn reject(
        &mut self,
        request_id: RequestId,
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

        debug!(
            "wt_session reject actual={:?} request_id={request_id} session_id={} err={status_code}",
            self.state, self.id
        );
        self.state = SessionState::Closed;
        self.closed_at = Some(now);
        self.close_reason = Some("wt_session reject".to_owned());

        effects.push(Effect::SendH3Headers {
            stream_id: self.id,
            headers: vec![(
                Bytes::from_static(b":status"),
                Bytes::from(status_code.to_string()),
            )],
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
            error_code: Some(u64::from(status_code)),
            reason: Some("wt_session reject".into()),
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    // User stream reset command handling.
    pub(super) fn reset_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let is_closed = if let Some(stream) = self.streams.get_mut(&stream_id) {
            effects.extend(stream.reset(request_id, error_code, now));
            stream.is_closed()
        } else {
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
            false
        };

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }

        effects
    }

    // User datagram send command handling.
    pub(super) fn send_datagram(
        &mut self,
        request_id: RequestId,
        data: Bytes,
        peer_max_datagram_size: u64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state != SessionState::Connected {
            debug!(
                "wt_session validate failed actual={:?} expected=connected request_id={request_id} session_id={}",
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

        if (data.len() as u64) > peer_max_datagram_size {
            debug!(
                "wt_datagram validate exceeded actual={} limit={peer_max_datagram_size} request_id={request_id} session_id={}",
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

        self.datagrams_sent += 1;
        self.datagram_bytes_sent += data.len() as u64;

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

    // User stream data send handling.
    pub(super) fn send_stream_data(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        data: Bytes,
        end_stream: bool,
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

        let session_credit = self.peer_max_data.saturating_sub(self.local_data_sent);

        let (stream_effects, sent, is_blocked, is_closed) = {
            let (fx, sent) = stream.write(
                request_id,
                data,
                end_stream,
                session_credit,
                self.peer_max_data,
            );
            (fx, sent, stream.has_pending_writes(), stream.is_closed())
        };

        effects.extend(stream_effects);
        self.local_data_sent += sent;

        if is_blocked && !self.blocked_streams.contains(&stream_id) {
            self.blocked_streams.insert(stream_id);
            self.blocked_streams_queue.push_back(stream_id);
        }

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }

        effects
    }

    // User stream stop command handling.
    pub(super) fn stop_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let is_closed = if let Some(stream) = self.streams.get_mut(&stream_id) {
            effects.extend(stream.stop(request_id, error_code, now));
            stream.is_closed()
        } else {
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
            false
        };

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }
        effects
    }

    // Stream diagnostics delegation.
    pub(super) fn stream_diagnostics(
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

    // User stream read request handling.
    pub(super) fn stream_read(
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

        let (stream_effects, consumed) = stream.read(request_id, max_bytes);
        let is_closed = stream.is_closed();

        effects.extend(stream_effects);

        if consumed > 0 {
            self.local_data_consumed += consumed;
            if let Some(credit_effect) = self.replenish_data_credit(false) {
                effects.push(credit_effect);
            }
        }

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }

        effects
    }

    // Session error abort handling.
    fn abort(&mut self, error_code: ErrorCode, reason: Cow<'static, str>, now: f64) -> Vec<Effect> {
        debug!(
            "wt_session abort actual={:?} session_id={} err={error_code}",
            self.state, self.id
        );
        self.state = SessionState::Closed;
        self.closed_at = Some(now);
        self.close_code = Some(error_code);
        self.close_reason = Some(reason.clone().into_owned());

        let mut effects = vec![
            Effect::ResetQuicStream {
                stream_id: self.id,
                error_code,
            },
            Effect::EmitSessionEvent {
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
                error_code: Some(error_code),
                reason: Some(reason),
            },
        ];
        effects.extend(self.reset_all_streams(error_code, now));
        effects
    }

    // Session diagnostics snapshot retrieval.
    fn diagnostics_snapshot(&self) -> SessionDiagnostics {
        SessionDiagnostics {
            active_streams: self.active_streams.clone(),
            blocked_streams: self.blocked_streams.clone(),
            close_code: self.close_code,
            close_reason: self.close_reason.clone(),
            closed_at: self.closed_at,
            created_at: self.created_at,
            datagram_bytes_received: self.datagram_bytes_received,
            datagram_bytes_sent: self.datagram_bytes_sent,
            datagrams_received: self.datagrams_received,
            datagrams_sent: self.datagrams_sent,
            flow_control_negotiated: self.flow_control_negotiated,
            headers: self.headers.clone(),
            is_client: self.is_client,
            local_data_consumed: self.local_data_consumed,
            local_data_received: self.local_data_received,
            local_data_sent: self.local_data_sent,
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
            pending_bidi_stream_requests: self.pending_bidi_stream_requests.clone(),
            pending_uni_stream_requests: self.pending_uni_stream_requests.clone(),
            ready_at: self.ready_at,
            session_id: self.id,
            state: self.state,
            wt_protocol: self.wt_protocol.clone(),
        }
    }

    // Blocked writes flushing.
    fn flush_blocked_writes(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.local_data_sent >= self.peer_max_data {
            return effects;
        }

        let count = self.blocked_streams_queue.len();
        for _ in 0..count {
            let Some(stream_id) = self.blocked_streams_queue.pop_front() else {
                break;
            };
            self.blocked_streams.remove(&stream_id);

            let session_credit = self.peer_max_data.saturating_sub(self.local_data_sent);
            if session_credit == 0 {
                self.blocked_streams_queue.push_front(stream_id);
                self.blocked_streams.insert(stream_id);
                break;
            }

            let Some(stream) = self.streams.get_mut(&stream_id) else {
                continue;
            };

            let (stream_effects, consumed) =
                stream.flush_writes(session_credit, self.peer_max_data);
            let has_more = stream.has_pending_writes();

            effects.extend(stream_effects);
            self.local_data_sent += consumed;

            if has_more {
                self.blocked_streams_queue.push_back(stream_id);
                self.blocked_streams.insert(stream_id);
            }
        }
        effects
    }

    // Closed stream handling.
    fn handle_closed(&mut self, stream_id: StreamId) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !self.active_streams.remove(&stream_id) {
            return effects;
        }
        self.blocked_streams.remove(&stream_id);

        if is_peer_initiated_stream(stream_id, self.is_client) {
            if is_unidirectional_stream(stream_id) {
                self.peer_streams_uni_closed += 1;
                if let Some(e) = self.replenish_streams_credit(true, false) {
                    effects.push(e);
                }
            } else {
                self.peer_streams_bidi_closed += 1;
                if let Some(e) = self.replenish_streams_credit(false, false) {
                    effects.push(e);
                }
            }
        }

        effects
    }

    // Data credit replenishment.
    fn replenish_data_credit(&mut self, force_send: bool) -> Option<Effect> {
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Closing | SessionState::Draining
        ) {
            return None;
        }

        let new_limit = next_data_limit(
            self.local_max_data,
            self.local_data_consumed,
            self.flow_control_window,
            self.flow_control_window_auto_scale_enabled,
            force_send,
        )?;

        let mut buf = BytesMut::with_capacity(8);
        if let Err(e) = write_varint(&mut buf, new_limit) {
            debug!("varint encode failed session_id={} err={e:?}", self.id);
            return None;
        }

        self.local_max_data = new_limit;

        Some(Effect::SendH3Capsule {
            stream_id: self.id,
            capsule_type: WT_CAPSULE_TYPE_MAX_DATA,
            capsule_data: buf.freeze(),
            end_stream: false,
        })
    }

    // Streams credit replenishment.
    fn replenish_streams_credit(&mut self, is_uni: bool, force_send: bool) -> Option<Effect> {
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Closing | SessionState::Draining
        ) {
            return None;
        }

        let (current, closed, initial, cap_type) = if is_uni {
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

        let new_limit = next_stream_limit(
            current,
            closed,
            initial,
            self.flow_control_window_auto_scale_enabled,
            force_send,
        )?;

        let mut buf = BytesMut::with_capacity(8);
        if let Err(e) = write_varint(&mut buf, new_limit) {
            debug!("varint encode failed session_id={} err={e:?}", self.id);
            return None;
        }

        if is_uni {
            self.local_max_streams_uni = new_limit;
        } else {
            self.local_max_streams_bidi = new_limit;
        }

        Some(Effect::SendH3Capsule {
            stream_id: self.id,
            capsule_type: cap_type,
            capsule_data: buf.freeze(),
            end_stream: false,
        })
    }

    // Reset all session streams.
    fn reset_all_streams(&mut self, error_code: ErrorCode, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        let mut stream_ids: Vec<_> = self.streams.keys().copied().collect();
        stream_ids.sort_unstable();

        for stream_id in stream_ids {
            if let Some(stream) = self.streams.get_mut(&stream_id).filter(|s| !s.is_closed()) {
                let can_send = matches!(
                    stream.direction(),
                    StreamDirection::Bidirectional | StreamDirection::SendOnly
                );
                let can_receive = matches!(
                    stream.direction(),
                    StreamDirection::Bidirectional | StreamDirection::ReceiveOnly
                );

                if can_send {
                    effects.extend(stream.reset(0, error_code, now));
                }
                if can_receive {
                    effects.extend(stream.stop(0, error_code, now));
                }
            }
            effects.extend(self.handle_closed(stream_id));
        }
        self.streams.clear();
        self.active_streams.clear();
        self.blocked_streams_queue.clear();
        self.blocked_streams.clear();

        while let Some(req_id) = self.pending_bidi_stream_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                source: ErrorSource::Session,
                error_code: Some(error_code),
                reason: "wt_session close".into(),
            });
        }
        while let Some(req_id) = self.pending_uni_stream_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                source: ErrorSource::Session,
                error_code: Some(error_code),
                reason: "wt_session close".into(),
            });
        }

        effects
    }
}

// Session initialization constraints and thresholds.
#[derive(Clone, Copy, Debug)]
pub(super) struct SessionParams {
    pub(super) flow_control_negotiated: bool,
    pub(super) flow_control_window: u64,
    pub(super) flow_control_window_auto_scale_enabled: bool,
    pub(super) initial_max_data: u64,
    pub(super) initial_max_streams_bidi: u64,
    pub(super) initial_max_streams_uni: u64,
    pub(super) max_stream_read_buffer_size: u64,
    pub(super) max_stream_write_buffer_size: u64,
    pub(super) peer_max_data: u64,
    pub(super) peer_max_streams_bidi: u64,
    pub(super) peer_max_streams_uni: u64,
}

#[cfg(test)]
mod tests;
