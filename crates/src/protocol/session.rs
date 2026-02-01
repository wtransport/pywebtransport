//! Session-level state machine and resource aggregator.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Cursor;

use bytes::{Bytes, BytesMut};
use serde::Serialize;
use tracing::{debug, error, info, warn};

use crate::common::constants::{
    CLOSE_WEBTRANSPORT_SESSION_TYPE, DRAIN_WEBTRANSPORT_SESSION_TYPE, ERR_FLOW_CONTROL_ERROR,
    ERR_H3_FRAME_UNEXPECTED, ERR_H3_GENERAL_PROTOCOL_ERROR, ERR_LIB_INTERNAL_ERROR,
    ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR, MAX_PROTOCOL_STREAMS_LIMIT,
    WT_DATA_BLOCKED_TYPE, WT_MAX_DATA_TYPE, WT_MAX_STREAM_DATA_TYPE, WT_MAX_STREAMS_BIDI_TYPE,
    WT_MAX_STREAMS_UNI_TYPE, WT_STREAM_DATA_BLOCKED_TYPE, WT_STREAMS_BLOCKED_BIDI_TYPE,
    WT_STREAMS_BLOCKED_UNI_TYPE,
};
use crate::common::types::{
    ErrorCode, EventType, Headers, RequestId, SessionId, SessionState, StreamDirection, StreamId,
    StreamState,
};
use crate::protocol::events::{Effect, RequestResult};
use crate::protocol::stream::Stream;
use crate::protocol::utils::{
    can_receive_on_stream, is_peer_initiated_stream, is_unidirectional_stream, next_data_limit,
    next_stream_limit, read_varint, serialize_headers, stream_dir_from_id, write_varint,
};

// Representation of a WebTransport session.
pub(crate) struct Session {
    pub(crate) id: SessionId,
    pub(crate) state: SessionState,
    pub(crate) path: String,
    pub(crate) headers: Headers,
    pub(crate) created_at: f64,
    pub(crate) local_max_data: u64,
    pub(crate) local_data_sent: u64,
    pub(crate) local_data_consumed: u64,
    pub(crate) peer_max_data: u64,
    pub(crate) peer_data_sent: u64,
    pub(crate) local_max_streams_bidi: u64,
    pub(crate) local_streams_bidi_opened: u64,
    pub(crate) peer_max_streams_bidi: u64,
    pub(crate) peer_streams_bidi_opened: u64,
    pub(crate) peer_streams_bidi_closed: u64,
    pub(crate) local_max_streams_uni: u64,
    pub(crate) local_streams_uni_opened: u64,
    pub(crate) peer_max_streams_uni: u64,
    pub(crate) peer_streams_uni_opened: u64,
    pub(crate) peer_streams_uni_closed: u64,
    pub(crate) pending_bidi_stream_requests: VecDeque<RequestId>,
    pub(crate) pending_uni_stream_requests: VecDeque<RequestId>,
    pub(crate) datagrams_sent: u64,
    pub(crate) datagram_bytes_sent: u64,
    pub(crate) datagrams_received: u64,
    pub(crate) datagram_bytes_received: u64,
    pub(crate) active_streams: HashSet<StreamId>,
    pub(crate) blocked_streams: HashSet<StreamId>,
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
    pub(crate) ready_at: Option<f64>,
    blocked_streams_queue: VecDeque<StreamId>,
    flow_control_window_auto_scale: bool,
    initial_max_streams_bidi: u64,
    initial_max_streams_uni: u64,
    is_client: bool,
    flow_control_window_size: u64,
    stream_read_buffer_size: u64,
    stream_write_buffer_size: u64,
    streams: HashMap<StreamId, Stream>,
}

impl Session {
    // New session entity creation.
    #[allow(
        clippy::too_many_arguments,
        reason = "Complex internal state initialization."
    )]
    pub(crate) fn new(
        id: SessionId,
        path: String,
        headers: Headers,
        created_at: f64,
        initial_max_data: u64,
        initial_max_streams_bidi: u64,
        initial_max_streams_uni: u64,
        peer_max_data: u64,
        peer_max_streams_bidi: u64,
        peer_max_streams_uni: u64,
        flow_control_window_size: u64,
        stream_read_buffer_size: u64,
        stream_write_buffer_size: u64,
        flow_control_window_auto_scale: bool,
        is_client: bool,
        state: SessionState,
    ) -> Self {
        Self {
            id,
            state,
            path,
            headers,
            created_at,
            local_max_data: initial_max_data,
            local_data_sent: 0,
            local_data_consumed: 0,
            peer_max_data,
            peer_data_sent: 0,
            local_max_streams_bidi: initial_max_streams_bidi,
            local_streams_bidi_opened: 0,
            peer_max_streams_bidi,
            peer_streams_bidi_opened: 0,
            peer_streams_bidi_closed: 0,
            local_max_streams_uni: initial_max_streams_uni,
            local_streams_uni_opened: 0,
            peer_max_streams_uni,
            peer_streams_uni_opened: 0,
            peer_streams_uni_closed: 0,
            pending_bidi_stream_requests: VecDeque::new(),
            pending_uni_stream_requests: VecDeque::new(),
            datagrams_sent: 0,
            datagram_bytes_sent: 0,
            datagrams_received: 0,
            datagram_bytes_received: 0,
            active_streams: HashSet::new(),
            blocked_streams: HashSet::new(),
            close_code: None,
            close_reason: None,
            closed_at: None,
            ready_at: None,
            blocked_streams_queue: VecDeque::new(),
            flow_control_window_auto_scale,
            initial_max_streams_bidi,
            initial_max_streams_uni,
            is_client,
            flow_control_window_size,
            stream_read_buffer_size,
            stream_write_buffer_size,
            streams: HashMap::new(),
        }
    }

    // User session acceptance handling.
    pub(crate) fn accept(&mut self, request_id: RequestId, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.is_client {
            warn!("Client cannot accept sessions (request_id={request_id})");
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: None,
                reason: "Client cannot accept sessions".to_owned(),
            });
            return effects;
        }

        if self.state != SessionState::Connecting {
            warn!("Session {} is not in connecting state", self.id);
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: format!("Session {} is not in connecting state", self.id),
            });
            return effects;
        }

        debug!("Session {} accepted", self.id);
        self.state = SessionState::Connected;
        self.ready_at = Some(now);

        effects.push(Effect::SendH3Headers {
            stream_id: self.id,
            status: 200,
            end_stream: false,
        });
        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionReady,
            code: None,
            data: None,
            headers: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            path: None,
            ready_at: Some(now),
            reason: None,
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });
        info!("Accepted session {}", self.id);

        effects
    }

    // QUIC stream binding.
    pub(crate) fn bind_stream(
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

        let mut stream = Stream::new(
            stream_id,
            self.id,
            direction,
            now,
            self.stream_read_buffer_size,
            self.stream_write_buffer_size,
        );
        stream.state = StreamState::Open;

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
                direction: Some(direction),
                session_id: Some(self.id),
            },
        ]
    }

    // User session closure handling.
    pub(crate) fn close(
        &mut self,
        request_id: RequestId,
        error_code: ErrorCode,
        reason: Option<String>,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state == SessionState::Closed {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
            return effects;
        }

        info!("Closing session {} by user request", self.id);
        debug!(
            "Closing session {} with code {error_code} reason: {reason:?}",
            self.id
        );

        self.state = SessionState::Closed;
        self.closed_at = Some(now);
        self.close_code = Some(error_code);
        self.close_reason.clone_from(&reason);

        effects.extend(self.reset_all_streams(error_code, now));

        let reason_str = reason.as_deref().unwrap_or("");
        let reason_bytes = reason_str.as_bytes();
        let truncated_reason = if reason_bytes.len() > 1024 {
            reason_bytes.get(..1024).unwrap_or(reason_bytes)
        } else {
            reason_bytes
        };

        let mut buf = BytesMut::with_capacity(4 + truncated_reason.len());
        buf.extend_from_slice(&(u32::try_from(error_code).unwrap_or(u32::MAX)).to_be_bytes());
        buf.extend_from_slice(truncated_reason);

        effects.push(Effect::SendH3Capsule {
            stream_id: self.id,
            capsule_type: CLOSE_WEBTRANSPORT_SESSION_TYPE,
            capsule_data: buf.freeze(),
            end_stream: true,
        });

        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionClosed,
            code: Some(error_code),
            data: None,
            headers: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            path: None,
            ready_at: None,
            reason,
        });

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

    // User stream creation request handling.
    pub(crate) fn create_stream(
        &mut self,
        request_id: RequestId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !matches!(self.state, SessionState::Connected | SessionState::Draining) {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: format!("Session {} is not connected or draining", self.id),
            });
            return effects;
        }

        let limit_exceeded = if is_unidirectional {
            self.local_streams_uni_opened >= self.peer_max_streams_uni
        } else {
            self.local_streams_bidi_opened >= self.peer_max_streams_bidi
        };

        if limit_exceeded {
            if self.is_client {
                let mut buf = BytesMut::with_capacity(8);
                if is_unidirectional {
                    debug!(
                        "Client uni stream creation for session {} blocked by flow control ({} >= {})",
                        self.id, self.local_streams_uni_opened, self.peer_max_streams_uni
                    );
                    self.pending_uni_stream_requests.push_back(request_id);
                    if let Err(e) = write_varint(&mut buf, self.peer_max_streams_uni) {
                        error!("Internal error encoding blocked limit: {e:?}");
                    } else {
                        effects.push(Effect::SendH3Capsule {
                            stream_id: self.id,
                            capsule_type: WT_STREAMS_BLOCKED_UNI_TYPE,
                            capsule_data: buf.freeze(),
                            end_stream: false,
                        });
                    }
                } else {
                    debug!(
                        "Client bidi stream creation for session {} blocked by flow control ({} >= {})",
                        self.id, self.local_streams_bidi_opened, self.peer_max_streams_bidi
                    );
                    self.pending_bidi_stream_requests.push_back(request_id);
                    if let Err(e) = write_varint(&mut buf, self.peer_max_streams_bidi) {
                        error!("Internal error encoding blocked limit: {e:?}");
                    } else {
                        effects.push(Effect::SendH3Capsule {
                            stream_id: self.id,
                            capsule_type: WT_STREAMS_BLOCKED_BIDI_TYPE,
                            capsule_data: buf.freeze(),
                            end_stream: false,
                        });
                    }
                }
            } else {
                warn!("Stream limit reached for session {}", self.id);
                effects.push(Effect::NotifyRequestFailed {
                    request_id,
                    error_code: None,
                    reason: "Stream limit reached".to_owned(),
                });
            }
            return effects;
        }

        if is_unidirectional {
            self.local_streams_uni_opened += 1;
        } else {
            self.local_streams_bidi_opened += 1;
        }

        debug!(
            "Creating {} stream for session {}",
            if is_unidirectional { "uni" } else { "bidi" },
            self.id
        );

        effects.push(Effect::CreateQuicStream {
            request_id,
            session_id: self.id,
            is_unidirectional,
        });

        effects
    }

    // User session diagnostics event handling.
    pub(crate) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diag = self.diagnostics_snapshot();
        let json_str = serde_json::to_string(&diag).unwrap_or_else(|_| "{}".to_owned());
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::Diagnostics(json_str),
        }]
    }

    // Failed QUIC stream creation handling.
    pub(crate) fn fail_stream(
        &mut self,
        request_id: RequestId,
        is_unidirectional: bool,
        error_code: Option<ErrorCode>,
        reason: String,
    ) -> Vec<Effect> {
        if is_unidirectional {
            if self.local_streams_uni_opened > 0 {
                self.local_streams_uni_opened -= 1;
            }
        } else if self.local_streams_bidi_opened > 0 {
            self.local_streams_bidi_opened -= 1;
        }

        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code,
            reason,
        }]
    }

    // Manual data credit grant handling.
    pub(crate) fn grant_data_credit(
        &mut self,
        request_id: RequestId,
        max_data: u64,
    ) -> Vec<Effect> {
        if max_data <= self.local_max_data {
            warn!(
                "Manual data credit grant ({max_data}) is not greater than current limit ({}). Ignoring.",
                self.local_max_data
            );
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            }];
        }

        let mut buf = BytesMut::with_capacity(8);
        if let Err(_e) = write_varint(&mut buf, max_data) {
            error!("Internal error: Granted data credit exceeds VarInt limit");
            return vec![Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_INTERNAL_ERROR),
                reason: "Granted credit exceeds protocol limits".to_owned(),
            }];
        }

        self.local_max_data = max_data;

        vec![
            Effect::SendH3Capsule {
                stream_id: self.id,
                capsule_type: WT_MAX_DATA_TYPE,
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
    pub(crate) fn grant_streams_credit(
        &mut self,
        request_id: RequestId,
        max_streams: u64,
        is_uni: bool,
    ) -> Vec<Effect> {
        if self.state == SessionState::Closed || self.state == SessionState::Draining {
            return vec![Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: format!("Cannot grant credit to session in state {:?}", self.state),
            }];
        }

        let current = if is_uni {
            self.local_max_streams_uni
        } else {
            self.local_max_streams_bidi
        };
        if max_streams <= current {
            if is_uni {
                warn!(
                    "Manual uni streams credit grant ({max_streams}) is not greater than current limit ({current}). Ignoring."
                );
            } else {
                warn!(
                    "Manual bidi streams credit grant ({max_streams}) is not greater than current limit ({current}). Ignoring."
                );
            }
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            }];
        }

        let cap_type = if is_uni {
            WT_MAX_STREAMS_UNI_TYPE
        } else {
            WT_MAX_STREAMS_BIDI_TYPE
        };

        let mut buf = BytesMut::with_capacity(8);
        if let Err(_e) = write_varint(&mut buf, max_streams) {
            error!("Internal error: Granted streams credit exceeds VarInt limit");
            return vec![Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_INTERNAL_ERROR),
                reason: "Granted credit exceeds protocol limits".to_owned(),
            }];
        }

        if is_uni {
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

    // Closed streams pruning.
    pub(crate) fn prune_closed_streams(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut ids_to_remove: Vec<StreamId> = self
            .streams
            .iter()
            .filter(|(_, stream)| stream.state == StreamState::Closed)
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
    pub(crate) fn recv_capsule(
        &mut self,
        capsule_type: u64,
        data: &Bytes,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut cur = Cursor::new(&data[..]);

        match capsule_type {
            WT_MAX_DATA_TYPE => {
                if let Ok(new_max) = read_varint(&mut cur) {
                    if new_max > self.peer_max_data {
                        debug!(
                            "Session {} flow credit received: peer_max_data updating from {} to {new_max}",
                            self.id, self.peer_max_data
                        );
                        self.peer_max_data = new_max;
                        effects.push(Effect::EmitSessionEvent {
                            session_id: self.id,
                            event_type: EventType::SessionMaxDataUpdated,
                            max_data: Some(new_max),
                            code: None,
                            data: None,
                            headers: None,
                            is_unidirectional: None,
                            max_streams: None,
                            path: None,
                            ready_at: None,
                            reason: None,
                        });
                        effects.extend(self.flush_blocked_writes());
                    } else if new_max < self.peer_max_data {
                        return self.abort(
                            ERR_FLOW_CONTROL_ERROR,
                            "Flow control limit decreased for MAX_DATA".to_owned(),
                            now,
                        );
                    }
                } else {
                    warn!(
                        "Error processing capsule for session {}: Malformed WT_MAX_DATA",
                        self.id
                    );
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "Capsule processing error".to_owned(),
                        now,
                    );
                }
            }
            WT_MAX_STREAMS_BIDI_TYPE => {
                if let Ok(new_max) = read_varint(&mut cur) {
                    if new_max > MAX_PROTOCOL_STREAMS_LIMIT {
                        return self.abort(
                            ERR_FLOW_CONTROL_ERROR,
                            format!("MAX_STREAMS_BIDI limit exceeds protocol maximum ({new_max})"),
                            now,
                        );
                    }
                    if new_max > self.peer_max_streams_bidi {
                        self.peer_max_streams_bidi = new_max;
                        effects.push(Effect::EmitSessionEvent {
                            session_id: self.id,
                            event_type: EventType::SessionMaxStreamsBidiUpdated,
                            max_streams: Some(new_max),
                            code: None,
                            data: None,
                            headers: None,
                            is_unidirectional: None,
                            max_data: None,
                            path: None,
                            ready_at: None,
                            reason: None,
                        });

                        if self.is_client {
                            while self.local_streams_bidi_opened < self.peer_max_streams_bidi
                                && !self.pending_bidi_stream_requests.is_empty()
                            {
                                if let Some(req_id) = self.pending_bidi_stream_requests.pop_front()
                                {
                                    self.local_streams_bidi_opened += 1;
                                    effects.push(Effect::CreateQuicStream {
                                        request_id: req_id,
                                        session_id: self.id,
                                        is_unidirectional: false,
                                    });
                                }
                            }
                        }
                    } else if new_max < self.peer_max_streams_bidi {
                        return self.abort(
                            ERR_FLOW_CONTROL_ERROR,
                            "Flow control limit decreased for MAX_STREAMS_BIDI".to_owned(),
                            now,
                        );
                    }
                } else {
                    warn!(
                        "Error processing capsule for session {}: Malformed WT_MAX_STREAMS_BIDI",
                        self.id
                    );
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "Capsule processing error".to_owned(),
                        now,
                    );
                }
            }
            WT_MAX_STREAMS_UNI_TYPE => {
                if let Ok(new_max) = read_varint(&mut cur) {
                    if new_max > MAX_PROTOCOL_STREAMS_LIMIT {
                        return self.abort(
                            ERR_FLOW_CONTROL_ERROR,
                            format!("MAX_STREAMS_UNI limit exceeds protocol maximum ({new_max})"),
                            now,
                        );
                    }
                    if new_max > self.peer_max_streams_uni {
                        self.peer_max_streams_uni = new_max;
                        effects.push(Effect::EmitSessionEvent {
                            session_id: self.id,
                            event_type: EventType::SessionMaxStreamsUniUpdated,
                            max_streams: Some(new_max),
                            code: None,
                            data: None,
                            headers: None,
                            is_unidirectional: None,
                            max_data: None,
                            path: None,
                            ready_at: None,
                            reason: None,
                        });

                        if self.is_client {
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
                        }
                    } else if new_max < self.peer_max_streams_uni {
                        return self.abort(
                            ERR_FLOW_CONTROL_ERROR,
                            "Flow control limit decreased for MAX_STREAMS_UNI".to_owned(),
                            now,
                        );
                    }
                } else {
                    warn!("Error reading varint for WT_MAX_STREAMS_UNI_TYPE");
                    return self.abort(
                        ERR_H3_GENERAL_PROTOCOL_ERROR,
                        "Capsule processing error".to_owned(),
                        now,
                    );
                }
            }
            WT_DATA_BLOCKED_TYPE => {
                debug!("Session {} received WT_DATA_BLOCKED from peer", self.id);
                if let Some(credit_effect) = self.replenish_data_credit(true) {
                    effects.push(credit_effect);
                } else {
                    effects.push(Effect::EmitSessionEvent {
                        session_id: self.id,
                        event_type: EventType::SessionDataBlocked,
                        code: None,
                        data: None,
                        headers: None,
                        is_unidirectional: None,
                        max_data: None,
                        max_streams: None,
                        path: None,
                        ready_at: None,
                        reason: None,
                    });
                }
            }
            WT_STREAMS_BLOCKED_BIDI_TYPE | WT_STREAMS_BLOCKED_UNI_TYPE => {
                let is_uni = capsule_type == WT_STREAMS_BLOCKED_UNI_TYPE;
                debug!(
                    "Session {} received WT_STREAMS_BLOCKED (uni={is_uni}) from peer",
                    self.id
                );
                if let Some(credit_effect) = self.replenish_streams_credit(is_uni, true) {
                    effects.push(credit_effect);
                } else {
                    effects.push(Effect::EmitSessionEvent {
                        session_id: self.id,
                        event_type: EventType::SessionStreamsBlocked,
                        is_unidirectional: Some(is_uni),
                        code: None,
                        data: None,
                        headers: None,
                        max_data: None,
                        max_streams: None,
                        path: None,
                        ready_at: None,
                        reason: None,
                    });
                }
            }
            CLOSE_WEBTRANSPORT_SESSION_TYPE => {
                let code = read_varint(&mut cur).unwrap_or_default();
                let pos = usize::try_from(cur.position()).unwrap_or(0);
                let reason_bytes = data.get(pos..).unwrap_or_default();
                let reason = String::from_utf8_lossy(reason_bytes).into_owned();

                info!(
                    "Received CLOSE_SESSION for {}: code={code:#x} reason='{reason}'",
                    self.id
                );

                self.state = SessionState::Closed;
                self.close_code = Some(code);
                self.close_reason = Some(reason.clone());
                self.closed_at = Some(now);

                effects.push(Effect::EmitSessionEvent {
                    session_id: self.id,
                    event_type: EventType::SessionClosed,
                    code: Some(code),
                    reason: Some(reason),
                    data: None,
                    headers: None,
                    is_unidirectional: None,
                    max_data: None,
                    max_streams: None,
                    path: None,
                    ready_at: None,
                });
                effects.extend(self.reset_all_streams(code, now));
            }
            DRAIN_WEBTRANSPORT_SESSION_TYPE => {
                info!("Received DRAIN_SESSION for {}", self.id);
                if self.state == SessionState::Connected {
                    self.state = SessionState::Draining;
                    effects.push(Effect::EmitSessionEvent {
                        session_id: self.id,
                        event_type: EventType::SessionDraining,
                        code: None,
                        data: None,
                        headers: None,
                        is_unidirectional: None,
                        max_data: None,
                        max_streams: None,
                        path: None,
                        ready_at: None,
                        reason: None,
                    });
                }
            }
            WT_MAX_STREAM_DATA_TYPE | WT_STREAM_DATA_BLOCKED_TYPE => {
                return self.abort(
                    ERR_H3_FRAME_UNEXPECTED,
                    format!("Forbidden capsule type received: {capsule_type:#x}"),
                    now,
                );
            }
            _ => {
                debug!(
                    "Ignoring unknown capsule type {capsule_type} for session {}",
                    self.id
                );
            }
        }

        effects
    }

    // CONNECT stream closure reception handling.
    pub(crate) fn recv_connect_close(&mut self, now: f64) -> Vec<Effect> {
        if self.state == SessionState::Closed {
            return Vec::new();
        }

        info!(
            "Session {} cleanly closed by peer (CONNECT stream FIN)",
            self.id
        );

        self.state = SessionState::Closed;
        self.closed_at = Some(now);
        self.close_code = Some(0);
        self.close_reason = Some("CONNECT stream cleanly closed".to_owned());

        let mut effects = self.reset_all_streams(0, now);

        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionClosed,
            code: Some(0),
            data: None,
            headers: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            path: None,
            ready_at: None,
            reason: Some("CONNECT stream cleanly closed".to_owned()),
        });

        effects
    }

    // Datagram reception handling.
    pub(crate) fn recv_datagram(&mut self, data: Bytes) -> Vec<Effect> {
        let mut effects = Vec::new();
        if matches!(self.state, SessionState::Connected | SessionState::Draining) {
            self.datagrams_received += 1;
            self.datagram_bytes_received += data.len() as u64;
            effects.push(Effect::EmitSessionEvent {
                session_id: self.id,
                event_type: EventType::DatagramReceived,
                data: Some(data),
                code: None,
                headers: None,
                is_unidirectional: None,
                max_data: None,
                max_streams: None,
                path: None,
                ready_at: None,
                reason: None,
            });
        } else {
            debug!(
                "Ignoring datagram for non-active session {} state {:?}",
                self.id, self.state
            );
        }
        effects
    }

    // Stream data reception handling.
    pub(crate) fn recv_stream_data(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state == SessionState::Closed || self.state == SessionState::Draining {
            debug!("Ignoring WT data for already closed/reset stream {stream_id}");
            return effects;
        }

        if !self.streams.contains_key(&stream_id) {
            if self.is_client {
                warn!("Client received WT data for unknown stream {stream_id}, ignoring.");
                return effects;
            }

            let direction = stream_dir_from_id(stream_id, self.is_client);

            match direction {
                StreamDirection::SendOnly => {
                    warn!("Received data on send-only stream {stream_id}, ignoring.");
                    return effects;
                }
                StreamDirection::ReceiveOnly => {
                    if self.peer_streams_uni_opened >= self.local_max_streams_uni {
                        warn!(
                            "Stream limit reached (uni): {} >= {}, ignoring stream {stream_id}",
                            self.peer_streams_uni_opened, self.local_max_streams_uni
                        );
                        return effects;
                    }
                    self.peer_streams_uni_opened += 1;
                }
                StreamDirection::Bidirectional => {
                    if self.peer_streams_bidi_opened >= self.local_max_streams_bidi {
                        warn!(
                            "Stream limit reached (bidi): {} >= {}, ignoring stream {stream_id}",
                            self.peer_streams_bidi_opened, self.local_max_streams_bidi
                        );
                        return effects;
                    }
                    self.peer_streams_bidi_opened += 1;
                }
            }

            debug!(
                "Creating new incoming stream {stream_id} for session {}",
                self.id
            );
            self.active_streams.insert(stream_id);

            let stream = Stream::new(
                stream_id,
                self.id,
                direction,
                now,
                self.stream_read_buffer_size,
                self.stream_write_buffer_size,
            );
            self.streams.insert(stream_id, stream);

            effects.push(Effect::EmitStreamEvent {
                stream_id,
                event_type: EventType::StreamOpened,
                direction: Some(direction),
                session_id: Some(self.id),
            });
        }

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return effects;
        };

        let (stream_effects, consumed) = stream.recv_data(data, end_stream, now);
        let is_closed = stream.state == StreamState::Closed;

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
    pub(crate) fn recv_stream_reset(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let is_closed = if let Some(stream) = self.streams.get_mut(&stream_id) {
            effects.extend(stream.recv_reset(error_code, now));
            stream.state == StreamState::Closed
        } else {
            false
        };

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }
        effects
    }

    // User session rejection handling.
    pub(crate) fn reject(
        &mut self,
        request_id: RequestId,
        status_code: u16,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.is_client {
            warn!("Client cannot reject sessions (request_id={request_id})");
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: None,
                reason: "Client cannot reject sessions".to_owned(),
            });
            return effects;
        }

        if self.state != SessionState::Connecting {
            warn!("Session {} is not in connecting state", self.id);
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: format!("Session {} is not in connecting state", self.id),
            });
            return effects;
        }

        debug!("Session {} rejected with status {status_code}", self.id);
        self.state = SessionState::Closed;
        self.closed_at = Some(now);
        self.close_reason = Some(format!("Rejected by application with status {status_code}"));

        effects.push(Effect::SendH3Headers {
            stream_id: self.id,
            status: status_code,
            end_stream: true,
        });
        effects.push(Effect::EmitSessionEvent {
            session_id: self.id,
            event_type: EventType::SessionClosed,
            code: Some(u64::from(status_code)),
            data: None,
            headers: None,
            is_unidirectional: None,
            max_data: None,
            max_streams: None,
            path: None,
            ready_at: None,
            reason: Some("Rejected by application".to_owned()),
        });
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        info!("Rejected session {} with status {status_code}", self.id);
        effects
    }

    // User stream reset command handling.
    pub(crate) fn reset_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let is_closed = if let Some(stream) = self.streams.get_mut(&stream_id) {
            effects.extend(stream.reset(request_id, error_code, now));
            stream.state == StreamState::Closed
        } else {
            false
        };

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }

        effects
    }

    // User datagram send command handling.
    pub(crate) fn send_datagram(
        &mut self,
        request_id: RequestId,
        data: Bytes,
        remote_max_size: u64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state != SessionState::Connected {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
                reason: format!("Session {} is not connected", self.id),
            });
            return effects;
        }

        if (data.len() as u64) > remote_max_size {
            warn!(
                "Datagram too large to send ({} > {remote_max_size})",
                data.len()
            );
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: None,
                reason: format!(
                    "Datagram size {} exceeds limit {remote_max_size}",
                    data.len()
                ),
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
    pub(crate) fn send_stream_data(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        data: Bytes,
        end_stream: bool,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: format!("Stream {stream_id} not found"),
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
            (
                fx,
                sent,
                stream.write_buffer_size > 0,
                stream.state == StreamState::Closed,
            )
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
    pub(crate) fn stop_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let is_closed = if let Some(stream) = self.streams.get_mut(&stream_id) {
            effects.extend(stream.stop(request_id, error_code, now));
            stream.state == StreamState::Closed
        } else {
            false
        };

        if is_closed && self.active_streams.contains(&stream_id) {
            effects.extend(self.handle_closed(stream_id));
        }
        effects
    }

    // Stream diagnostics delegation.
    pub(crate) fn stream_diagnostics(
        &self,
        stream_id: StreamId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        if let Some(stream) = self.streams.get(&stream_id) {
            return stream.diagnose(request_id);
        }

        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            reason: format!("Stream {stream_id} not found"),
        }]
    }

    // User stream read request handling.
    pub(crate) fn stream_read(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        max_bytes: u64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                reason: format!("Stream {stream_id} not found"),
            });
            return effects;
        };

        let (stream_effects, consumed) = stream.read(request_id, max_bytes);
        let is_closed = stream.state == StreamState::Closed;

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

    // Unread stream data return handling.
    pub(crate) fn unread_stream(&mut self, stream_id: StreamId, data: Bytes) -> Vec<Effect> {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            return stream.unread(data);
        }
        Vec::new()
    }

    // Session error abort handling.
    fn abort(&mut self, error_code: ErrorCode, reason: String, now: f64) -> Vec<Effect> {
        self.state = SessionState::Closed;
        self.closed_at = Some(now);
        self.close_code = Some(error_code);
        self.close_reason = Some(reason.clone());

        let mut effects = vec![
            Effect::ResetQuicStream {
                stream_id: self.id,
                error_code,
            },
            Effect::EmitSessionEvent {
                session_id: self.id,
                event_type: EventType::SessionClosed,
                code: Some(error_code),
                reason: Some(reason),
                data: None,
                headers: None,
                is_unidirectional: None,
                max_data: None,
                max_streams: None,
                path: None,
                ready_at: None,
            },
        ];
        effects.extend(self.reset_all_streams(error_code, now));
        effects
    }

    // Session diagnostics snapshot retrieval.
    fn diagnostics_snapshot(&self) -> SessionDiagnostics {
        SessionDiagnostics {
            session_id: self.id,
            state: self.state,
            path: self.path.clone(),
            headers: self.headers.clone(),
            created_at: self.created_at,
            local_max_data: self.local_max_data,
            local_data_sent: self.local_data_sent,
            local_data_consumed: self.local_data_consumed,
            peer_max_data: self.peer_max_data,
            peer_data_sent: self.peer_data_sent,
            local_max_streams_bidi: self.local_max_streams_bidi,
            local_streams_bidi_opened: self.local_streams_bidi_opened,
            peer_max_streams_bidi: self.peer_max_streams_bidi,
            peer_streams_bidi_opened: self.peer_streams_bidi_opened,
            peer_streams_bidi_closed: self.peer_streams_bidi_closed,
            local_max_streams_uni: self.local_max_streams_uni,
            local_streams_uni_opened: self.local_streams_uni_opened,
            peer_max_streams_uni: self.peer_max_streams_uni,
            peer_streams_uni_opened: self.peer_streams_uni_opened,
            peer_streams_uni_closed: self.peer_streams_uni_closed,
            pending_bidi_stream_requests: self.pending_bidi_stream_requests.clone(),
            pending_uni_stream_requests: self.pending_uni_stream_requests.clone(),
            datagrams_sent: self.datagrams_sent,
            datagram_bytes_sent: self.datagram_bytes_sent,
            datagrams_received: self.datagrams_received,
            datagram_bytes_received: self.datagram_bytes_received,
            active_streams: self.active_streams.clone(),
            blocked_streams: self.blocked_streams.clone(),
            close_code: self.close_code,
            close_reason: self.close_reason.clone(),
            closed_at: self.closed_at,
            ready_at: self.ready_at,
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

            debug!(
                "Draining write buffer for stream {stream_id} (session {}) with {session_credit} credit",
                self.id
            );
            let (stream_effects, consumed) =
                stream.flush_writes(session_credit, self.peer_max_data);
            let has_more = stream.write_buffer_size > 0;

            effects.extend(stream_effects);
            self.local_data_sent += consumed;

            if has_more {
                self.blocked_streams_queue.push_back(stream_id);
                self.blocked_streams.insert(stream_id);
            } else {
                debug!("Stream {stream_id} send side closed (from buffer drain)");
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
        if self.state == SessionState::Closed || self.state == SessionState::Draining {
            return None;
        }

        let new_limit = next_data_limit(
            self.local_max_data,
            self.local_data_consumed,
            self.flow_control_window_size,
            self.flow_control_window_auto_scale,
            force_send,
        )?;

        let mut buf = BytesMut::with_capacity(8);
        if let Err(e) = write_varint(&mut buf, new_limit) {
            error!("Internal error: Auto-scaled data limit exceeds VarInt limit: {e:?}");
            return None;
        }

        self.local_max_data = new_limit;

        debug!(
            "Session {} data credit update: limit={} new_limit={new_limit}",
            self.id, self.local_max_data
        );

        Some(Effect::SendH3Capsule {
            stream_id: self.id,
            capsule_type: WT_MAX_DATA_TYPE,
            capsule_data: buf.freeze(),
            end_stream: false,
        })
    }

    // Streams credit replenishment.
    fn replenish_streams_credit(&mut self, is_uni: bool, force_send: bool) -> Option<Effect> {
        if self.state == SessionState::Closed || self.state == SessionState::Draining {
            return None;
        }

        let (current, closed, initial, cap_type) = if is_uni {
            (
                self.local_max_streams_uni,
                self.peer_streams_uni_closed,
                self.initial_max_streams_uni,
                WT_MAX_STREAMS_UNI_TYPE,
            )
        } else {
            (
                self.local_max_streams_bidi,
                self.peer_streams_bidi_closed,
                self.initial_max_streams_bidi,
                WT_MAX_STREAMS_BIDI_TYPE,
            )
        };

        let new_limit = next_stream_limit(
            current,
            closed,
            initial,
            self.flow_control_window_auto_scale,
            force_send,
        )?;

        let mut buf = BytesMut::with_capacity(8);
        if let Err(e) = write_varint(&mut buf, new_limit) {
            error!("Internal error: Auto-scaled stream limit exceeds VarInt limit: {e:?}");
            return None;
        }

        debug!(
            "Session {} stream credit auto-increment: type={} closed={closed} limit={current} new_limit={new_limit}",
            self.id,
            if is_uni { "uni" } else { "bidi" }
        );

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

        // Sort for deterministic behavior
        let mut stream_ids: Vec<_> = self.streams.keys().copied().collect();
        stream_ids.sort_unstable();

        for stream_id in stream_ids {
            if let Some(stream) = self
                .streams
                .get_mut(&stream_id)
                .filter(|s| s.state != StreamState::Closed)
            {
                effects.extend(stream.reset(0, error_code, now));
                if can_receive_on_stream(stream_id, self.is_client) {
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
                error_code: Some(error_code),
                reason: "Session closed".to_owned(),
            });
        }
        while let Some(req_id) = self.pending_uni_stream_requests.pop_front() {
            effects.push(Effect::NotifyRequestFailed {
                request_id: req_id,
                error_code: Some(error_code),
                reason: "Session closed".to_owned(),
            });
        }

        effects
    }
}

// Diagnostic information snapshot for a session.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct SessionDiagnostics {
    pub(crate) session_id: SessionId,
    pub(crate) state: SessionState,
    pub(crate) path: String,
    #[serde(serialize_with = "serialize_headers")]
    pub(crate) headers: Headers,
    pub(crate) created_at: f64,
    pub(crate) local_max_data: u64,
    pub(crate) local_data_sent: u64,
    pub(crate) local_data_consumed: u64,
    pub(crate) peer_max_data: u64,
    pub(crate) peer_data_sent: u64,
    pub(crate) local_max_streams_bidi: u64,
    pub(crate) local_streams_bidi_opened: u64,
    pub(crate) peer_max_streams_bidi: u64,
    pub(crate) peer_streams_bidi_opened: u64,
    pub(crate) peer_streams_bidi_closed: u64,
    pub(crate) local_max_streams_uni: u64,
    pub(crate) local_streams_uni_opened: u64,
    pub(crate) peer_max_streams_uni: u64,
    pub(crate) peer_streams_uni_opened: u64,
    pub(crate) peer_streams_uni_closed: u64,
    pub(crate) pending_bidi_stream_requests: VecDeque<RequestId>,
    pub(crate) pending_uni_stream_requests: VecDeque<RequestId>,
    pub(crate) datagrams_sent: u64,
    pub(crate) datagram_bytes_sent: u64,
    pub(crate) datagrams_received: u64,
    pub(crate) datagram_bytes_received: u64,
    pub(crate) active_streams: HashSet<StreamId>,
    pub(crate) blocked_streams: HashSet<StreamId>,
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
    pub(crate) ready_at: Option<f64>,
}

#[cfg(test)]
mod tests;
