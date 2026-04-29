//! Connection-level state machine and session manager.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use tracing::debug;

use crate::common::constants::{
    ERR_H3_REQUEST_REJECTED, ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_INTERNAL_ERROR,
    ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR, ERR_WT_ALPN_ERROR,
    ERR_WT_BUFFERED_STREAM_REJECTED, WT_AVAILABLE_PROTOCOLS, WT_PROTOCOL, WT_UPGRADE_TOKEN,
};
use crate::common::types::{
    ConnectionHandle, ConnectionState, ErrorCode, ErrorSource, EventType, Headers, RequestId,
    SessionId, SessionState, StreamDirection, StreamId,
};
use crate::protocol::H3Settings;
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};
use crate::protocol::session::{Session, SessionParams};
use crate::protocol::utils::{
    encode_wt_protocol_list, find_header, find_header_str, is_unidirectional_stream,
    parse_wt_protocol_list, parse_wt_protocol_string, stream_dir_from_id,
};

// Diagnostic information snapshot for a connection.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionDiagnostics {
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
    pub(crate) connected_at: Option<f64>,
    pub(crate) connection_handle: ConnectionHandle,
    pub(crate) early_event_count: usize,
    pub(crate) handshake_complete: bool,
    pub(crate) is_client: bool,
    pub(crate) local_goaway_sent: bool,
    pub(crate) peer_goaway_received: bool,
    pub(crate) peer_initial_max_data: u64,
    pub(crate) peer_initial_max_streams_bidi: u64,
    pub(crate) peer_initial_max_streams_uni: u64,
    pub(crate) peer_max_datagram_frame_size: Option<u64>,
    pub(crate) peer_settings_received: bool,
    pub(crate) pending_request_count: usize,
    pub(crate) session_count: usize,
    pub(crate) state: ConnectionState,
    pub(crate) stream_count: usize,
}

// Representation of a WebTransport connection state machine.
pub(super) struct Connection {
    close_code: Option<ErrorCode>,
    close_reason: Option<String>,
    closed_at: Option<f64>,
    connected_at: Option<f64>,
    early_event_buffer: HashMap<StreamId, Vec<(f64, ProtocolEvent)>>,
    early_event_count: usize,
    early_event_ttl: f64,
    flow_control_window: u64,
    flow_control_window_auto_scale_enabled: bool,
    handle: ConnectionHandle,
    handshake_complete: bool,
    initial_max_data: u64,
    initial_max_streams_bidi: u64,
    initial_max_streams_uni: u64,
    is_client: bool,
    local_goaway_sent: bool,
    max_session_pending_events: u64,
    max_sessions: u64,
    max_stream_read_buffer_size: u64,
    max_stream_write_buffer_size: u64,
    max_total_pending_events: u64,
    peer_goaway_received: bool,
    peer_initial_max_data: u64,
    peer_initial_max_streams_bidi: u64,
    peer_initial_max_streams_uni: u64,
    peer_max_datagram_frame_size: Option<u64>,
    peer_settings_received: bool,
    pending_requests: HashMap<StreamId, RequestId>,
    pending_session_configs: HashMap<RequestId, SessionInitData>,
    sessions: HashMap<SessionId, Session>,
    state: ConnectionState,
    stream_map: HashMap<StreamId, SessionId>,
}

impl Connection {
    // Connection entity initialization.
    pub(super) fn new(handle: ConnectionHandle, is_client: bool, params: ConnectionParams) -> Self {
        Self {
            close_code: None,
            close_reason: None,
            closed_at: None,
            connected_at: None,
            early_event_buffer: HashMap::new(),
            early_event_count: 0,
            early_event_ttl: params.early_event_ttl,
            flow_control_window: params.flow_control_window,
            flow_control_window_auto_scale_enabled: params.flow_control_window_auto_scale_enabled,
            handle,
            handshake_complete: false,
            initial_max_data: params.initial_max_data,
            initial_max_streams_bidi: params.initial_max_streams_bidi,
            initial_max_streams_uni: params.initial_max_streams_uni,
            is_client,
            local_goaway_sent: false,
            max_session_pending_events: params.max_session_pending_events,
            max_sessions: params.max_sessions,
            max_stream_read_buffer_size: params.max_stream_read_buffer_size,
            max_stream_write_buffer_size: params.max_stream_write_buffer_size,
            max_total_pending_events: params.max_total_pending_events,
            peer_goaway_received: false,
            peer_initial_max_data: 0,
            peer_initial_max_streams_bidi: 0,
            peer_initial_max_streams_uni: 0,
            peer_max_datagram_frame_size: None,
            peer_settings_received: false,
            pending_requests: HashMap::new(),
            pending_session_configs: HashMap::new(),
            sessions: HashMap::new(),
            state: ConnectionState::Idle,
            stream_map: HashMap::new(),
        }
    }

    // User session acceptance handling (delegated).
    pub(super) fn accept_session(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        wt_protocol: Option<String>,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.accept(request_id, wt_protocol, now);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "wt_session resolve failed".into(),
        }]
    }

    // H3 session binding.
    pub(super) fn bind_session(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        self.pending_requests.insert(stream_id, request_id);
        Vec::new()
    }

    // QUIC stream binding (delegated).
    pub(super) fn bind_stream(
        &mut self,
        session_id: SessionId,
        stream_id: StreamId,
        request_id: RequestId,
        is_unidirectional: bool,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let effects = session.bind_stream(stream_id, request_id, is_unidirectional, now);
            self.stream_map.insert(stream_id, session_id);
            return effects;
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: None,
            reason: "wt_session resolve failed".into(),
        }]
    }

    // Connection closure handling.
    pub(super) fn close(
        &mut self,
        request_id: RequestId,
        error_code: ErrorCode,
        reason: Option<Cow<'static, str>>,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if !matches!(
            self.state,
            ConnectionState::Closed | ConnectionState::Closing
        ) {
            debug!(
                "wt_connection close actual={:?} connection_handle={} request_id={request_id}",
                self.state, self.handle
            );
            self.state = ConnectionState::Closing;
            self.close_code = Some(error_code);
            self.close_reason = reason.clone().map(Cow::into_owned);
            self.closed_at = Some(now);
            effects.push(Effect::CloseQuicConnection { error_code, reason });
        }
        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });
        effects
    }

    // User session closure handling (delegated).
    pub(super) fn close_session(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        error_code: ErrorCode,
        reason: Option<Cow<'static, str>>,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.close(request_id, error_code, reason, now);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        }]
    }

    // User session creation request handling.
    pub(super) fn create_session(
        &mut self,
        request_id: RequestId,
        path: String,
        mut headers: Headers,
        wt_available_protocols: Option<Vec<String>>,
        now: f64,
    ) -> Vec<Effect> {
        if !self.is_client {
            debug!(
                "wt_connection validate failed actual={} connection_handle={} expected=true request_id={request_id}",
                self.is_client, self.handle
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Connection,
                error_code: None,
                reason: "wt_connection validate failed".into(),
            }];
        }

        if self.state != ConnectionState::Connected {
            debug!(
                "wt_connection validate failed actual={:?} connection_handle={} expected=connected request_id={request_id}",
                self.state, self.handle
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Connection,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                reason: "wt_connection validate failed".into(),
            }];
        }

        if self.peer_goaway_received {
            debug!(
                "wt_connection validate failed actual={} connection_handle={} expected=false request_id={request_id}",
                self.peer_goaway_received, self.handle
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Connection,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                reason: "wt_connection validate failed".into(),
            }];
        }

        let active_limit = if self.has_flow_control() {
            self.max_sessions
        } else {
            1
        };

        let current_total = (self.sessions.len() + self.pending_session_configs.len()) as u64;

        if active_limit > 0 && current_total >= active_limit {
            debug!(
                "wt_session validate exceeded actual={current_total} connection_handle={} limit={active_limit} request_id={request_id}",
                self.handle
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Connection,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                reason: "wt_session validate exceeded".into(),
            }];
        }

        if let Some(ref protos) = wt_available_protocols {
            match encode_wt_protocol_list(protos) {
                Ok(encoded) => {
                    headers.push((Bytes::from_static(WT_AVAILABLE_PROTOCOLS), encoded));
                }
                Err(e) => {
                    debug!(
                        "wt_available_protocols encode failed connection_handle={} request_id={request_id} err={e:?}",
                        self.handle
                    );
                    return vec![Effect::NotifyRequestFailed {
                        request_id,
                        source: ErrorSource::Connection,
                        error_code: None,
                        reason: "wt_available_protocols encode failed".into(),
                    }];
                }
            }
        }

        self.pending_session_configs.insert(
            request_id,
            SessionInitData {
                created_at: now,
                headers: headers.clone(),
                path: path.clone(),
                wt_available_protocols,
            },
        );

        vec![Effect::CreateH3Session {
            request_id,
            path,
            headers,
        }]
    }

    // User stream creation request handling (delegated).
    pub(super) fn create_stream(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.create_stream(request_id, is_unidirectional);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "wt_session resolve failed".into(),
        }]
    }

    // Connection diagnostics event handling.
    pub(super) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diag = self.diagnostics_snapshot();
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::ConnectionDiagnostics(Box::new(diag)),
        }]
    }

    // TLS keying material export handling (delegated).
    pub(super) fn export_keying_material(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        label: String,
        context: &[u8],
        length: u32,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get(&session_id) {
            return session.export_keying_material(request_id, label, context, length);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "wt_session resolve failed".into(),
        }]
    }

    // H3 session creation failure handling.
    pub(super) fn fail_session(
        &mut self,
        request_id: RequestId,
        error_code: Option<ErrorCode>,
        reason: Cow<'static, str>,
    ) -> Vec<Effect> {
        debug!(
            "wt_session create failed connection_handle={} request_id={request_id} err={reason}",
            self.handle
        );
        self.pending_session_configs.remove(&request_id);
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code,
            reason,
        }]
    }

    // QUIC stream creation failure handling (delegated).
    pub(super) fn fail_stream(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        is_unidirectional: bool,
        error_code: Option<ErrorCode>,
        reason: Cow<'static, str>,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.fail_stream(request_id, is_unidirectional, error_code, reason);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code,
            reason,
        }]
    }

    // Graceful connection shutdown handling.
    pub(super) fn graceful_close(&mut self, request_id: RequestId) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !self.local_goaway_sent {
            debug!(
                "wt_connection drain actual=false connection_handle={} request_id={request_id}",
                self.handle
            );
            self.local_goaway_sent = true;
            effects.push(Effect::SendH3Goaway);
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });
        effects
    }

    // Manual data credit grant handling (delegated).
    pub(super) fn grant_data_credit(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        max_data: u64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.grant_data_credit(request_id, max_data);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "wt_session resolve failed".into(),
        }]
    }

    // Manual streams credit grant handling (delegated).
    pub(super) fn grant_streams_credit(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        is_unidirectional: bool,
        max_streams: u64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.grant_streams_credit(request_id, is_unidirectional, max_streams);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "wt_session resolve failed".into(),
        }]
    }

    // Handshake completion handling.
    pub(super) fn handshake_completed(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state == ConnectionState::Idle {
            debug!(
                "wt_connection open actual={:?} connection_handle={}",
                self.state, self.handle
            );
            self.state = ConnectionState::Connecting;
        }

        if self.state != ConnectionState::Connecting {
            debug!(
                "wt_connection validate failed actual={:?} connection_handle={} expected=connecting",
                self.state, self.handle
            );
            return effects;
        }

        self.handshake_complete = true;

        if !self.is_client {
            debug!(
                "wt_connection open actual={:?} connection_handle={}",
                self.state, self.handle
            );
            self.state = ConnectionState::Connected;
            self.connected_at = Some(now);
            effects.push(Effect::EmitConnectionEvent {
                connection_handle: self.handle,
                event_type: EventType::ConnectionEstablished,
                error_code: None,
                reason: None,
            });
        } else if let Some((client_effects, _)) = self.check_connection_ready(now) {
            effects.extend(client_effects);
        }

        effects
    }

    // Flow control negotiation status verification.
    pub(super) fn has_flow_control(&self) -> bool {
        let local_intent = self.initial_max_data > 0
            || self.initial_max_streams_bidi > 0
            || self.initial_max_streams_uni > 0;
        let peer_intent = self.peer_initial_max_data > 0
            || self.peer_initial_max_streams_bidi > 0
            || self.peer_initial_max_streams_uni > 0;

        local_intent && peer_intent
    }

    // Client topological role predicate.
    pub(super) fn is_client(&self) -> bool {
        self.is_client
    }

    // Connection established state predicate.
    pub(super) fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected
    }

    // Connection pre-established state predicate.
    pub(super) fn is_pre_connected(&self) -> bool {
        matches!(
            self.state,
            ConnectionState::Idle | ConnectionState::Connecting
        )
    }

    // Session existence predicate by stream identifier.
    pub(super) fn is_session_stream(&self, stream_id: StreamId) -> bool {
        self.sessions.contains_key(&stream_id)
    }

    // Peer datagram frame size limit accessor.
    pub(super) fn peer_max_datagram_frame_size(&self) -> Option<u64> {
        self.peer_max_datagram_frame_size
    }

    // Early events pruning.
    pub(super) fn prune_early_events(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut streams_to_remove = Vec::new();
        let mut terminated_child_streams = HashSet::new();

        let mut stream_ids: Vec<StreamId> = self.early_event_buffer.keys().copied().collect();
        stream_ids.sort_unstable();

        for stream_id in stream_ids {
            if let Some(events) = self.early_event_buffer.get_mut(&stream_id) {
                let mut valid_events = Vec::new();

                for (timestamp, evt) in events.drain(..) {
                    if now - timestamp < self.early_event_ttl {
                        valid_events.push((timestamp, evt));
                    } else {
                        self.early_event_count -= 1;

                        if let ProtocolEvent::TransportStreamDataReceived {
                            stream_id: child_id,
                            ..
                        } = evt
                            && terminated_child_streams.insert(child_id)
                        {
                            debug!(
                                "wt_stream abort connection_handle={} stream_id={child_id}",
                                self.handle
                            );
                            let dir = stream_dir_from_id(child_id, self.is_client);
                            let can_send = matches!(
                                dir,
                                StreamDirection::Bidirectional | StreamDirection::SendOnly
                            );
                            let can_receive = matches!(
                                dir,
                                StreamDirection::Bidirectional | StreamDirection::ReceiveOnly
                            );

                            if can_send {
                                effects.push(Effect::ResetQuicStream {
                                    stream_id: child_id,
                                    error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                                });
                            }
                            if can_receive {
                                effects.push(Effect::StopQuicStream {
                                    stream_id: child_id,
                                    error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                                });
                            }
                        }
                    }
                }

                if valid_events.is_empty() {
                    streams_to_remove.push(stream_id);
                } else {
                    *events = valid_events;
                }
            }
        }

        for stream_id in streams_to_remove {
            self.early_event_buffer.remove(&stream_id);
            if !self.sessions.contains_key(&stream_id) {
                debug!(
                    "wt_stream abort connection_handle={} stream_id={stream_id}",
                    self.handle
                );
                if is_unidirectional_stream(stream_id) {
                    effects.push(Effect::StopQuicStream {
                        stream_id,
                        error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                    });
                } else {
                    effects.push(Effect::ResetQuicStream {
                        stream_id,
                        error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                    });
                    effects.push(Effect::StopQuicStream {
                        stream_id,
                        error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                    });
                }
            }
        }

        effects
    }

    // Closed resources pruning.
    pub(super) fn prune_resources(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();

        let mut closed_session_ids: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.is_closed())
            .map(|(id, _)| *id)
            .collect();

        closed_session_ids.sort_unstable();
        let closed_session_set: HashSet<SessionId> = closed_session_ids.iter().copied().collect();

        for sid in closed_session_ids {
            debug!(
                "wt_session destroy connection_handle={} session_id={sid}",
                self.handle
            );
            self.sessions.remove(&sid);
            effects.push(Effect::CleanupH3Stream { stream_id: sid });
        }

        if !closed_session_set.is_empty() {
            self.stream_map
                .retain(|_, sess_id| !closed_session_set.contains(sess_id));
        }

        let mut active_session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        active_session_ids.sort_unstable();

        for session_id in active_session_ids {
            if let Some(session) = self.sessions.get_mut(&session_id) {
                let session_effects = session.prune_closed_streams();
                for effect in &session_effects {
                    if let Effect::CleanupH3Stream { stream_id } = effect {
                        self.stream_map.remove(stream_id);
                    }
                }
                effects.extend(session_effects);
            }
        }

        effects
    }

    // Capsule reception handling (delegated).
    pub(super) fn recv_capsule(
        &mut self,
        session_id: SessionId,
        capsule_type: u64,
        data: &[u8],
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_capsule(capsule_type, data, now);
        }
        Vec::new()
    }

    // CONNECT stream closure reception handling (delegated).
    pub(super) fn recv_connect_close(&mut self, session_id: SessionId, now: f64) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_connect_close(now);
        }
        Vec::new()
    }

    // Datagram reception handling (delegated).
    pub(super) fn recv_datagram(
        &mut self,
        session_id: SessionId,
        data: Bytes,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_datagram(data);
        }

        let event = ProtocolEvent::TransportDatagramFrameReceived { data };

        self.buffer_early_event(session_id, event, now);

        Vec::new()
    }

    // GOAWAY frame reception handling.
    pub(super) fn recv_goaway(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();

        self.peer_goaway_received = true;

        let mut session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(session) = self.sessions.get_mut(&session_id) {
                effects.extend(session.drain());
            }
        }
        effects
    }

    // HEADERS frame reception handling.
    pub(super) fn recv_headers(
        &mut self,
        stream_id: StreamId,
        headers: Headers,
        stream_ended: bool,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.is_client {
            let Some(request_id) = self.pending_requests.remove(&stream_id) else {
                debug!(
                    "wt_stream resolve failed connection_handle={} stream_id={stream_id}",
                    self.handle
                );
                if stream_ended {
                    effects.extend(self.recv_connect_close(stream_id, now));
                }
                return effects;
            };

            let Some(init_data) = self.pending_session_configs.remove(&request_id) else {
                debug!(
                    "wt_session resolve failed connection_handle={} request_id={request_id}",
                    self.handle
                );
                if stream_ended {
                    effects.extend(self.recv_connect_close(stream_id, now));
                }
                effects.push(Effect::NotifyRequestFailed {
                    request_id,
                    source: ErrorSource::Unspecified,
                    error_code: Some(ERR_LIB_INTERNAL_ERROR),
                    reason: "wt_session resolve failed".into(),
                });
                return effects;
            };

            let status_str = find_header_str(&headers, ":status");
            let status_ok = status_str.as_deref() == Some("200");

            if status_ok {
                let wt_protocol = std::str::from_utf8(WT_PROTOCOL)
                    .ok()
                    .and_then(|key| find_header(&headers, key))
                    .and_then(|val| parse_wt_protocol_string(&val));

                if let Some(ref requested_protos) = init_data.wt_available_protocols {
                    let is_valid = match wt_protocol {
                        Some(ref negotiated) => requested_protos.contains(negotiated),
                        None => false,
                    };

                    if !is_valid {
                        let reason = if let Some(ref negotiated) = wt_protocol {
                            debug!(
                                "wt_protocol validate invalid actual={negotiated} connection_handle={} expected=wt_available_protocols",
                                self.handle
                            );
                            "wt_protocol validate invalid"
                        } else {
                            debug!(
                                "wt_protocol validate invalid connection_handle={} expected=wt_available_protocols",
                                self.handle
                            );
                            "wt_protocol validate invalid"
                        };

                        if stream_ended {
                            effects.extend(self.recv_connect_close(stream_id, now));
                        }
                        effects.push(Effect::NotifyRequestFailed {
                            request_id,
                            source: ErrorSource::Session,
                            error_code: Some(ERR_WT_ALPN_ERROR),
                            reason: reason.into(),
                        });
                        effects.push(Effect::StopQuicStream {
                            stream_id,
                            error_code: ERR_WT_ALPN_ERROR,
                        });
                        effects.push(Effect::ResetQuicStream {
                            stream_id,
                            error_code: ERR_WT_ALPN_ERROR,
                        });
                        return effects;
                    }
                }

                let session = Session::new(
                    stream_id,
                    init_data.path.clone(),
                    init_data.headers.clone(),
                    wt_protocol.clone(),
                    SessionState::Connected,
                    self.is_client,
                    SessionParams {
                        flow_control_negotiated: self.has_flow_control(),
                        flow_control_window: self.flow_control_window,
                        flow_control_window_auto_scale_enabled: self
                            .flow_control_window_auto_scale_enabled,
                        initial_max_data: self.initial_max_data,
                        initial_max_streams_bidi: self.initial_max_streams_bidi,
                        initial_max_streams_uni: self.initial_max_streams_uni,
                        max_stream_read_buffer_size: self.max_stream_read_buffer_size,
                        max_stream_write_buffer_size: self.max_stream_write_buffer_size,
                        peer_max_data: self.peer_initial_max_data,
                        peer_max_streams_bidi: self.peer_initial_max_streams_bidi,
                        peer_max_streams_uni: self.peer_initial_max_streams_uni,
                    },
                    init_data.created_at,
                );
                self.sessions.insert(stream_id, session);

                effects.push(Effect::EmitSessionEvent {
                    session_id: stream_id,
                    event_type: EventType::SessionReady,
                    path: Some(init_data.path),
                    headers: Some(init_data.headers),
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
                    result: RequestResult::SessionId(stream_id),
                });

                if let Some(events) = self.early_event_buffer.remove(&stream_id) {
                    if self.early_event_count >= events.len() {
                        self.early_event_count -= events.len();
                    }
                    for (_, evt) in events {
                        effects.push(Effect::ProcessProtocolEvent {
                            event: Box::new(evt),
                        });
                    }
                }
            } else {
                let status_val = status_str.unwrap_or_else(|| "Unknown".to_owned());
                debug!(
                    "wt_session abort connection_handle={} err={status_val:?}",
                    self.handle
                );
                effects.push(Effect::NotifyRequestFailed {
                    request_id,
                    source: ErrorSource::Session,
                    error_code: Some(ERR_H3_REQUEST_REJECTED),
                    reason: "wt_session abort".into(),
                });
            }
        } else {
            if self.sessions.contains_key(&stream_id) {
                debug!(
                    "wt_session validate failed connection_handle={} expected=false stream_id={stream_id}",
                    self.handle
                );
                if stream_ended {
                    effects.extend(self.recv_connect_close(stream_id, now));
                }
                return effects;
            }

            if self.state != ConnectionState::Connected {
                debug!(
                    "wt_connection validate failed actual={:?} connection_handle={} expected=connected stream_id={stream_id}",
                    self.state, self.handle
                );
                effects.push(Effect::SendH3Headers {
                    stream_id,
                    headers: vec![(Bytes::from_static(b":status"), Bytes::from("429"))],
                    end_stream: true,
                });
                effects.push(Effect::StopQuicStream {
                    stream_id,
                    error_code: ERR_H3_REQUEST_REJECTED,
                });
                return effects;
            }

            if self.local_goaway_sent {
                debug!(
                    "wt_connection validate failed actual={} connection_handle={} expected=false stream_id={stream_id}",
                    self.local_goaway_sent, self.handle
                );
                effects.push(Effect::SendH3Headers {
                    stream_id,
                    headers: vec![(Bytes::from_static(b":status"), Bytes::from("429"))],
                    end_stream: true,
                });
                effects.push(Effect::StopQuicStream {
                    stream_id,
                    error_code: ERR_H3_REQUEST_REJECTED,
                });
                return effects;
            }

            let active_limit = if self.has_flow_control() {
                self.max_sessions
            } else {
                1
            };

            if active_limit > 0 && self.sessions.len() as u64 >= active_limit {
                debug!(
                    "wt_session validate exceeded actual={} connection_handle={} limit={active_limit} stream_id={stream_id}",
                    self.sessions.len(),
                    self.handle
                );
                effects.push(Effect::SendH3Headers {
                    stream_id,
                    headers: vec![(Bytes::from_static(b":status"), Bytes::from("429"))],
                    end_stream: true,
                });
                effects.push(Effect::StopQuicStream {
                    stream_id,
                    error_code: ERR_H3_REQUEST_REJECTED,
                });
                return effects;
            }

            let method = find_header_str(&headers, ":method");
            let protocol = find_header_str(&headers, ":protocol");

            let method_connect = method.as_deref() == Some("CONNECT");
            let proto_wt = protocol.as_deref().map(str::as_bytes) == Some(WT_UPGRADE_TOKEN);

            if !method_connect || !proto_wt {
                debug!(
                    "wt_session validate invalid connection_handle={} stream_id={stream_id}",
                    self.handle
                );
                effects.push(Effect::SendH3Headers {
                    stream_id,
                    headers: vec![(Bytes::from_static(b":status"), Bytes::from("400"))],
                    end_stream: true,
                });
                effects.push(Effect::StopQuicStream {
                    stream_id,
                    error_code: ERR_H3_REQUEST_REJECTED,
                });
                return effects;
            }

            let path_header = find_header_str(&headers, ":path");
            let path = path_header.unwrap_or_else(|| "/".to_owned());

            let wt_available_protocols = std::str::from_utf8(WT_AVAILABLE_PROTOCOLS)
                .ok()
                .and_then(|key| find_header(&headers, key))
                .and_then(|val| parse_wt_protocol_list(&val));

            let session = Session::new(
                stream_id,
                path.clone(),
                headers.clone(),
                None,
                SessionState::Connecting,
                self.is_client,
                SessionParams {
                    flow_control_negotiated: self.has_flow_control(),
                    flow_control_window: self.flow_control_window,
                    flow_control_window_auto_scale_enabled: self
                        .flow_control_window_auto_scale_enabled,
                    initial_max_data: self.initial_max_data,
                    initial_max_streams_bidi: self.initial_max_streams_bidi,
                    initial_max_streams_uni: self.initial_max_streams_uni,
                    max_stream_read_buffer_size: self.max_stream_read_buffer_size,
                    max_stream_write_buffer_size: self.max_stream_write_buffer_size,
                    peer_max_data: self.peer_initial_max_data,
                    peer_max_streams_bidi: self.peer_initial_max_streams_bidi,
                    peer_max_streams_uni: self.peer_initial_max_streams_uni,
                },
                now,
            );
            self.sessions.insert(stream_id, session);

            effects.push(Effect::EmitSessionEvent {
                session_id: stream_id,
                event_type: EventType::SessionRequest,
                path: Some(path),
                headers: Some(headers),
                wt_available_protocols,
                wt_protocol: None,
                data: None,
                is_unidirectional: None,
                max_data: None,
                max_streams: None,
                ready_at: None,
                error_code: None,
                reason: None,
            });

            if let Some(events) = self.early_event_buffer.remove(&stream_id) {
                if self.early_event_count >= events.len() {
                    self.early_event_count -= events.len();
                }
                for (_, evt) in events {
                    effects.push(Effect::ProcessProtocolEvent {
                        event: Box::new(evt),
                    });
                }
            }
        }

        if stream_ended {
            effects.extend(self.recv_connect_close(stream_id, now));
        }

        effects
    }

    // SETTINGS frame reception handling.
    pub(super) fn recv_settings(&mut self, settings: &H3Settings, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        self.peer_settings_received = true;

        if let Some(val) = settings.wt_initial_max_data {
            self.peer_initial_max_data = val;
        }
        if let Some(val) = settings.wt_initial_max_streams_bidi {
            self.peer_initial_max_streams_bidi = val;
        }
        if let Some(val) = settings.wt_initial_max_streams_uni {
            self.peer_initial_max_streams_uni = val;
        }

        if let Some((client_effects, _)) = self.check_connection_ready(now) {
            effects.extend(client_effects);
        }

        effects
    }

    // Transport stream stop_sending reception handling (delegated).
    pub(super) fn recv_stop_sending(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Vec<Effect> {
        if let Some(session) = self.session_for_stream_mut(stream_id) {
            return session.recv_stop_sending(stream_id, error_code);
        }
        Vec::new()
    }

    // Stream data reception handling (delegated).
    pub(super) fn recv_stream_data(
        &mut self,
        session_id: SessionId,
        stream_id: StreamId,
        data: Bytes,
        fin: bool,
        now: f64,
    ) -> Vec<Effect> {
        self.stream_map.entry(stream_id).or_insert(session_id);

        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_stream_data(stream_id, data, fin, now);
        }

        let event = ProtocolEvent::TransportStreamDataReceived {
            stream_id,
            data,
            end_stream: fin,
        };

        if !self.buffer_early_event(session_id, event, now) {
            debug!(
                "wt_stream abort connection_handle={} stream_id={stream_id}",
                self.handle
            );

            let mut effects = Vec::new();

            if is_unidirectional_stream(stream_id) {
                effects.push(Effect::StopQuicStream {
                    stream_id,
                    error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                });
            } else {
                effects.push(Effect::ResetQuicStream {
                    stream_id,
                    error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                });
                effects.push(Effect::StopQuicStream {
                    stream_id,
                    error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                });
            }

            return effects;
        }

        Vec::new()
    }

    // Transport stream reset reception handling (delegated).
    pub(super) fn recv_stream_reset(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.session_for_stream_mut(stream_id) {
            return session.recv_stream_reset(stream_id, error_code, now);
        }
        Vec::new()
    }

    // Transport parameters reception handling.
    pub(super) fn recv_transport_parameters(
        &mut self,
        peer_max_datagram_frame_size: u64,
    ) -> Vec<Effect> {
        debug!(
            "wt_transport_parameters receive connection_handle={}",
            self.handle
        );
        self.peer_max_datagram_frame_size = Some(peer_max_datagram_frame_size);
        Vec::new()
    }

    // User session rejection handling (delegated).
    pub(super) fn reject_session(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        status_code: u16,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.reject(request_id, status_code, now);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "wt_session resolve failed".into(),
        }]
    }

    // User stream reset command handling (delegated).
    pub(super) fn reset_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.session_for_stream_mut(stream_id) {
            return session.reset_stream(stream_id, request_id, error_code, now);
        }
        debug!(
            "wt_stream resolve failed connection_handle={} request_id={request_id} stream_id={stream_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            reason: "wt_stream resolve failed".into(),
        }]
    }

    // User datagram send command handling (delegated).
    pub(super) fn send_datagram(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        data: Bytes,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let max = self.peer_max_datagram_frame_size.unwrap_or(0);
            return session.send_datagram(request_id, data, max);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "wt_session resolve failed".into(),
        }]
    }

    // User stream data send handling (delegated).
    pub(super) fn send_stream_data(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        data: Bytes,
        end_stream: bool,
    ) -> Vec<Effect> {
        if let Some(session) = self.session_for_stream_mut(stream_id) {
            return session.send_stream_data(stream_id, request_id, data, end_stream);
        }
        debug!(
            "wt_stream resolve failed connection_handle={} request_id={request_id} stream_id={stream_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            reason: "wt_stream resolve failed".into(),
        }]
    }

    // Session diagnostics delegation.
    pub(super) fn session_diagnostics(
        &self,
        session_id: SessionId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get(&session_id) {
            return session.diagnose(request_id);
        }
        debug!(
            "wt_session resolve failed connection_handle={} request_id={request_id} session_id={session_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "wt_session resolve failed".into(),
        }]
    }

    // User stream stop command handling (delegated).
    pub(super) fn stop_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.session_for_stream_mut(stream_id) {
            return session.stop_stream(stream_id, request_id, error_code, now);
        }
        debug!(
            "wt_stream resolve failed connection_handle={} request_id={request_id} stream_id={stream_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            reason: "wt_stream resolve failed".into(),
        }]
    }

    // Stream diagnostics delegation.
    pub(super) fn stream_diagnostics(
        &self,
        stream_id: StreamId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get(&session_id)
        {
            return session.stream_diagnostics(stream_id, request_id);
        }
        debug!(
            "wt_stream resolve failed connection_handle={} request_id={request_id} stream_id={stream_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            reason: "wt_stream resolve failed".into(),
        }]
    }

    // User stream read request handling (delegated).
    pub(super) fn stream_read(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        max_bytes: u64,
    ) -> Vec<Effect> {
        if let Some(session) = self.session_for_stream_mut(stream_id) {
            return session.stream_read(stream_id, request_id, max_bytes);
        }
        debug!(
            "wt_stream resolve failed connection_handle={} request_id={request_id} stream_id={stream_id}",
            self.handle
        );
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            reason: "wt_stream resolve failed".into(),
        }]
    }

    // Connection termination handling.
    pub(super) fn terminated(
        &mut self,
        error_code: ErrorCode,
        reason: Cow<'static, str>,
        now: f64,
    ) -> Vec<Effect> {
        if self.state == ConnectionState::Closed {
            return Vec::new();
        }

        let previous_state = self.state;

        if self.state != ConnectionState::Closing {
            self.closed_at = Some(now);
            self.close_code = Some(error_code);
            self.close_reason = Some(reason.clone().into_owned());
        }
        self.state = ConnectionState::Closed;

        let mut effects = Vec::new();

        if previous_state == ConnectionState::Closing {
            debug!(
                "wt_connection close actual={:?} connection_handle={} err={reason}",
                previous_state, self.handle
            );
        } else {
            debug!(
                "wt_connection abort actual={:?} connection_handle={} err={reason}",
                previous_state, self.handle
            );
        }

        self.pending_session_configs.clear();
        self.pending_requests.clear();

        let mut session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(session) = self.sessions.get_mut(&session_id) {
                effects.extend(session.close(0, error_code, Some(reason.clone()), now));
            }
        }

        effects.push(Effect::EmitConnectionEvent {
            connection_handle: self.handle,
            event_type: EventType::ConnectionClosed,
            error_code: Some(error_code),
            reason: Some(reason),
        });

        effects
    }

    // Early protocol event buffering with capacity checks.
    fn buffer_early_event(
        &mut self,
        session_id: SessionId,
        event: ProtocolEvent,
        now: f64,
    ) -> bool {
        if (self.early_event_count as u64) >= self.max_total_pending_events {
            debug!(
                "wt_connection validate exceeded actual={} connection_handle={} expected=max_total_pending_events",
                self.early_event_count, self.handle
            );
            return false;
        }

        let session_buffer = self.early_event_buffer.entry(session_id).or_default();

        if (session_buffer.len() as u64) >= self.max_session_pending_events {
            debug!(
                "wt_session validate exceeded actual={} connection_handle={} expected=max_session_pending_events session_id={session_id}",
                session_buffer.len(),
                self.handle
            );
            return false;
        }

        session_buffer.push((now, event));
        self.early_event_count += 1;

        true
    }

    // Connection readiness state verification.
    fn check_connection_ready(&mut self, now: f64) -> Option<(Vec<Effect>, bool)> {
        if self.state == ConnectionState::Connecting
            && self.handshake_complete
            && self.peer_settings_received
        {
            debug!(
                "wt_connection open actual={:?} connection_handle={}",
                self.state, self.handle
            );
            self.state = ConnectionState::Connected;
            self.connected_at = Some(now);

            let effects = vec![Effect::EmitConnectionEvent {
                connection_handle: self.handle,
                event_type: EventType::ConnectionEstablished,
                error_code: None,
                reason: None,
            }];
            return Some((effects, true));
        }
        None
    }

    // Connection diagnostics snapshot retrieval.
    fn diagnostics_snapshot(&self) -> ConnectionDiagnostics {
        ConnectionDiagnostics {
            close_code: self.close_code,
            close_reason: self.close_reason.clone(),
            closed_at: self.closed_at,
            connected_at: self.connected_at,
            connection_handle: self.handle,
            early_event_count: self.early_event_count,
            handshake_complete: self.handshake_complete,
            is_client: self.is_client,
            local_goaway_sent: self.local_goaway_sent,
            peer_goaway_received: self.peer_goaway_received,
            peer_initial_max_data: self.peer_initial_max_data,
            peer_initial_max_streams_bidi: self.peer_initial_max_streams_bidi,
            peer_initial_max_streams_uni: self.peer_initial_max_streams_uni,
            peer_max_datagram_frame_size: self.peer_max_datagram_frame_size,
            peer_settings_received: self.peer_settings_received,
            pending_request_count: self.pending_requests.len(),
            session_count: self.sessions.len(),
            state: self.state,
            stream_count: self.stream_map.len(),
        }
    }

    // Helper for resolving mutable session access via stream routing table.
    fn session_for_stream_mut(&mut self, stream_id: StreamId) -> Option<&mut Session> {
        let session_id = *self.stream_map.get(&stream_id)?;
        self.sessions.get_mut(&session_id)
    }
}

// Connection initialization constraints and thresholds.
#[derive(Clone, Copy, Debug)]
pub(super) struct ConnectionParams {
    pub(super) early_event_ttl: f64,
    pub(super) flow_control_window: u64,
    pub(super) flow_control_window_auto_scale_enabled: bool,
    pub(super) initial_max_data: u64,
    pub(super) initial_max_streams_bidi: u64,
    pub(super) initial_max_streams_uni: u64,
    pub(super) max_session_pending_events: u64,
    pub(super) max_sessions: u64,
    pub(super) max_stream_read_buffer_size: u64,
    pub(super) max_stream_write_buffer_size: u64,
    pub(super) max_total_pending_events: u64,
}

// Data required to initialize a pending session.
#[derive(Clone, Debug)]
struct SessionInitData {
    created_at: f64,
    headers: Headers,
    path: String,
    wt_available_protocols: Option<Vec<String>>,
}

#[cfg(test)]
mod tests;
