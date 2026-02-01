//! Connection-level state machine and session manager.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use serde::Serialize;
use tracing::{debug, error, warn};

use crate::common::constants::{
    DRAIN_WEBTRANSPORT_SESSION_TYPE, ERR_H3_REQUEST_REJECTED, ERR_LIB_CONNECTION_STATE_ERROR,
    ERR_LIB_INTERNAL_ERROR, ERR_LIB_SESSION_STATE_ERROR, ERR_WT_BUFFERED_STREAM_REJECTED,
    SETTINGS_WT_INITIAL_MAX_DATA, SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI,
    SETTINGS_WT_INITIAL_MAX_STREAMS_UNI,
};
use crate::common::types::{
    ConnectionId, ConnectionState, ErrorCode, EventType, Headers, RequestId, SessionId,
    SessionState, StreamId,
};
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};
use crate::protocol::session::Session;
use crate::protocol::utils::find_header_str;

// Representation of a WebTransport connection state machine.
pub(crate) struct Connection {
    pub(crate) id: String,
    pub(crate) is_client: bool,
    pub(crate) state: ConnectionState,
    pub(crate) max_datagram_size: u64,
    pub(crate) remote_max_datagram_frame_size: Option<u64>,
    pub(crate) handshake_complete: bool,
    pub(crate) peer_settings_received: bool,
    pub(crate) local_goaway_sent: bool,
    pub(crate) sessions: HashMap<SessionId, Session>,
    pub(crate) stream_map: HashMap<StreamId, SessionId>,
    pub(crate) pending_requests: HashMap<StreamId, RequestId>,
    pub(crate) pending_session_configs: HashMap<RequestId, SessionInitData>,
    pub(crate) early_event_buffer: HashMap<StreamId, Vec<(f64, ProtocolEvent)>>,
    pub(crate) early_event_count: usize,
    pub(crate) peer_initial_max_data: u64,
    pub(crate) peer_initial_max_streams_bidi: u64,
    pub(crate) peer_initial_max_streams_uni: u64,
    pub(crate) connected_at: Option<f64>,
    pub(crate) closed_at: Option<f64>,
    flow_control_window_auto_scale: bool,
    initial_max_data: u64,
    initial_max_streams_bidi: u64,
    initial_max_streams_uni: u64,
    flow_control_window_size: u64,
    max_sessions: u64,
    stream_read_buffer_size: u64,
    stream_write_buffer_size: u64,
}

impl Connection {
    // Connection entity initialization.
    #[allow(
        clippy::too_many_arguments,
        reason = "Complex internal state initialization."
    )]
    pub(crate) fn new(
        id: ConnectionId,
        is_client: bool,
        max_datagram_size: u64,
        flow_control_window_size: u64,
        max_sessions: u64,
        initial_max_data: u64,
        initial_max_streams_bidi: u64,
        initial_max_streams_uni: u64,
        stream_read_buffer_size: u64,
        stream_write_buffer_size: u64,
        flow_control_window_auto_scale: bool,
    ) -> Self {
        Self {
            id,
            is_client,
            state: ConnectionState::Idle,
            max_datagram_size,
            remote_max_datagram_frame_size: None,
            handshake_complete: false,
            peer_settings_received: false,
            local_goaway_sent: false,
            sessions: HashMap::new(),
            stream_map: HashMap::new(),
            pending_requests: HashMap::new(),
            pending_session_configs: HashMap::new(),
            early_event_buffer: HashMap::new(),
            early_event_count: 0,
            peer_initial_max_data: 0,
            peer_initial_max_streams_bidi: 0,
            peer_initial_max_streams_uni: 0,
            connected_at: None,
            closed_at: None,
            flow_control_window_auto_scale,
            initial_max_data,
            initial_max_streams_bidi,
            initial_max_streams_uni,
            flow_control_window_size,
            max_sessions,
            stream_read_buffer_size,
            stream_write_buffer_size,
        }
    }

    // User session acceptance handling (delegated).
    pub(crate) fn accept_session(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.accept(request_id, now);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // H3 session binding.
    pub(crate) fn bind_session(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        self.pending_requests.insert(stream_id, request_id);
        Vec::new()
    }

    // QUIC stream binding (delegated).
    pub(crate) fn bind_stream(
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
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: None,
            reason: format!("Session {session_id} not found during stream bind"),
        }]
    }

    // Connection closure handling.
    pub(crate) fn close(
        &mut self,
        request_id: RequestId,
        error_code: ErrorCode,
        reason: Option<String>,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.state != ConnectionState::Closed && self.state != ConnectionState::Closing {
            self.state = ConnectionState::Closing;
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
    pub(crate) fn close_session(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        code: ErrorCode,
        reason: Option<String>,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.close(request_id, code, reason, now);
        }
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        }]
    }

    // User session creation request handling.
    pub(crate) fn create_session(
        &mut self,
        request_id: RequestId,
        path: String,
        headers: Headers,
        now: f64,
    ) -> Vec<Effect> {
        if !self.is_client {
            return vec![Effect::NotifyRequestFailed {
                request_id,
                error_code: None,
                reason: "Server cannot create sessions using this method".to_owned(),
            }];
        }

        if self.state != ConnectionState::Connected {
            return vec![Effect::NotifyRequestFailed {
                request_id,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                reason: format!(
                    "Cannot create session, connection state is {:?}",
                    self.state
                ),
            }];
        }

        self.pending_session_configs.insert(
            request_id,
            SessionInitData {
                path: path.clone(),
                headers: headers.clone(),
                created_at: now,
            },
        );

        vec![Effect::CreateH3Session {
            request_id,
            path,
            headers,
        }]
    }

    // User stream creation request handling (delegated).
    pub(crate) fn create_stream(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        is_uni: bool,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.create_stream(request_id, is_uni);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // Connection diagnostics event handling.
    pub(crate) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diag = self.diagnostics_snapshot();
        let json_str = serde_json::to_string(&diag).unwrap_or_else(|_| "{}".to_owned());
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::Diagnostics(json_str),
        }]
    }

    // H3 session creation failure handling.
    pub(crate) fn fail_session(&mut self, request_id: RequestId, reason: String) -> Vec<Effect> {
        error!("H3 Session creation failed for request {request_id}: {reason}");
        self.pending_session_configs.remove(&request_id);
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: None,
            reason,
        }]
    }

    // QUIC stream creation failure handling (delegated).
    pub(crate) fn fail_stream(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        is_unidirectional: bool,
        error_code: Option<ErrorCode>,
        reason: String,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.fail_stream(request_id, is_unidirectional, error_code, reason);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code,
            reason,
        }]
    }

    // Graceful connection shutdown handling.
    pub(crate) fn graceful_close(&mut self, request_id: RequestId, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !self.local_goaway_sent {
            self.local_goaway_sent = true;
            effects.push(Effect::SendH3Goaway);

            if self.state != ConnectionState::Closing && self.state != ConnectionState::Closed {
                self.state = ConnectionState::Closing;
                self.closed_at = Some(now);
            }
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });
        effects
    }

    // Manual data credit grant handling (delegated).
    pub(crate) fn grant_data_credit(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        max_data: u64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.grant_data_credit(request_id, max_data);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // Manual streams credit grant handling (delegated).
    pub(crate) fn grant_streams_credit(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        max_streams: u64,
        is_uni: bool,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.grant_streams_credit(request_id, max_streams, is_uni);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // Handshake completion handling.
    pub(crate) fn handshake_completed(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        self.handshake_complete = true;

        if let Some((client_effects, _)) = self.check_connection_ready(now) {
            effects.extend(client_effects);
        }
        effects
    }

    // Early events pruning.
    pub(crate) fn prune_early_events(&mut self, now: f64, timeout: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut streams_to_remove = Vec::new();

        let mut stream_ids: Vec<StreamId> = self.early_event_buffer.keys().copied().collect();
        stream_ids.sort_unstable();

        for stream_id in stream_ids {
            if let Some(events) = self.early_event_buffer.get_mut(&stream_id) {
                let mut valid_events = Vec::new();
                for (timestamp, evt) in events.drain(..) {
                    if now - timestamp < timeout {
                        valid_events.push((timestamp, evt));
                    } else if self.early_event_count > 0 {
                        self.early_event_count -= 1;
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
                debug!("Early event buffer timed out for stream {stream_id}, resetting");
                effects.push(Effect::ResetQuicStream {
                    stream_id,
                    error_code: ERR_WT_BUFFERED_STREAM_REJECTED,
                });
            }
        }

        effects
    }

    // Closed resources pruning.
    pub(crate) fn prune_resources(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();

        let mut closed_session_ids: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.state == SessionState::Closed)
            .map(|(id, _)| *id)
            .collect();

        closed_session_ids.sort_unstable();
        let closed_session_set: HashSet<SessionId> = closed_session_ids.iter().copied().collect();

        for sid in closed_session_ids {
            debug!("Cleaning up closed session {sid} from state");
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
    pub(crate) fn recv_capsule(
        &mut self,
        session_id: SessionId,
        capsule_type: u64,
        data: &Bytes,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_capsule(capsule_type, data, now);
        }
        Vec::new()
    }

    // CONNECT stream closure reception handling (delegated).
    pub(crate) fn recv_connect_close(&mut self, session_id: SessionId, now: f64) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_connect_close(now);
        }
        Vec::new()
    }

    // Datagram reception handling (delegated).
    pub(crate) fn recv_datagram(
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
    pub(crate) fn recv_goaway(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state != ConnectionState::Closing && self.state != ConnectionState::Closed {
            self.state = ConnectionState::Closing;
            self.closed_at = Some(now);
        }

        let mut session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(session) = self
                .sessions
                .get_mut(&session_id)
                .filter(|s| s.state == SessionState::Connected)
            {
                session.state = SessionState::Draining;
                effects.push(Effect::SendH3Capsule {
                    stream_id: session_id,
                    capsule_type: DRAIN_WEBTRANSPORT_SESSION_TYPE,
                    capsule_data: Bytes::new(),
                    end_stream: false,
                });
                effects.push(Effect::EmitSessionEvent {
                    session_id,
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
        effects
    }

    // HEADERS frame reception handling.
    pub(crate) fn recv_headers(
        &mut self,
        stream_id: StreamId,
        headers: Headers,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.is_client {
            let Some(request_id) = self.pending_requests.remove(&stream_id) else {
                warn!("Received headers on unknown client stream {stream_id} (no pending request)");
                return Vec::new();
            };

            let Some(init_data) = self.pending_session_configs.remove(&request_id) else {
                error!("Internal State Error: Missing init data for request {request_id:?}");
                return vec![Effect::NotifyRequestFailed {
                    request_id,
                    error_code: Some(ERR_LIB_INTERNAL_ERROR),
                    reason: "Internal state inconsistency: Session init data missing".to_owned(),
                }];
            };

            let status_str = find_header_str(&headers, ":status");
            let status_ok = status_str.as_deref() == Some("200");

            if status_ok {
                let session = Session::new(
                    stream_id,
                    init_data.path.clone(),
                    init_data.headers.clone(),
                    init_data.created_at,
                    self.config_initial_max_data(),
                    self.config_initial_max_streams_bidi(),
                    self.config_initial_max_streams_uni(),
                    self.peer_initial_max_data,
                    self.peer_initial_max_streams_bidi,
                    self.peer_initial_max_streams_uni,
                    self.flow_control_window_size,
                    self.stream_read_buffer_size,
                    self.stream_write_buffer_size,
                    self.flow_control_window_auto_scale,
                    self.is_client,
                    SessionState::Connected,
                );
                self.sessions.insert(stream_id, session);

                effects.push(Effect::EmitSessionEvent {
                    session_id: stream_id,
                    event_type: EventType::SessionReady,
                    code: None,
                    data: None,
                    headers: Some(init_data.headers),
                    is_unidirectional: None,
                    max_data: None,
                    max_streams: None,
                    path: Some(init_data.path),
                    ready_at: Some(now),
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
                let reason = format!("Session creation failed with status {status_val:?}");
                effects.push(Effect::NotifyRequestFailed {
                    request_id,
                    error_code: Some(ERR_H3_REQUEST_REJECTED),
                    reason,
                });
            }
        } else {
            if self.sessions.contains_key(&stream_id) {
                debug!("Received trailers on existing session stream {stream_id}, ignoring.");
                return Vec::new();
            }

            if self.state != ConnectionState::Connected {
                debug!(
                    "Rejecting new session on stream {stream_id}: connection state is {:?}",
                    self.state
                );
                effects.push(Effect::SendH3Headers {
                    stream_id,
                    status: 429,
                    end_stream: true,
                });
                return effects;
            }

            if self.max_sessions > 0 && self.sessions.len() as u64 >= self.max_sessions {
                warn!(
                    "Session limit ({}) reached, rejecting new session on stream {stream_id}",
                    self.max_sessions
                );
                effects.push(Effect::SendH3Headers {
                    stream_id,
                    status: 429,
                    end_stream: true,
                });
                return effects;
            }

            let method = find_header_str(&headers, ":method");
            let protocol = find_header_str(&headers, ":protocol");

            let method_connect = method.as_deref() == Some("CONNECT");
            let proto_wt = protocol.as_deref() == Some("webtransport");

            if !method_connect || !proto_wt {
                debug!("Rejecting non-WebTransport request on stream {stream_id}");
                effects.push(Effect::SendH3Headers {
                    stream_id,
                    status: 400,
                    end_stream: true,
                });
                return effects;
            }

            let path_header = find_header_str(&headers, ":path");
            let path = path_header.unwrap_or_else(|| "/".to_owned());

            let session = Session::new(
                stream_id,
                path.clone(),
                headers.clone(),
                now,
                self.config_initial_max_data(),
                self.config_initial_max_streams_bidi(),
                self.config_initial_max_streams_uni(),
                self.peer_initial_max_data,
                self.peer_initial_max_streams_bidi,
                self.peer_initial_max_streams_uni,
                self.flow_control_window_size,
                self.stream_read_buffer_size,
                self.stream_write_buffer_size,
                self.flow_control_window_auto_scale,
                self.is_client,
                SessionState::Connecting,
            );
            self.sessions.insert(stream_id, session);

            effects.push(Effect::EmitSessionEvent {
                session_id: stream_id,
                event_type: EventType::SessionRequest,
                path: Some(path),
                headers: Some(headers),
                code: None,
                data: None,
                is_unidirectional: None,
                max_data: None,
                max_streams: None,
                ready_at: None,
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
        effects
    }

    // SETTINGS frame reception handling.
    pub(crate) fn recv_settings(&mut self, settings: &HashMap<u64, u64>, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        self.peer_settings_received = true;

        if let Some(val) = settings.get(&SETTINGS_WT_INITIAL_MAX_DATA) {
            self.peer_initial_max_data = *val;
        }
        if let Some(val) = settings.get(&SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI) {
            self.peer_initial_max_streams_bidi = *val;
        }
        if let Some(val) = settings.get(&SETTINGS_WT_INITIAL_MAX_STREAMS_UNI) {
            self.peer_initial_max_streams_uni = *val;
        }

        if let Some((client_effects, _)) = self.check_connection_ready(now) {
            effects.extend(client_effects);
        }

        effects
    }

    // Stream data reception handling (delegated).
    pub(crate) fn recv_stream_data(
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
        self.buffer_early_event(session_id, event, now);
        Vec::new()
    }

    // Transport stream reset reception handling (delegated).
    pub(crate) fn recv_stream_reset(
        &mut self,
        stream_id: StreamId,
        code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self
            .stream_map
            .get(&stream_id)
            .and_then(|sid| self.sessions.get_mut(sid))
        {
            return session.recv_stream_reset(stream_id, code, now);
        }
        Vec::new()
    }

    // Transport parameters reception handling.
    pub(crate) fn recv_transport_parameters(
        &mut self,
        remote_max_datagram_frame_size: u64,
    ) -> Vec<Effect> {
        debug!(
            "Received transport parameters: remote_max_datagram_frame_size={remote_max_datagram_frame_size}"
        );
        self.remote_max_datagram_frame_size = Some(remote_max_datagram_frame_size);
        Vec::new()
    }

    // User session rejection handling (delegated).
    pub(crate) fn reject_session(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        code: u16,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.reject(request_id, code, now);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // User stream reset command handling (delegated).
    pub(crate) fn reset_stream(
        &mut self,
        session_id: SessionId,
        stream_id: StreamId,
        request_id: RequestId,
        code: ErrorCode,
    ) -> Vec<Effect> {
        let now = 0.0;
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.reset_stream(stream_id, request_id, code, now);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // User datagram send command handling (delegated).
    pub(crate) fn send_datagram(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        data: Bytes,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let max = self.remote_max_datagram_frame_size.unwrap_or(0);
            return session.send_datagram(request_id, data, max);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // User stream data send handling (delegated).
    pub(crate) fn send_stream_data(
        &mut self,
        session_id: SessionId,
        stream_id: StreamId,
        request_id: RequestId,
        data: Bytes,
        fin: bool,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.send_stream_data(stream_id, request_id, data, fin);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // Session diagnostics delegation.
    pub(crate) fn session_diagnostics(
        &self,
        session_id: SessionId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get(&session_id) {
            return session.diagnose(request_id);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // User stream stop command handling (delegated).
    pub(crate) fn stop_stream(
        &mut self,
        session_id: SessionId,
        stream_id: StreamId,
        request_id: RequestId,
        code: ErrorCode,
    ) -> Vec<Effect> {
        let now = 0.0;
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.stop_stream(stream_id, request_id, code, now);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // Stream diagnostics delegation.
    pub(crate) fn stream_diagnostics(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get(&session_id) {
            return session.stream_diagnostics(stream_id, request_id);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // User stream read request handling (delegated).
    pub(crate) fn stream_read(
        &mut self,
        session_id: SessionId,
        stream_id: StreamId,
        request_id: RequestId,
        max_bytes: u64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.stream_read(stream_id, request_id, max_bytes);
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            error_code: Some(ERR_LIB_SESSION_STATE_ERROR),
            reason: "Session not found".to_owned(),
        }]
    }

    // Connection termination handling.
    pub(crate) fn terminated(
        &mut self,
        error_code: ErrorCode,
        reason_phrase: String,
        now: f64,
    ) -> Vec<Effect> {
        if self.state == ConnectionState::Closed {
            return Vec::new();
        }

        self.state = ConnectionState::Closed;
        self.closed_at = Some(now);

        let mut effects = Vec::new();
        let reason_msg = format!("Connection terminated: {reason_phrase}");

        self.pending_session_configs.clear();
        self.pending_requests.clear();

        let mut session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(session) = self.sessions.get_mut(&session_id) {
                effects.extend(session.close(0, error_code, Some(reason_msg.clone()), now));
            }
        }

        effects.push(Effect::EmitConnectionEvent {
            event_type: EventType::ConnectionClosed,
            connection_id: self.id.clone(),
            error_code: Some(error_code),
            reason: Some(reason_phrase),
        });

        effects
    }

    // Unread stream data return handling (delegated).
    pub(crate) fn unread_stream(&mut self, stream_id: StreamId, data: Bytes) -> Vec<Effect> {
        if let Some(session) = self
            .stream_map
            .get(&stream_id)
            .and_then(|sid| self.sessions.get_mut(sid))
        {
            return session.unread_stream(stream_id, data);
        }
        Vec::new()
    }

    // Early protocol event buffering.
    fn buffer_early_event(&mut self, session_id: SessionId, event: ProtocolEvent, now: f64) {
        self.early_event_count += 1;
        self.early_event_buffer
            .entry(session_id)
            .or_default()
            .push((now, event));
    }

    // Connection readiness state verification.
    fn check_connection_ready(&mut self, now: f64) -> Option<(Vec<Effect>, bool)> {
        if self.state == ConnectionState::Connecting
            && self.handshake_complete
            && self.peer_settings_received
        {
            debug!("Client connection fully ready (QUIC + H3 SETTINGS).");
            self.state = ConnectionState::Connected;
            self.connected_at = Some(now);

            let effects = vec![Effect::EmitConnectionEvent {
                event_type: EventType::ConnectionEstablished,
                connection_id: self.id.clone(),
                error_code: None,
                reason: None,
            }];
            return Some((effects, true));
        }
        None
    }

    // Initial max data configuration accessor.
    fn config_initial_max_data(&self) -> u64 {
        self.initial_max_data
    }

    // Initial max bidirectional streams accessor.
    fn config_initial_max_streams_bidi(&self) -> u64 {
        self.initial_max_streams_bidi
    }

    // Initial max unidirectional streams accessor.
    fn config_initial_max_streams_uni(&self) -> u64 {
        self.initial_max_streams_uni
    }

    // Connection diagnostics snapshot retrieval.
    fn diagnostics_snapshot(&self) -> ConnectionDiagnostics {
        ConnectionDiagnostics {
            connection_id: self.id.clone(),
            is_client: self.is_client,
            state: self.state,
            max_datagram_size: self.max_datagram_size,
            remote_max_datagram_frame_size: self.remote_max_datagram_frame_size,
            handshake_complete: self.handshake_complete,
            peer_settings_received: self.peer_settings_received,
            local_goaway_sent: self.local_goaway_sent,
            session_count: self.sessions.len(),
            stream_count: self.stream_map.len(),
            pending_request_count: self.pending_requests.len(),
            early_event_count: self.early_event_count,
            connected_at: self.connected_at,
            closed_at: self.closed_at,
        }
    }
}

// Diagnostic information snapshot for a connection.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ConnectionDiagnostics {
    pub(crate) connection_id: String,
    pub(crate) is_client: bool,
    pub(crate) state: ConnectionState,
    pub(crate) max_datagram_size: u64,
    pub(crate) remote_max_datagram_frame_size: Option<u64>,
    pub(crate) handshake_complete: bool,
    pub(crate) peer_settings_received: bool,
    pub(crate) local_goaway_sent: bool,
    pub(crate) session_count: usize,
    pub(crate) stream_count: usize,
    pub(crate) pending_request_count: usize,
    pub(crate) early_event_count: usize,
    pub(crate) connected_at: Option<f64>,
    pub(crate) closed_at: Option<f64>,
}

// Data required to initialize a pending session.
#[derive(Clone, Debug)]
pub(crate) struct SessionInitData {
    pub(crate) path: String,
    pub(crate) headers: Headers,
    pub(crate) created_at: f64,
}

#[cfg(test)]
mod tests;
