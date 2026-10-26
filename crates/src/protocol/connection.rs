//! Connection state machine.

use std::borrow::Cow;
use std::collections::VecDeque;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use tracing::debug;

use crate::common::constants::{
    ERR_APP_NO_ERROR, ERR_H3_REQUEST_REJECTED, ERR_LIB_CONNECTION_STATE_ERROR,
    ERR_LIB_INTERNAL_ERROR, ERR_LIB_SESSION_STATE_ERROR, ERR_LIB_STREAM_STATE_ERROR,
    ERR_WT_ALPN_ERROR, ERR_WT_BUFFERED_STREAM_REJECTED, QUIC_STREAM_TYPE_COUNT, QUIC_VARINT_LIMIT,
    WT_AVAILABLE_PROTOCOLS, WT_PROTOCOL, WT_UPGRADE_TOKEN,
};
use crate::common::types::{
    ConnectionHandle, ConnectionState, ErrorCode, ErrorSource, EventType, Headers, RequestId,
    SessionId, StreamId,
};
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};
use crate::protocol::h3::H3Settings;
use crate::protocol::session::{Session, SessionParameters};
use crate::protocol::utils;

#[derive(Clone, Debug)]
pub(crate) struct ConnectionDiagnostics {
    pub(crate) close_code: Option<ErrorCode>,
    pub(crate) close_reason: Option<String>,
    pub(crate) closed_at: Option<f64>,
    pub(crate) connected_at: Option<f64>,
    pub(crate) connection_handle: ConnectionHandle,
    pub(crate) handshake_completed: bool,
    pub(crate) is_client: bool,
    pub(crate) local_goaway_sent: bool,
    pub(crate) peer_goaway_received: bool,
    pub(crate) peer_initial_max_data: u64,
    pub(crate) peer_initial_max_streams_bidi: u64,
    pub(crate) peer_initial_max_streams_uni: u64,
    pub(crate) peer_max_datagram_frame_size: Option<u64>,
    pub(crate) peer_settings_received: bool,
    pub(crate) state: ConnectionState,
}

pub(super) struct Connection {
    blocked_session_requests: VecDeque<RequestId>,
    close_code: Option<ErrorCode>,
    close_reason: Option<String>,
    closed_at: Option<f64>,
    connected_at: Option<f64>,
    early_event_buffer: FxHashMap<SessionId, Vec<(f64, ProtocolEvent)>>,
    early_event_count: usize,
    early_event_ttl: f64,
    handle: ConnectionHandle,
    handshake_completed: bool,
    initial_max_data: u64,
    initial_max_streams_bidi: u64,
    initial_max_streams_uni: u64,
    is_client: bool,
    local_goaway_sent: bool,
    max_connection_pending_events: u64,
    max_pending_capsules: u64,
    max_pending_datagrams: u64,
    max_pending_streams: u64,
    max_session_pending_events: u64,
    max_sessions: u64,
    max_write_buffer_size: u64,
    peer_goaway_received: bool,
    peer_initial_max_data: u64,
    peer_initial_max_streams_bidi: u64,
    peer_initial_max_streams_uni: u64,
    peer_max_datagram_frame_size: Option<u64>,
    peer_settings_received: bool,
    pending_session_requests: FxHashMap<StreamId, RequestId>,
    request_id: Option<RequestId>,
    session_receive_window: u64,
    session_requests: FxHashMap<RequestId, SessionRequest>,
    sessions: FxHashMap<SessionId, Session>,
    state: ConnectionState,
    stream_map: FxHashMap<StreamId, SessionId>,
}

impl Connection {
    pub(super) fn new(
        handle: ConnectionHandle,
        request_id: Option<RequestId>,
        is_client: bool,
        parameters: ConnectionParameters,
    ) -> Self {
        Self {
            blocked_session_requests: VecDeque::new(),
            close_code: None,
            close_reason: None,
            closed_at: None,
            connected_at: None,
            early_event_buffer: FxHashMap::default(),
            early_event_count: 0,
            early_event_ttl: parameters.early_event_ttl,
            handle,
            handshake_completed: false,
            initial_max_data: parameters.initial_max_data,
            initial_max_streams_bidi: parameters.initial_max_streams_bidi,
            initial_max_streams_uni: parameters.initial_max_streams_uni,
            is_client,
            local_goaway_sent: false,
            max_connection_pending_events: parameters.max_connection_pending_events,
            max_pending_capsules: parameters.max_pending_capsules,
            max_pending_datagrams: parameters.max_pending_datagrams,
            max_pending_streams: parameters.max_pending_streams,
            max_session_pending_events: parameters.max_session_pending_events,
            max_sessions: parameters.max_sessions,
            max_write_buffer_size: parameters.max_write_buffer_size,
            peer_goaway_received: false,
            peer_initial_max_data: 0,
            peer_initial_max_streams_bidi: 0,
            peer_initial_max_streams_uni: 0,
            peer_max_datagram_frame_size: None,
            peer_settings_received: false,
            pending_session_requests: FxHashMap::default(),
            request_id,
            session_receive_window: parameters.session_receive_window,
            session_requests: FxHashMap::default(),
            sessions: FxHashMap::default(),
            state: ConnectionState::Idle,
            stream_map: FxHashMap::default(),
        }
    }

    pub(super) fn accept_session(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        headers: Headers,
        wt_protocol: Option<String>,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.accept(request_id, headers, wt_protocol, now);
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

    pub(super) fn bind_session(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        self.pending_session_requests.insert(stream_id, request_id);

        let is_optimistic = self
            .session_requests
            .get(&request_id)
            .is_some_and(|session_request| session_request.optimistic);

        if is_optimistic && let Some(session_request) = self.session_requests.remove(&request_id) {
            let session = Session::new(
                stream_id,
                session_request.path.clone(),
                session_request.headers.clone(),
                session_request.wt_available_protocols,
                self.is_client,
                SessionParameters {
                    flow_control_negotiated: self.has_flow_control(),
                    initial_max_data: self.initial_max_data,
                    initial_max_streams_bidi: self.initial_max_streams_bidi,
                    initial_max_streams_uni: self.initial_max_streams_uni,
                    max_pending_capsules: self.max_pending_capsules,
                    max_pending_datagrams: self.max_pending_datagrams,
                    max_pending_streams: self.max_pending_streams,
                    max_write_buffer_size: self.max_write_buffer_size,
                    peer_max_data: self.peer_initial_max_data,
                    peer_max_streams_bidi: self.peer_initial_max_streams_bidi,
                    peer_max_streams_uni: self.peer_initial_max_streams_uni,
                    receive_window: self.session_receive_window,
                },
                session_request.created_at,
            );
            self.sessions.insert(stream_id, session);

            return vec![
                Effect::EmitSessionEvent {
                    session_id: stream_id,
                    event_type: EventType::SessionPending,
                    path: Some(session_request.path),
                    headers: Some(session_request.headers),
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
                Effect::NotifyRequestDone {
                    request_id,
                    result: RequestResult::SessionId(stream_id),
                },
            ];
        }

        Vec::new()
    }

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
            if session.has_stream(stream_id) {
                self.stream_map.insert(stream_id, session_id);
            }
            return effects;
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

    pub(super) fn close(
        &mut self,
        request_id: Option<RequestId>,
        error_code: ErrorCode,
        reason: Option<Cow<'static, str>>,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if matches!(
            self.state,
            ConnectionState::Closed | ConnectionState::Closing
        ) {
            if let Some(request_id) = request_id {
                effects.push(Effect::NotifyRequestDone {
                    request_id,
                    result: RequestResult::None,
                });
            }
            return effects;
        }

        debug!(
            "wt_connection close actual={:?} connection_handle={} err={error_code}",
            self.state, self.handle
        );
        self.close_code = Some(error_code);
        self.close_reason = reason.clone().map(Cow::into_owned);
        self.closed_at = Some(now);
        self.state = ConnectionState::Closing;

        effects.push(Effect::CloseQuicConnection { error_code, reason });

        if let Some(request_id) = request_id {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
        }

        effects
    }

    pub(super) fn close_gracefully(&mut self, request_id: RequestId) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.state != ConnectionState::Connected {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            });
            return effects;
        }

        if !self.local_goaway_sent {
            debug!(
                "wt_connection drain actual=false connection_handle={} request_id={request_id}",
                self.handle
            );
            self.local_goaway_sent = true;

            let identifier = if self.is_client {
                0
            } else {
                self.sessions
                    .keys()
                    .copied()
                    .max()
                    .map_or(0, |session_id| {
                        session_id.saturating_add(QUIC_STREAM_TYPE_COUNT)
                    })
                    .min(QUIC_VARINT_LIMIT)
            };

            effects.push(Effect::SendH3Goaway { identifier });
        }

        let mut session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(session) = self.sessions.get_mut(&session_id) {
                effects.extend(session.drain());
            }
        }

        effects.push(Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        });

        effects
    }

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

    pub(super) fn complete_stream_write(&mut self, stream_id: StreamId) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.complete_stream_write(stream_id);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
        }
        Vec::new()
    }

    pub(super) fn create_session(
        &mut self,
        request_id: RequestId,
        mut session_request: SessionRequest,
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

        let current_total = (self.sessions.len() + self.session_requests.len()) as u64;

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

        if !utils::is_valid_user_headers(&session_request.headers, true) {
            debug!(
                "h3_headers validate invalid connection_handle={} request_id={request_id}",
                self.handle
            );
            return vec![Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Connection,
                error_code: None,
                reason: "h3_headers validate invalid".into(),
            }];
        }

        if let Some(ref wt_available_protocols) = session_request.wt_available_protocols {
            match utils::encode_wt_protocol_list(wt_available_protocols) {
                Ok(encoded) => {
                    session_request
                        .headers
                        .push((Bytes::from_static(WT_AVAILABLE_PROTOCOLS), encoded));
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

        self.session_requests
            .insert(request_id, session_request.clone());

        vec![Effect::CreateH3ConnectStream {
            request_id,
            authority: session_request.authority,
            path: session_request.path,
            headers: session_request.headers,
        }]
    }

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

    pub(super) fn defer_session(&mut self, request_id: RequestId) -> Vec<Effect> {
        self.blocked_session_requests.push_back(request_id);

        Vec::new()
    }

    pub(super) fn defer_stream(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.defer_stream(request_id, is_unidirectional);
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

    pub(super) fn diagnose(&self, request_id: RequestId) -> Vec<Effect> {
        let diagnostics = self.diagnostics_snapshot();

        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::ConnectionDiagnostics(Box::new(diagnostics)),
        }]
    }

    pub(super) fn diagnose_session(
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

    pub(super) fn diagnose_stream(
        &self,
        stream_id: StreamId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get(&session_id)
        {
            return session.diagnose_stream(stream_id, request_id);
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

    pub(super) fn export_keying_material(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        label: &str,
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
        self.session_requests.remove(&request_id);

        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Session,
            error_code,
            reason,
        }]
    }

    pub(super) fn handle_handshake_completed(&mut self, now: f64) -> Vec<Effect> {
        if self.state == ConnectionState::Idle {
            self.state = ConnectionState::Connecting;
        }

        if self.state != ConnectionState::Connecting {
            debug!(
                "wt_connection validate failed actual={:?} connection_handle={} expected=connecting",
                self.state, self.handle
            );
            return Vec::new();
        }

        self.handshake_completed = true;

        let mut effects = vec![Effect::CreateH3LocalStreams];

        if !self.is_client || self.peer_settings_received {
            effects.extend(self.open(now));
        }

        effects
    }

    pub(super) fn handle_stream_data_acknowledged(&mut self, stream_id: StreamId) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.handle_stream_data_acknowledged(stream_id);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
        }
        Vec::new()
    }

    pub(super) fn handle_stream_opened(
        &mut self,
        session_id: SessionId,
        stream_id: StreamId,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let effects = session.handle_stream_opened(stream_id, now);
            if session.has_stream(stream_id) {
                self.stream_map.insert(stream_id, session_id);
            }
            return effects;
        }

        let event = ProtocolEvent::H3WebTransportStreamOpened {
            session_id,
            stream_id,
        };

        if !self.buffer_early_event(session_id, event, now) {
            debug!(
                "wt_stream abort connection_handle={} stream_id={stream_id}",
                self.handle
            );
            let mut effects = Vec::new();

            if utils::is_unidirectional_stream(stream_id) {
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

    pub(super) fn handle_terminated(
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
            self.close_code = Some(error_code);
            self.close_reason = Some(reason.clone().into_owned());
            self.closed_at = Some(now);
        }
        self.state = ConnectionState::Closed;

        let mut effects = Vec::new();

        let failure_reason = if previous_state == ConnectionState::Closing {
            debug!(
                "wt_connection close actual={:?} connection_handle={} err={error_code}",
                previous_state, self.handle
            );
            "wt_connection close"
        } else {
            debug!(
                "wt_connection abort actual={:?} connection_handle={} err={error_code}",
                previous_state, self.handle
            );
            "wt_connection abort"
        };

        let mut request_ids: Vec<RequestId> = self
            .session_requests
            .drain()
            .map(|(request_id, _)| request_id)
            .chain(self.request_id.take())
            .collect();
        request_ids.sort_unstable();

        self.blocked_session_requests.clear();
        self.pending_session_requests.clear();

        let mut session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(session) = self.sessions.get_mut(&session_id) {
                effects.extend(session.recv_connect_close(error_code, reason.clone(), now));
            }
        }

        effects.push(Effect::EmitConnectionEvent {
            connection_handle: self.handle,
            event_type: EventType::ConnectionClosed,
            error_code: Some(error_code),
            reason: Some(reason),
        });

        for request_id in request_ids {
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Connection,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                reason: failure_reason.into(),
            });
        }

        effects
    }

    pub(super) fn has_stream(&self, stream_id: StreamId) -> bool {
        self.stream_map.contains_key(&stream_id)
    }

    pub(super) fn is_client(&self) -> bool {
        self.is_client
    }

    pub(super) fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected
    }

    pub(super) fn is_pre_connected(&self) -> bool {
        matches!(
            self.state,
            ConnectionState::Connecting | ConnectionState::Idle
        )
    }

    pub(super) fn is_session_closed(&self, session_id: SessionId) -> bool {
        self.sessions
            .get(&session_id)
            .is_some_and(Session::is_closed)
    }

    pub(super) fn is_session_stream(&self, stream_id: StreamId) -> bool {
        self.sessions.contains_key(&stream_id)
    }

    pub(super) fn peer_max_datagram_frame_size(&self) -> Option<u64> {
        self.peer_max_datagram_frame_size
    }

    pub(super) fn prune_early_events(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut session_ids_to_remove = Vec::new();

        let mut session_ids: Vec<SessionId> = self.early_event_buffer.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(events) = self.early_event_buffer.get_mut(&session_id) {
                let mut valid_events = Vec::new();

                for (timestamp, event) in events.drain(..) {
                    if now - timestamp < self.early_event_ttl {
                        valid_events.push((timestamp, event));
                    } else {
                        self.early_event_count -= 1;

                        if let ProtocolEvent::H3WebTransportStreamOpened { stream_id, .. } = event {
                            debug!(
                                "wt_stream abort connection_handle={} stream_id={stream_id}",
                                self.handle
                            );
                            if utils::is_unidirectional_stream(stream_id) {
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
                }

                if valid_events.is_empty() {
                    session_ids_to_remove.push(session_id);
                } else {
                    *events = valid_events;
                }
            }
        }

        for session_id in session_ids_to_remove {
            self.early_event_buffer.remove(&session_id);
        }

        effects
    }

    pub(super) fn prune_resources(&mut self) {
        let mut closed_session_ids: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.is_closed())
            .map(|(session_id, _)| *session_id)
            .collect();

        closed_session_ids.sort_unstable();

        for session_id in &closed_session_ids {
            debug!(
                "wt_session destroy connection_handle={} session_id={session_id}",
                self.handle
            );
            self.sessions.remove(session_id);
        }

        if !closed_session_ids.is_empty() {
            self.stream_map
                .retain(|_, session_id| closed_session_ids.binary_search(session_id).is_err());
        }
    }

    pub(super) fn read_datagram(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.read_datagram(request_id);
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

    pub(super) fn read_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        max_size: u64,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.read_stream(stream_id, request_id, max_size);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
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

    pub(super) fn recv_capsule(
        &mut self,
        session_id: SessionId,
        capsule_type: u64,
        capsule_value: &[u8],
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_capsule(capsule_type, capsule_value, now);
        }
        Vec::new()
    }

    pub(super) fn recv_connect_close(&mut self, session_id: SessionId, now: f64) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_connect_close(ERR_APP_NO_ERROR, "".into(), now);
        }
        Vec::new()
    }

    pub(super) fn recv_datagram(
        &mut self,
        session_id: SessionId,
        data: Bytes,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.recv_datagram(data);
        }

        let event = ProtocolEvent::H3DatagramReceived { session_id, data };

        self.buffer_early_event(session_id, event, now);

        Vec::new()
    }

    pub(super) fn recv_goaway(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();

        self.peer_goaway_received = true;

        while let Some(request_id) = self.blocked_session_requests.pop_front() {
            debug!(
                "wt_connection validate failed actual={} connection_handle={} expected=false request_id={request_id}",
                self.peer_goaway_received, self.handle
            );
            self.session_requests.remove(&request_id);
            effects.push(Effect::NotifyRequestFailed {
                request_id,
                source: ErrorSource::Connection,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                reason: "wt_connection validate failed".into(),
            });
        }

        let mut session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(session) = self.sessions.get_mut(&session_id) {
                effects.extend(session.drain());
            }
        }

        effects
    }

    pub(super) fn recv_headers(
        &mut self,
        stream_id: StreamId,
        headers: Headers,
        now: f64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if self.is_client {
            let Some(request_id) = self.pending_session_requests.remove(&stream_id) else {
                debug!(
                    "wt_stream resolve failed connection_handle={} stream_id={stream_id}",
                    self.handle
                );
                return effects;
            };

            let is_optimistic = self.sessions.contains_key(&stream_id);

            let wt_available_protocols = if is_optimistic {
                self.sessions
                    .get(&stream_id)
                    .and_then(Session::wt_available_protocols)
                    .map(<[String]>::to_vec)
            } else {
                let Some(session_request) = self.session_requests.get(&request_id) else {
                    debug!(
                        "wt_session resolve failed connection_handle={} request_id={request_id}",
                        self.handle
                    );
                    effects.push(Effect::NotifyRequestFailed {
                        request_id,
                        source: ErrorSource::Unspecified,
                        error_code: Some(ERR_LIB_INTERNAL_ERROR),
                        reason: "wt_session resolve failed".into(),
                    });
                    return effects;
                };
                session_request.wt_available_protocols.clone()
            };

            let status = utils::find_header(&headers, b":status");
            let status_ok = status.is_some_and(|status| status == b"200".as_slice());

            if status_ok {
                let wt_protocol = utils::combine_header(&headers, WT_PROTOCOL)
                    .and_then(|value| utils::parse_wt_protocol_string(&value));

                if let Some(ref wt_available_protocols) = wt_available_protocols {
                    let is_valid = match wt_protocol {
                        Some(ref wt_protocol) => wt_available_protocols.contains(wt_protocol),
                        None => false,
                    };

                    if !is_valid {
                        let reason = if let Some(ref wt_protocol) = wt_protocol {
                            debug!(
                                "wt_protocol validate invalid actual={wt_protocol} connection_handle={} expected=wt_available_protocols",
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

                        if let Some(session) = self.sessions.get_mut(&stream_id) {
                            effects.extend(session.abort(ERR_WT_ALPN_ERROR, reason.into(), now));
                        } else {
                            self.session_requests.remove(&request_id);
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
                        }
                        return effects;
                    }
                }

                if let Some(session) = self.sessions.get_mut(&stream_id) {
                    effects.extend(session.confirm(wt_protocol, now));
                } else if let Some(session_request) = self.session_requests.remove(&request_id) {
                    let mut session = Session::new(
                        stream_id,
                        session_request.path,
                        session_request.headers,
                        session_request.wt_available_protocols,
                        self.is_client,
                        SessionParameters {
                            flow_control_negotiated: self.has_flow_control(),
                            initial_max_data: self.initial_max_data,
                            initial_max_streams_bidi: self.initial_max_streams_bidi,
                            initial_max_streams_uni: self.initial_max_streams_uni,
                            max_pending_capsules: self.max_pending_capsules,
                            max_pending_datagrams: self.max_pending_datagrams,
                            max_pending_streams: self.max_pending_streams,
                            max_write_buffer_size: self.max_write_buffer_size,
                            peer_max_data: self.peer_initial_max_data,
                            peer_max_streams_bidi: self.peer_initial_max_streams_bidi,
                            peer_max_streams_uni: self.peer_initial_max_streams_uni,
                            receive_window: self.session_receive_window,
                        },
                        session_request.created_at,
                    );
                    effects.extend(session.confirm(wt_protocol, now));
                    self.sessions.insert(stream_id, session);

                    effects.push(Effect::NotifyRequestDone {
                        request_id,
                        result: RequestResult::SessionId(stream_id),
                    });
                }

                effects.extend(self.replay_early_events(stream_id, now));
            } else {
                debug!(
                    "wt_session abort connection_handle={} err={status:?}",
                    self.handle
                );

                self.session_requests.remove(&request_id);

                if let Some(session) = self.sessions.get_mut(&stream_id) {
                    effects.extend(session.abort(
                        ERR_H3_REQUEST_REJECTED,
                        "wt_session abort".into(),
                        now,
                    ));
                } else {
                    effects.push(Effect::NotifyRequestFailed {
                        request_id,
                        source: ErrorSource::Session,
                        error_code: Some(ERR_H3_REQUEST_REJECTED),
                        reason: "wt_session abort".into(),
                    });
                    effects.push(Effect::StopQuicStream {
                        stream_id,
                        error_code: ERR_H3_REQUEST_REJECTED,
                    });
                    effects.push(Effect::ResetQuicStream {
                        stream_id,
                        error_code: ERR_H3_REQUEST_REJECTED,
                    });
                }

                effects.extend(self.replay_early_events(stream_id, now));
            }
        } else {
            if self.sessions.contains_key(&stream_id) {
                debug!(
                    "wt_session validate failed connection_handle={} expected=false stream_id={stream_id}",
                    self.handle
                );
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

            let is_connect_method = utils::find_header(&headers, b":method")
                .is_some_and(|method| method == b"CONNECT".as_slice());
            let is_webtransport_upgrade = utils::find_header(&headers, b":protocol")
                .is_some_and(|protocol| protocol == WT_UPGRADE_TOKEN);

            if !is_connect_method || !is_webtransport_upgrade {
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

            let path = utils::find_header(&headers, b":path")
                .and_then(|path| str::from_utf8(path).ok())
                .unwrap_or("/")
                .to_owned();

            let wt_available_protocols = utils::combine_header(&headers, WT_AVAILABLE_PROTOCOLS)
                .and_then(|value| utils::parse_wt_protocol_list(&value));

            let session = Session::new(
                stream_id,
                path.clone(),
                headers.clone(),
                wt_available_protocols.clone(),
                self.is_client,
                SessionParameters {
                    flow_control_negotiated: self.has_flow_control(),
                    initial_max_data: self.initial_max_data,
                    initial_max_streams_bidi: self.initial_max_streams_bidi,
                    initial_max_streams_uni: self.initial_max_streams_uni,
                    max_pending_capsules: self.max_pending_capsules,
                    max_pending_datagrams: self.max_pending_datagrams,
                    max_pending_streams: self.max_pending_streams,
                    max_write_buffer_size: self.max_write_buffer_size,
                    peer_max_data: self.peer_initial_max_data,
                    peer_max_streams_bidi: self.peer_initial_max_streams_bidi,
                    peer_max_streams_uni: self.peer_initial_max_streams_uni,
                    receive_window: self.session_receive_window,
                },
                now,
            );
            self.sessions.insert(stream_id, session);

            effects.push(Effect::EmitSessionEvent {
                session_id: stream_id,
                event_type: EventType::SessionRequestReceived,
                path: Some(path),
                headers: Some(headers),
                wt_available_protocols,
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

            effects.extend(self.replay_early_events(stream_id, now));
        }

        effects
    }

    pub(super) fn recv_max_streams(&mut self, is_unidirectional: bool) -> Vec<Effect> {
        let mut effects = Vec::new();

        if !is_unidirectional {
            while let Some(request_id) = self.blocked_session_requests.pop_front() {
                if let Some(session_request) = self.session_requests.get(&request_id) {
                    effects.push(Effect::CreateH3ConnectStream {
                        request_id,
                        authority: session_request.authority.clone(),
                        path: session_request.path.clone(),
                        headers: session_request.headers.clone(),
                    });
                }
            }
        }

        let mut session_ids: Vec<SessionId> = self.sessions.keys().copied().collect();
        session_ids.sort_unstable();

        for session_id in session_ids {
            if let Some(session) = self.sessions.get_mut(&session_id) {
                effects.extend(session.flush_blocked_stream_requests(is_unidirectional));
            }
        }

        effects
    }

    pub(super) fn recv_settings(&mut self, settings: &H3Settings, now: f64) -> Vec<Effect> {
        self.peer_settings_received = true;

        if let Some(peer_initial_max_data) = settings.wt_initial_max_data {
            self.peer_initial_max_data = peer_initial_max_data;
        }
        if let Some(peer_initial_max_streams_bidi) = settings.wt_initial_max_streams_bidi {
            self.peer_initial_max_streams_bidi = peer_initial_max_streams_bidi;
        }
        if let Some(peer_initial_max_streams_uni) = settings.wt_initial_max_streams_uni {
            self.peer_initial_max_streams_uni = peer_initial_max_streams_uni;
        }

        if self.state == ConnectionState::Connecting && self.handshake_completed {
            return self.open(now);
        }

        Vec::new()
    }

    pub(super) fn recv_stop_sending(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.recv_stop_sending(stream_id, error_code);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
        }
        if let Some(session) = self.sessions.get_mut(&stream_id) {
            return session.abort(error_code, "wt_session abort".into(), now);
        }
        Vec::new()
    }

    pub(super) fn recv_stream_data(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        fin: bool,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.recv_stream_data(stream_id, data, fin, now);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
        }
        Vec::new()
    }

    pub(super) fn recv_stream_reset(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.recv_stream_reset(stream_id, error_code);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
        }
        if let Some(session) = self.sessions.get_mut(&stream_id) {
            return session.abort(error_code, "wt_session abort".into(), now);
        }
        Vec::new()
    }

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

    pub(super) fn reject_session(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        headers: Headers,
        status_code: u16,
        now: f64,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return session.reject(request_id, headers, status_code, now);
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

    pub(super) fn reset_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.reset_stream(stream_id, request_id, error_code);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
        }
        debug!(
            "wt_stream resolve failed connection_handle={} request_id={request_id} stream_id={stream_id}",
            self.handle
        );
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        }]
    }

    pub(super) fn send_datagram(
        &mut self,
        session_id: SessionId,
        request_id: RequestId,
        data: Bytes,
    ) -> Vec<Effect> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let peer_max_datagram_frame_size = self.peer_max_datagram_frame_size.unwrap_or(0);
            return session.send_datagram(request_id, data, peer_max_datagram_frame_size);
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

    pub(super) fn stop_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        error_code: ErrorCode,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.stop_stream(stream_id, request_id, error_code);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
        }
        debug!(
            "wt_stream resolve failed connection_handle={} request_id={request_id} stream_id={stream_id}",
            self.handle
        );
        vec![Effect::NotifyRequestDone {
            request_id,
            result: RequestResult::None,
        }]
    }

    pub(super) fn write_stream(
        &mut self,
        stream_id: StreamId,
        request_id: RequestId,
        data: Bytes,
        end_stream: bool,
    ) -> Vec<Effect> {
        if let Some(&session_id) = self.stream_map.get(&stream_id)
            && let Some(session) = self.sessions.get_mut(&session_id)
        {
            let effects = session.write_stream(stream_id, request_id, data, end_stream);
            if !session.has_stream(stream_id) {
                self.stream_map.remove(&stream_id);
            }
            return effects;
        }
        debug!(
            "wt_stream resolve failed connection_handle={} request_id={request_id} stream_id={stream_id}",
            self.handle
        );
        if data.is_empty() && end_stream {
            return vec![Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::None,
            }];
        }
        vec![Effect::NotifyRequestFailed {
            request_id,
            source: ErrorSource::Stream,
            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
            reason: "wt_stream resolve failed".into(),
        }]
    }

    fn buffer_early_event(
        &mut self,
        session_id: SessionId,
        event: ProtocolEvent,
        now: f64,
    ) -> bool {
        if (self.early_event_count as u64) >= self.max_connection_pending_events {
            debug!(
                "wt_connection validate exceeded actual={} connection_handle={} expected=max_connection_pending_events",
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

    fn diagnostics_snapshot(&self) -> ConnectionDiagnostics {
        ConnectionDiagnostics {
            close_code: self.close_code,
            close_reason: self.close_reason.clone(),
            closed_at: self.closed_at,
            connected_at: self.connected_at,
            connection_handle: self.handle,
            handshake_completed: self.handshake_completed,
            is_client: self.is_client,
            local_goaway_sent: self.local_goaway_sent,
            peer_goaway_received: self.peer_goaway_received,
            peer_initial_max_data: self.peer_initial_max_data,
            peer_initial_max_streams_bidi: self.peer_initial_max_streams_bidi,
            peer_initial_max_streams_uni: self.peer_initial_max_streams_uni,
            peer_max_datagram_frame_size: self.peer_max_datagram_frame_size,
            peer_settings_received: self.peer_settings_received,
            state: self.state,
        }
    }

    fn has_flow_control(&self) -> bool {
        let local_intent = self.initial_max_data > 0
            || self.initial_max_streams_bidi > 0
            || self.initial_max_streams_uni > 0;
        let peer_intent = self.peer_initial_max_data > 0
            || self.peer_initial_max_streams_bidi > 0
            || self.peer_initial_max_streams_uni > 0;

        local_intent && peer_intent
    }

    fn open(&mut self, now: f64) -> Vec<Effect> {
        debug!(
            "wt_connection open actual={:?} connection_handle={}",
            self.state, self.handle
        );
        self.connected_at = Some(now);
        self.state = ConnectionState::Connected;

        let mut effects = vec![Effect::EmitConnectionEvent {
            connection_handle: self.handle,
            event_type: EventType::ConnectionEstablished,
            error_code: None,
            reason: None,
        }];

        if let Some(request_id) = self.request_id.take() {
            effects.push(Effect::NotifyRequestDone {
                request_id,
                result: RequestResult::ConnectionHandle(self.handle),
            });
        }

        effects
    }

    fn replay_early_events(&mut self, session_id: SessionId, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();

        let Some(events) = self.early_event_buffer.remove(&session_id) else {
            return effects;
        };
        self.early_event_count -= events.len();

        let is_session_present = self.sessions.contains_key(&session_id);

        for (_, event) in events {
            match event {
                ProtocolEvent::H3DatagramReceived { session_id, data } if is_session_present => {
                    effects.extend(self.recv_datagram(session_id, data, now));
                }
                ProtocolEvent::H3WebTransportStreamOpened {
                    session_id,
                    stream_id,
                } => {
                    if is_session_present {
                        effects.extend(self.handle_stream_opened(session_id, stream_id, now));
                    } else {
                        debug!(
                            "wt_stream abort connection_handle={} stream_id={stream_id}",
                            self.handle
                        );
                        if utils::is_unidirectional_stream(stream_id) {
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
                _ => {}
            }
        }

        effects
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ConnectionParameters {
    pub(super) early_event_ttl: f64,
    pub(super) initial_max_data: u64,
    pub(super) initial_max_streams_bidi: u64,
    pub(super) initial_max_streams_uni: u64,
    pub(super) max_connection_pending_events: u64,
    pub(super) max_pending_capsules: u64,
    pub(super) max_pending_datagrams: u64,
    pub(super) max_pending_streams: u64,
    pub(super) max_session_pending_events: u64,
    pub(super) max_sessions: u64,
    pub(super) max_write_buffer_size: u64,
    pub(super) session_receive_window: u64,
}

#[derive(Clone, Debug)]
pub(super) struct SessionRequest {
    pub(super) authority: String,
    pub(super) created_at: f64,
    pub(super) headers: Headers,
    pub(super) optimistic: bool,
    pub(super) path: String,
    pub(super) wt_available_protocols: Option<Vec<String>>,
}

#[cfg(test)]
mod tests;
