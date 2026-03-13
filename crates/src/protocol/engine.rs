//! Protocol engine orchestrator and event loop management.

use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use tracing::{debug, error, warn};

use crate::common::constants::{
    ERR_H3_INTERNAL_ERROR, ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_INTERNAL_ERROR,
    ERR_LIB_STREAM_STATE_ERROR, H3_STREAM_TYPE_CONTROL, H3_STREAM_TYPE_QPACK_DECODER,
    H3_STREAM_TYPE_QPACK_ENCODER, UPGRADE_TOKEN_WEBTRANSPORT,
};
use crate::common::error::WebTransportError;
use crate::common::types::{
    ConnectionId, ConnectionState, ErrorCode, ErrorSource, EventType, Headers, StreamId,
};
use crate::protocol::connection::Connection;
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::h3::H3;
use crate::protocol::utils::{merge_headers, write_varint};

// Orchestrates the unified protocol state machine.
pub(crate) struct WebTransportEngine {
    connection: Connection,
    h3: H3,
    pending_user_actions: VecDeque<ProtocolEvent>,
}

impl WebTransportEngine {
    // Engine initialization with comprehensive configuration.
    #[allow(
        clippy::too_many_arguments,
        reason = "Complex internal state initialization."
    )]
    pub(crate) fn new(
        connection_id: ConnectionId,
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
        max_capsule_size: u64,
        max_pending_events_per_session: u64,
        max_total_pending_events: u64,
        early_event_ttl: f64,
    ) -> Result<Self, WebTransportError> {
        let connection = Connection::new(
            connection_id,
            is_client,
            max_datagram_size,
            flow_control_window_size,
            max_sessions,
            initial_max_data,
            initial_max_streams_bidi,
            initial_max_streams_uni,
            stream_read_buffer_size,
            stream_write_buffer_size,
            flow_control_window_auto_scale,
            max_pending_events_per_session,
            max_total_pending_events,
            early_event_ttl,
        );

        let h3 = H3::new(
            is_client,
            initial_max_data,
            initial_max_streams_bidi,
            initial_max_streams_uni,
            max_capsule_size,
        )?;

        Ok(Self {
            connection,
            h3,
            pending_user_actions: VecDeque::new(),
        })
    }

    // H3 stream state cleanup.
    pub(crate) fn cleanup_stream(&mut self, stream_id: StreamId) {
        self.h3.cleanup_stream(stream_id);
    }

    // Capsule encoding to H3 DATA frame.
    pub(crate) fn encode_capsule(
        stream_id: StreamId,
        capsule_type: u64,
        capsule_data: Bytes,
        end_stream: bool,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let data = H3::encode_capsule(stream_id, capsule_type, capsule_data)?;
        Ok(vec![Effect::SendQuicData {
            stream_id,
            data,
            end_stream,
        }])
    }

    // Datagram encoding to H3 frame.
    pub(crate) fn encode_datagram(
        stream_id: StreamId,
        data: Bytes,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let payload = H3::encode_datagram(stream_id, data)?;
        let total_len = payload.iter().map(Bytes::len).sum();
        let mut merged = BytesMut::with_capacity(total_len);
        for p in payload {
            merged.extend_from_slice(&p);
        }

        Ok(vec![Effect::SendQuicDatagram {
            data: merged.freeze(),
        }])
    }

    // GOAWAY frame encoding.
    pub(crate) fn encode_goaway(&mut self) -> Vec<Effect> {
        if let Some(control_id) = self.h3.local_control_stream_id() {
            if let Ok(data) = H3::encode_goaway(0) {
                return vec![Effect::SendQuicData {
                    stream_id: control_id,
                    data,
                    end_stream: false,
                }];
            }
            error!("Failed to encode GOAWAY frame");
        }
        Vec::new()
    }

    // Headers encoding to H3 HEADERS frame.
    pub(crate) fn encode_headers(
        &mut self,
        stream_id: StreamId,
        headers: &Headers,
        end_stream: bool,
    ) -> Result<Vec<Effect>, WebTransportError> {
        self.h3.encode_headers(stream_id, headers, end_stream)
    }

    // Session establishment CONNECT request encoding.
    pub(crate) fn encode_session_request(
        &mut self,
        stream_id: StreamId,
        path: String,
        authority: String,
        headers: &Headers,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let initial_headers: Headers = vec![
            (
                Bytes::from_static(b":method"),
                Bytes::from_static(b"CONNECT"),
            ),
            (Bytes::from_static(b":scheme"), Bytes::from_static(b"https")),
            (Bytes::from_static(b":authority"), Bytes::from(authority)),
            (Bytes::from_static(b":path"), Bytes::from(path)),
            (
                Bytes::from_static(b":protocol"),
                Bytes::from_static(UPGRADE_TOKEN_WEBTRANSPORT),
            ),
        ];
        let final_headers = merge_headers(&initial_headers, headers);
        self.h3.encode_headers(stream_id, &final_headers, false)
    }

    // Stream creation preamble encoding.
    pub(crate) fn encode_stream_creation(
        &mut self,
        stream_id: StreamId,
        control_stream_id: StreamId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        self.h3
            .encode_stream_creation(stream_id, control_stream_id, is_unidirectional)
    }

    // Protocol event handling.
    pub(crate) fn handle_event(&mut self, event: ProtocolEvent, now: f64) -> Vec<Effect> {
        let mut all_effects = Vec::new();
        let mut events_to_process = VecDeque::new();
        events_to_process.push_back(event);

        while let Some(current_event) = events_to_process.pop_front() {
            let mut new_effects = Vec::new();
            let mut re_queue_pending_actions = false;

            match current_event {
                ProtocolEvent::InternalBindH3Session {
                    request_id,
                    stream_id,
                } => {
                    new_effects.extend(self.connection.bind_session(stream_id, request_id));
                }
                ProtocolEvent::InternalBindQuicStream {
                    request_id,
                    stream_id,
                    session_id,
                    is_unidirectional,
                } => {
                    new_effects.extend(self.connection.bind_stream(
                        session_id,
                        stream_id,
                        request_id,
                        is_unidirectional,
                        now,
                    ));
                }
                ProtocolEvent::InternalCleanupEarlyEvents => {
                    new_effects.extend(self.connection.prune_early_events(now));
                }
                ProtocolEvent::InternalCleanupResources => {
                    new_effects.extend(self.connection.prune_resources());
                }
                ProtocolEvent::InternalFailH3Session {
                    request_id,
                    error_code,
                    reason,
                } => {
                    new_effects
                        .extend(self.connection.fail_session(request_id, error_code, reason));
                }
                ProtocolEvent::InternalFailQuicStream {
                    request_id,
                    session_id,
                    is_unidirectional,
                    error_code,
                    reason,
                } => {
                    new_effects.extend(self.connection.fail_stream(
                        session_id,
                        request_id,
                        is_unidirectional,
                        error_code,
                        reason,
                    ));
                }
                ProtocolEvent::TransportConnectionTerminated { error_code, reason } => {
                    new_effects.extend(self.connection.terminated(error_code, reason.clone(), now));
                    let reason = format!("Connection terminated before ready: {reason}");
                    new_effects.extend(
                        self.fail_pending_user_actions(
                            Some(ERR_LIB_CONNECTION_STATE_ERROR),
                            &reason,
                        ),
                    );
                }
                ProtocolEvent::TransportDatagramFrameReceived { .. }
                | ProtocolEvent::TransportStreamDataReceived { .. } => {
                    let was_settings_received = self.h3.is_settings_received();

                    let (h3_events, h3_effects) = self
                        .h3
                        .handle_transport_event(&current_event, &self.connection);

                    new_effects.extend(h3_effects);
                    for evt in h3_events.into_iter().rev() {
                        events_to_process.push_front(evt);
                    }

                    if self.connection.is_client
                        && !was_settings_received
                        && self.h3.is_settings_received()
                    {
                        debug!("Client received peer H3 SETTINGS.");
                    }
                }
                ProtocolEvent::TransportHandshakeCompleted => {
                    if self.connection.state == ConnectionState::Idle {
                        debug!("State transition: IDLE -> CONNECTING");
                        self.connection.state = ConnectionState::Connecting;
                    }

                    if self.connection.state == ConnectionState::Connecting {
                        debug!("TransportHandshakeCompleted received.");
                        let fx = self.connection.handshake_completed(now);
                        new_effects.extend(fx);

                        if self.connection.is_client {
                            if self.connection.state == ConnectionState::Connected {
                                re_queue_pending_actions = true;
                            }
                        } else if !self.connection.is_client {
                            self.connection.state = ConnectionState::Connected;
                            self.connection.connected_at = Some(now);
                            new_effects.push(Effect::EmitConnectionEvent {
                                connection_id: self.connection.id.clone(),
                                event_type: EventType::ConnectionEstablished,
                                error_code: None,
                                reason: None,
                            });
                        }
                    } else {
                        warn!(
                            "Received TransportHandshakeCompleted in unexpected state: {:?}",
                            self.connection.state
                        );
                    }
                }
                ProtocolEvent::TransportQuicParametersReceived {
                    remote_max_datagram_frame_size,
                } => {
                    new_effects.extend(
                        self.connection
                            .recv_transport_parameters(remote_max_datagram_frame_size),
                    );
                }
                ProtocolEvent::TransportStopSendingReceived {
                    stream_id,
                    error_code,
                } => {
                    new_effects.extend(self.connection.recv_stop_sending(stream_id, error_code));
                }
                ProtocolEvent::TransportStreamResetReceived {
                    stream_id,
                    error_code,
                } => {
                    new_effects.extend(
                        self.connection
                            .recv_stream_reset(stream_id, error_code, now),
                    );
                }
                ProtocolEvent::CapsuleReceived {
                    stream_id,
                    capsule_type,
                    capsule_data,
                } => {
                    new_effects.extend(self.connection.recv_capsule(
                        stream_id,
                        capsule_type,
                        &capsule_data,
                        now,
                    ));
                }
                ProtocolEvent::ConnectStreamClosed { stream_id } => {
                    new_effects.extend(self.connection.recv_connect_close(stream_id, now));
                }
                ProtocolEvent::DatagramReceived { stream_id, data } => {
                    new_effects.extend(self.connection.recv_datagram(stream_id, data, now));
                }
                ProtocolEvent::GoawayReceived => {
                    new_effects.extend(self.connection.recv_goaway(now));
                }
                ProtocolEvent::HeadersReceived {
                    stream_id,
                    headers,
                    stream_ended,
                } => {
                    new_effects.extend(self.connection.recv_headers(
                        stream_id,
                        headers,
                        stream_ended,
                        now,
                    ));
                }
                ProtocolEvent::SettingsReceived { settings } => {
                    debug!("Processing H3 SETTINGS frame.");
                    new_effects.extend(self.connection.recv_settings(&settings, now));
                    if self.connection.is_client
                        && self.connection.state == ConnectionState::Connected
                    {
                        re_queue_pending_actions = true;
                    }
                }
                ProtocolEvent::WebTransportStreamDataReceived {
                    session_id,
                    stream_id,
                    data,
                    stream_ended,
                } => {
                    new_effects.extend(self.connection.recv_stream_data(
                        session_id,
                        stream_id,
                        data,
                        stream_ended,
                        now,
                    ));
                }
                ProtocolEvent::UserAcceptSession {
                    request_id,
                    session_id,
                    subprotocol,
                } => {
                    new_effects.extend(self.connection.accept_session(
                        session_id,
                        request_id,
                        subprotocol,
                        now,
                    ));
                }
                ProtocolEvent::UserCloseSession {
                    request_id,
                    session_id,
                    error_code,
                    reason,
                } => {
                    new_effects.extend(
                        self.connection
                            .close_session(session_id, request_id, error_code, reason, now),
                    );
                }
                ProtocolEvent::UserConnectionGracefulClose { request_id } => {
                    new_effects.extend(self.connection.graceful_close(request_id, now));
                }
                ProtocolEvent::UserCreateSession {
                    request_id,
                    path,
                    headers,
                    subprotocols,
                } => {
                    if self.connection.is_client
                        && (self.connection.state == ConnectionState::Idle
                            || self.connection.state == ConnectionState::Connecting)
                    {
                        debug!("Client not fully connected, buffering UserCreateSession.");
                        self.pending_user_actions
                            .push_back(ProtocolEvent::UserCreateSession {
                                request_id,
                                path,
                                headers,
                                subprotocols,
                            });
                    } else {
                        new_effects.extend(self.connection.create_session(
                            request_id,
                            path,
                            headers,
                            subprotocols,
                            now,
                        ));
                    }
                }
                ProtocolEvent::UserCreateStream {
                    request_id,
                    session_id,
                    is_unidirectional,
                } => {
                    if self.connection.is_client
                        && (self.connection.state == ConnectionState::Idle
                            || self.connection.state == ConnectionState::Connecting)
                    {
                        debug!("Client not fully connected, buffering UserCreateStream.");
                        self.pending_user_actions
                            .push_back(ProtocolEvent::UserCreateStream {
                                request_id,
                                session_id,
                                is_unidirectional,
                            });
                    } else {
                        new_effects.extend(self.connection.create_stream(
                            session_id,
                            request_id,
                            is_unidirectional,
                        ));
                    }
                }
                ProtocolEvent::UserExportKeyingMaterial {
                    request_id,
                    session_id,
                    label,
                    context,
                    length,
                } => {
                    new_effects.extend(
                        self.connection.export_keying_material(
                            session_id, request_id, label, &context, length,
                        ),
                    );
                }
                ProtocolEvent::UserGetConnectionDiagnostics { request_id } => {
                    new_effects.extend(self.connection.diagnose(request_id));
                }
                ProtocolEvent::UserGetSessionDiagnostics {
                    request_id,
                    session_id,
                } => {
                    new_effects.extend(self.connection.session_diagnostics(session_id, request_id));
                }
                ProtocolEvent::UserGetStreamDiagnostics {
                    request_id,
                    stream_id,
                } => {
                    if let Some(session_id) = self.connection.stream_map.get(&stream_id) {
                        new_effects.extend(self.connection.stream_diagnostics(
                            *session_id,
                            stream_id,
                            request_id,
                        ));
                    } else {
                        new_effects.push(Effect::NotifyRequestFailed {
                            request_id,
                            source: ErrorSource::Stream,
                            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                            reason: "Stream not associated with any session".to_owned(),
                        });
                    }
                }
                ProtocolEvent::UserGrantDataCredit {
                    request_id,
                    session_id,
                    max_data,
                } => {
                    new_effects.extend(
                        self.connection
                            .grant_data_credit(session_id, request_id, max_data),
                    );
                }
                ProtocolEvent::UserGrantStreamsCredit {
                    request_id,
                    session_id,
                    is_unidirectional,
                    max_streams,
                } => {
                    new_effects.extend(self.connection.grant_streams_credit(
                        session_id,
                        request_id,
                        is_unidirectional,
                        max_streams,
                    ));
                }
                ProtocolEvent::UserRejectSession {
                    request_id,
                    session_id,
                    status_code,
                } => {
                    new_effects.extend(self.connection.reject_session(
                        session_id,
                        request_id,
                        status_code,
                        now,
                    ));
                }
                ProtocolEvent::UserResetStream {
                    request_id,
                    stream_id,
                    error_code,
                } => {
                    if let Some(session_id) = self.connection.stream_map.get(&stream_id) {
                        new_effects.extend(self.connection.reset_stream(
                            *session_id,
                            stream_id,
                            request_id,
                            error_code,
                        ));
                    } else {
                        new_effects.push(Effect::NotifyRequestFailed {
                            request_id,
                            source: ErrorSource::Stream,
                            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                            reason: "Stream not found".to_owned(),
                        });
                    }
                }
                ProtocolEvent::UserSendDatagram {
                    request_id,
                    session_id,
                    data,
                } => {
                    new_effects.extend(self.connection.send_datagram(session_id, request_id, data));
                }
                ProtocolEvent::UserSendStreamData {
                    request_id,
                    stream_id,
                    data,
                    end_stream,
                } => {
                    if let Some(session_id) = self.connection.stream_map.get(&stream_id) {
                        new_effects.extend(self.connection.send_stream_data(
                            *session_id,
                            stream_id,
                            request_id,
                            data,
                            end_stream,
                        ));
                    } else {
                        new_effects.push(Effect::NotifyRequestFailed {
                            request_id,
                            source: ErrorSource::Stream,
                            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                            reason: "Stream not found".to_owned(),
                        });
                    }
                }
                ProtocolEvent::UserStopSending {
                    request_id,
                    stream_id,
                    error_code,
                } => {
                    if let Some(session_id) = self.connection.stream_map.get(&stream_id) {
                        new_effects.extend(self.connection.stop_stream(
                            *session_id,
                            stream_id,
                            request_id,
                            error_code,
                        ));
                    } else {
                        new_effects.push(Effect::NotifyRequestFailed {
                            request_id,
                            source: ErrorSource::Stream,
                            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                            reason: "Stream not found".to_owned(),
                        });
                    }
                }
                ProtocolEvent::UserStreamRead {
                    request_id,
                    stream_id,
                    max_bytes,
                } => {
                    if let Some(session_id) = self.connection.stream_map.get(&stream_id) {
                        new_effects.extend(self.connection.stream_read(
                            *session_id,
                            stream_id,
                            request_id,
                            max_bytes,
                        ));
                    } else {
                        new_effects.push(Effect::NotifyRequestFailed {
                            request_id,
                            source: ErrorSource::Stream,
                            error_code: Some(ERR_LIB_STREAM_STATE_ERROR),
                            reason: "Stream not found".to_owned(),
                        });
                    }
                }
                ProtocolEvent::ConnectionClose {
                    request_id,
                    error_code,
                    reason,
                } => {
                    new_effects.extend(self.connection.close(request_id, error_code, reason, now));
                    let fail_reason = "Connection closed by application".to_owned();
                    new_effects.extend(self.fail_pending_user_actions(
                        Some(ERR_LIB_CONNECTION_STATE_ERROR),
                        &fail_reason,
                    ));
                }
            }

            all_effects.extend(new_effects);

            if re_queue_pending_actions && !self.pending_user_actions.is_empty() {
                debug!(
                    "Connection is ready, re-queueing {} pending user actions.",
                    self.pending_user_actions.len()
                );
                while let Some(pending_event) = self.pending_user_actions.pop_back() {
                    events_to_process.push_front(pending_event);
                }
            }
        }

        all_effects
    }

    // HTTP/3 transport initialization.
    pub(crate) fn initialize_h3_transport(
        &mut self,
        control_id: StreamId,
        encoder_id: StreamId,
        decoder_id: StreamId,
    ) -> Result<Vec<Effect>, WebTransportError> {
        self.h3
            .set_local_stream_ids(control_id, encoder_id, decoder_id)?;

        let settings_bytes = match self.h3.initialize_settings() {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to encode SETTINGS frame: {e:?}");
                return Ok(vec![Effect::CloseQuicConnection {
                    error_code: ERR_H3_INTERNAL_ERROR,
                    reason: Some("Failed to encode settings".to_owned()),
                }]);
            }
        };

        let mut control_data = BytesMut::new();
        write_varint(&mut control_data, H3_STREAM_TYPE_CONTROL).map_err(|_e| {
            WebTransportError::Protocol(
                Some(ERR_LIB_INTERNAL_ERROR),
                "Failed to encode control stream type".to_owned(),
            )
        })?;
        control_data.extend_from_slice(&settings_bytes);

        let mut encoder_data = BytesMut::new();
        write_varint(&mut encoder_data, H3_STREAM_TYPE_QPACK_ENCODER).map_err(|_e| {
            WebTransportError::Protocol(
                Some(ERR_LIB_INTERNAL_ERROR),
                "Failed to encode encoder stream type".to_owned(),
            )
        })?;

        let mut decoder_data = BytesMut::new();
        write_varint(&mut decoder_data, H3_STREAM_TYPE_QPACK_DECODER).map_err(|_e| {
            WebTransportError::Protocol(
                Some(ERR_LIB_INTERNAL_ERROR),
                "Failed to encode decoder stream type".to_owned(),
            )
        })?;

        debug!("Set infrastructure stream type Control for stream_id={control_id}");
        debug!("Set infrastructure stream type QPACK Encoder for stream_id={encoder_id}");
        debug!("Set infrastructure stream type QPACK Decoder for stream_id={decoder_id}");

        let effects = vec![
            Effect::SendQuicData {
                stream_id: control_id,
                data: control_data.freeze(),
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: encoder_id,
                data: encoder_data.freeze(),
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: decoder_id,
                data: decoder_data.freeze(),
                end_stream: false,
            },
        ];

        Ok(effects)
    }

    // Pending user actions failure handling.
    fn fail_pending_user_actions(
        &mut self,
        error_code: Option<ErrorCode>,
        reason: &str,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        while let Some(action) = self.pending_user_actions.pop_front() {
            let req_id = match action {
                ProtocolEvent::UserCreateSession { request_id, .. }
                | ProtocolEvent::UserCreateStream { request_id, .. } => Some(request_id),
                _ => None,
            };

            if let Some(id) = req_id {
                effects.push(Effect::NotifyRequestFailed {
                    request_id: id,
                    source: ErrorSource::Connection,
                    error_code,
                    reason: reason.to_owned(),
                });
            }
        }
        effects
    }
}

#[cfg(test)]
mod tests;
