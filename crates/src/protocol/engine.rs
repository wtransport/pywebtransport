//! Protocol engine orchestrator and event loop management.

use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use tracing::debug;

use crate::common::constants::{
    ERR_H3_INTERNAL_ERROR, ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_INTERNAL_ERROR,
    H3_STREAM_TYPE_CONTROL, H3_STREAM_TYPE_QPACK_DECODER, H3_STREAM_TYPE_QPACK_ENCODER,
    WT_UPGRADE_TOKEN,
};
use crate::common::error::WebTransportError;
use crate::common::types::{ConnectionHandle, ErrorCode, ErrorSource, Headers, StreamId};
use crate::protocol::connection::{Connection, ConnectionParams};
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::h3::{H3, H3Params};
use crate::protocol::utils::write_varint;

// Engine initialization constraints and thresholds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EngineParams {
    pub(crate) early_event_ttl: f64,
    pub(crate) flow_control_window: u64,
    pub(crate) initial_max_data: u64,
    pub(crate) initial_max_streams_bidi: u64,
    pub(crate) initial_max_streams_uni: u64,
    pub(crate) max_capsule_size: u64,
    pub(crate) max_field_section_size: u64,
    pub(crate) max_pending_capsules: u64,
    pub(crate) max_pending_datagrams: u64,
    pub(crate) max_pending_streams: u64,
    pub(crate) max_session_pending_events: u64,
    pub(crate) max_sessions: u64,
    pub(crate) max_stream_read_buffer_size: u64,
    pub(crate) max_stream_write_buffer_size: u64,
    pub(crate) max_total_pending_events: u64,
}

// Orchestrates the unified protocol state machine.
pub(crate) struct WebTransportEngine {
    connection: Connection,
    h3: H3,
    pending_user_actions: VecDeque<ProtocolEvent>,
}

impl WebTransportEngine {
    // Engine initialization with comprehensive configuration.
    pub(crate) fn new(handle: ConnectionHandle, is_client: bool, params: EngineParams) -> Self {
        let connection = Connection::new(
            handle,
            is_client,
            ConnectionParams {
                early_event_ttl: params.early_event_ttl,
                flow_control_window: params.flow_control_window,
                initial_max_data: params.initial_max_data,
                initial_max_streams_bidi: params.initial_max_streams_bidi,
                initial_max_streams_uni: params.initial_max_streams_uni,
                max_pending_capsules: params.max_pending_capsules,
                max_pending_datagrams: params.max_pending_datagrams,
                max_pending_streams: params.max_pending_streams,
                max_session_pending_events: params.max_session_pending_events,
                max_sessions: params.max_sessions,
                max_stream_read_buffer_size: params.max_stream_read_buffer_size,
                max_stream_write_buffer_size: params.max_stream_write_buffer_size,
                max_total_pending_events: params.max_total_pending_events,
            },
        );

        let h3 = H3::new(
            is_client,
            H3Params {
                initial_max_data: params.initial_max_data,
                initial_max_streams_bidi: params.initial_max_streams_bidi,
                initial_max_streams_uni: params.initial_max_streams_uni,
                max_capsule_size: params.max_capsule_size,
                max_field_section_size: params.max_field_section_size,
            },
        );

        Self {
            connection,
            h3,
            pending_user_actions: VecDeque::new(),
        }
    }

    // H3 stream state cleanup.
    pub(crate) fn cleanup_stream(&mut self, stream_id: StreamId) -> Vec<Effect> {
        self.h3.cleanup_stream(stream_id)
    }

    // Capsule encoding to H3 DATA frame.
    pub(crate) fn encode_capsule(
        stream_id: StreamId,
        capsule_type: u64,
        capsule_data: Bytes,
        end_stream: bool,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let chunks = H3::encode_capsule(stream_id, capsule_type, capsule_data)?;
        let mut effects = Vec::with_capacity(chunks.len());
        let last_idx = chunks.len().saturating_sub(1);

        for (i, chunk) in chunks.into_iter().enumerate() {
            effects.push(Effect::SendQuicData {
                stream_id,
                data: chunk,
                end_stream: if i == last_idx { end_stream } else { false },
            });
        }

        Ok(effects)
    }

    // Datagram encoding to H3 frame.
    pub(crate) fn encode_datagram(
        stream_id: StreamId,
        data: Bytes,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let mut parts = H3::encode_datagram(stream_id, data)?.into_iter();
        let header = parts.next().unwrap_or_default();
        let payload = parts.next().unwrap_or_default();

        Ok(vec![Effect::SendQuicDatagram { header, payload }])
    }

    // GOAWAY frame encoding.
    pub(crate) fn encode_goaway(&mut self) -> Vec<Effect> {
        if let Some(control_id) = self.h3.local_control_stream_id() {
            match H3::encode_goaway(0) {
                Ok(data) => {
                    vec![Effect::SendQuicData {
                        stream_id: control_id,
                        data,
                        end_stream: false,
                    }]
                }
                Err(e) => {
                    debug!("h3_goaway encode failed stream_id={control_id} err={e:?}");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
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
        authority: String,
        path: String,
        headers: &Headers,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let mut request_headers: Headers = vec![
            (
                Bytes::from_static(b":method"),
                Bytes::from_static(b"CONNECT"),
            ),
            (Bytes::from_static(b":scheme"), Bytes::from_static(b"https")),
            (Bytes::from_static(b":authority"), Bytes::from(authority)),
            (Bytes::from_static(b":path"), Bytes::from(path)),
            (
                Bytes::from_static(b":protocol"),
                Bytes::from_static(WT_UPGRADE_TOKEN),
            ),
        ];

        request_headers.extend_from_slice(headers);

        self.h3.encode_headers(stream_id, &request_headers, false)
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
                ProtocolEvent::H3CapsuleReceived {
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
                ProtocolEvent::H3ConnectStreamClosed { stream_id } => {
                    new_effects.extend(self.connection.recv_connect_close(stream_id, now));
                }
                ProtocolEvent::H3DatagramReceived { stream_id, data } => {
                    new_effects.extend(self.connection.recv_datagram(stream_id, data, now));
                }
                ProtocolEvent::H3GoawayReceived => {
                    new_effects.extend(self.connection.recv_goaway());
                }
                ProtocolEvent::H3HeadersReceived {
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
                ProtocolEvent::H3SettingsReceived { settings } => {
                    new_effects.extend(self.connection.recv_settings(&settings, now));
                    if self.connection.is_client() && self.connection.is_connected() {
                        re_queue_pending_actions = true;
                    }
                }
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
                ProtocolEvent::TransportConnectionTerminated { error_code, reason } => {
                    new_effects.extend(self.connection.terminated(error_code, reason.clone(), now));
                    new_effects.extend(self.fail_pending_user_actions(
                        Some(ERR_LIB_CONNECTION_STATE_ERROR),
                        "wt_connection abort",
                    ));
                }
                ProtocolEvent::TransportDatagramFrameReceived { .. }
                | ProtocolEvent::TransportStreamDataReceived { .. } => {
                    let (h3_events, h3_effects) = self
                        .h3
                        .handle_transport_event(&current_event, &self.connection);

                    new_effects.extend(h3_effects);
                    for evt in h3_events.into_iter().rev() {
                        events_to_process.push_front(evt);
                    }
                }
                ProtocolEvent::TransportHandshakeCompleted => {
                    let fx = self.connection.handshake_completed(now);
                    new_effects.extend(fx);

                    if self.connection.is_client() && self.connection.is_connected() {
                        re_queue_pending_actions = true;
                    }
                }
                ProtocolEvent::TransportQuicParametersReceived {
                    peer_max_datagram_frame_size,
                } => {
                    new_effects.extend(
                        self.connection
                            .recv_transport_parameters(peer_max_datagram_frame_size),
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
                ProtocolEvent::UserAcceptSession {
                    request_id,
                    session_id,
                    wt_protocol,
                } => {
                    new_effects.extend(self.connection.accept_session(
                        session_id,
                        request_id,
                        wt_protocol,
                        now,
                    ));
                }
                ProtocolEvent::UserCloseConnection {
                    request_id,
                    error_code,
                    reason,
                } => {
                    new_effects.extend(self.connection.close(request_id, error_code, reason, now));
                    new_effects.extend(self.fail_pending_user_actions(
                        Some(ERR_LIB_CONNECTION_STATE_ERROR),
                        "wt_connection close",
                    ));
                }
                ProtocolEvent::UserCloseConnectionGracefully { request_id } => {
                    new_effects.extend(self.connection.graceful_close(request_id));
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
                ProtocolEvent::UserCreateSession {
                    request_id,
                    authority,
                    path,
                    headers,
                    wt_available_protocols,
                } => {
                    if self.connection.is_client() && self.connection.is_pre_connected() {
                        self.pending_user_actions
                            .push_back(ProtocolEvent::UserCreateSession {
                                request_id,
                                authority,
                                path,
                                headers,
                                wt_available_protocols,
                            });
                    } else {
                        new_effects.extend(self.connection.create_session(
                            request_id,
                            authority,
                            path,
                            headers,
                            wt_available_protocols,
                            false,
                            now,
                        ));
                    }
                }
                ProtocolEvent::UserCreateSessionOptimistic {
                    request_id,
                    authority,
                    path,
                    headers,
                    wt_available_protocols,
                } => {
                    if self.connection.is_client() && self.connection.is_pre_connected() {
                        self.pending_user_actions.push_back(
                            ProtocolEvent::UserCreateSessionOptimistic {
                                request_id,
                                authority,
                                path,
                                headers,
                                wt_available_protocols,
                            },
                        );
                    } else {
                        new_effects.extend(self.connection.create_session(
                            request_id,
                            authority,
                            path,
                            headers,
                            wt_available_protocols,
                            true,
                            now,
                        ));
                    }
                }
                ProtocolEvent::UserCreateStream {
                    request_id,
                    session_id,
                    is_unidirectional,
                } => {
                    if self.connection.is_client() && self.connection.is_pre_connected() {
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
                            session_id, request_id, &label, &context, length,
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
                    new_effects.extend(self.connection.stream_diagnostics(stream_id, request_id));
                }
                ProtocolEvent::UserReadStream {
                    request_id,
                    stream_id,
                    max_bytes,
                } => {
                    new_effects.extend(
                        self.connection
                            .stream_read(stream_id, request_id, max_bytes),
                    );
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
                    new_effects.extend(
                        self.connection
                            .reset_stream(stream_id, request_id, error_code, now),
                    );
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
                    new_effects.extend(
                        self.connection
                            .send_stream_data(stream_id, request_id, data, end_stream, now),
                    );
                }
                ProtocolEvent::UserStopSending {
                    request_id,
                    stream_id,
                    error_code,
                } => {
                    new_effects.extend(
                        self.connection
                            .stop_stream(stream_id, request_id, error_code, now),
                    );
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
            }

            all_effects.extend(new_effects);

            if re_queue_pending_actions && !self.pending_user_actions.is_empty() {
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
                debug!("h3_settings encode failed err={e:?}");
                return Ok(vec![Effect::CloseQuicConnection {
                    error_code: ERR_H3_INTERNAL_ERROR,
                    reason: Some("h3_settings encode failed".into()),
                }]);
            }
        };

        let mut control_data = BytesMut::new();
        write_varint(&mut control_data, H3_STREAM_TYPE_CONTROL).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
        control_data.extend_from_slice(&settings_bytes);

        let mut encoder_data = BytesMut::new();
        write_varint(&mut encoder_data, H3_STREAM_TYPE_QPACK_ENCODER).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        let mut decoder_data = BytesMut::new();
        write_varint(&mut decoder_data, H3_STREAM_TYPE_QPACK_DECODER).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

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
        reason: &'static str,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        while let Some(action) = self.pending_user_actions.pop_front() {
            let req_id = match action {
                ProtocolEvent::UserCreateSession { request_id, .. }
                | ProtocolEvent::UserCreateSessionOptimistic { request_id, .. }
                | ProtocolEvent::UserCreateStream { request_id, .. } => Some(request_id),
                _ => None,
            };

            if let Some(id) = req_id {
                effects.push(Effect::NotifyRequestFailed {
                    request_id: id,
                    source: ErrorSource::Connection,
                    error_code,
                    reason: reason.into(),
                });
            }
        }

        effects
    }
}

#[cfg(test)]
mod tests;
