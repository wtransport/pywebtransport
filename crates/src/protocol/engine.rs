//! WebTransport engine orchestrator.

use std::collections::VecDeque;

use bytes::Bytes;
use tracing::debug;

use crate::common::constants::{ERR_H3_NO_ERROR, ERR_LIB_CONNECTION_STATE_ERROR};
use crate::common::error::WebTransportError;
use crate::common::types::{
    ConnectionHandle, ErrorCode, ErrorSource, Headers, RequestId, SessionId, StreamId,
};
use crate::protocol::connection::{Connection, ConnectionParameters, SessionRequest};
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::h3::{H3, H3Parameters};

#[derive(Clone, Copy, Debug)]
pub(crate) struct EngineParameters {
    pub(crate) early_event_ttl: f64,
    pub(crate) initial_max_data: u64,
    pub(crate) initial_max_streams_bidi: u64,
    pub(crate) initial_max_streams_uni: u64,
    pub(crate) max_capsule_size: u64,
    pub(crate) max_connection_pending_events: u64,
    pub(crate) max_field_section_size: u64,
    pub(crate) max_pending_capsules: u64,
    pub(crate) max_pending_datagrams: u64,
    pub(crate) max_pending_streams: u64,
    pub(crate) max_session_pending_events: u64,
    pub(crate) max_sessions: u64,
    pub(crate) max_write_buffer_size: u64,
    pub(crate) session_receive_window: u64,
}

pub(crate) struct WebTransportEngine {
    connection: Connection,
    h3: H3,
    pending_user_events: VecDeque<ProtocolEvent>,
}

impl WebTransportEngine {
    pub(crate) fn new(
        handle: ConnectionHandle,
        request_id: Option<RequestId>,
        is_client: bool,
        parameters: EngineParameters,
    ) -> Self {
        let connection = Connection::new(
            handle,
            request_id,
            is_client,
            ConnectionParameters {
                early_event_ttl: parameters.early_event_ttl,
                initial_max_data: parameters.initial_max_data,
                initial_max_streams_bidi: parameters.initial_max_streams_bidi,
                initial_max_streams_uni: parameters.initial_max_streams_uni,
                max_connection_pending_events: parameters.max_connection_pending_events,
                max_pending_capsules: parameters.max_pending_capsules,
                max_pending_datagrams: parameters.max_pending_datagrams,
                max_pending_streams: parameters.max_pending_streams,
                max_session_pending_events: parameters.max_session_pending_events,
                max_sessions: parameters.max_sessions,
                max_write_buffer_size: parameters.max_write_buffer_size,
                session_receive_window: parameters.session_receive_window,
            },
        );

        let h3 = H3::new(
            is_client,
            H3Parameters {
                initial_max_data: parameters.initial_max_data,
                initial_max_streams_bidi: parameters.initial_max_streams_bidi,
                initial_max_streams_uni: parameters.initial_max_streams_uni,
                max_capsule_size: parameters.max_capsule_size,
                max_field_section_size: parameters.max_field_section_size,
            },
        );

        Self {
            connection,
            h3,
            pending_user_events: VecDeque::new(),
        }
    }

    pub(crate) fn encode_capsule(
        stream_id: StreamId,
        capsule_type: u64,
        capsule_value: Bytes,
        end_stream: bool,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let chunks = H3::encode_capsule(stream_id, capsule_type, capsule_value)?;
        let mut effects = Vec::with_capacity(chunks.len());
        let last_index = chunks.len().saturating_sub(1);

        for (index, chunk) in chunks.into_iter().enumerate() {
            effects.push(Effect::SendQuicData {
                stream_id,
                data: chunk,
                end_stream: end_stream && index == last_index,
            });
        }

        Ok(effects)
    }

    pub(crate) fn encode_connect_request(
        &mut self,
        stream_id: StreamId,
        authority: String,
        path: String,
        headers: &Headers,
    ) -> Result<Vec<Effect>, WebTransportError> {
        self.h3
            .encode_connect_request(stream_id, authority, path, headers)
    }

    pub(crate) fn encode_datagram(
        stream_id: StreamId,
        data: Bytes,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let mut parts = H3::encode_datagram(stream_id, data)?.into_iter();
        let header = parts.next().unwrap_or_default();
        let payload = parts.next().unwrap_or_default();

        Ok(vec![Effect::SendQuicDatagram { header, payload }])
    }

    pub(crate) fn encode_goaway(&mut self, identifier: u64) -> Vec<Effect> {
        if let Some(stream_id) = self.h3.local_control_stream_id() {
            match H3::encode_goaway(identifier) {
                Ok(data) => {
                    vec![Effect::SendQuicData {
                        stream_id,
                        data,
                        end_stream: false,
                    }]
                }
                Err(e) => {
                    debug!("h3_goaway encode failed stream_id={stream_id} err={e:?}");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    }

    pub(crate) fn encode_headers(
        &mut self,
        stream_id: StreamId,
        headers: &Headers,
        end_stream: bool,
    ) -> Result<Vec<Effect>, WebTransportError> {
        self.h3.encode_headers(stream_id, headers, end_stream)
    }

    pub(crate) fn encode_stream_header(
        stream_id: StreamId,
        session_id: SessionId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        H3::encode_stream_header(stream_id, session_id, is_unidirectional)
    }

    pub(crate) fn handle_event(&mut self, event: ProtocolEvent, now: f64) -> Vec<Effect> {
        let mut all_effects = Vec::new();
        let mut events_to_process = VecDeque::new();
        events_to_process.push_back(event);

        while let Some(current_event) = events_to_process.pop_front() {
            let mut new_effects = Vec::new();
            let mut replay_pending_user_events = false;

            match current_event {
                ProtocolEvent::H3CapsuleReceived {
                    stream_id,
                    capsule_type,
                    capsule_value,
                } => {
                    new_effects.extend(self.connection.recv_capsule(
                        stream_id,
                        capsule_type,
                        &capsule_value,
                        now,
                    ));
                    if self.connection.is_session_closed(stream_id) {
                        new_effects.extend(self.h3.stop_request_stream(stream_id));
                    }
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
                ProtocolEvent::H3HeadersReceived { stream_id, headers } => {
                    new_effects.extend(self.connection.recv_headers(stream_id, headers, now));
                    if self.connection.is_session_stream(stream_id) {
                        new_effects.extend(self.h3.resume_request_stream(stream_id));
                    } else {
                        new_effects.extend(self.h3.cleanup_stream(stream_id));
                    }
                }
                ProtocolEvent::H3SettingsReceived { settings } => {
                    new_effects.extend(self.connection.recv_settings(&settings, now));
                    if self.connection.is_client() && self.connection.is_connected() {
                        replay_pending_user_events = true;
                    }
                }
                ProtocolEvent::H3WebTransportStreamOpened {
                    session_id,
                    stream_id,
                } => {
                    new_effects.extend(
                        self.connection
                            .handle_stream_opened(session_id, stream_id, now),
                    );
                }
                ProtocolEvent::InternalBindH3ConnectStream {
                    request_id,
                    stream_id,
                } => {
                    new_effects.extend(self.connection.bind_session(stream_id, request_id));
                    new_effects.extend(self.h3.bind_connect_stream(stream_id));
                }
                ProtocolEvent::InternalBindH3LocalStreams {
                    control_stream_id,
                    encoder_stream_id,
                    decoder_stream_id,
                } => {
                    new_effects.extend(self.h3.bind_local_streams(
                        control_stream_id,
                        encoder_stream_id,
                        decoder_stream_id,
                    ));
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
                    self.connection.prune_resources();
                }
                ProtocolEvent::InternalCloseConnection => {
                    new_effects.extend(self.connection.close(None, ERR_H3_NO_ERROR, None, now));
                    new_effects.extend(self.fail_pending_user_events(
                        Some(ERR_LIB_CONNECTION_STATE_ERROR),
                        "wt_connection close",
                    ));
                }
                ProtocolEvent::InternalCompleteQuicData { stream_id } => {
                    new_effects.extend(self.connection.complete_stream_write(stream_id));
                }
                ProtocolEvent::InternalDeferH3ConnectStream { request_id } => {
                    new_effects.extend(self.connection.defer_session(request_id));
                }
                ProtocolEvent::InternalDeferQuicStream {
                    request_id,
                    session_id,
                    is_unidirectional,
                } => {
                    new_effects.extend(self.connection.defer_stream(
                        session_id,
                        request_id,
                        is_unidirectional,
                    ));
                }
                ProtocolEvent::InternalFailH3ConnectStream {
                    request_id,
                    error_code,
                    reason,
                } => {
                    new_effects
                        .extend(self.connection.fail_session(request_id, error_code, reason));
                }
                ProtocolEvent::InternalFailH3LocalStreams => {
                    new_effects.extend(H3::fail_local_streams());
                }
                ProtocolEvent::TransportConnectionTerminated { error_code, reason } => {
                    new_effects.extend(self.connection.handle_terminated(
                        error_code,
                        reason.clone(),
                        now,
                    ));
                    new_effects.extend(self.fail_pending_user_events(
                        Some(ERR_LIB_CONNECTION_STATE_ERROR),
                        "wt_connection abort",
                    ));
                }
                ProtocolEvent::TransportHandshakeCompleted => {
                    new_effects.extend(self.connection.handle_handshake_completed(now));

                    if self.connection.is_client() && self.connection.is_connected() {
                        replay_pending_user_events = true;
                    }
                }
                ProtocolEvent::TransportMaxStreamsReceived { is_unidirectional } => {
                    new_effects.extend(self.connection.recv_max_streams(is_unidirectional));
                }
                ProtocolEvent::TransportQuicParametersReceived {
                    peer_max_datagram_frame_size,
                } => {
                    new_effects.extend(
                        self.connection
                            .recv_transport_parameters(peer_max_datagram_frame_size),
                    );
                }
                ProtocolEvent::TransportResetStreamReceived {
                    stream_id,
                    error_code,
                } if self.connection.has_stream(stream_id) => {
                    new_effects.extend(
                        self.connection
                            .recv_stream_reset(stream_id, error_code, now),
                    );
                }
                ProtocolEvent::TransportResetStreamReceived {
                    stream_id,
                    error_code,
                } if self.connection.is_session_stream(stream_id) => {
                    new_effects.extend(self.h3.cleanup_stream(stream_id));
                    new_effects.extend(
                        self.connection
                            .recv_stream_reset(stream_id, error_code, now),
                    );
                }
                ProtocolEvent::TransportStopSendingReceived {
                    stream_id,
                    error_code,
                } if self.connection.has_stream(stream_id)
                    || self.connection.is_session_stream(stream_id) =>
                {
                    new_effects.extend(
                        self.connection
                            .recv_stop_sending(stream_id, error_code, now),
                    );
                }
                ProtocolEvent::TransportStreamDataAcknowledged { stream_id } => {
                    new_effects.extend(self.connection.handle_stream_data_acknowledged(stream_id));
                }
                ProtocolEvent::TransportStreamDataReceived {
                    stream_id,
                    data,
                    end_stream,
                } if self.connection.has_stream(stream_id) => {
                    new_effects.extend(
                        self.connection
                            .recv_stream_data(stream_id, data, end_stream, now),
                    );
                }
                ProtocolEvent::TransportDatagramReceived { .. }
                | ProtocolEvent::TransportResetStreamReceived { .. }
                | ProtocolEvent::TransportStopSendingReceived { .. }
                | ProtocolEvent::TransportStreamDataReceived { .. }
                | ProtocolEvent::TransportStreamOpened { .. } => {
                    let (events, effects) = self
                        .h3
                        .handle_transport_event(&current_event, &self.connection);

                    new_effects.extend(effects);
                    for event in events.into_iter().rev() {
                        events_to_process.push_front(event);
                    }
                }
                ProtocolEvent::UserAcceptSession {
                    request_id,
                    session_id,
                    headers,
                    wt_protocol,
                } => {
                    new_effects.extend(self.connection.accept_session(
                        session_id,
                        request_id,
                        headers,
                        wt_protocol,
                        now,
                    ));
                }
                ProtocolEvent::UserCloseConnection {
                    request_id,
                    error_code,
                    reason,
                } => {
                    new_effects.extend(self.connection.close(
                        Some(request_id),
                        error_code,
                        reason,
                        now,
                    ));
                    new_effects.extend(self.fail_pending_user_events(
                        Some(ERR_LIB_CONNECTION_STATE_ERROR),
                        "wt_connection close",
                    ));
                }
                ProtocolEvent::UserCloseConnectionGracefully { request_id } => {
                    new_effects.extend(self.connection.close_gracefully(request_id));
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
                        self.pending_user_events
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
                            SessionRequest {
                                authority,
                                created_at: now,
                                headers,
                                optimistic: false,
                                path,
                                wt_available_protocols,
                            },
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
                        self.pending_user_events.push_back(
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
                            SessionRequest {
                                authority,
                                created_at: now,
                                headers,
                                optimistic: true,
                                path,
                                wt_available_protocols,
                            },
                        ));
                    }
                }
                ProtocolEvent::UserCreateStream {
                    request_id,
                    session_id,
                    is_unidirectional,
                } => {
                    if self.connection.is_client() && self.connection.is_pre_connected() {
                        self.pending_user_events
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
                ProtocolEvent::UserDiagnoseConnection { request_id } => {
                    new_effects.extend(self.connection.diagnose(request_id));
                }
                ProtocolEvent::UserDiagnoseSession {
                    request_id,
                    session_id,
                } => {
                    new_effects.extend(self.connection.diagnose_session(session_id, request_id));
                }
                ProtocolEvent::UserDiagnoseStream {
                    request_id,
                    stream_id,
                } => {
                    new_effects.extend(self.connection.diagnose_stream(stream_id, request_id));
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
                ProtocolEvent::UserReadDatagram {
                    request_id,
                    session_id,
                } => {
                    new_effects.extend(self.connection.read_datagram(session_id, request_id));
                }
                ProtocolEvent::UserReadStream {
                    request_id,
                    stream_id,
                    max_size,
                } => {
                    new_effects
                        .extend(self.connection.read_stream(stream_id, request_id, max_size));
                }
                ProtocolEvent::UserRejectSession {
                    request_id,
                    session_id,
                    headers,
                    status_code,
                } => {
                    new_effects.extend(self.connection.reject_session(
                        session_id,
                        request_id,
                        headers,
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
                            .reset_stream(stream_id, request_id, error_code),
                    );
                }
                ProtocolEvent::UserSendDatagram {
                    request_id,
                    session_id,
                    data,
                } => {
                    new_effects.extend(self.connection.send_datagram(session_id, request_id, data));
                }
                ProtocolEvent::UserStopSending {
                    request_id,
                    stream_id,
                    error_code,
                } => {
                    new_effects.extend(
                        self.connection
                            .stop_stream(stream_id, request_id, error_code),
                    );
                }
                ProtocolEvent::UserWriteStream {
                    request_id,
                    stream_id,
                    data,
                    end_stream,
                } => {
                    new_effects.extend(
                        self.connection
                            .write_stream(stream_id, request_id, data, end_stream),
                    );
                }
            }

            all_effects.extend(new_effects);

            if replay_pending_user_events && !self.pending_user_events.is_empty() {
                while let Some(pending_event) = self.pending_user_events.pop_back() {
                    events_to_process.push_front(pending_event);
                }
            }
        }

        all_effects
    }

    fn fail_pending_user_events(
        &mut self,
        error_code: Option<ErrorCode>,
        reason: &'static str,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        while let Some(event) = self.pending_user_events.pop_front() {
            let request_id = match event {
                ProtocolEvent::UserCreateSession { request_id, .. }
                | ProtocolEvent::UserCreateSessionOptimistic { request_id, .. }
                | ProtocolEvent::UserCreateStream { request_id, .. } => Some(request_id),
                _ => None,
            };

            if let Some(request_id) = request_id {
                effects.push(Effect::NotifyRequestFailed {
                    request_id,
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
