//! QUIC endpoint manager.

use std::borrow::Cow;
use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use quinn_proto::{
    ClientConfig as QuicClientConfig, Connection as QuicConnection,
    ConnectionHandle as QuicConnectionHandle, DatagramEvent as QuicDatagramEvent,
    Endpoint as QuicEndpoint, Transmit as QuicTransmit,
};
use rustc_hash::FxHashMap;
use tracing::debug;

use crate::common::config::{RustBaseConfig, RustServerConfig};
use crate::common::constants::{
    ERR_LIB_CONNECTION_LIMIT_EXCEEDED, ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_INTERNAL_ERROR,
    QUIC_UDP_PAYLOAD_SIZE_LIMIT, UDP_TRANSMIT_BATCH_CAPACITY,
};
use crate::common::error::WebTransportError;
use crate::common::types::{ErrorCode, RequestId};
use crate::protocol::engine::{EngineParameters, WebTransportEngine};
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::transport::connection::TransportConnection;

pub(crate) struct TransportEndpoint {
    base_config: RustBaseConfig,
    client_config: Option<QuicClientConfig>,
    connections: FxHashMap<QuicConnectionHandle, TransportConnection>,
    endpoint: QuicEndpoint,
    handles_workspace: Vec<QuicConnectionHandle>,
    server_config: Option<RustServerConfig>,
    transmit_workspace: Vec<u8>,
}

impl TransportEndpoint {
    pub(crate) fn new_client(
        endpoint: QuicEndpoint,
        base_config: RustBaseConfig,
        client_config: QuicClientConfig,
    ) -> Result<Self, WebTransportError> {
        let workspace_capacity = usize::try_from(QUIC_UDP_PAYLOAD_SIZE_LIMIT)
            .map_err(|e| {
                debug!("quic_udp_payload_size_limit convert failed expected=usize err={e:?}");
                WebTransportError::Unknown(
                    Some(ERR_LIB_INTERNAL_ERROR),
                    "quic_udp_payload_size_limit convert failed".into(),
                )
            })?
            .saturating_mul(UDP_TRANSMIT_BATCH_CAPACITY);

        Ok(Self {
            base_config,
            client_config: Some(client_config),
            connections: FxHashMap::default(),
            endpoint,
            handles_workspace: Vec::new(),
            server_config: None,
            transmit_workspace: Vec::with_capacity(workspace_capacity),
        })
    }

    pub(crate) fn new_server(
        endpoint: QuicEndpoint,
        base_config: RustBaseConfig,
        server_config: RustServerConfig,
    ) -> Result<Self, WebTransportError> {
        let workspace_capacity = usize::try_from(QUIC_UDP_PAYLOAD_SIZE_LIMIT)
            .map_err(|e| {
                debug!("quic_udp_payload_size_limit convert failed expected=usize err={e:?}");
                WebTransportError::Unknown(
                    Some(ERR_LIB_INTERNAL_ERROR),
                    "quic_udp_payload_size_limit convert failed".into(),
                )
            })?
            .saturating_mul(UDP_TRANSMIT_BATCH_CAPACITY);

        Ok(Self {
            base_config,
            client_config: None,
            connections: FxHashMap::default(),
            endpoint,
            handles_workspace: Vec::new(),
            server_config: Some(server_config),
            transmit_workspace: Vec::with_capacity(workspace_capacity),
        })
    }

    pub(crate) fn connect(
        &mut self,
        request_id: RequestId,
        remote_address: SocketAddr,
        server_name: &str,
        now_instant: Instant,
    ) -> TransportEvent {
        let Some(config) = &self.client_config else {
            debug!("quic_endpoint validate failed request_id={request_id}");
            return TransportEvent::CommandFailed {
                request_id,
                error_code: Some(ERR_LIB_INTERNAL_ERROR),
                reason: "quic_endpoint validate failed".into(),
            };
        };

        if self.connections.len() as u64 >= self.base_config.max_connections {
            debug!(
                "quic_connection validate exceeded actual={} expected=max_connections request_id={request_id}",
                self.connections.len()
            );
            return TransportEvent::CommandFailed {
                request_id,
                error_code: Some(ERR_LIB_CONNECTION_LIMIT_EXCEEDED),
                reason: "quic_connection validate exceeded".into(),
            };
        }

        let (handle, quic_connection) =
            match self
                .endpoint
                .connect(now_instant, config.clone(), remote_address, server_name)
            {
                Ok(connection) => connection,
                Err(e) => {
                    debug!("quic_connection create failed request_id={request_id} err={e:?}");
                    return TransportEvent::CommandFailed {
                        request_id,
                        error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                        reason: "quic_connection create failed".into(),
                    };
                }
            };

        self.spawn_connection(handle, quic_connection, Some(request_id), true, now_instant)
    }

    pub(crate) fn handle_datagram(
        &mut self,
        remote_address: SocketAddr,
        local_address: Option<SocketAddr>,
        data: BytesMut,
        now: f64,
        now_instant: Instant,
    ) -> TransportEvent {
        match self.endpoint.handle(
            now_instant,
            remote_address,
            local_address.map(|address| address.ip()),
            None,
            data,
            &mut self.transmit_workspace,
        ) {
            Some(QuicDatagramEvent::ConnectionEvent(handle, event)) => {
                let mut is_drained = false;

                let Some(connection) = self.connections.get_mut(&handle) else {
                    return TransportEvent::Consumed;
                };

                connection.handle_quic_connection_event(event, now, now_instant);

                while let Some(endpoint_event) = connection.poll_endpoint_events() {
                    if endpoint_event.is_drained() {
                        is_drained = true;
                    }

                    self.endpoint.handle_event(handle, endpoint_event);
                }

                let effects = Self::collect_effects(connection);

                if is_drained {
                    self.connections.remove(&handle);
                }

                TransportEvent::ConnectionEffects { handle, effects }
            }
            Some(QuicDatagramEvent::NewConnection(incoming)) => {
                let Some(_) = &self.server_config else {
                    let transmit = self.endpoint.refuse(incoming, &mut self.transmit_workspace);

                    return TransportEvent::Transmit(transmit);
                };

                if self.connections.len() as u64 >= self.base_config.max_connections {
                    debug!(
                        "quic_connection validate exceeded actual={} expected=max_connections",
                        self.connections.len()
                    );
                    let transmit = self.endpoint.refuse(incoming, &mut self.transmit_workspace);

                    return TransportEvent::Transmit(transmit);
                }

                match self.endpoint.accept(
                    incoming,
                    now_instant,
                    &mut self.transmit_workspace,
                    None,
                ) {
                    Ok((handle, quic_connection)) => {
                        self.spawn_connection(handle, quic_connection, None, false, now_instant)
                    }
                    Err(_) => TransportEvent::Consumed,
                }
            }
            Some(QuicDatagramEvent::Response(transmit)) => TransportEvent::Transmit(transmit),
            None => TransportEvent::Consumed,
        }
    }

    pub(crate) fn handle_protocol_event(
        &mut self,
        handle: QuicConnectionHandle,
        request_id: RequestId,
        event: ProtocolEvent,
        now: f64,
        now_instant: Instant,
    ) -> TransportEvent {
        let mut is_drained = false;

        let Some(connection) = self.connections.get_mut(&handle) else {
            debug!(
                "quic_connection resolve failed connection_handle={} request_id={request_id}",
                handle.0
            );
            return TransportEvent::CommandFailed {
                request_id,
                error_code: Some(ERR_LIB_CONNECTION_STATE_ERROR),
                reason: "quic_connection resolve failed".into(),
            };
        };

        connection.handle_protocol_event(event, now, now_instant);

        while let Some(endpoint_event) = connection.poll_endpoint_events() {
            if endpoint_event.is_drained() {
                is_drained = true;
            }

            self.endpoint.handle_event(handle, endpoint_event);
        }

        let effects = Self::collect_effects(connection);

        if is_drained {
            self.connections.remove(&handle);
        }

        TransportEvent::ConnectionEffects { handle, effects }
    }

    pub(crate) fn handle_timeout(
        &mut self,
        now: f64,
        now_instant: Instant,
    ) -> Vec<(QuicConnectionHandle, Vec<Effect>)> {
        let mut results = Vec::new();
        let mut drained_handles = Vec::new();

        self.handles_workspace.clear();
        self.handles_workspace
            .extend(self.connections.keys().copied());
        self.handles_workspace.sort_unstable();

        for handle in &self.handles_workspace {
            let Some(connection) = self.connections.get_mut(handle) else {
                continue;
            };

            if connection
                .poll_timeout()
                .is_some_and(|timeout| now_instant >= timeout)
            {
                connection.handle_timeout(now, now_instant);

                let effects = Self::collect_effects(connection);

                if !effects.is_empty() {
                    results.push((*handle, effects));
                }
            }

            while let Some(endpoint_event) = connection.poll_endpoint_events() {
                if endpoint_event.is_drained() {
                    drained_handles.push(*handle);
                }

                self.endpoint.handle_event(*handle, endpoint_event);
            }
        }

        for handle in drained_handles {
            self.connections.remove(&handle);
        }

        results
    }

    pub(crate) fn poll_timeout(&mut self) -> Option<Instant> {
        let mut earliest: Option<Instant> = None;

        self.handles_workspace.clear();
        self.handles_workspace
            .extend(self.connections.keys().copied());
        self.handles_workspace.sort_unstable();

        for handle in &self.handles_workspace {
            if let Some(timeout) = self
                .connections
                .get_mut(handle)
                .and_then(TransportConnection::poll_timeout)
            {
                earliest = Some(
                    earliest.map_or(timeout, |earliest_timeout| earliest_timeout.min(timeout)),
                );
            }
        }

        earliest
    }

    pub(crate) fn poll_transmit(&mut self, now_instant: Instant) -> Option<QuicTransmit> {
        self.transmit_workspace.clear();

        self.handles_workspace.clear();
        self.handles_workspace
            .extend(self.connections.keys().copied());
        self.handles_workspace.sort_unstable();

        for handle in &self.handles_workspace {
            if let Some(connection) = self.connections.get_mut(handle)
                && let Some(transmit) =
                    connection.poll_transmit(&mut self.transmit_workspace, now_instant)
            {
                return Some(transmit);
            }
        }

        None
    }

    pub(crate) fn shutdown(
        &mut self,
        now: f64,
        now_instant: Instant,
    ) -> Vec<(QuicConnectionHandle, Vec<Effect>)> {
        let mut results = Vec::new();
        let mut drained_handles = Vec::new();

        self.handles_workspace.clear();
        self.handles_workspace
            .extend(self.connections.keys().copied());
        self.handles_workspace.sort_unstable();

        for handle in &self.handles_workspace {
            let Some(connection) = self.connections.get_mut(handle) else {
                continue;
            };

            connection.handle_protocol_event(
                ProtocolEvent::InternalCloseConnection,
                now,
                now_instant,
            );

            let effects = Self::collect_effects(connection);

            if !effects.is_empty() {
                results.push((*handle, effects));
            }

            while let Some(endpoint_event) = connection.poll_endpoint_events() {
                if endpoint_event.is_drained() {
                    drained_handles.push(*handle);
                }

                self.endpoint.handle_event(*handle, endpoint_event);
            }
        }

        for handle in drained_handles {
            self.connections.remove(&handle);
        }

        results
    }

    pub(crate) fn transmit_workspace(&self) -> &[u8] {
        &self.transmit_workspace
    }

    fn build_engine_parameters(config: &RustBaseConfig) -> EngineParameters {
        EngineParameters {
            early_event_ttl: config.pending_event_ttl.as_secs_f64(),
            initial_max_data: config.initial_max_data,
            initial_max_streams_bidi: config.initial_max_streams_bidi,
            initial_max_streams_uni: config.initial_max_streams_uni,
            max_capsule_size: config.max_capsule_size,
            max_connection_pending_events: config.max_connection_pending_events,
            max_field_section_size: config.max_field_section_size,
            max_pending_capsules: config.max_pending_capsules,
            max_pending_datagrams: config.max_pending_datagrams,
            max_pending_streams: config.max_pending_streams,
            max_session_pending_events: config.max_session_pending_events,
            max_sessions: config.max_sessions,
            max_write_buffer_size: config.max_write_buffer_size,
            session_receive_window: config.session_receive_window,
        }
    }

    fn collect_effects(connection: &mut TransportConnection) -> Vec<Effect> {
        let mut effects = Vec::new();

        while let Some(effect) = connection.poll_effects() {
            effects.push(effect);
        }

        effects
    }

    fn spawn_connection(
        &mut self,
        handle: QuicConnectionHandle,
        quic_connection: QuicConnection,
        request_id: Option<RequestId>,
        is_client: bool,
        now_instant: Instant,
    ) -> TransportEvent {
        let remote_address = quic_connection.remote_address();
        let base_config = &self.base_config;

        let engine = WebTransportEngine::new(
            handle.0 as u64,
            request_id,
            is_client,
            Self::build_engine_parameters(base_config),
        );

        let resource_cleanup_interval = if base_config.resource_cleanup_interval.is_zero() {
            None
        } else {
            Some(base_config.resource_cleanup_interval)
        };

        let early_event_ttl = if base_config.pending_event_ttl.is_zero() {
            None
        } else {
            Some(base_config.pending_event_ttl)
        };

        let mut transport_connection = TransportConnection::new(
            quic_connection,
            engine,
            resource_cleanup_interval,
            early_event_ttl,
            now_instant,
        );

        while let Some(endpoint_event) = transport_connection.poll_endpoint_events() {
            self.endpoint.handle_event(handle, endpoint_event);
        }

        let effects = Self::collect_effects(&mut transport_connection);

        self.connections.insert(handle, transport_connection);

        TransportEvent::ConnectionSpawned {
            handle,
            remote_address,
            effects,
        }
    }
}

#[derive(Debug)]
pub(crate) enum TransportEvent {
    CommandFailed {
        request_id: RequestId,
        error_code: Option<ErrorCode>,
        reason: Cow<'static, str>,
    },
    ConnectionEffects {
        handle: QuicConnectionHandle,
        effects: Vec<Effect>,
    },
    ConnectionSpawned {
        handle: QuicConnectionHandle,
        remote_address: SocketAddr,
        effects: Vec<Effect>,
    },
    Consumed,
    Transmit(QuicTransmit),
}

#[cfg(test)]
mod tests;
