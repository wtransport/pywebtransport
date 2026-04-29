//! Multiplexer and connection factory for the transport layer.

use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use quinn_proto::{
    ClientConfig, ConnectionHandle, DatagramEvent, Endpoint as QuinnEndpoint, Transmit,
};
use rustc_hash::FxHashMap;
use tracing::debug;

use crate::common::config::{RustBaseConfig, RustServerConfig};
use crate::common::constants;
use crate::common::error::WebTransportError;
use crate::protocol::engine::{EngineParams, WebTransportEngine};
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::transport::connection::TransportConnection;

// L4 UDP router and WebTransport connection multiplexer.
pub(crate) struct TransportEndpoint {
    base_config: RustBaseConfig,
    client_config: Option<ClientConfig>,
    connections: FxHashMap<ConnectionHandle, TransportConnection>,
    endpoint: QuinnEndpoint,
    server_config: Option<RustServerConfig>,
    transmit_workspace: Vec<u8>,
}

impl TransportEndpoint {
    // Creates a new endpoint for a client used for active outbound connections.
    pub(crate) fn new_client(
        endpoint: QuinnEndpoint,
        base_config: RustBaseConfig,
        client_config: ClientConfig,
    ) -> Result<Self, WebTransportError> {
        let workspace_capacity =
            usize::try_from(constants::UDP_MAX_DATAGRAM_SIZE).map_err(|e| {
                debug!("udp_max_datagram_size convert failed expected=usize err={e:?}");
                WebTransportError::Unknown(
                    Some(constants::ERR_LIB_INTERNAL_ERROR),
                    "udp_max_datagram_size convert failed".into(),
                )
            })?;

        Ok(Self {
            base_config,
            client_config: Some(client_config),
            connections: FxHashMap::default(),
            endpoint,
            server_config: None,
            transmit_workspace: Vec::with_capacity(workspace_capacity),
        })
    }

    // Creates a new endpoint for a server capable of accepting passive connections.
    pub(crate) fn new_server(
        endpoint: QuinnEndpoint,
        base_config: RustBaseConfig,
        server_config: RustServerConfig,
    ) -> Result<Self, WebTransportError> {
        let workspace_capacity =
            usize::try_from(constants::UDP_MAX_DATAGRAM_SIZE).map_err(|e| {
                debug!("udp_max_datagram_size convert failed expected=usize err={e:?}");
                WebTransportError::Unknown(
                    Some(constants::ERR_LIB_INTERNAL_ERROR),
                    "udp_max_datagram_size convert failed".into(),
                )
            })?;

        Ok(Self {
            base_config,
            client_config: None,
            connections: FxHashMap::default(),
            endpoint,
            server_config: Some(server_config),
            transmit_workspace: Vec::with_capacity(workspace_capacity),
        })
    }

    // Initiates an outbound QUIC connection and initializes the WebTransport engine.
    pub(crate) fn connect(
        &mut self,
        remote: SocketAddr,
        server_name: &str,
        now_instant: Instant,
    ) -> Result<(ConnectionHandle, SocketAddr), WebTransportError> {
        let Some(config) = &self.client_config else {
            return Err(WebTransportError::Unknown(
                Some(constants::ERR_LIB_INTERNAL_ERROR),
                "quic_endpoint validate failed".into(),
            ));
        };

        let (handle, quic_conn) = self
            .endpoint
            .connect(now_instant, config.clone(), remote, server_name)
            .map_err(|e| {
                debug!("quic_connection create failed err={e:?}");
                WebTransportError::Connection(
                    Some(constants::ERR_LIB_CONNECTION_STATE_ERROR),
                    "quic_connection create failed".into(),
                )
            })?;

        let remote_address = quic_conn.remote_address();
        let t_cfg = &self.base_config;

        let engine = WebTransportEngine::new(
            handle.0 as u64,
            true,
            EngineParams {
                early_event_ttl: t_cfg.pending_event_ttl.as_secs_f64(),
                flow_control_window: t_cfg.flow_control_window,
                flow_control_window_auto_scale_enabled: t_cfg
                    .flow_control_window_auto_scale_enabled,
                initial_max_data: t_cfg.initial_max_data,
                initial_max_streams_bidi: t_cfg.initial_max_streams_bidi,
                initial_max_streams_uni: t_cfg.initial_max_streams_uni,
                max_capsule_size: t_cfg.max_capsule_size,
                max_field_section_size: t_cfg.max_field_section_size,
                max_session_pending_events: t_cfg.max_session_pending_events,
                max_sessions: t_cfg.max_sessions,
                max_stream_read_buffer_size: t_cfg.max_stream_read_buffer_size,
                max_stream_write_buffer_size: t_cfg.max_stream_write_buffer_size,
                max_total_pending_events: t_cfg.max_total_pending_events,
            },
        )?;

        let gc_interval = if t_cfg.resource_cleanup_interval.is_zero() {
            None
        } else {
            Some(t_cfg.resource_cleanup_interval)
        };

        let early_event_ttl = if t_cfg.pending_event_ttl.is_zero() {
            None
        } else {
            Some(t_cfg.pending_event_ttl)
        };

        let mut transport_conn =
            TransportConnection::new(quic_conn, engine, gc_interval, early_event_ttl, now_instant);

        while let Some(endpoint_event) = transport_conn.poll_endpoint_events() {
            self.endpoint.handle_event(handle, endpoint_event);
        }

        self.connections.insert(handle, transport_conn);

        Ok((handle, remote_address))
    }

    // Processes an incoming UDP datagram and routes it to the appropriate state machine.
    pub(crate) fn handle_datagram(
        &mut self,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        data: BytesMut,
        now: f64,
        now_instant: Instant,
    ) -> TransportEvent {
        match self.endpoint.handle(
            now_instant,
            remote,
            local.map(|a| a.ip()),
            None,
            data,
            &mut self.transmit_workspace,
        ) {
            Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                let mut is_drained = false;

                let effects = if let Some(conn) = self.connections.get_mut(&handle) {
                    conn.handle_connection_event(event, now, now_instant);

                    while let Some(endpoint_event) = conn.poll_endpoint_events() {
                        if endpoint_event.is_drained() {
                            is_drained = true;
                        }

                        self.endpoint.handle_event(handle, endpoint_event);
                    }

                    Self::collect_effects(conn)
                } else {
                    return TransportEvent::Consumed;
                };

                if is_drained {
                    self.connections.remove(&handle);
                }

                TransportEvent::ConnectionEffects { handle, effects }
            }
            Some(DatagramEvent::NewConnection(incoming)) => {
                let Some(_) = &self.server_config else {
                    let transmit = self.endpoint.refuse(incoming, &mut self.transmit_workspace);

                    return TransportEvent::Transmit(transmit);
                };

                match self.endpoint.accept(
                    incoming,
                    now_instant,
                    &mut self.transmit_workspace,
                    None,
                ) {
                    Ok((handle, quic_conn)) => {
                        let remote_address = quic_conn.remote_address();
                        let t_cfg = &self.base_config;

                        let Ok(engine) = WebTransportEngine::new(
                            handle.0 as u64,
                            false,
                            EngineParams {
                                early_event_ttl: t_cfg.pending_event_ttl.as_secs_f64(),
                                flow_control_window: t_cfg.flow_control_window,
                                flow_control_window_auto_scale_enabled: t_cfg
                                    .flow_control_window_auto_scale_enabled,
                                initial_max_data: t_cfg.initial_max_data,
                                initial_max_streams_bidi: t_cfg.initial_max_streams_bidi,
                                initial_max_streams_uni: t_cfg.initial_max_streams_uni,
                                max_capsule_size: t_cfg.max_capsule_size,
                                max_field_section_size: t_cfg.max_field_section_size,
                                max_session_pending_events: t_cfg.max_session_pending_events,
                                max_sessions: t_cfg.max_sessions,
                                max_stream_read_buffer_size: t_cfg.max_stream_read_buffer_size,
                                max_stream_write_buffer_size: t_cfg.max_stream_write_buffer_size,
                                max_total_pending_events: t_cfg.max_total_pending_events,
                            },
                        ) else {
                            return TransportEvent::Consumed;
                        };

                        let gc_interval = if t_cfg.resource_cleanup_interval.is_zero() {
                            None
                        } else {
                            Some(t_cfg.resource_cleanup_interval)
                        };

                        let early_event_ttl = if t_cfg.pending_event_ttl.is_zero() {
                            None
                        } else {
                            Some(t_cfg.pending_event_ttl)
                        };

                        let mut transport_conn = TransportConnection::new(
                            quic_conn,
                            engine,
                            gc_interval,
                            early_event_ttl,
                            now_instant,
                        );

                        while let Some(endpoint_event) = transport_conn.poll_endpoint_events() {
                            self.endpoint.handle_event(handle, endpoint_event);
                        }

                        let effects = Self::collect_effects(&mut transport_conn);

                        self.connections.insert(handle, transport_conn);

                        TransportEvent::ConnectionSpawned {
                            handle,
                            effects,
                            remote_address,
                        }
                    }
                    Err(_) => TransportEvent::Consumed,
                }
            }
            Some(DatagramEvent::Response(transmit)) => TransportEvent::Transmit(transmit),
            None => TransportEvent::Consumed,
        }
    }

    // Evaluates timeouts across the entire multiplexer and triggers necessary logic.
    pub(crate) fn handle_timeout(
        &mut self,
        now: f64,
        now_instant: Instant,
    ) -> Vec<(ConnectionHandle, Vec<Effect>)> {
        let mut results = Vec::new();
        let mut drained_handles = Vec::new();
        let mut handles: Vec<ConnectionHandle> = self.connections.keys().copied().collect();

        handles.sort_unstable();

        for handle in handles {
            if let Some(conn) = self.connections.get_mut(&handle) {
                if conn.timeout().is_some_and(|t| now_instant >= t) {
                    conn.handle_timeout(now, now_instant);

                    let effects = Self::collect_effects(conn);

                    if !effects.is_empty() {
                        results.push((handle, effects));
                    }
                }

                while let Some(endpoint_event) = conn.poll_endpoint_events() {
                    if endpoint_event.is_drained() {
                        drained_handles.push(handle);
                    }

                    self.endpoint.handle_event(handle, endpoint_event);
                }
            }
        }

        for handle in drained_handles {
            self.connections.remove(&handle);
        }

        results
    }

    // Routes a user-level application event to the specified connection handle.
    pub(crate) fn handle_user_event(
        &mut self,
        handle: ConnectionHandle,
        event: ProtocolEvent,
        now: f64,
        now_instant: Instant,
    ) -> Option<TransportEvent> {
        let mut is_drained = false;

        let effects = if let Some(conn) = self.connections.get_mut(&handle) {
            conn.handle_user_event(event, now, now_instant);

            while let Some(endpoint_event) = conn.poll_endpoint_events() {
                if endpoint_event.is_drained() {
                    is_drained = true;
                }

                self.endpoint.handle_event(handle, endpoint_event);
            }

            Self::collect_effects(conn)
        } else {
            return None;
        };

        if is_drained {
            self.connections.remove(&handle);
        }

        if effects.is_empty() {
            None
        } else {
            Some(TransportEvent::ConnectionEffects { handle, effects })
        }
    }

    // Polls for any pending endpoint-level or connection-level transmission datagrams.
    pub(crate) fn poll_transmit(&mut self, now_instant: Instant) -> Option<Transmit> {
        self.transmit_workspace.clear();

        let mut handles: Vec<ConnectionHandle> = self.connections.keys().copied().collect();

        handles.sort_unstable();

        for handle in handles {
            if let Some(conn) = self.connections.get_mut(&handle)
                && let Some(transmit) =
                    conn.poll_transmit(&mut self.transmit_workspace, now_instant)
            {
                return Some(transmit);
            }
        }

        None
    }

    // Computes the earliest wakeup instant required by any entity within the endpoint.
    pub(crate) fn timeout(&mut self) -> Option<Instant> {
        let mut earliest: Option<Instant> = None;

        let mut handles: Vec<ConnectionHandle> = self.connections.keys().copied().collect();

        handles.sort_unstable();

        for handle in handles {
            if let Some(t) = self
                .connections
                .get_mut(&handle)
                .and_then(TransportConnection::timeout)
            {
                earliest = Some(earliest.map_or(t, |e| e.min(t)));
            }
        }

        earliest
    }

    // Provides a read-only view of the internal transmit workspace.
    pub(crate) fn transmit_workspace(&self) -> &[u8] {
        &self.transmit_workspace
    }

    // Drains and collects all pending effects materialized within a connection.
    fn collect_effects(conn: &mut TransportConnection) -> Vec<Effect> {
        let mut effects = Vec::new();

        while let Some(eff) = conn.poll_events() {
            effects.push(eff);
        }

        effects
    }
}

// Terminal actions produced by the multiplexer for the external I/O runtime.
#[derive(Debug)]
pub(crate) enum TransportEvent {
    ConnectionEffects {
        handle: ConnectionHandle,
        effects: Vec<Effect>,
    },
    ConnectionSpawned {
        handle: ConnectionHandle,
        effects: Vec<Effect>,
        remote_address: SocketAddr,
    },
    Consumed,
    Transmit(Transmit),
}

#[cfg(test)]
mod tests;
