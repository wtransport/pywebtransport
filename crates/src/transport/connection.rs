//! Single QUIC connection orchestrator bridging WebTransport engine and Quinn state machine.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use quinn_proto::{
    Connection as QuinnConnection, ConnectionEvent, Dir as QuinnDir, EndpointEvent,
    Event as QuinnEvent, ReadError, StreamEvent, StreamId as QuinnStreamId, Transmit, VarInt,
    WriteError,
};

use crate::protocol::engine::WebTransportEngine;
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::utils::find_header_str;

// Complete state machine for a single WebTransport connection.
pub(super) struct TransportConnection {
    engine: WebTransportEngine,
    quic: QuinnConnection,
    pending_effects: VecDeque<Effect>,
    send_buffers: HashMap<QuinnStreamId, SendBuffer>,
    early_event_ttl: Option<Duration>,
    gc_interval: Option<Duration>,
    next_early_event_time: Option<Instant>,
    next_gc_time: Option<Instant>,
}

impl TransportConnection {
    // Initializes a new connection orchestrator with associated timers.
    pub(super) fn new(
        quic: QuinnConnection,
        engine: WebTransportEngine,
        gc_interval: Option<Duration>,
        early_event_ttl: Option<Duration>,
        now_instant: Instant,
    ) -> Self {
        Self {
            engine,
            quic,
            pending_effects: VecDeque::new(),
            send_buffers: HashMap::new(),
            early_event_ttl,
            gc_interval,
            next_early_event_time: early_event_ttl.map(|interval| now_instant + interval),
            next_gc_time: gc_interval.map(|interval| now_instant + interval),
        }
    }

    // Processes incoming network events from the UDP multiplexer.
    pub(super) fn handle_connection_event(
        &mut self,
        event: ConnectionEvent,
        now: f64,
        now_instant: Instant,
    ) {
        self.quic.handle_event(event);

        self.poll_quic_events(now, now_instant);
    }

    // Handles network and application timeouts.
    pub(super) fn handle_timeout(&mut self, now_instant: Instant, now: f64) {
        if self
            .next_gc_time
            .is_some_and(|gc_time| now_instant >= gc_time)
        {
            self.dispatch_protocol_event(ProtocolEvent::InternalCleanupResources, now, now_instant);

            self.next_gc_time = self.gc_interval.map(|interval| now_instant + interval);
        }

        if self
            .next_early_event_time
            .is_some_and(|ttl_time| now_instant >= ttl_time)
        {
            self.dispatch_protocol_event(
                ProtocolEvent::InternalCleanupEarlyEvents,
                now,
                now_instant,
            );

            self.next_early_event_time =
                self.early_event_ttl.map(|interval| now_instant + interval);
        }

        self.quic.handle_timeout(now_instant);

        self.poll_quic_events(now, now_instant);
    }

    // Processes user-layer actions pushed from the application.
    pub(super) fn handle_user_event(
        &mut self,
        event: ProtocolEvent,
        now: f64,
        now_instant: Instant,
    ) {
        self.dispatch_protocol_event(event, now, now_instant);

        self.poll_quic_events(now, now_instant);
    }

    // Retrieves control-plane events destined for the endpoint multiplexer.
    pub(super) fn poll_endpoint_events(&mut self) -> Option<EndpointEvent> {
        self.quic.poll_endpoint_events()
    }

    // Retrieves pending application-level effects.
    pub(super) fn poll_events(&mut self) -> Option<Effect> {
        self.pending_effects.pop_front()
    }

    // Retrieves network transmission datagrams using the provided workspace buffer.
    pub(super) fn poll_transmit(
        &mut self,
        now_instant: Instant,
        workspace: &mut Vec<u8>,
    ) -> Option<Transmit> {
        self.quic.poll_transmit(now_instant, 1, workspace)
    }

    // Retrieves the current validated remote socket address of the QUIC connection.
    pub(super) fn remote_address(&self) -> SocketAddr {
        self.quic.remote_address()
    }

    // Polls the earliest timeout across network and application layers.
    pub(super) fn timeout(&mut self) -> Option<Instant> {
        let mut earliest = self.quic.poll_timeout();

        if let Some(gc_time) = self.next_gc_time {
            earliest = Some(earliest.map_or(gc_time, |t| t.min(gc_time)));
        }

        if let Some(ttl_time) = self.next_early_event_time {
            earliest = Some(earliest.map_or(ttl_time, |t| t.min(ttl_time)));
        }

        earliest
    }

    // Internal dispatcher for protocol events.
    fn dispatch_protocol_event(&mut self, event: ProtocolEvent, now: f64, now_instant: Instant) {
        let effects = self.engine.handle_event(event, now);

        self.process_effects(effects, now, now_instant);
    }

    // Flushes pending bytes for a flow-controlled QUIC stream.
    fn flush_stream(&mut self, id: QuinnStreamId) {
        let Some(buf) = self.send_buffers.get_mut(&id) else {
            return;
        };

        let mut stream = self.quic.send_stream(id);

        while let Some(chunk) = buf.chunks.front() {
            match stream.write(chunk) {
                Ok(written) => {
                    if written == chunk.len() {
                        buf.chunks.pop_front();
                    } else {
                        let Some(mut remaining) = buf.chunks.pop_front() else {
                            break;
                        };

                        remaining.advance(written);
                        buf.chunks.push_front(remaining);
                    }
                }
                Err(WriteError::Blocked) => {
                    break;
                }
                Err(WriteError::Stopped(_) | WriteError::ClosedStream) => {
                    buf.chunks.clear();

                    break;
                }
            }
        }

        if buf.chunks.is_empty() && buf.finished {
            stream.finish().ok();
        }
    }

    // Extracts and dispatches QUIC network events.
    fn poll_quic_events(&mut self, now: f64, now_instant: Instant) {
        while let Some(event) = self.quic.poll() {
            match event {
                QuinnEvent::Connected => {
                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportHandshakeCompleted,
                        now,
                        now_instant,
                    );

                    if let Some(max_size) = self.quic.datagrams().max_size() {
                        self.dispatch_protocol_event(
                            ProtocolEvent::TransportQuicParametersReceived {
                                remote_max_datagram_frame_size: max_size as u64,
                            },
                            now,
                            now_instant,
                        );
                    }

                    if let (Some(c_id), Some(e_id), Some(d_id)) = (
                        self.quic.streams().open(QuinnDir::Uni),
                        self.quic.streams().open(QuinnDir::Uni),
                        self.quic.streams().open(QuinnDir::Uni),
                    ) {
                        match self.engine.initialize_h3_transport(
                            u64::from(c_id),
                            u64::from(e_id),
                            u64::from(d_id),
                        ) {
                            Ok(effects) => self.process_effects(effects, now, now_instant),
                            Err(e) => tracing::error!("Failed to initialize H3 transport: {:?}", e),
                        }
                    } else {
                        tracing::error!(
                            "Failed to open H3 infrastructure streams. Peer might not have granted sufficient initial unidirectional streams."
                        );
                    }
                }
                QuinnEvent::ConnectionLost { reason } => {
                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportConnectionTerminated {
                            error_code: 0,
                            reason: reason.to_string(),
                        },
                        now,
                        now_instant,
                    );
                }
                QuinnEvent::DatagramReceived => {
                    self.read_datagrams(now, now_instant);
                }
                QuinnEvent::Stream(StreamEvent::Opened { dir }) => {
                    while let Some(stream_id) = self.quic.streams().accept(dir) {
                        self.read_stream_data(stream_id, now, now_instant);
                    }
                }
                QuinnEvent::Stream(StreamEvent::Readable { id }) => {
                    self.read_stream_data(id, now, now_instant);
                }
                QuinnEvent::Stream(StreamEvent::Stopped { id, error_code }) => {
                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportStopSendingReceived {
                            stream_id: u64::from(id),
                            error_code: error_code.into_inner(),
                        },
                        now,
                        now_instant,
                    );

                    self.engine.cleanup_stream(u64::from(id));
                }
                QuinnEvent::Stream(StreamEvent::Writable { id }) => {
                    self.flush_stream(id);
                }
                _ => {}
            }
        }
    }

    // Executes internal operations or forwards application effects.
    fn process_effects(&mut self, effects: Vec<Effect>, now: f64, now_instant: Instant) {
        for effect in effects {
            match effect {
                Effect::CloseQuicConnection { error_code, reason } => {
                    let reason_bytes = reason.unwrap_or_default().into_bytes();
                    let code = VarInt::try_from(error_code).unwrap_or(VarInt::from(0u32));

                    self.quic.close(now_instant, code, reason_bytes.into());
                }
                Effect::CreateH3Session {
                    request_id,
                    path,
                    headers,
                } => match self.quic.streams().open(QuinnDir::Bi) {
                    Some(q_id) => {
                        let stream_id = u64::from(q_id);
                        let authority = find_header_str(&headers, ":authority")
                            .unwrap_or_else(|| "localhost".to_owned());

                        match self
                            .engine
                            .encode_session_request(stream_id, path, authority, &headers)
                        {
                            Ok(h3_effects) => {
                                self.process_effects(h3_effects, now, now_instant);

                                self.dispatch_protocol_event(
                                    ProtocolEvent::InternalBindH3Session {
                                        request_id,
                                        stream_id,
                                    },
                                    now,
                                    now_instant,
                                );
                            }
                            Err(e) => {
                                self.dispatch_protocol_event(
                                    ProtocolEvent::InternalFailH3Session {
                                        request_id,
                                        error_code: None,
                                        reason: e.to_string(),
                                    },
                                    now,
                                    now_instant,
                                );
                            }
                        }
                    }
                    None => {
                        self.dispatch_protocol_event(
                            ProtocolEvent::InternalFailH3Session {
                                request_id,
                                error_code: None,
                                reason: "Failed to open QUIC stream".to_owned(),
                            },
                            now,
                            now_instant,
                        );
                    }
                },
                Effect::CreateQuicStream {
                    request_id,
                    session_id,
                    is_unidirectional,
                } => {
                    let dir = if is_unidirectional {
                        QuinnDir::Uni
                    } else {
                        QuinnDir::Bi
                    };

                    match self.quic.streams().open(dir) {
                        Some(q_id) => {
                            let stream_id = u64::from(q_id);
                            let h3_effects = self.engine.encode_stream_creation(
                                stream_id,
                                session_id,
                                is_unidirectional,
                            );

                            self.process_effects(h3_effects, now, now_instant);

                            self.dispatch_protocol_event(
                                ProtocolEvent::InternalBindQuicStream {
                                    request_id,
                                    stream_id,
                                    session_id,
                                    is_unidirectional,
                                },
                                now,
                                now_instant,
                            );
                        }
                        None => {
                            self.dispatch_protocol_event(
                                ProtocolEvent::InternalFailQuicStream {
                                    request_id,
                                    session_id,
                                    is_unidirectional,
                                    error_code: None,
                                    reason: "Failed to open QUIC stream".to_owned(),
                                },
                                now,
                                now_instant,
                            );
                        }
                    }
                }
                Effect::ProcessProtocolEvent { event } => {
                    self.dispatch_protocol_event(*event, now, now_instant);
                }
                Effect::ResetQuicStream {
                    stream_id,
                    error_code,
                } => {
                    let Ok(var_int) = VarInt::try_from(stream_id) else {
                        continue;
                    };

                    let q_id = QuinnStreamId::from(var_int);
                    let code = VarInt::try_from(error_code).unwrap_or(VarInt::from(0u32));
                    let mut stream = self.quic.send_stream(q_id);

                    stream.reset(code).ok();

                    self.send_buffers.remove(&q_id);
                }
                Effect::SendH3Capsule {
                    stream_id,
                    capsule_type,
                    capsule_data,
                    end_stream,
                } => {
                    if let Ok(h3_effects) = WebTransportEngine::encode_capsule(
                        stream_id,
                        capsule_type,
                        capsule_data,
                        end_stream,
                    ) {
                        self.process_effects(h3_effects, now, now_instant);
                    }
                }
                Effect::SendH3Datagram { stream_id, data } => {
                    if let Ok(h3_effects) = WebTransportEngine::encode_datagram(stream_id, &data) {
                        self.process_effects(h3_effects, now, now_instant);
                    }
                }
                Effect::SendH3Goaway => {
                    let h3_effects = self.engine.encode_goaway();

                    self.process_effects(h3_effects, now, now_instant);
                }
                Effect::SendH3Headers {
                    stream_id,
                    status,
                    end_stream,
                } => {
                    if let Ok(h3_effects) =
                        self.engine.encode_headers(stream_id, status, end_stream)
                    {
                        self.process_effects(h3_effects, now, now_instant);
                    }
                }
                Effect::SendQuicData {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    let Ok(var_int) = VarInt::try_from(stream_id) else {
                        continue;
                    };

                    let q_id = QuinnStreamId::from(var_int);
                    let buf = self.send_buffers.entry(q_id).or_default();

                    if !data.is_empty() {
                        buf.chunks.push_back(data);
                    }

                    if end_stream {
                        buf.finished = true;
                    }

                    self.flush_stream(q_id);
                }
                Effect::SendQuicDatagram { data } => {
                    let _transmit = self.quic.datagrams().send(data, false);
                }
                Effect::StopQuicStream {
                    stream_id,
                    error_code,
                } => {
                    let Ok(var_int) = VarInt::try_from(stream_id) else {
                        continue;
                    };

                    let q_id = QuinnStreamId::from(var_int);
                    let code = VarInt::try_from(error_code).unwrap_or(VarInt::from(0u32));
                    let mut stream = self.quic.recv_stream(q_id);

                    stream.stop(code).ok();
                }
                app_effect => {
                    self.pending_effects.push_back(app_effect);
                }
            }
        }
    }

    // Ingests incoming QUIC datagrams into the protocol engine.
    fn read_datagrams(&mut self, now: f64, now_instant: Instant) {
        let mut dgrams = Vec::new();

        while let Some(dgram) = self.quic.datagrams().recv() {
            dgrams.push(dgram);
        }

        for data in dgrams {
            self.dispatch_protocol_event(
                ProtocolEvent::TransportDatagramFrameReceived { data },
                now,
                now_instant,
            );
        }
    }

    // Reads incoming QUIC streams utilizing zero-copy architecture.
    fn read_stream_data(&mut self, id: QuinnStreamId, now: f64, now_instant: Instant) {
        let mut chunks = Vec::new();
        let mut is_finished = false;
        let mut is_reset = None;

        {
            let mut recv = self.quic.recv_stream(id);

            if let Ok(mut chunks_reader) = recv.read(true) {
                loop {
                    match chunks_reader.next(usize::MAX) {
                        Ok(Some(chunk)) => {
                            chunks.push(chunk.bytes);
                        }
                        Ok(None) => {
                            is_finished = true;

                            break;
                        }
                        Err(ReadError::Reset(error_code)) => {
                            is_reset = Some(error_code.into_inner());

                            break;
                        }
                        Err(ReadError::Blocked) => {
                            break;
                        }
                    }
                }

                let _transmit = chunks_reader.finalize();
            }
        }

        for chunk in chunks {
            self.dispatch_protocol_event(
                ProtocolEvent::TransportStreamDataReceived {
                    stream_id: u64::from(id),
                    data: chunk,
                    end_stream: false,
                },
                now,
                now_instant,
            );
        }

        if is_finished {
            self.dispatch_protocol_event(
                ProtocolEvent::TransportStreamDataReceived {
                    stream_id: u64::from(id),
                    data: Bytes::new(),
                    end_stream: true,
                },
                now,
                now_instant,
            );
        }

        if let Some(code) = is_reset {
            self.dispatch_protocol_event(
                ProtocolEvent::TransportStreamResetReceived {
                    stream_id: u64::from(id),
                    error_code: code,
                },
                now,
                now_instant,
            );

            self.engine.cleanup_stream(u64::from(id));
        }
    }
}

// Internal send buffer for flow-controlled QUIC streams.
#[derive(Default)]
struct SendBuffer {
    chunks: VecDeque<Bytes>,
    finished: bool,
}

#[cfg(test)]
mod tests;
