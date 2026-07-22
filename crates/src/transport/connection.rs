//! Single QUIC connection orchestrator bridging WebTransport engine and QUIC state machine.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use quinn_proto::{
    Connection as QuinnConnection, ConnectionEvent, Dir as QuinnDir, EndpointEvent,
    Event as QuinnEvent, ReadError, StreamEvent, StreamId as QuinnStreamId, Transmit, VarInt,
    WriteError,
};
use rustc_hash::FxHashMap;
use tracing::debug;

use crate::common::constants::{
    ERR_H3_INTERNAL_ERROR, ERR_H3_STREAM_CREATION_ERROR, ERR_LIB_INTERNAL_ERROR,
    UDP_TRANSMIT_BATCH_CAPACITY,
};
use crate::common::types::ErrorSource;
use crate::protocol::engine::WebTransportEngine;
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};

// Complete state machine for a single WebTransport connection.
pub(super) struct TransportConnection {
    early_event_ttl: Option<Duration>,
    engine: WebTransportEngine,
    gc_interval: Option<Duration>,
    next_early_event_time: Option<Instant>,
    next_gc_time: Option<Instant>,
    pending_bidi_stream_requests: VecDeque<Effect>,
    pending_effects: VecDeque<Effect>,
    pending_session_requests: VecDeque<Effect>,
    pending_uni_stream_requests: VecDeque<Effect>,
    quic: QuinnConnection,
    send_buffers: FxHashMap<QuinnStreamId, SendBuffer>,
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
            early_event_ttl,
            engine,
            gc_interval,
            next_early_event_time: early_event_ttl.map(|interval| now_instant + interval),
            next_gc_time: gc_interval.map(|interval| now_instant + interval),
            pending_bidi_stream_requests: VecDeque::new(),
            pending_effects: VecDeque::new(),
            pending_session_requests: VecDeque::new(),
            pending_uni_stream_requests: VecDeque::new(),
            quic,
            send_buffers: FxHashMap::default(),
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
    pub(super) fn handle_timeout(&mut self, now: f64, now_instant: Instant) {
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
        workspace: &mut Vec<u8>,
        now_instant: Instant,
    ) -> Option<Transmit> {
        self.quic
            .poll_transmit(now_instant, UDP_TRANSMIT_BATCH_CAPACITY, workspace)
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

    // Evaluates protocol effects through a flattened trampoline loop.
    fn dispatch_effects(&mut self, initial_effects: Vec<Effect>, now: f64, now_instant: Instant) {
        let mut event_queue = VecDeque::new();
        self.process_effects(initial_effects, &mut event_queue, now_instant);

        while let Some(event) = event_queue.pop_front() {
            let effects = self.engine.handle_event(event, now);
            self.process_effects(effects, &mut event_queue, now_instant);
        }
    }

    // Internal dispatcher for protocol events.
    fn dispatch_protocol_event(&mut self, event: ProtocolEvent, now: f64, now_instant: Instant) {
        let effects = self.engine.handle_event(event, now);

        self.dispatch_effects(effects, now, now_instant);
    }

    // Flushes pending bytes for a flow-controlled QUIC stream.
    fn flush_stream(&mut self, stream_id: QuinnStreamId) {
        let mut should_remove = false;

        if let Some(buf) = self.send_buffers.get_mut(&stream_id) {
            let mut stream = self.quic.send_stream(stream_id);

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
                        should_remove = true;

                        break;
                    }
                }
            }

            if buf.chunks.is_empty() && buf.finished {
                stream.finish().ok();
                should_remove = true;
            }
        }

        if should_remove {
            self.send_buffers.remove(&stream_id);
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
                                peer_max_datagram_frame_size: max_size as u64,
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
                            Ok(effects) => self.dispatch_effects(effects, now, now_instant),
                            Err(e) => {
                                debug!("h3_stream open failed err={e:?}");
                                self.quic.close(
                                    now_instant,
                                    VarInt::try_from(ERR_H3_INTERNAL_ERROR)
                                        .unwrap_or(VarInt::from(0u32)),
                                    Bytes::from_static(b"h3_stream open failed"),
                                );
                            }
                        }
                    } else {
                        debug!("quic_stream create failed");
                        self.quic.close(
                            now_instant,
                            VarInt::try_from(ERR_H3_STREAM_CREATION_ERROR)
                                .unwrap_or(VarInt::from(0u32)),
                            Bytes::from_static(b"quic_stream create failed"),
                        );
                    }
                }
                QuinnEvent::ConnectionLost { reason } => {
                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportConnectionTerminated {
                            error_code: 0,
                            reason: reason.to_string().into(),
                        },
                        now,
                        now_instant,
                    );
                }
                QuinnEvent::DatagramReceived => {
                    self.read_datagrams(now, now_instant);
                }
                QuinnEvent::Stream(StreamEvent::Available { dir }) => {
                    let mut effects_to_retry = Vec::new();

                    if dir == QuinnDir::Bi {
                        while let Some(effect) = self.pending_session_requests.pop_front() {
                            effects_to_retry.push(effect);
                        }
                        while let Some(effect) = self.pending_bidi_stream_requests.pop_front() {
                            effects_to_retry.push(effect);
                        }
                    } else {
                        while let Some(effect) = self.pending_uni_stream_requests.pop_front() {
                            effects_to_retry.push(effect);
                        }
                    }

                    if !effects_to_retry.is_empty() {
                        self.dispatch_effects(effects_to_retry, now, now_instant);
                    }
                }
                QuinnEvent::Stream(StreamEvent::Opened { dir }) => {
                    while let Some(stream_id) = self.quic.streams().accept(dir) {
                        self.read_stream_data(stream_id, now, now_instant);
                    }
                }
                QuinnEvent::Stream(StreamEvent::Readable { id: stream_id }) => {
                    self.read_stream_data(stream_id, now, now_instant);
                }
                QuinnEvent::Stream(StreamEvent::Stopped {
                    id: stream_id,
                    error_code,
                }) => {
                    self.send_buffers.remove(&stream_id);
                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportStopSendingReceived {
                            stream_id: u64::from(stream_id),
                            error_code: error_code.into_inner(),
                        },
                        now,
                        now_instant,
                    );

                    let effects = self.engine.cleanup_stream(u64::from(stream_id));
                    self.dispatch_effects(effects, now, now_instant);
                }
                QuinnEvent::Stream(StreamEvent::Writable { id: stream_id }) => {
                    self.flush_stream(stream_id);
                }
                _ => {}
            }
        }
    }

    // Executes internal operations or forwards application effects.
    fn process_effects(
        &mut self,
        effects: Vec<Effect>,
        local_event_queue: &mut VecDeque<ProtocolEvent>,
        now_instant: Instant,
    ) {
        for effect in effects {
            match effect {
                Effect::CloseQuicConnection { error_code, reason } => {
                    let reason = match reason {
                        Some(Cow::Borrowed(s)) => Bytes::from_static(s.as_bytes()),
                        Some(Cow::Owned(s)) => Bytes::from(s),
                        None => Bytes::new(),
                    };
                    let error_code = VarInt::try_from(error_code).unwrap_or(VarInt::from(0u32));

                    self.quic.close(now_instant, error_code, reason);
                }
                Effect::CreateH3Session {
                    request_id,
                    authority,
                    path,
                    headers,
                } => {
                    if let Some(q_id) = self.quic.streams().open(QuinnDir::Bi) {
                        let stream_id = u64::from(q_id);

                        match self
                            .engine
                            .encode_session_request(stream_id, authority, path, &headers)
                        {
                            Ok(h3_effects) => {
                                self.process_effects(h3_effects, local_event_queue, now_instant);

                                local_event_queue.push_back(ProtocolEvent::InternalBindH3Session {
                                    request_id,
                                    stream_id,
                                });
                            }
                            Err(e) => {
                                debug!("wt_session encode failed err={e:?}");
                                local_event_queue.push_back(ProtocolEvent::InternalFailH3Session {
                                    request_id,
                                    error_code: None,
                                    reason: e.to_string().into(),
                                });
                            }
                        }
                    } else {
                        self.pending_session_requests
                            .push_back(Effect::CreateH3Session {
                                request_id,
                                authority,
                                path,
                                headers,
                            });
                    }
                }
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

                    if let Some(q_id) = self.quic.streams().open(dir) {
                        let stream_id = u64::from(q_id);
                        let h3_effects = self.engine.encode_stream_creation(
                            stream_id,
                            session_id,
                            is_unidirectional,
                        );

                        self.process_effects(h3_effects, local_event_queue, now_instant);

                        local_event_queue.push_back(ProtocolEvent::InternalBindQuicStream {
                            request_id,
                            stream_id,
                            session_id,
                            is_unidirectional,
                        });
                    } else {
                        let pending_effect = Effect::CreateQuicStream {
                            request_id,
                            session_id,
                            is_unidirectional,
                        };
                        if is_unidirectional {
                            self.pending_uni_stream_requests.push_back(pending_effect);
                        } else {
                            self.pending_bidi_stream_requests.push_back(pending_effect);
                        }
                    }
                }
                Effect::ExportTlsKeyingMaterial {
                    request_id,
                    label,
                    context,
                    length,
                } => {
                    let mut buf = vec![0u8; length as usize];
                    match self.quic.crypto_session().export_keying_material(
                        &mut buf,
                        label.as_bytes(),
                        &context,
                    ) {
                        Ok(()) => {
                            self.pending_effects.push_back(Effect::NotifyRequestDone {
                                request_id,
                                result: RequestResult::KeyingMaterial(Bytes::from(buf)),
                            });
                        }
                        Err(e) => {
                            debug!("tls_keying_material resolve failed err={e:?}");
                            self.pending_effects.push_back(Effect::NotifyRequestFailed {
                                request_id,
                                source: ErrorSource::Session,
                                error_code: Some(ERR_LIB_INTERNAL_ERROR),
                                reason: "tls_keying_material resolve failed".into(),
                            });
                        }
                    }
                }
                Effect::ProcessProtocolEvent { event } => {
                    local_event_queue.push_back(*event);
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
                    match WebTransportEngine::encode_capsule(
                        stream_id,
                        capsule_type,
                        capsule_data,
                        end_stream,
                    ) {
                        Ok(h3_effects) => {
                            self.process_effects(h3_effects, local_event_queue, now_instant);
                        }
                        Err(e) => {
                            debug!("wt_capsule encode failed err={e:?}");
                        }
                    }
                }
                Effect::SendH3Datagram { stream_id, data } => {
                    match WebTransportEngine::encode_datagram(stream_id, data) {
                        Ok(h3_effects) => {
                            self.process_effects(h3_effects, local_event_queue, now_instant);
                        }
                        Err(e) => {
                            debug!("wt_datagram encode failed err={e:?}");
                        }
                    }
                }
                Effect::SendH3Goaway => {
                    let h3_effects = self.engine.encode_goaway();

                    self.process_effects(h3_effects, local_event_queue, now_instant);
                }
                Effect::SendH3Headers {
                    stream_id,
                    headers,
                    end_stream,
                } => match self.engine.encode_headers(stream_id, &headers, end_stream) {
                    Ok(h3_effects) => {
                        self.process_effects(h3_effects, local_event_queue, now_instant);
                    }
                    Err(e) => {
                        debug!("h3_headers encode failed err={e:?}");
                    }
                },
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
                Effect::SendQuicDatagram { header, payload } => {
                    let mut buf = BytesMut::with_capacity(header.len() + payload.len());
                    buf.put(header);
                    buf.put(payload);
                    if let Err(e) = self.quic.datagrams().send(buf.freeze(), false) {
                        debug!("quic_datagram send failed err={e:?}");
                    }
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
    fn read_stream_data(&mut self, stream_id: QuinnStreamId, now: f64, now_instant: Instant) {
        let mut chunks = Vec::new();
        let mut is_finished = false;
        let mut is_reset = None;

        {
            let mut recv = self.quic.recv_stream(stream_id);

            if let Ok(mut chunks_reader) = recv.read(false) {
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
                    stream_id: u64::from(stream_id),
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
                    stream_id: u64::from(stream_id),
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
                    stream_id: u64::from(stream_id),
                    error_code: code,
                },
                now,
                now_instant,
            );

            let effects = self.engine.cleanup_stream(u64::from(stream_id));
            self.dispatch_effects(effects, now, now_instant);
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
