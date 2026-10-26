//! QUIC connection coordinator.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use quinn_proto::{
    Connection as QuicConnection, ConnectionError as QuicConnectionError,
    ConnectionEvent as QuicConnectionEvent, Dir as QuicDir, EndpointEvent as QuicEndpointEvent,
    Event as QuicEvent, ReadError as QuicReadError, StreamEvent as QuicStreamEvent,
    StreamId as QuicStreamId, Transmit as QuicTransmit, VarInt as QuicVarInt,
    WriteError as QuicWriteError,
};
use rustc_hash::FxHashMap;
use tracing::debug;

use crate::common::constants::{
    ERR_LIB_CONNECTION_STATE_ERROR, ERR_LIB_INTERNAL_ERROR, UDP_TRANSMIT_BATCH_CAPACITY,
};
use crate::common::types::{ErrorCode, ErrorSource};
use crate::protocol::engine::WebTransportEngine;
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};

pub(super) struct TransportConnection {
    connection: QuicConnection,
    early_event_ttl: Option<Duration>,
    engine: WebTransportEngine,
    next_early_event_time: Option<Instant>,
    next_resource_cleanup_time: Option<Instant>,
    pending_effects: VecDeque<Effect>,
    pending_receives: FxHashMap<QuicStreamId, u64>,
    resource_cleanup_interval: Option<Duration>,
    send_buffers: FxHashMap<QuicStreamId, SendBuffer>,
}

impl TransportConnection {
    pub(super) fn new(
        connection: QuicConnection,
        engine: WebTransportEngine,
        resource_cleanup_interval: Option<Duration>,
        early_event_ttl: Option<Duration>,
        now_instant: Instant,
    ) -> Self {
        Self {
            connection,
            early_event_ttl,
            engine,
            next_early_event_time: early_event_ttl.map(|interval| now_instant + interval),
            next_resource_cleanup_time: resource_cleanup_interval
                .map(|interval| now_instant + interval),
            pending_effects: VecDeque::new(),
            pending_receives: FxHashMap::default(),
            resource_cleanup_interval,
            send_buffers: FxHashMap::default(),
        }
    }

    pub(super) fn handle_protocol_event(
        &mut self,
        event: ProtocolEvent,
        now: f64,
        now_instant: Instant,
    ) {
        self.dispatch_protocol_event(event, now, now_instant);

        self.process_quic_events(now, now_instant);
    }

    pub(super) fn handle_quic_connection_event(
        &mut self,
        event: QuicConnectionEvent,
        now: f64,
        now_instant: Instant,
    ) {
        self.connection.handle_event(event);

        self.process_quic_events(now, now_instant);
    }

    pub(super) fn handle_timeout(&mut self, now: f64, now_instant: Instant) {
        if self
            .next_resource_cleanup_time
            .is_some_and(|resource_cleanup_time| now_instant >= resource_cleanup_time)
        {
            self.dispatch_protocol_event(ProtocolEvent::InternalCleanupResources, now, now_instant);

            self.next_resource_cleanup_time = self
                .resource_cleanup_interval
                .map(|interval| now_instant + interval);
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

        self.connection.handle_timeout(now_instant);

        self.process_quic_events(now, now_instant);
    }

    pub(super) fn poll_effects(&mut self) -> Option<Effect> {
        self.pending_effects.pop_front()
    }

    pub(super) fn poll_endpoint_events(&mut self) -> Option<QuicEndpointEvent> {
        self.connection.poll_endpoint_events()
    }

    pub(super) fn poll_timeout(&mut self) -> Option<Instant> {
        let mut earliest = self.connection.poll_timeout();

        if let Some(resource_cleanup_time) = self.next_resource_cleanup_time {
            earliest = Some(earliest.map_or(resource_cleanup_time, |earliest_timeout| {
                earliest_timeout.min(resource_cleanup_time)
            }));
        }

        if let Some(ttl_time) = self.next_early_event_time {
            earliest =
                Some(earliest.map_or(ttl_time, |earliest_timeout| earliest_timeout.min(ttl_time)));
        }

        earliest
    }

    pub(super) fn poll_transmit(
        &mut self,
        workspace: &mut Vec<u8>,
        now_instant: Instant,
    ) -> Option<QuicTransmit> {
        self.connection
            .poll_transmit(now_instant, UDP_TRANSMIT_BATCH_CAPACITY, workspace)
    }

    fn dispatch_effects(&mut self, initial_effects: Vec<Effect>, now: f64, now_instant: Instant) {
        let mut event_queue = VecDeque::new();
        self.process_effects(initial_effects, &mut event_queue, now_instant);

        while let Some(event) = event_queue.pop_front() {
            let effects = self.engine.handle_event(event, now);
            self.process_effects(effects, &mut event_queue, now_instant);
        }
    }

    fn dispatch_protocol_event(&mut self, event: ProtocolEvent, now: f64, now_instant: Instant) {
        let effects = self.engine.handle_event(event, now);

        self.dispatch_effects(effects, now, now_instant);
    }

    fn flush_stream(&mut self, stream_id: QuicStreamId) -> Option<ProtocolEvent> {
        let buffer = self.send_buffers.get_mut(&stream_id)?;
        let mut stream = self.connection.send_stream(stream_id);

        while let Some(chunk) = buffer.chunks.front_mut() {
            match stream.write(chunk) {
                Ok(written) if written == chunk.len() => {
                    buffer.chunks.pop_front();
                }
                Ok(written) => {
                    chunk.advance(written);
                }
                Err(QuicWriteError::Blocked) => {
                    return None;
                }
                Err(QuicWriteError::Stopped(_) | QuicWriteError::ClosedStream) => {
                    self.send_buffers.remove(&stream_id);
                    return None;
                }
            }
        }

        if buffer.finished {
            stream.finish().ok();
        }
        self.send_buffers.remove(&stream_id);

        Some(ProtocolEvent::InternalCompleteQuicData {
            stream_id: u64::from(stream_id),
        })
    }

    fn process_effects(
        &mut self,
        effects: Vec<Effect>,
        event_queue: &mut VecDeque<ProtocolEvent>,
        now_instant: Instant,
    ) {
        for effect in effects {
            match effect {
                Effect::CloseQuicConnection { error_code, reason } => {
                    if self.connection.is_closed() {
                        continue;
                    }

                    self.connection.close(
                        now_instant,
                        QuicVarInt::try_from(error_code).unwrap_or(QuicVarInt::from(0u32)),
                        match &reason {
                            Some(Cow::Borrowed(reason)) => Bytes::from_static(reason.as_bytes()),
                            Some(Cow::Owned(reason)) => Bytes::from(reason.clone()),
                            None => Bytes::new(),
                        },
                    );

                    event_queue.push_back(ProtocolEvent::TransportConnectionTerminated {
                        error_code,
                        reason: reason.unwrap_or_default(),
                    });
                }
                Effect::CreateH3ConnectStream {
                    request_id,
                    authority,
                    path,
                    headers,
                } => {
                    if let Some(stream_id) = self.connection.streams().open(QuicDir::Bi) {
                        let stream_id = u64::from(stream_id);

                        match self
                            .engine
                            .encode_connect_request(stream_id, authority, path, &headers)
                        {
                            Ok(effects) => {
                                self.process_effects(effects, event_queue, now_instant);

                                event_queue.push_back(ProtocolEvent::InternalBindH3ConnectStream {
                                    request_id,
                                    stream_id,
                                });
                            }
                            Err(e) => {
                                debug!("wt_session encode failed err={e:?}");
                                event_queue.push_back(ProtocolEvent::InternalFailH3ConnectStream {
                                    request_id,
                                    error_code: None,
                                    reason: "wt_session encode failed".into(),
                                });
                            }
                        }
                    } else {
                        event_queue
                            .push_back(ProtocolEvent::InternalDeferH3ConnectStream { request_id });
                    }
                }
                Effect::CreateH3LocalStreams => {
                    if let (
                        Some(control_stream_id),
                        Some(encoder_stream_id),
                        Some(decoder_stream_id),
                    ) = (
                        self.connection.streams().open(QuicDir::Uni),
                        self.connection.streams().open(QuicDir::Uni),
                        self.connection.streams().open(QuicDir::Uni),
                    ) {
                        event_queue.push_back(ProtocolEvent::InternalBindH3LocalStreams {
                            control_stream_id: u64::from(control_stream_id),
                            encoder_stream_id: u64::from(encoder_stream_id),
                            decoder_stream_id: u64::from(decoder_stream_id),
                        });
                    } else {
                        event_queue.push_back(ProtocolEvent::InternalFailH3LocalStreams);
                    }
                }
                Effect::CreateQuicStream {
                    request_id,
                    session_id,
                    is_unidirectional,
                } => {
                    let direction = if is_unidirectional {
                        QuicDir::Uni
                    } else {
                        QuicDir::Bi
                    };

                    if let Some(stream_id) = self.connection.streams().open(direction) {
                        let stream_id = u64::from(stream_id);
                        let effects = WebTransportEngine::encode_stream_header(
                            stream_id,
                            session_id,
                            is_unidirectional,
                        );

                        self.process_effects(effects, event_queue, now_instant);

                        event_queue.push_back(ProtocolEvent::InternalBindQuicStream {
                            request_id,
                            stream_id,
                            session_id,
                            is_unidirectional,
                        });
                    } else {
                        event_queue.push_back(ProtocolEvent::InternalDeferQuicStream {
                            request_id,
                            session_id,
                            is_unidirectional,
                        });
                    }
                }
                Effect::ExportTlsKeyingMaterial {
                    request_id,
                    label,
                    context,
                    length,
                } => {
                    let mut buffer = vec![0u8; length as usize];
                    match self.connection.crypto_session().export_keying_material(
                        &mut buffer,
                        label.as_bytes(),
                        &context,
                    ) {
                        Ok(()) => {
                            self.pending_effects.push_back(Effect::NotifyRequestDone {
                                request_id,
                                result: RequestResult::KeyingMaterial(Bytes::from(buffer)),
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
                Effect::ReceiveQuicData {
                    stream_id,
                    max_size,
                } => {
                    let Ok(stream_id) = QuicVarInt::try_from(stream_id) else {
                        continue;
                    };

                    let stream_id = QuicStreamId::from(stream_id);
                    self.pending_receives.insert(stream_id, max_size);

                    if let Some(event) = self.recv_stream_data(stream_id) {
                        event_queue.push_back(event);
                    }
                }
                Effect::ResetQuicStream {
                    stream_id,
                    error_code,
                } => {
                    let Ok(stream_id) = QuicVarInt::try_from(stream_id) else {
                        continue;
                    };

                    let stream_id = QuicStreamId::from(stream_id);
                    let error_code =
                        QuicVarInt::try_from(error_code).unwrap_or(QuicVarInt::from(0u32));
                    let mut stream = self.connection.send_stream(stream_id);

                    stream.reset(error_code).ok();

                    self.send_buffers.remove(&stream_id);
                }
                Effect::SendH3Capsule {
                    stream_id,
                    capsule_type,
                    capsule_value,
                    end_stream,
                } => {
                    match WebTransportEngine::encode_capsule(
                        stream_id,
                        capsule_type,
                        capsule_value,
                        end_stream,
                    ) {
                        Ok(effects) => {
                            self.process_effects(effects, event_queue, now_instant);
                        }
                        Err(e) => {
                            debug!("wt_capsule encode failed err={e:?}");
                        }
                    }
                }
                Effect::SendH3Datagram { session_id, data } => {
                    match WebTransportEngine::encode_datagram(session_id, data) {
                        Ok(effects) => {
                            self.process_effects(effects, event_queue, now_instant);
                        }
                        Err(e) => {
                            debug!("wt_datagram encode failed err={e:?}");
                        }
                    }
                }
                Effect::SendH3Goaway { identifier } => {
                    let effects = self.engine.encode_goaway(identifier);

                    self.process_effects(effects, event_queue, now_instant);
                }
                Effect::SendH3Headers {
                    stream_id,
                    headers,
                    end_stream,
                } => match self.engine.encode_headers(stream_id, &headers, end_stream) {
                    Ok(effects) => {
                        self.process_effects(effects, event_queue, now_instant);
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
                    let Ok(stream_id) = QuicVarInt::try_from(stream_id) else {
                        continue;
                    };

                    let stream_id = QuicStreamId::from(stream_id);
                    let buffer = self.send_buffers.entry(stream_id).or_default();

                    if !data.is_empty() {
                        buffer.chunks.push_back(data);
                    }

                    if end_stream {
                        buffer.finished = true;
                    }

                    if let Some(event) = self.flush_stream(stream_id) {
                        event_queue.push_back(event);
                    }
                }
                Effect::SendQuicDatagram { header, payload } => {
                    let mut buffer = BytesMut::with_capacity(header.len() + payload.len());
                    buffer.put(header);
                    buffer.put(payload);
                    if let Err(e) = self.connection.datagrams().send(buffer.freeze(), false) {
                        debug!("quic_datagram send failed err={e:?}");
                    }
                }
                Effect::StopQuicStream {
                    stream_id,
                    error_code,
                } => {
                    let Ok(stream_id) = QuicVarInt::try_from(stream_id) else {
                        continue;
                    };

                    let stream_id = QuicStreamId::from(stream_id);
                    let error_code =
                        QuicVarInt::try_from(error_code).unwrap_or(QuicVarInt::from(0u32));
                    self.pending_receives.remove(&stream_id);
                    self.connection.recv_stream(stream_id).stop(error_code).ok();
                }
                effect => {
                    self.pending_effects.push_back(effect);
                }
            }
        }
    }

    fn process_quic_events(&mut self, now: f64, now_instant: Instant) {
        while let Some(event) = self.connection.poll() {
            match event {
                QuicEvent::Connected => {
                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportHandshakeCompleted,
                        now,
                        now_instant,
                    );

                    if let Some(max_size) = self.connection.datagrams().max_size() {
                        self.dispatch_protocol_event(
                            ProtocolEvent::TransportQuicParametersReceived {
                                peer_max_datagram_frame_size: max_size as u64,
                            },
                            now,
                            now_instant,
                        );
                    }
                }
                QuicEvent::ConnectionLost { reason } => {
                    let error_code: ErrorCode = match &reason {
                        QuicConnectionError::ApplicationClosed(e) => u64::from(e.error_code),
                        QuicConnectionError::CidsExhausted
                        | QuicConnectionError::LocallyClosed
                        | QuicConnectionError::Reset
                        | QuicConnectionError::TimedOut
                        | QuicConnectionError::VersionMismatch => ERR_LIB_CONNECTION_STATE_ERROR,
                        QuicConnectionError::ConnectionClosed(e) => u64::from(e.error_code),
                        QuicConnectionError::TransportError(e) => u64::from(e.code),
                    };

                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportConnectionTerminated {
                            error_code,
                            reason: reason.to_string().into(),
                        },
                        now,
                        now_instant,
                    );
                }
                QuicEvent::DatagramReceived => {
                    self.recv_datagrams(now, now_instant);
                }
                QuicEvent::Stream(QuicStreamEvent::Available { dir: direction }) => {
                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportMaxStreamsReceived {
                            is_unidirectional: direction == QuicDir::Uni,
                        },
                        now,
                        now_instant,
                    );
                }
                QuicEvent::Stream(QuicStreamEvent::Finished { id: stream_id }) => {
                    self.dispatch_protocol_event(
                        ProtocolEvent::TransportStreamDataAcknowledged {
                            stream_id: u64::from(stream_id),
                        },
                        now,
                        now_instant,
                    );
                }
                QuicEvent::Stream(QuicStreamEvent::Opened { dir: direction }) => {
                    while let Some(stream_id) = self.connection.streams().accept(direction) {
                        self.dispatch_protocol_event(
                            ProtocolEvent::TransportStreamOpened {
                                stream_id: u64::from(stream_id),
                            },
                            now,
                            now_instant,
                        );
                    }
                }
                QuicEvent::Stream(QuicStreamEvent::Readable { id: stream_id }) => {
                    if let Some(event) = self.recv_stream_data(stream_id) {
                        self.dispatch_protocol_event(event, now, now_instant);
                    }
                }
                QuicEvent::Stream(QuicStreamEvent::Stopped {
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
                }
                QuicEvent::Stream(QuicStreamEvent::Writable { id: stream_id }) => {
                    if let Some(event) = self.flush_stream(stream_id) {
                        self.dispatch_protocol_event(event, now, now_instant);
                    }
                }
                _ => {}
            }
        }
    }

    fn recv_datagrams(&mut self, now: f64, now_instant: Instant) {
        let mut datagrams = Vec::new();

        while let Some(datagram) = self.connection.datagrams().recv() {
            datagrams.push(datagram);
        }

        for data in datagrams {
            self.dispatch_protocol_event(
                ProtocolEvent::TransportDatagramReceived { data },
                now,
                now_instant,
            );
        }
    }

    fn recv_stream_data(&mut self, stream_id: QuicStreamId) -> Option<ProtocolEvent> {
        let max_size = *self.pending_receives.get(&stream_id)?;
        let limit = usize::try_from(max_size).unwrap_or(usize::MAX);

        let mut chunks = Vec::new();
        let mut size = 0;
        let mut is_finished = false;
        let mut reset_error_code = None;

        let mut stream = self.connection.recv_stream(stream_id);
        let Ok(mut chunks_reader) = stream.read(true) else {
            self.pending_receives.remove(&stream_id);
            return None;
        };

        while size < limit {
            match chunks_reader.next(limit - size) {
                Ok(Some(chunk)) => {
                    size += chunk.bytes.len();
                    chunks.push(chunk.bytes);
                }
                Ok(None) => {
                    is_finished = true;
                    break;
                }
                Err(QuicReadError::Reset(error_code)) => {
                    reset_error_code = Some(error_code.into_inner());
                    break;
                }
                Err(QuicReadError::Blocked) => {
                    break;
                }
            }
        }

        let _transmit = chunks_reader.finalize();

        if let Some(error_code) = reset_error_code {
            self.pending_receives.remove(&stream_id);

            return Some(ProtocolEvent::TransportResetStreamReceived {
                stream_id: u64::from(stream_id),
                error_code,
            });
        }

        if size == 0 && !is_finished {
            return None;
        }

        self.pending_receives.remove(&stream_id);

        let data = if chunks.len() == 1 {
            chunks.pop().unwrap_or_default()
        } else {
            let mut buffer = BytesMut::with_capacity(size);
            for chunk in chunks {
                buffer.put(chunk);
            }
            buffer.freeze()
        };

        Some(ProtocolEvent::TransportStreamDataReceived {
            stream_id: u64::from(stream_id),
            data,
            end_stream: is_finished,
        })
    }
}

#[derive(Default)]
struct SendBuffer {
    chunks: VecDeque<Bytes>,
    finished: bool,
}

#[cfg(test)]
mod tests;
