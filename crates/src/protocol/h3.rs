//! HTTP/3 implementation.

use std::cmp;
use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rustc_hash::FxHashMap;
use tracing::{debug, trace};

use crate::common::constants::{
    ERR_H3_CLOSED_CRITICAL_STREAM, ERR_H3_DATAGRAM_ERROR, ERR_H3_EXCESSIVE_LOAD,
    ERR_H3_FRAME_ERROR, ERR_H3_FRAME_UNEXPECTED, ERR_H3_GENERAL_PROTOCOL_ERROR, ERR_H3_ID_ERROR,
    ERR_H3_INTERNAL_ERROR, ERR_H3_MESSAGE_ERROR, ERR_H3_MISSING_SETTINGS, ERR_H3_REQUEST_CANCELLED,
    ERR_H3_SETTINGS_ERROR, ERR_H3_STREAM_CREATION_ERROR, ERR_QPACK_DECODER_STREAM_ERROR,
    ERR_QPACK_DECOMPRESSION_FAILED, ERR_QPACK_ENCODER_STREAM_ERROR, ERR_WT_REQUIREMENTS_NOT_MET,
    ERR_WT_SESSION_GONE, H3_FRAME_TYPE_CANCEL_PUSH, H3_FRAME_TYPE_DATA, H3_FRAME_TYPE_GOAWAY,
    H3_FRAME_TYPE_HEADERS, H3_FRAME_TYPE_MAX_PUSH_ID, H3_FRAME_TYPE_PUSH_PROMISE,
    H3_FRAME_TYPE_RESERVED_CONTINUATION, H3_FRAME_TYPE_RESERVED_PING,
    H3_FRAME_TYPE_RESERVED_PRIORITY, H3_FRAME_TYPE_RESERVED_WINDOW_UPDATE, H3_FRAME_TYPE_SETTINGS,
    H3_FRAME_TYPE_WT_STREAM, H3_STREAM_TYPE_CONTROL, H3_STREAM_TYPE_PUSH,
    H3_STREAM_TYPE_QPACK_DECODER, H3_STREAM_TYPE_QPACK_ENCODER, H3_STREAM_TYPE_WEBTRANSPORT,
    QUIC_VARINT_LIMIT, SETTINGS_ENABLE_CONNECT_PROTOCOL, SETTINGS_H3_DATAGRAM,
    SETTINGS_MAX_FIELD_SECTION_SIZE, SETTINGS_QPACK_BLOCKED_STREAMS,
    SETTINGS_QPACK_MAX_TABLE_CAPACITY, SETTINGS_RESERVED_ENABLE_PUSH,
    SETTINGS_RESERVED_INITIAL_WINDOW_SIZE, SETTINGS_RESERVED_MAX_CONCURRENT_STREAMS,
    SETTINGS_RESERVED_MAX_FRAME_SIZE, SETTINGS_WT_ENABLED, SETTINGS_WT_INITIAL_MAX_DATA,
    SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, WT_UPGRADE_TOKEN,
};
use crate::common::types::{ErrorCode, Headers, SessionId, StreamId};
use crate::protocol::connection::Connection;
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::qpack::{Decoder, Encoder, QpackError};
use crate::protocol::utils;

const SETTINGS_ENTRIES_LIMIT: usize = 64;

#[derive(Clone, Debug, Default)]
pub(crate) struct H3Settings {
    pub(crate) enable_connect_protocol: Option<u64>,
    pub(crate) h3_datagram: Option<u64>,
    pub(crate) max_field_section_size: Option<u64>,
    pub(crate) qpack_blocked_streams: Option<u64>,
    pub(crate) qpack_max_table_capacity: Option<u64>,
    pub(crate) unknown: Vec<(u64, u64)>,
    pub(crate) wt_enabled: Option<u64>,
    pub(crate) wt_initial_max_data: Option<u64>,
    pub(crate) wt_initial_max_streams_bidi: Option<u64>,
    pub(crate) wt_initial_max_streams_uni: Option<u64>,
}

pub(super) struct H3 {
    decoder: Decoder,
    encoder: Encoder,
    initial_max_data: u64,
    initial_max_streams_bidi: u64,
    initial_max_streams_uni: u64,
    is_client: bool,
    local_control_stream_id: Option<StreamId>,
    local_decoder_stream_id: Option<StreamId>,
    local_encoder_stream_id: Option<StreamId>,
    max_capsule_size: u64,
    max_field_section_size: u64,
    peer_control_stream_id: Option<StreamId>,
    peer_decoder_stream_id: Option<StreamId>,
    peer_encoder_stream_id: Option<StreamId>,
    peer_goaway_identifier: Option<u64>,
    peer_max_push_id: Option<u64>,
    settings_received: bool,
    streams: FxHashMap<StreamId, H3Stream>,
}

impl H3 {
    pub(super) fn new(is_client: bool, parameters: H3Parameters) -> Self {
        Self {
            decoder: Decoder::new(),
            encoder: Encoder::new(),
            initial_max_data: parameters.initial_max_data,
            initial_max_streams_bidi: parameters.initial_max_streams_bidi,
            initial_max_streams_uni: parameters.initial_max_streams_uni,
            is_client,
            local_control_stream_id: None,
            local_decoder_stream_id: None,
            local_encoder_stream_id: None,
            max_capsule_size: parameters.max_capsule_size,
            max_field_section_size: parameters.max_field_section_size,
            peer_control_stream_id: None,
            peer_decoder_stream_id: None,
            peer_encoder_stream_id: None,
            peer_goaway_identifier: None,
            peer_max_push_id: None,
            settings_received: false,
            streams: FxHashMap::default(),
        }
    }

    pub(super) fn abort_request_stream(stream_id: StreamId) -> Vec<Effect> {
        debug!("h3_stream abort stream_id={stream_id}");
        vec![
            Effect::ResetQuicStream {
                stream_id,
                error_code: ERR_H3_REQUEST_CANCELLED,
            },
            Effect::StopQuicStream {
                stream_id,
                error_code: ERR_H3_REQUEST_CANCELLED,
            },
        ]
    }

    pub(super) fn bind_connect_stream(
        &mut self,
        stream_id: StreamId,
    ) -> (Vec<ProtocolEvent>, Vec<Effect>) {
        if self
            .peer_goaway_identifier
            .is_some_and(|peer_goaway_identifier| stream_id >= peer_goaway_identifier)
        {
            return (
                vec![ProtocolEvent::H3ConnectStreamRejected { stream_id }],
                Vec::new(),
            );
        }

        self.streams.insert(
            stream_id,
            H3Stream {
                is_connect_stream: true,
                is_request_stream: true,
                ..Default::default()
            },
        );

        (
            Vec::new(),
            vec![Effect::ReceiveQuicData {
                stream_id,
                max_size: 1,
            }],
        )
    }

    pub(super) fn bind_local_streams(
        &mut self,
        control_stream_id: StreamId,
        encoder_stream_id: StreamId,
        decoder_stream_id: StreamId,
    ) -> Vec<Effect> {
        let Some(settings_frame) = self.initialize_settings() else {
            debug!("h3_settings encode failed");
            return H3Error::Connection {
                error_code: ERR_H3_INTERNAL_ERROR,
                reason: "h3_settings encode failed",
            }
            .into_effects();
        };

        let Some([control_data, encoder_data, decoder_data]) =
            Self::initialize_local_streams(&settings_frame)
        else {
            debug!("h3_stream encode failed");
            return H3Error::Connection {
                error_code: ERR_H3_INTERNAL_ERROR,
                reason: "h3_stream encode failed",
            }
            .into_effects();
        };

        self.local_control_stream_id = Some(control_stream_id);
        self.local_encoder_stream_id = Some(encoder_stream_id);
        self.local_decoder_stream_id = Some(decoder_stream_id);

        debug!("h3_control_stream create stream_id={control_stream_id}");
        debug!("qpack_encoder_stream create stream_id={encoder_stream_id}");
        debug!("qpack_decoder_stream create stream_id={decoder_stream_id}");

        vec![
            Effect::SendQuicData {
                stream_id: control_stream_id,
                data: control_data,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: encoder_stream_id,
                data: encoder_data,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id: decoder_stream_id,
                data: decoder_data,
                end_stream: false,
            },
        ]
    }

    pub(super) fn cleanup_stream(&mut self, stream_id: StreamId) {
        self.streams.remove(&stream_id);
    }

    pub(super) fn encode_capsule(
        stream_id: StreamId,
        capsule_type: u64,
        capsule_value: Bytes,
        end_stream: bool,
    ) -> Vec<Effect> {
        let mut capsule_header = BytesMut::with_capacity(16);

        let frame_header = utils::write_varint(&mut capsule_header, capsule_type)
            .and_then(|()| utils::write_varint(&mut capsule_header, capsule_value.len() as u64))
            .and_then(|()| {
                encode_frame_header(
                    H3_FRAME_TYPE_DATA,
                    capsule_header.len() + capsule_value.len(),
                )
            });

        let Some(frame_header) = frame_header else {
            debug!("wt_capsule encode failed stream_id={stream_id}");
            return H3Error::Connection {
                error_code: ERR_H3_INTERNAL_ERROR,
                reason: "wt_capsule encode failed",
            }
            .into_effects();
        };

        vec![
            Effect::SendQuicData {
                stream_id,
                data: frame_header,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id,
                data: capsule_header.freeze(),
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id,
                data: capsule_value,
                end_stream,
            },
        ]
    }

    pub(super) fn encode_datagram(session_id: SessionId, data: Bytes) -> Vec<Effect> {
        let mut header = BytesMut::new();

        if utils::write_varint(&mut header, session_id / 4).is_none() {
            debug!("wt_datagram encode failed session_id={session_id}");
            return H3Error::Connection {
                error_code: ERR_H3_INTERNAL_ERROR,
                reason: "wt_datagram encode failed",
            }
            .into_effects();
        }

        vec![Effect::SendQuicDatagram {
            header: header.freeze(),
            payload: data,
        }]
    }

    pub(super) fn encode_goaway(&self, identifier: u64) -> Vec<Effect> {
        let Some(stream_id) = self.local_control_stream_id else {
            return Vec::new();
        };

        let mut payload = BytesMut::with_capacity(8);

        let header = utils::write_varint(&mut payload, identifier)
            .and_then(|()| encode_frame_header(H3_FRAME_TYPE_GOAWAY, payload.len()));

        let Some(header) = header else {
            debug!("h3_goaway encode failed stream_id={stream_id}");
            return H3Error::Connection {
                error_code: ERR_H3_INTERNAL_ERROR,
                reason: "h3_goaway encode failed",
            }
            .into_effects();
        };

        let mut frame = BytesMut::with_capacity(header.len() + payload.len());
        frame.put(header);
        frame.put(payload);

        vec![Effect::SendQuicData {
            stream_id,
            data: frame.freeze(),
            end_stream: false,
        }]
    }

    pub(super) fn encode_headers(
        &mut self,
        stream_id: StreamId,
        headers: &Headers,
        end_stream: bool,
    ) -> Vec<Effect> {
        let frame_payload = match self.encoder.encode(stream_id, headers) {
            Ok(frame_payload) => Bytes::from(frame_payload),
            Err(e) => {
                debug!("qpack_encoder encode failed stream_id={stream_id} err={e:?}");
                return H3Error::Connection {
                    error_code: ERR_H3_INTERNAL_ERROR,
                    reason: "qpack_encoder encode failed",
                }
                .into_effects();
            }
        };

        let Some(frame_header) = encode_frame_header(H3_FRAME_TYPE_HEADERS, frame_payload.len())
        else {
            debug!("h3_headers encode failed stream_id={stream_id}");
            return H3Error::Connection {
                error_code: ERR_H3_INTERNAL_ERROR,
                reason: "h3_headers encode failed",
            }
            .into_effects();
        };

        vec![
            Effect::SendQuicData {
                stream_id,
                data: frame_header,
                end_stream: false,
            },
            Effect::SendQuicData {
                stream_id,
                data: frame_payload,
                end_stream,
            },
        ]
    }

    pub(super) fn encode_stream_header(
        stream_id: StreamId,
        session_id: SessionId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        let mut buffer = BytesMut::with_capacity(16);

        let stream_type = if is_unidirectional {
            H3_STREAM_TYPE_WEBTRANSPORT
        } else {
            H3_FRAME_TYPE_WT_STREAM
        };

        if utils::write_varint(&mut buffer, stream_type)
            .and_then(|()| utils::write_varint(&mut buffer, session_id))
            .is_none()
        {
            debug!("wt_stream encode failed stream_id={stream_id}");
            return H3Error::Connection {
                error_code: ERR_H3_INTERNAL_ERROR,
                reason: "wt_stream encode failed",
            }
            .into_effects();
        }

        vec![Effect::SendQuicData {
            stream_id,
            data: buffer.freeze(),
            end_stream: false,
        }]
    }

    pub(super) fn fail_local_streams() -> Vec<Effect> {
        debug!("h3_stream create failed");
        H3Error::Connection {
            error_code: ERR_H3_GENERAL_PROTOCOL_ERROR,
            reason: "h3_stream create failed",
        }
        .into_effects()
    }

    pub(super) fn handle_transport_event(
        &mut self,
        event: &ProtocolEvent,
        connection: &Connection,
    ) -> (Vec<ProtocolEvent>, Vec<Effect>) {
        let mut events = Vec::new();

        let result = match event {
            ProtocolEvent::TransportDatagramReceived { data } => {
                Self::receive_datagram(data.clone(), &mut events).map(|()| Vec::new())
            }
            ProtocolEvent::TransportResetStreamReceived { stream_id, .. } => {
                self.receive_stream_reset(*stream_id).map(|()| Vec::new())
            }
            ProtocolEvent::TransportStopSendingReceived { stream_id, .. } => {
                self.receive_stop_sending(*stream_id).map(|()| Vec::new())
            }
            ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data,
                end_stream,
            } => {
                if utils::is_unidirectional_stream(*stream_id) {
                    self.receive_unidirectional_stream_data(
                        *stream_id,
                        data,
                        &mut events,
                        *end_stream,
                        connection,
                    )
                } else {
                    self.receive_request_data(
                        *stream_id,
                        data,
                        &mut events,
                        *end_stream,
                        connection,
                    )
                }
            }
            ProtocolEvent::TransportStreamOpened { stream_id } => {
                Ok(self.handle_stream_opened(*stream_id))
            }
            _ => Ok(Vec::new()),
        };

        (events, result.unwrap_or_else(H3Error::into_effects))
    }

    pub(super) fn resume_request_stream(&self, stream_id: StreamId) -> Vec<Effect> {
        if !self.streams.contains_key(&stream_id) {
            return Vec::new();
        }

        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: u64::MAX,
        }]
    }

    pub(super) fn stop_request_stream(&mut self, stream_id: StreamId) -> Vec<Effect> {
        if self.streams.remove(&stream_id).is_none() {
            return Vec::new();
        }

        vec![Effect::StopQuicStream {
            stream_id,
            error_code: ERR_WT_SESSION_GONE,
        }]
    }

    fn handle_control_frame(
        &mut self,
        frame_type: u64,
        frame_data: &[u8],
        events: &mut Vec<ProtocolEvent>,
        connection: &Connection,
    ) -> Result<(), H3Error> {
        match frame_type {
            H3_FRAME_TYPE_CANCEL_PUSH => {
                parse_varint_payload(frame_data)?;
                debug!("h3_cancel_push validate invalid");
                return Err(H3Error::Connection {
                    error_code: ERR_H3_ID_ERROR,
                    reason: "h3_cancel_push validate invalid",
                });
            }
            H3_FRAME_TYPE_GOAWAY => {
                let identifier = parse_varint_payload(frame_data)?;

                if self.is_client
                    && (utils::is_unidirectional_stream(identifier)
                        || utils::is_peer_initiated_stream(identifier, self.is_client))
                {
                    debug!("h3_goaway validate invalid actual={identifier}");
                    return Err(H3Error::Connection {
                        error_code: ERR_H3_ID_ERROR,
                        reason: "h3_goaway validate invalid",
                    });
                }
                if let Some(peer_goaway_identifier) = self.peer_goaway_identifier
                    && identifier > peer_goaway_identifier
                {
                    debug!(
                        "h3_goaway validate exceeded actual={identifier} limit={peer_goaway_identifier}"
                    );
                    return Err(H3Error::Connection {
                        error_code: ERR_H3_ID_ERROR,
                        reason: "h3_goaway validate exceeded",
                    });
                }
                self.peer_goaway_identifier = Some(identifier);
                debug!("h3_goaway receive");
                events.push(ProtocolEvent::H3GoawayReceived);

                if self.is_client {
                    let mut rejected_stream_ids: Vec<StreamId> = self
                        .streams
                        .extract_if(|stream_id, stream| {
                            stream.is_connect_stream
                                && !stream.headers_processed
                                && *stream_id >= identifier
                        })
                        .map(|(stream_id, _)| stream_id)
                        .collect();
                    rejected_stream_ids.sort_unstable();

                    events.extend(
                        rejected_stream_ids
                            .into_iter()
                            .map(|stream_id| ProtocolEvent::H3ConnectStreamRejected { stream_id }),
                    );
                }
            }
            H3_FRAME_TYPE_MAX_PUSH_ID => {
                let peer_max_push_id = parse_varint_payload(frame_data)?;

                if self.peer_max_push_id > Some(peer_max_push_id) {
                    debug!("h3_max_push_id validate invalid actual={peer_max_push_id}");
                    return Err(H3Error::Connection {
                        error_code: ERR_H3_ID_ERROR,
                        reason: "h3_max_push_id validate invalid",
                    });
                }
                self.peer_max_push_id = Some(peer_max_push_id);
                debug!("h3_max_push_id receive");
            }
            H3_FRAME_TYPE_SETTINGS => {
                let settings = parse_settings(frame_data)?;
                validate_settings(&settings, connection)?;

                self.settings_received = true;
                events.push(ProtocolEvent::H3SettingsReceived { settings });
            }
            _ => {}
        }

        Ok(())
    }

    fn handle_request_frame(
        &mut self,
        stream_id: StreamId,
        stream: &mut H3Stream,
        frame_type: u64,
        frame_data: &[u8],
        events: &mut Vec<ProtocolEvent>,
    ) -> Result<(), H3Error> {
        match frame_type {
            H3_FRAME_TYPE_DATA => {
                if stream.is_connect_stream {
                    stream.capsule_buffer.extend_from_slice(frame_data);
                    self.parse_capsules(stream_id, stream, events)?;
                } else if !frame_data.is_empty() {
                    trace!(
                        "h3_frame receive size={} stream_id={stream_id}",
                        frame_data.len()
                    );
                }
            }
            H3_FRAME_TYPE_HEADERS => {
                let max_field_section_size = self.max_field_section_size;
                let headers = self
                    .decoder
                    .decode(stream_id, frame_data, max_field_section_size)
                    .map_err(|e| {
                        if matches!(e, QpackError::FieldSectionTooLarge) {
                            debug!(
                                "h3_field_section validate exceeded limit={max_field_section_size} stream_id={stream_id}"
                            );
                            return H3Error::Stream { stream_id, error_code: ERR_H3_MESSAGE_ERROR };
                        }
                        debug!("h3_headers decode invalid stream_id={stream_id} err={e:?}");
                        if matches!(e, QpackError::HeaderTooLong) {
                            H3Error::Stream { stream_id, error_code: ERR_QPACK_DECOMPRESSION_FAILED }
                        } else {
                            H3Error::Connection { error_code: ERR_QPACK_DECOMPRESSION_FAILED, reason: "h3_headers decode invalid" }
                        }
                    })?;

                if self.is_client {
                    validate_response_headers(stream_id, &headers)?;
                } else {
                    validate_request_headers(stream_id, &headers)?;
                }

                trace!(
                    "h3_headers receive size={} stream_id={stream_id}",
                    frame_data.len()
                );

                let is_interim_response = utils::find_header(&headers, b":status")
                    .is_some_and(|status| status.starts_with(b"1"));

                if is_interim_response {
                    return Ok(());
                }

                let is_webtransport_upgrade = utils::find_header(&headers, b":protocol")
                    .is_some_and(|protocol| protocol.eq_ignore_ascii_case(WT_UPGRADE_TOKEN));
                let is_successful_status = utils::find_header(&headers, b":status")
                    .is_some_and(|status| status.starts_with(b"2"));

                if is_webtransport_upgrade {
                    stream.is_connect_stream = true;
                }
                if self.is_client && !is_successful_status {
                    stream.is_connect_stream = false;
                }
                stream.headers_processed = true;

                events.push(ProtocolEvent::H3HeadersReceived { stream_id, headers });
            }
            _ => {}
        }

        Ok(())
    }

    fn handle_stream_opened(&mut self, stream_id: StreamId) -> Vec<Effect> {
        self.streams.insert(stream_id, H3Stream::default());

        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: 1,
        }]
    }

    fn initialize_local_streams(settings_frame: &Bytes) -> Option<[Bytes; 3]> {
        let mut control_data = BytesMut::new();
        utils::write_varint(&mut control_data, H3_STREAM_TYPE_CONTROL)?;
        control_data.extend_from_slice(settings_frame);

        let mut encoder_data = BytesMut::new();
        utils::write_varint(&mut encoder_data, H3_STREAM_TYPE_QPACK_ENCODER)?;

        let mut decoder_data = BytesMut::new();
        utils::write_varint(&mut decoder_data, H3_STREAM_TYPE_QPACK_DECODER)?;

        Some([
            control_data.freeze(),
            encoder_data.freeze(),
            decoder_data.freeze(),
        ])
    }

    fn initialize_settings(&self) -> Option<Bytes> {
        let settings = H3Settings {
            enable_connect_protocol: Some(1),
            h3_datagram: Some(1),
            max_field_section_size: Some(self.max_field_section_size),
            qpack_blocked_streams: Some(0),
            qpack_max_table_capacity: Some(0),
            wt_enabled: Some(1),
            wt_initial_max_data: Some(self.initial_max_data),
            wt_initial_max_streams_bidi: Some(self.initial_max_streams_bidi),
            wt_initial_max_streams_uni: Some(self.initial_max_streams_uni),
            ..Default::default()
        };
        let payload = encode_settings(&settings)?;
        let header = encode_frame_header(H3_FRAME_TYPE_SETTINGS, payload.len())?;

        let mut frame = BytesMut::with_capacity(header.len() + payload.len());
        frame.put(header);
        frame.put(payload);

        Some(frame.freeze())
    }

    fn parse_capsules(
        &mut self,
        stream_id: StreamId,
        stream: &mut H3Stream,
        events: &mut Vec<ProtocolEvent>,
    ) -> Result<(), H3Error> {
        let max_capsule_size = self.max_capsule_size;

        loop {
            if stream.capsule_buffer.is_empty() {
                break;
            }

            let mut cursor = Cursor::new(&stream.capsule_buffer[..]);
            let Some(capsule_type) = utils::read_varint(&mut cursor) else {
                break;
            };
            let Some(capsule_length) = utils::read_varint(&mut cursor) else {
                break;
            };

            if capsule_length > max_capsule_size {
                debug!(
                    "wt_capsule validate exceeded actual={capsule_length} limit={max_capsule_size} stream_id={stream_id}"
                );
                return Err(H3Error::Stream {
                    stream_id,
                    error_code: ERR_H3_MESSAGE_ERROR,
                });
            }

            let capsule_length = usize::try_from(capsule_length).map_err(|e| {
                debug!("wt_capsule convert failed expected=usize stream_id={stream_id} err={e:?}");
                H3Error::Stream {
                    stream_id,
                    error_code: ERR_H3_MESSAGE_ERROR,
                }
            })?;

            let header_length = stream.capsule_buffer.len() - cursor.remaining();

            if stream.capsule_buffer.len() < header_length + capsule_length {
                break;
            }

            stream.capsule_buffer.advance(header_length);
            let capsule_value = stream.capsule_buffer.split_to(capsule_length).freeze();

            events.push(ProtocolEvent::H3CapsuleReceived {
                stream_id,
                capsule_type,
                capsule_value,
            });
        }

        Ok(())
    }

    fn parse_stream_data(
        &mut self,
        stream_id: StreamId,
        stream: &mut H3Stream,
        events: &mut Vec<ProtocolEvent>,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(), H3Error> {
        let is_control_stream = stream.stream_type == Some(H3_STREAM_TYPE_CONTROL);
        let max_field_section_size =
            usize::try_from(self.max_field_section_size).unwrap_or(usize::MAX);
        let max_settings_size =
            SETTINGS_ENTRIES_LIMIT * 2 * utils::varint_length(QUIC_VARINT_LIMIT);

        loop {
            let (frame_type, remaining_length) = if let Some(partial_frame) = stream.partial_frame {
                partial_frame
            } else {
                let mut cursor = Cursor::new(&stream.buffer[..]);
                let Some(frame_type) = utils::read_varint(&mut cursor) else {
                    break;
                };

                if is_control_stream {
                    if frame_type != H3_FRAME_TYPE_SETTINGS && !self.settings_received {
                        debug!(
                            "h3_settings validate invalid actual={frame_type} expected=h3_frame_type_settings stream_id={stream_id}"
                        );
                        return Err(H3Error::Connection {
                            error_code: ERR_H3_MISSING_SETTINGS,
                            reason: "h3_settings validate invalid",
                        });
                    }

                    match frame_type {
                        H3_FRAME_TYPE_DATA
                        | H3_FRAME_TYPE_HEADERS
                        | H3_FRAME_TYPE_PUSH_PROMISE
                        | H3_FRAME_TYPE_RESERVED_CONTINUATION
                        | H3_FRAME_TYPE_RESERVED_PING
                        | H3_FRAME_TYPE_RESERVED_PRIORITY
                        | H3_FRAME_TYPE_RESERVED_WINDOW_UPDATE
                        | H3_FRAME_TYPE_WT_STREAM => {
                            debug!(
                                "h3_control_stream validate invalid actual={frame_type} stream_id={stream_id}"
                            );
                            return Err(H3Error::Connection {
                                error_code: ERR_H3_FRAME_UNEXPECTED,
                                reason: "h3_control_stream validate invalid",
                            });
                        }
                        H3_FRAME_TYPE_MAX_PUSH_ID if self.is_client => {
                            debug!("h3_max_push_id validate invalid stream_id={stream_id}");
                            return Err(H3Error::Connection {
                                error_code: ERR_H3_FRAME_UNEXPECTED,
                                reason: "h3_max_push_id validate invalid",
                            });
                        }
                        H3_FRAME_TYPE_SETTINGS if self.settings_received => {
                            debug!("h3_settings validate invalid stream_id={stream_id}");
                            return Err(H3Error::Connection {
                                error_code: ERR_H3_FRAME_UNEXPECTED,
                                reason: "h3_settings validate invalid",
                            });
                        }
                        _ => {}
                    }
                } else {
                    match frame_type {
                        H3_FRAME_TYPE_CANCEL_PUSH
                        | H3_FRAME_TYPE_GOAWAY
                        | H3_FRAME_TYPE_MAX_PUSH_ID
                        | H3_FRAME_TYPE_RESERVED_CONTINUATION
                        | H3_FRAME_TYPE_RESERVED_PING
                        | H3_FRAME_TYPE_RESERVED_PRIORITY
                        | H3_FRAME_TYPE_RESERVED_WINDOW_UPDATE
                        | H3_FRAME_TYPE_SETTINGS
                        | H3_FRAME_TYPE_WT_STREAM => {
                            debug!(
                                "h3_request_stream validate invalid actual={frame_type} stream_id={stream_id}"
                            );
                            return Err(H3Error::Connection {
                                error_code: ERR_H3_FRAME_UNEXPECTED,
                                reason: "h3_request_stream validate invalid",
                            });
                        }
                        H3_FRAME_TYPE_DATA if !stream.headers_processed => {
                            debug!("h3_data validate invalid stream_id={stream_id}");
                            return Err(H3Error::Connection {
                                error_code: ERR_H3_FRAME_UNEXPECTED,
                                reason: "h3_data validate invalid",
                            });
                        }
                        H3_FRAME_TYPE_HEADERS if stream.headers_processed => {
                            debug!("h3_headers validate invalid stream_id={stream_id}");
                            return Err(H3Error::Connection {
                                error_code: ERR_H3_FRAME_UNEXPECTED,
                                reason: "h3_headers validate invalid",
                            });
                        }
                        H3_FRAME_TYPE_PUSH_PROMISE
                            if !self.is_client || stream.headers_processed =>
                        {
                            debug!("h3_push_promise validate invalid stream_id={stream_id}");
                            return Err(H3Error::Connection {
                                error_code: ERR_H3_FRAME_UNEXPECTED,
                                reason: "h3_push_promise validate invalid",
                            });
                        }
                        _ => {}
                    }
                }

                let Some(payload_length) = utils::read_varint(&mut cursor) else {
                    break;
                };
                let payload_length = usize::try_from(payload_length).map_err(|e| {
                    debug!("varint convert failed expected=usize stream_id={stream_id} err={e:?}");
                    H3Error::Connection {
                        error_code: ERR_H3_EXCESSIVE_LOAD,
                        reason: "varint convert failed",
                    }
                })?;
                let header_length = stream.buffer.len() - cursor.remaining();

                match frame_type {
                    H3_FRAME_TYPE_CANCEL_PUSH
                    | H3_FRAME_TYPE_GOAWAY
                    | H3_FRAME_TYPE_MAX_PUSH_ID
                        if payload_length > utils::varint_length(QUIC_VARINT_LIMIT) =>
                    {
                        debug!(
                            "h3_frame validate invalid actual={payload_length} stream_id={stream_id}"
                        );
                        return Err(H3Error::Connection {
                            error_code: ERR_H3_FRAME_ERROR,
                            reason: "h3_frame validate invalid",
                        });
                    }
                    H3_FRAME_TYPE_HEADERS if payload_length > max_field_section_size => {
                        debug!(
                            "h3_headers validate exceeded actual={payload_length} limit={max_field_section_size} stream_id={stream_id}"
                        );
                        return Err(H3Error::Stream {
                            stream_id,
                            error_code: ERR_H3_MESSAGE_ERROR,
                        });
                    }
                    H3_FRAME_TYPE_PUSH_PROMISE => {
                        let received_length = cmp::min(payload_length, cursor.remaining());
                        let received_payload =
                            cursor.chunk().get(..received_length).unwrap_or_default();

                        if utils::read_varint(&mut Cursor::new(received_payload)).is_none() {
                            if received_length < payload_length {
                                break;
                            }
                            debug!(
                                "h3_frame validate invalid actual={payload_length} stream_id={stream_id}"
                            );
                            return Err(H3Error::Connection {
                                error_code: ERR_H3_FRAME_ERROR,
                                reason: "h3_frame validate invalid",
                            });
                        }
                        debug!("h3_push_promise validate invalid stream_id={stream_id}");
                        return Err(H3Error::Connection {
                            error_code: ERR_H3_ID_ERROR,
                            reason: "h3_push_promise validate invalid",
                        });
                    }
                    H3_FRAME_TYPE_SETTINGS if payload_length > max_settings_size => {
                        debug!(
                            "h3_settings validate exceeded actual={payload_length} limit={max_settings_size} stream_id={stream_id}"
                        );
                        return Err(H3Error::Connection {
                            error_code: ERR_H3_EXCESSIVE_LOAD,
                            reason: "h3_settings validate exceeded",
                        });
                    }
                    _ => {}
                }

                stream.buffer.advance(header_length);

                (frame_type, payload_length)
            };

            let is_buffered = matches!(
                frame_type,
                H3_FRAME_TYPE_CANCEL_PUSH
                    | H3_FRAME_TYPE_GOAWAY
                    | H3_FRAME_TYPE_HEADERS
                    | H3_FRAME_TYPE_MAX_PUSH_ID
                    | H3_FRAME_TYPE_SETTINGS
            );
            let chunk_length = cmp::min(remaining_length, stream.buffer.len());

            if chunk_length < remaining_length && (is_buffered || chunk_length == 0) {
                stream.partial_frame = Some((frame_type, remaining_length));
                break;
            }

            let frame_data = stream.buffer.split_to(chunk_length);
            stream.partial_frame = (chunk_length < remaining_length)
                .then_some((frame_type, remaining_length - chunk_length));

            if is_control_stream {
                self.handle_control_frame(frame_type, &frame_data, events, connection)?;
            } else {
                self.handle_request_frame(stream_id, stream, frame_type, &frame_data, events)?;
            }
        }

        if stream_ended && (stream.partial_frame.is_some() || !stream.buffer.is_empty()) {
            debug!("h3_frame validate invalid stream_id={stream_id}");
            return Err(H3Error::Connection {
                error_code: ERR_H3_FRAME_ERROR,
                reason: "h3_frame validate invalid",
            });
        }

        if stream_ended && stream.is_connect_stream {
            if !stream.headers_processed {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(H3Error::Stream {
                    stream_id,
                    error_code: ERR_H3_MESSAGE_ERROR,
                });
            }
            if !stream.capsule_buffer.is_empty() {
                debug!("wt_capsule validate invalid stream_id={stream_id}");
                return Err(H3Error::Stream {
                    stream_id,
                    error_code: ERR_H3_MESSAGE_ERROR,
                });
            }
        }

        Ok(())
    }

    fn parse_stream_header(
        &mut self,
        stream_id: StreamId,
        stream: H3Stream,
        events: &mut Vec<ProtocolEvent>,
        stream_ended: bool,
    ) -> Result<Vec<Effect>, H3Error> {
        let mut cursor = Cursor::new(&stream.buffer[..]);

        let Some(session_id) = utils::read_varint(&mut cursor) else {
            if stream_ended {
                return Ok(Vec::new());
            }

            self.streams.insert(stream_id, stream);

            return Ok(vec![Effect::ReceiveQuicData {
                stream_id,
                max_size: 1,
            }]);
        };

        if !utils::is_valid_session_id(session_id) {
            debug!("wt_session validate invalid actual={session_id}");
            return Err(H3Error::Connection {
                error_code: ERR_H3_ID_ERROR,
                reason: "wt_session validate invalid",
            });
        }

        events.push(ProtocolEvent::H3WebTransportStreamOpened {
            session_id,
            stream_id,
        });

        Ok(Vec::new())
    }

    fn receive_datagram(mut data: Bytes, events: &mut Vec<ProtocolEvent>) -> Result<(), H3Error> {
        let mut cursor = Cursor::new(&data[..]);

        let quarter_stream_id = utils::read_varint(&mut cursor).ok_or_else(|| {
            debug!("varint decode invalid");
            H3Error::Connection {
                error_code: ERR_H3_DATAGRAM_ERROR,
                reason: "varint decode invalid",
            }
        })?;
        let session_id = quarter_stream_id * 4;

        if session_id > QUIC_VARINT_LIMIT {
            debug!("wt_datagram validate exceeded actual={session_id} expected=quic_varint_limit");
            return Err(H3Error::Connection {
                error_code: ERR_H3_DATAGRAM_ERROR,
                reason: "wt_datagram validate exceeded",
            });
        }

        let header_length = data.len() - cursor.remaining();

        data.advance(header_length);
        events.push(ProtocolEvent::H3DatagramReceived { session_id, data });

        Ok(())
    }

    fn receive_request_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        events: &mut Vec<ProtocolEvent>,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<Vec<Effect>, H3Error> {
        let mut stream = self.streams.remove(&stream_id).unwrap_or_default();

        stream.buffer.extend_from_slice(data);

        if !stream.is_request_stream && stream.stream_type.is_none() {
            let mut cursor = Cursor::new(&stream.buffer[..]);

            match utils::read_varint(&mut cursor) {
                Some(H3_FRAME_TYPE_WT_STREAM) => {
                    stream.buffer.clear();
                    stream.stream_type = Some(H3_STREAM_TYPE_WEBTRANSPORT);
                }
                Some(_) => {
                    if self.is_client {
                        debug!("h3_stream validate invalid stream_id={stream_id}");
                        return Err(H3Error::Connection {
                            error_code: ERR_H3_STREAM_CREATION_ERROR,
                            reason: "h3_stream validate invalid",
                        });
                    }
                    stream.is_request_stream = true;
                }
                None => {
                    if stream_ended {
                        return Ok(Vec::new());
                    }

                    self.streams.insert(stream_id, stream);

                    return Ok(vec![Effect::ReceiveQuicData {
                        stream_id,
                        max_size: 1,
                    }]);
                }
            }
        }

        if stream.stream_type == Some(H3_STREAM_TYPE_WEBTRANSPORT) {
            return self.parse_stream_header(stream_id, stream, events, stream_ended);
        }

        let was_headers_processed = stream.headers_processed;
        self.parse_stream_data(stream_id, &mut stream, events, stream_ended, connection)
            .inspect_err(|error| {
                if let H3Error::Stream { error_code, .. } = *error
                    && stream.is_connect_stream
                {
                    events.push(ProtocolEvent::H3ConnectStreamClosed {
                        stream_id,
                        error_code: Some(error_code),
                    });
                }
            })?;

        if self.is_client && stream.headers_processed && !stream.is_connect_stream {
            return Ok(Self::abort_request_stream(stream_id));
        }

        if stream_ended {
            if stream.is_connect_stream {
                debug!("wt_stream close stream_id={stream_id}");
                events.push(ProtocolEvent::H3ConnectStreamClosed {
                    stream_id,
                    error_code: None,
                });
            }

            return Ok(Vec::new());
        }

        let is_headers_received = !was_headers_processed && stream.headers_processed;
        let max_size = if stream.headers_processed {
            u64::MAX
        } else if let Some((_, remaining_length)) = stream.partial_frame {
            (remaining_length - stream.buffer.len()) as u64
        } else {
            1
        };
        self.streams.insert(stream_id, stream);

        if is_headers_received {
            return Ok(Vec::new());
        }

        Ok(vec![Effect::ReceiveQuicData {
            stream_id,
            max_size,
        }])
    }

    fn receive_stop_sending(&self, stream_id: StreamId) -> Result<(), H3Error> {
        let is_critical_stream = [
            self.local_control_stream_id,
            self.local_decoder_stream_id,
            self.local_encoder_stream_id,
        ]
        .contains(&Some(stream_id));

        if is_critical_stream {
            debug!("h3_control_stream validate invalid stream_id={stream_id}");
            return Err(H3Error::Connection {
                error_code: ERR_H3_CLOSED_CRITICAL_STREAM,
                reason: "h3_control_stream validate invalid",
            });
        }

        Ok(())
    }

    fn receive_stream_reset(&mut self, stream_id: StreamId) -> Result<(), H3Error> {
        let is_critical_stream = [
            self.peer_control_stream_id,
            self.peer_decoder_stream_id,
            self.peer_encoder_stream_id,
        ]
        .contains(&Some(stream_id));

        if is_critical_stream {
            debug!("h3_control_stream validate invalid stream_id={stream_id}");
            return Err(H3Error::Connection {
                error_code: ERR_H3_CLOSED_CRITICAL_STREAM,
                reason: "h3_control_stream validate invalid",
            });
        }

        self.streams.remove(&stream_id);

        Ok(())
    }

    fn receive_unidirectional_stream_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        events: &mut Vec<ProtocolEvent>,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<Vec<Effect>, H3Error> {
        let mut stream = self.streams.remove(&stream_id).unwrap_or_default();

        stream.buffer.extend_from_slice(data);

        let stream_type = if let Some(stream_type) = stream.stream_type {
            stream_type
        } else {
            let mut cursor = Cursor::new(&stream.buffer[..]);
            let Some(stream_type) = utils::read_varint(&mut cursor) else {
                if stream_ended {
                    return Ok(Vec::new());
                }

                self.streams.insert(stream_id, stream);

                return Ok(vec![Effect::ReceiveQuicData {
                    stream_id,
                    max_size: 1,
                }]);
            };
            stream.buffer.clear();

            match stream_type {
                H3_STREAM_TYPE_CONTROL
                | H3_STREAM_TYPE_QPACK_DECODER
                | H3_STREAM_TYPE_QPACK_ENCODER => {
                    self.register_peer_infrastructure_stream(stream_id, stream_type)?;
                }
                H3_STREAM_TYPE_PUSH => {
                    let error_code = if self.is_client {
                        ERR_H3_ID_ERROR
                    } else {
                        ERR_H3_STREAM_CREATION_ERROR
                    };
                    debug!("h3_push_stream validate invalid stream_id={stream_id}");
                    return Err(H3Error::Connection {
                        error_code,
                        reason: "h3_push_stream validate invalid",
                    });
                }
                H3_STREAM_TYPE_WEBTRANSPORT => {}
                _ => {
                    debug!("h3_stream validate invalid actual={stream_type} stream_id={stream_id}");
                    return Ok(vec![Effect::StopQuicStream {
                        stream_id,
                        error_code: ERR_H3_STREAM_CREATION_ERROR,
                    }]);
                }
            }

            stream.stream_type = Some(stream_type);
            stream_type
        };

        match stream_type {
            H3_STREAM_TYPE_CONTROL => {
                if stream_ended {
                    debug!("h3_control_stream validate invalid stream_id={stream_id}");
                    return Err(H3Error::Connection {
                        error_code: ERR_H3_CLOSED_CRITICAL_STREAM,
                        reason: "h3_control_stream validate invalid",
                    });
                }

                self.parse_stream_data(stream_id, &mut stream, events, stream_ended, connection)?;
            }
            H3_STREAM_TYPE_QPACK_DECODER => {
                if !stream.buffer.is_empty() {
                    self.encoder.feed_decoder(&stream.buffer).map_err(|e| {
                        debug!(
                            "qpack_decoder_stream decode invalid stream_id={stream_id} err={e:?}"
                        );
                        H3Error::Connection {
                            error_code: ERR_QPACK_DECODER_STREAM_ERROR,
                            reason: "qpack_decoder_stream decode invalid",
                        }
                    })?;
                    stream.buffer.clear();
                }
            }
            H3_STREAM_TYPE_QPACK_ENCODER => {
                if !stream.buffer.is_empty() {
                    self.decoder.feed_encoder(&stream.buffer).map_err(|e| {
                        debug!(
                            "qpack_encoder_stream decode invalid stream_id={stream_id} err={e:?}"
                        );
                        H3Error::Connection {
                            error_code: ERR_QPACK_ENCODER_STREAM_ERROR,
                            reason: "qpack_encoder_stream decode invalid",
                        }
                    })?;
                    stream.buffer.clear();
                }
            }
            H3_STREAM_TYPE_WEBTRANSPORT => {
                return self.parse_stream_header(stream_id, stream, events, stream_ended);
            }
            _ => {}
        }

        if stream_ended {
            debug!("h3_control_stream validate invalid stream_id={stream_id}");
            return Err(H3Error::Connection {
                error_code: ERR_H3_CLOSED_CRITICAL_STREAM,
                reason: "h3_control_stream validate invalid",
            });
        }

        self.streams.insert(stream_id, stream);

        Ok(vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: u64::MAX,
        }])
    }

    fn register_peer_infrastructure_stream(
        &mut self,
        stream_id: StreamId,
        stream_type: u64,
    ) -> Result<(), H3Error> {
        let role_occupied = match stream_type {
            H3_STREAM_TYPE_CONTROL => self.peer_control_stream_id.is_some(),
            H3_STREAM_TYPE_QPACK_DECODER => self.peer_decoder_stream_id.is_some(),
            H3_STREAM_TYPE_QPACK_ENCODER => self.peer_encoder_stream_id.is_some(),
            _ => false,
        };

        if role_occupied {
            debug!("h3_stream validate failed actual={stream_type} stream_id={stream_id}");
            return Err(H3Error::Connection {
                error_code: ERR_H3_STREAM_CREATION_ERROR,
                reason: "h3_stream validate failed",
            });
        }

        match stream_type {
            H3_STREAM_TYPE_CONTROL => self.peer_control_stream_id = Some(stream_id),
            H3_STREAM_TYPE_QPACK_DECODER => self.peer_decoder_stream_id = Some(stream_id),
            H3_STREAM_TYPE_QPACK_ENCODER => self.peer_encoder_stream_id = Some(stream_id),
            _ => {}
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct H3Parameters {
    pub(super) initial_max_data: u64,
    pub(super) initial_max_streams_bidi: u64,
    pub(super) initial_max_streams_uni: u64,
    pub(super) max_capsule_size: u64,
    pub(super) max_field_section_size: u64,
}

#[derive(Debug)]
enum H3Error {
    Connection {
        error_code: ErrorCode,
        reason: &'static str,
    },
    Stream {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
}

impl H3Error {
    fn into_effects(self) -> Vec<Effect> {
        match self {
            Self::Connection { error_code, reason } => vec![Effect::CloseQuicConnection {
                error_code,
                reason: Some(reason.into()),
            }],
            Self::Stream {
                stream_id,
                error_code,
            } => {
                debug!("h3_stream abort stream_id={stream_id}");
                vec![
                    Effect::ResetQuicStream {
                        stream_id,
                        error_code,
                    },
                    Effect::StopQuicStream {
                        stream_id,
                        error_code,
                    },
                ]
            }
        }
    }
}

#[derive(Debug, Default)]
struct H3Stream {
    buffer: BytesMut,
    capsule_buffer: BytesMut,
    headers_processed: bool,
    is_connect_stream: bool,
    is_request_stream: bool,
    partial_frame: Option<(u64, usize)>,
    stream_type: Option<u64>,
}

fn encode_frame_header(frame_type: u64, payload_length: usize) -> Option<Bytes> {
    let mut buffer = BytesMut::with_capacity(16);

    utils::write_varint(&mut buffer, frame_type)?;

    utils::write_varint(&mut buffer, payload_length as u64)?;

    Some(buffer.freeze())
}

fn encode_settings(settings: &H3Settings) -> Option<Bytes> {
    let known: &[(u64, Option<u64>)] = &[
        (
            SETTINGS_QPACK_MAX_TABLE_CAPACITY,
            settings.qpack_max_table_capacity,
        ),
        (
            SETTINGS_MAX_FIELD_SECTION_SIZE,
            settings.max_field_section_size,
        ),
        (
            SETTINGS_QPACK_BLOCKED_STREAMS,
            settings.qpack_blocked_streams,
        ),
        (
            SETTINGS_ENABLE_CONNECT_PROTOCOL,
            settings.enable_connect_protocol,
        ),
        (SETTINGS_H3_DATAGRAM, settings.h3_datagram),
        (SETTINGS_WT_INITIAL_MAX_DATA, settings.wt_initial_max_data),
        (
            SETTINGS_WT_INITIAL_MAX_STREAMS_UNI,
            settings.wt_initial_max_streams_uni,
        ),
        (
            SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI,
            settings.wt_initial_max_streams_bidi,
        ),
        (SETTINGS_WT_ENABLED, settings.wt_enabled),
    ];

    let mut buffer = BytesMut::with_capacity(128);

    for &(identifier, value) in known {
        let Some(value) = value else { continue };
        utils::write_varint(&mut buffer, identifier)?;
        utils::write_varint(&mut buffer, value)?;
    }

    for &(identifier, value) in &settings.unknown {
        utils::write_varint(&mut buffer, identifier)?;
        utils::write_varint(&mut buffer, value)?;
    }

    Some(buffer.freeze())
}

fn parse_settings(data: &[u8]) -> Result<H3Settings, H3Error> {
    let mut settings = H3Settings::default();
    let mut cursor = Cursor::new(data);
    let mut count = 0usize;

    while cursor.has_remaining() {
        if count >= SETTINGS_ENTRIES_LIMIT {
            debug!("h3_settings validate exceeded actual={count} expected=settings_entries_limit");
            return Err(H3Error::Connection {
                error_code: ERR_H3_EXCESSIVE_LOAD,
                reason: "h3_settings validate exceeded",
            });
        }
        let identifier = utils::read_varint(&mut cursor).ok_or_else(|| {
            debug!("varint decode invalid");
            H3Error::Connection {
                error_code: ERR_H3_FRAME_ERROR,
                reason: "varint decode invalid",
            }
        })?;
        let value = utils::read_varint(&mut cursor).ok_or_else(|| {
            debug!("varint decode invalid");
            H3Error::Connection {
                error_code: ERR_H3_FRAME_ERROR,
                reason: "varint decode invalid",
            }
        })?;

        if matches!(
            identifier,
            SETTINGS_RESERVED_ENABLE_PUSH
                | SETTINGS_RESERVED_INITIAL_WINDOW_SIZE
                | SETTINGS_RESERVED_MAX_CONCURRENT_STREAMS
                | SETTINGS_RESERVED_MAX_FRAME_SIZE
        ) {
            debug!("h3_settings validate invalid actual={identifier}");
            return Err(H3Error::Connection {
                error_code: ERR_H3_SETTINGS_ERROR,
                reason: "h3_settings validate invalid",
            });
        }

        let slot = match identifier {
            SETTINGS_ENABLE_CONNECT_PROTOCOL => &mut settings.enable_connect_protocol,
            SETTINGS_H3_DATAGRAM => &mut settings.h3_datagram,
            SETTINGS_MAX_FIELD_SECTION_SIZE => &mut settings.max_field_section_size,
            SETTINGS_QPACK_BLOCKED_STREAMS => &mut settings.qpack_blocked_streams,
            SETTINGS_QPACK_MAX_TABLE_CAPACITY => &mut settings.qpack_max_table_capacity,
            SETTINGS_WT_ENABLED => &mut settings.wt_enabled,
            SETTINGS_WT_INITIAL_MAX_DATA => &mut settings.wt_initial_max_data,
            SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI => &mut settings.wt_initial_max_streams_bidi,
            SETTINGS_WT_INITIAL_MAX_STREAMS_UNI => &mut settings.wt_initial_max_streams_uni,
            _ => {
                if settings
                    .unknown
                    .iter()
                    .any(|&(unknown_identifier, _)| unknown_identifier == identifier)
                {
                    debug!("h3_settings validate invalid actual={identifier}");
                    return Err(H3Error::Connection {
                        error_code: ERR_H3_SETTINGS_ERROR,
                        reason: "h3_settings validate invalid",
                    });
                }
                settings.unknown.push((identifier, value));
                count += 1;
                continue;
            }
        };

        if slot.is_some() {
            debug!("h3_settings validate invalid actual={identifier}");
            return Err(H3Error::Connection {
                error_code: ERR_H3_SETTINGS_ERROR,
                reason: "h3_settings validate invalid",
            });
        }
        *slot = Some(value);
        count += 1;
    }

    Ok(settings)
}

fn parse_varint_payload(data: &[u8]) -> Result<u64, H3Error> {
    let mut cursor = Cursor::new(data);

    let varint = utils::read_varint(&mut cursor).ok_or_else(|| {
        debug!("varint decode invalid");
        H3Error::Connection {
            error_code: ERR_H3_FRAME_ERROR,
            reason: "varint decode invalid",
        }
    })?;

    if cursor.has_remaining() {
        debug!("h3_frame validate invalid");
        return Err(H3Error::Connection {
            error_code: ERR_H3_FRAME_ERROR,
            reason: "h3_frame validate invalid",
        });
    }

    Ok(varint)
}

fn validate_request_headers(stream_id: StreamId, headers: &Headers) -> Result<(), H3Error> {
    let mut after_pseudo = false;
    let mut method: Option<&[u8]> = None;
    let mut scheme: Option<&[u8]> = None;
    let mut authority: Option<&[u8]> = None;
    let mut path: Option<&[u8]> = None;
    let mut protocol: Option<&[u8]> = None;
    let mut hosts: Vec<&[u8]> = Vec::new();

    for (key, value) in headers {
        let key = key.as_ref();
        let value = value.as_ref();

        if !utils::is_valid_header(key, value)
            || utils::is_connection_specific_header(key, value, true)
        {
            debug!("h3_headers validate invalid stream_id={stream_id}");
            return Err(H3Error::Stream {
                stream_id,
                error_code: ERR_H3_MESSAGE_ERROR,
            });
        }

        if key.starts_with(b":") {
            let pseudo_header = match key {
                b":method" => &mut method,
                b":scheme" => &mut scheme,
                b":authority" => &mut authority,
                b":path" => &mut path,
                b":protocol" => &mut protocol,
                _ => {
                    debug!("h3_headers validate invalid stream_id={stream_id}");
                    return Err(H3Error::Stream {
                        stream_id,
                        error_code: ERR_H3_MESSAGE_ERROR,
                    });
                }
            };
            if after_pseudo || pseudo_header.replace(value).is_some() {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(H3Error::Stream {
                    stream_id,
                    error_code: ERR_H3_MESSAGE_ERROR,
                });
            }
        } else {
            after_pseudo = true;

            if key == b"host" {
                hosts.push(value);
            }
        }
    }

    let is_valid_pseudo = match (method, protocol) {
        (Some(b"CONNECT"), None) => {
            authority.is_some_and(|authority| !authority.is_empty())
                && scheme.is_none()
                && path.is_none()
        }
        (Some(b"CONNECT"), Some(_)) | (Some(_), None) => scheme.is_some() && path.is_some(),
        _ => false,
    };
    let is_valid_path = path.is_none_or(|path| path.iter().all(u8::is_ascii_graphic));

    if !is_valid_pseudo || !is_valid_path {
        debug!("h3_headers validate invalid stream_id={stream_id}");
        return Err(H3Error::Stream {
            stream_id,
            error_code: ERR_H3_MESSAGE_ERROR,
        });
    }

    let is_http_uri = scheme.is_some_and(|scheme| {
        scheme.eq_ignore_ascii_case(b"http") || scheme.eq_ignore_ascii_case(b"https")
    });

    if is_http_uri {
        let is_valid_authority = match (authority, hosts.as_slice()) {
            (Some(authority), []) => !authority.is_empty(),
            (Some(authority), [host]) => !authority.is_empty() && *host == authority,
            (None, [_, ..]) => hosts.iter().all(|host| !host.is_empty()),
            _ => false,
        };
        if !is_valid_authority
            || !path.is_some_and(|path| {
                matches!((path, method), ([b'/', ..], _) | (b"*", Some(b"OPTIONS")))
            })
        {
            debug!("h3_headers validate invalid stream_id={stream_id}");
            return Err(H3Error::Stream {
                stream_id,
                error_code: ERR_H3_MESSAGE_ERROR,
            });
        }
    }

    Ok(())
}

fn validate_response_headers(stream_id: StreamId, headers: &Headers) -> Result<(), H3Error> {
    let mut after_pseudo = false;
    let mut status: Option<&[u8]> = None;

    for (key, value) in headers {
        let key = key.as_ref();
        let value = value.as_ref();

        if !utils::is_valid_header(key, value)
            || utils::is_connection_specific_header(key, value, false)
        {
            debug!("h3_headers validate invalid stream_id={stream_id}");
            return Err(H3Error::Stream {
                stream_id,
                error_code: ERR_H3_MESSAGE_ERROR,
            });
        }

        if key.starts_with(b":") {
            if after_pseudo || key != b":status" || status.replace(value).is_some() {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(H3Error::Stream {
                    stream_id,
                    error_code: ERR_H3_MESSAGE_ERROR,
                });
            }
        } else {
            after_pseudo = true;
        }
    }

    let is_valid_status =
        status.is_some_and(|status| status.len() == 3 && status.iter().all(u8::is_ascii_digit));

    if !is_valid_status {
        debug!("h3_headers validate invalid stream_id={stream_id}");
        return Err(H3Error::Stream {
            stream_id,
            error_code: ERR_H3_MESSAGE_ERROR,
        });
    }

    Ok(())
}

fn validate_settings(settings: &H3Settings, connection: &Connection) -> Result<(), H3Error> {
    if settings
        .enable_connect_protocol
        .is_some_and(|value| value != 1)
    {
        debug!("h3_settings validate invalid expected=enable_connect_protocol");
        return Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            reason: "h3_settings validate invalid",
        });
    }

    let quic_supports_datagrams = connection
        .peer_max_datagram_frame_size()
        .unwrap_or_default()
        > 0;

    if !quic_supports_datagrams && settings.h3_datagram == Some(1) {
        debug!("h3_settings validate invalid expected=max_datagram_frame_size");
        return Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            reason: "h3_settings validate invalid",
        });
    }

    if connection.is_client() && settings.wt_enabled.is_some_and(|value| value > 1) {
        debug!("h3_settings validate invalid expected=wt_enabled");
        return Err(H3Error::Connection {
            error_code: ERR_H3_SETTINGS_ERROR,
            reason: "h3_settings validate invalid",
        });
    }

    if settings.wt_enabled.unwrap_or(0) == 0 {
        debug!("h3_settings validate invalid expected=wt_enabled");
        return Err(H3Error::Connection {
            error_code: ERR_WT_REQUIREMENTS_NOT_MET,
            reason: "h3_settings validate invalid",
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests;
