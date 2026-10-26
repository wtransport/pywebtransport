//! HTTP/3 implementation.

use std::cmp;
use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rustc_hash::FxHashMap;
use tracing::{debug, trace};

use crate::common::constants::{
    ERR_H3_CLOSED_CRITICAL_STREAM, ERR_H3_DATAGRAM_ERROR, ERR_H3_EXCESSIVE_LOAD,
    ERR_H3_FRAME_ERROR, ERR_H3_FRAME_UNEXPECTED, ERR_H3_GENERAL_PROTOCOL_ERROR, ERR_H3_ID_ERROR,
    ERR_H3_INTERNAL_ERROR, ERR_H3_MESSAGE_ERROR, ERR_H3_MISSING_SETTINGS, ERR_H3_SETTINGS_ERROR,
    ERR_H3_STREAM_CREATION_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_QPACK_DECODER_STREAM_ERROR,
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
use crate::common::error::WebTransportError;
use crate::common::types::{Headers, SessionId, StreamId};
use crate::protocol::connection::Connection;
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::qpack::{DecodeStatus, Decoder, Encoder};
use crate::protocol::utils;

const CONTROL_FRAME_SIZE_LIMIT: usize = 4096;
const QPACK_DECODER_BLOCKED_STREAM_CAPACITY: u32 = 0;
const QPACK_DECODER_TABLE_CAPACITY: u32 = 0;
const QPACK_ENCODER_BLOCKED_STREAM_CAPACITY: u32 = 0;
const QPACK_ENCODER_TABLE_CAPACITY: u32 = 0;
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
    blocked_streams: u32,
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
    max_table_capacity: u32,
    peer_control_stream_id: Option<StreamId>,
    peer_decoder_stream_id: Option<StreamId>,
    peer_encoder_stream_id: Option<StreamId>,
    peer_max_push_id: Option<u64>,
    settings_received: bool,
    streams: FxHashMap<StreamId, H3Stream>,
}

impl H3 {
    pub(super) fn new(is_client: bool, parameters: H3Parameters) -> Self {
        Self {
            blocked_streams: QPACK_DECODER_BLOCKED_STREAM_CAPACITY,
            decoder: Decoder::new(
                QPACK_DECODER_TABLE_CAPACITY,
                QPACK_DECODER_BLOCKED_STREAM_CAPACITY,
            ),
            encoder: Encoder::new(
                QPACK_ENCODER_TABLE_CAPACITY,
                QPACK_ENCODER_BLOCKED_STREAM_CAPACITY,
            ),
            initial_max_data: parameters.initial_max_data,
            initial_max_streams_bidi: parameters.initial_max_streams_bidi,
            initial_max_streams_uni: parameters.initial_max_streams_uni,
            is_client,
            local_control_stream_id: None,
            local_decoder_stream_id: None,
            local_encoder_stream_id: None,
            max_capsule_size: parameters.max_capsule_size,
            max_field_section_size: parameters.max_field_section_size,
            max_table_capacity: QPACK_DECODER_TABLE_CAPACITY,
            peer_control_stream_id: None,
            peer_decoder_stream_id: None,
            peer_encoder_stream_id: None,
            peer_max_push_id: None,
            settings_received: false,
            streams: FxHashMap::default(),
        }
    }

    pub(super) fn bind_connect_stream(&mut self, stream_id: StreamId) -> Vec<Effect> {
        self.streams.insert(
            stream_id,
            H3Stream {
                is_connect_stream: true,
                is_request_stream: true,
                ..Default::default()
            },
        );

        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: u64::MAX,
        }]
    }

    pub(super) fn bind_local_streams(
        &mut self,
        control_stream_id: StreamId,
        encoder_stream_id: StreamId,
        decoder_stream_id: StreamId,
    ) -> Vec<Effect> {
        let settings_frame = match self.initialize_settings() {
            Ok(settings_frame) => settings_frame,
            Err(e) => {
                debug!("h3_settings encode failed err={e:?}");
                return vec![Effect::CloseQuicConnection {
                    error_code: ERR_H3_INTERNAL_ERROR,
                    reason: Some("h3_settings encode failed".into()),
                }];
            }
        };

        let [control_data, encoder_data, decoder_data] = match self.initialize_local_streams(
            control_stream_id,
            encoder_stream_id,
            decoder_stream_id,
            &settings_frame,
        ) {
            Ok(streams) => streams,
            Err(e) => {
                debug!("h3_stream encode failed err={e:?}");
                return vec![Effect::CloseQuicConnection {
                    error_code: ERR_H3_INTERNAL_ERROR,
                    reason: Some("h3_stream encode failed".into()),
                }];
            }
        };

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

    pub(super) fn cleanup_stream(&mut self, stream_id: StreamId) -> Vec<Effect> {
        let mut effects = Vec::new();
        let cancel_instructions = self.decoder.abandon_header_block(stream_id);

        if !cancel_instructions.is_empty()
            && let Some(local_decoder_stream_id) = self.local_decoder_stream_id
        {
            effects.push(Effect::SendQuicData {
                stream_id: local_decoder_stream_id,
                data: Bytes::from(cancel_instructions),
                end_stream: false,
            });
        }

        self.streams.remove(&stream_id);

        effects
    }

    pub(super) fn encode_capsule(
        stream_id: StreamId,
        capsule_type: u64,
        capsule_value: Bytes,
    ) -> Result<Vec<Bytes>, WebTransportError> {
        if !utils::is_valid_session_id(stream_id) {
            debug!("h3_stream validate invalid actual={stream_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_STREAM_CREATION_ERROR),
                "h3_stream validate invalid".into(),
            ));
        }

        let mut capsule_header = BytesMut::with_capacity(16);

        utils::write_varint(&mut capsule_header, capsule_type).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        utils::write_varint(&mut capsule_header, capsule_value.len() as u64).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        let capsule_header = capsule_header.freeze();
        let payload_len = capsule_header.len() + capsule_value.len();
        let frame_header = encode_frame_header(H3_FRAME_TYPE_DATA, payload_len)?;

        Ok(vec![frame_header, capsule_header, capsule_value])
    }

    pub(super) fn encode_connect_request(
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

        self.encode_headers(stream_id, &request_headers, false)
    }

    pub(super) fn encode_datagram(
        session_id: SessionId,
        data: Bytes,
    ) -> Result<Vec<Bytes>, WebTransportError> {
        if !utils::is_valid_session_id(session_id) {
            debug!("wt_session validate invalid actual={session_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_STREAM_CREATION_ERROR),
                "wt_session validate invalid".into(),
            ));
        }

        let mut header = BytesMut::new();

        utils::write_varint(&mut header, session_id / 4).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        Ok(vec![header.freeze(), data])
    }

    pub(super) fn encode_goaway(identifier: u64) -> Result<Bytes, WebTransportError> {
        let mut buffer = BytesMut::with_capacity(8);

        utils::write_varint(&mut buffer, identifier).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        let payload = buffer.freeze();
        let header = encode_frame_header(H3_FRAME_TYPE_GOAWAY, payload.len())?;

        let mut frame = BytesMut::with_capacity(header.len() + payload.len());
        frame.put(header);
        frame.put(payload);

        Ok(frame.freeze())
    }

    pub(super) fn encode_headers(
        &mut self,
        stream_id: StreamId,
        headers: &Headers,
        end_stream: bool,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let mut effects = Vec::new();

        let (frame_payload, encoder_instructions) =
            self.encoder.encode(stream_id, headers).map_err(|e| {
                debug!("qpack_encoder encode failed err={e:?}");
                WebTransportError::Protocol(
                    Some(ERR_QPACK_ENCODER_STREAM_ERROR),
                    "qpack_encoder encode failed".into(),
                )
            })?;

        if !encoder_instructions.is_empty()
            && let Some(local_encoder_stream_id) = self.local_encoder_stream_id
        {
            effects.push(Effect::SendQuicData {
                stream_id: local_encoder_stream_id,
                data: Bytes::from(encoder_instructions),
                end_stream: false,
            });
        }

        let frame_payload = Bytes::from(frame_payload);
        let frame_header = encode_frame_header(H3_FRAME_TYPE_HEADERS, frame_payload.len())?;

        effects.push(Effect::SendQuicData {
            stream_id,
            data: frame_header,
            end_stream: false,
        });
        effects.push(Effect::SendQuicData {
            stream_id,
            data: frame_payload,
            end_stream,
        });

        Ok(effects)
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

        if let Err(e) = utils::write_varint(&mut buffer, stream_type) {
            debug!("varint encode failed err={e:?}");
            return Vec::new();
        }

        if let Err(e) = utils::write_varint(&mut buffer, session_id) {
            debug!("varint encode failed err={e:?}");
            return Vec::new();
        }

        vec![Effect::SendQuicData {
            stream_id,
            data: buffer.freeze(),
            end_stream: false,
        }]
    }

    pub(super) fn fail_local_streams() -> Vec<Effect> {
        debug!("h3_stream create failed");
        vec![Effect::CloseQuicConnection {
            error_code: ERR_H3_GENERAL_PROTOCOL_ERROR,
            reason: Some("h3_stream create failed".into()),
        }]
    }

    pub(super) fn handle_transport_event(
        &mut self,
        event: &ProtocolEvent,
        connection: &Connection,
    ) -> (Vec<ProtocolEvent>, Vec<Effect>) {
        let mut h3_events = Vec::new();
        let mut effects = Vec::new();

        let result = match event {
            ProtocolEvent::TransportDatagramReceived { data } => {
                Self::recv_datagram(data.clone()).map(|events| (events, Vec::new()))
            }
            ProtocolEvent::TransportResetStreamReceived { stream_id, .. } => {
                self.recv_stream_reset(*stream_id)
            }
            ProtocolEvent::TransportStopSendingReceived { stream_id, .. } => {
                self.recv_stop_sending(*stream_id)
            }
            ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data,
                end_stream,
            } => {
                if utils::is_unidirectional_stream(*stream_id) {
                    self.recv_unidirectional_stream_data(*stream_id, data, *end_stream, connection)
                } else {
                    self.recv_request_data(*stream_id, data, *end_stream)
                }
            }
            ProtocolEvent::TransportStreamOpened { stream_id } => {
                Ok((Vec::new(), self.handle_stream_opened(*stream_id)))
            }
            _ => Ok((Vec::new(), Vec::new())),
        };

        match result {
            Ok((new_events, new_effects)) => {
                h3_events.extend(new_events);
                effects.extend(new_effects);
            }
            Err(e) => match e {
                WebTransportError::Stream(stream_id, error_code, _reason) => {
                    let error_code = error_code.unwrap_or(ERR_H3_INTERNAL_ERROR);
                    debug!("wt_stream abort stream_id={stream_id} err={error_code}");
                    effects.push(Effect::ResetQuicStream {
                        stream_id,
                        error_code,
                    });
                    effects.push(Effect::StopQuicStream {
                        stream_id,
                        error_code,
                    });

                    effects.extend(self.cleanup_stream(stream_id));
                }
                WebTransportError::Configuration(error_code, reason)
                | WebTransportError::Protocol(error_code, reason)
                | WebTransportError::Unknown(error_code, reason) => {
                    let error_code = error_code.unwrap_or(ERR_H3_INTERNAL_ERROR);
                    effects.push(Effect::CloseQuicConnection {
                        error_code,
                        reason: Some(reason),
                    });
                }
            },
        }

        (h3_events, effects)
    }

    pub(super) fn local_control_stream_id(&self) -> Option<StreamId> {
        self.local_control_stream_id
    }

    pub(super) fn resume_request_stream(&self, stream_id: StreamId) -> Vec<Effect> {
        let Some(stream) = self.streams.get(&stream_id) else {
            return Vec::new();
        };

        if stream.blocked || stream.ended {
            return Vec::new();
        }

        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: u64::MAX,
        }]
    }

    pub(super) fn stop_request_stream(&mut self, stream_id: StreamId) -> Vec<Effect> {
        if !self.streams.contains_key(&stream_id) {
            return Vec::new();
        }

        let mut effects = vec![Effect::StopQuicStream {
            stream_id,
            error_code: ERR_WT_SESSION_GONE,
        }];
        effects.extend(self.cleanup_stream(stream_id));

        effects
    }

    fn decode_headers(
        &mut self,
        stream_id: StreamId,
        frame_data: Option<Bytes>,
    ) -> Result<HeaderDecodeStatus, WebTransportError> {
        let mut effects = Vec::new();

        let (decoder_instructions, status) = if let Some(frame_data) = frame_data {
            let (instructions, status) = self
                .decoder
                .decode_header(stream_id, frame_data)
                .map_err(|e| {
                    debug!("h3_headers decode invalid stream_id={stream_id} err={e:?}");
                    WebTransportError::Protocol(
                        Some(ERR_QPACK_DECOMPRESSION_FAILED),
                        "h3_headers decode invalid".into(),
                    )
                })?;
            (instructions, Some(status))
        } else {
            match self.decoder.resume_header(stream_id) {
                Ok((instructions, Some(headers))) => {
                    (instructions, Some(DecodeStatus::Done(headers)))
                }
                Ok((instructions, None)) => (instructions, Some(DecodeStatus::Blocked)),
                Err(e) => {
                    debug!("h3_headers decode invalid stream_id={stream_id} err={e:?}");
                    return Err(WebTransportError::Protocol(
                        Some(ERR_QPACK_DECOMPRESSION_FAILED),
                        "h3_headers decode invalid".into(),
                    ));
                }
            }
        };

        if !decoder_instructions.is_empty()
            && let Some(local_decoder_stream_id) = self.local_decoder_stream_id
        {
            effects.push(Effect::SendQuicData {
                stream_id: local_decoder_stream_id,
                data: Bytes::from(decoder_instructions),
                end_stream: false,
            });
        }

        match status {
            Some(DecodeStatus::Done(headers)) => Ok(HeaderDecodeStatus::Done(headers, effects)),
            Some(DecodeStatus::Blocked) => Ok(HeaderDecodeStatus::Blocked),
            None => {
                debug!("h3_headers decode invalid stream_id={stream_id}");
                Err(WebTransportError::Protocol(
                    Some(ERR_H3_INTERNAL_ERROR),
                    "h3_headers decode invalid".into(),
                ))
            }
        }
    }

    fn handle_control_frame(
        &mut self,
        frame_type: u64,
        frame_data: &[u8],
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut effects = Vec::new();
        let mut events = Vec::new();

        if frame_type != H3_FRAME_TYPE_SETTINGS && !self.settings_received {
            debug!(
                "h3_settings validate invalid actual={frame_type} expected=h3_frame_type_settings"
            );
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_MISSING_SETTINGS),
                "h3_settings validate invalid".into(),
            ));
        }

        match frame_type {
            H3_FRAME_TYPE_CANCEL_PUSH => {
                parse_push_id(frame_data)?;
                debug!("h3_cancel_push validate invalid");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_ID_ERROR),
                    "h3_cancel_push validate invalid".into(),
                ));
            }
            H3_FRAME_TYPE_DATA
            | H3_FRAME_TYPE_HEADERS
            | H3_FRAME_TYPE_PUSH_PROMISE
            | H3_FRAME_TYPE_RESERVED_CONTINUATION
            | H3_FRAME_TYPE_RESERVED_PING
            | H3_FRAME_TYPE_RESERVED_PRIORITY
            | H3_FRAME_TYPE_RESERVED_WINDOW_UPDATE => {
                debug!("h3_control_stream validate invalid");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_FRAME_UNEXPECTED),
                    "h3_control_stream validate invalid".into(),
                ));
            }
            H3_FRAME_TYPE_GOAWAY => {
                debug!("h3_goaway receive");
                events.push(ProtocolEvent::H3GoawayReceived);
            }
            H3_FRAME_TYPE_MAX_PUSH_ID => {
                if self.is_client {
                    debug!("h3_max_push_id validate invalid");
                    return Err(WebTransportError::Protocol(
                        Some(ERR_H3_FRAME_UNEXPECTED),
                        "h3_max_push_id validate invalid".into(),
                    ));
                }

                let peer_max_push_id = parse_push_id(frame_data)?;

                if self.peer_max_push_id > Some(peer_max_push_id) {
                    debug!("h3_max_push_id validate invalid actual={peer_max_push_id}");
                    return Err(WebTransportError::Protocol(
                        Some(ERR_H3_ID_ERROR),
                        "h3_max_push_id validate invalid".into(),
                    ));
                }
                self.peer_max_push_id = Some(peer_max_push_id);
                debug!("h3_max_push_id receive");
            }
            H3_FRAME_TYPE_SETTINGS => {
                if self.settings_received {
                    debug!("h3_settings validate invalid");
                    return Err(WebTransportError::Protocol(
                        Some(ERR_H3_FRAME_UNEXPECTED),
                        "h3_settings validate invalid".into(),
                    ));
                }
                let settings = parse_settings(frame_data)?;
                validate_settings(&settings, connection)?;

                let encoder_instructions = self
                    .encoder
                    .apply_settings(
                        settings.qpack_max_table_capacity.unwrap_or(0),
                        settings.qpack_blocked_streams.unwrap_or(0),
                    )
                    .map_err(|e| {
                        debug!("qpack_encoder validate failed err={e:?}");
                        WebTransportError::Protocol(
                            Some(ERR_H3_INTERNAL_ERROR),
                            "qpack_encoder validate failed".into(),
                        )
                    })?;

                if !encoder_instructions.is_empty()
                    && let Some(stream_id) = self.local_encoder_stream_id
                {
                    effects.push(Effect::SendQuicData {
                        stream_id,
                        data: Bytes::from(encoder_instructions),
                        end_stream: false,
                    });
                }
                self.settings_received = true;
                events.push(ProtocolEvent::H3SettingsReceived { settings });
            }
            H3_FRAME_TYPE_WT_STREAM => {
                debug!("h3_control_stream validate invalid");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_FRAME_ERROR),
                    "h3_control_stream validate invalid".into(),
                ));
            }
            _ => {}
        }

        Ok((events, effects))
    }

    fn handle_request_frame(
        &mut self,
        stream_id: StreamId,
        stream: &mut H3Stream,
        frame_type: u64,
        frame_data: Option<Bytes>,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut events = Vec::new();
        let mut effects = Vec::new();

        match frame_type {
            H3_FRAME_TYPE_CANCEL_PUSH
            | H3_FRAME_TYPE_GOAWAY
            | H3_FRAME_TYPE_MAX_PUSH_ID
            | H3_FRAME_TYPE_RESERVED_CONTINUATION
            | H3_FRAME_TYPE_RESERVED_PING
            | H3_FRAME_TYPE_RESERVED_PRIORITY
            | H3_FRAME_TYPE_RESERVED_WINDOW_UPDATE
            | H3_FRAME_TYPE_SETTINGS => {
                debug!("h3_stream validate invalid actual={frame_type} stream_id={stream_id}");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_FRAME_UNEXPECTED),
                    "h3_stream validate invalid".into(),
                ));
            }
            H3_FRAME_TYPE_DATA => {
                let payload = frame_data.unwrap_or_default();

                if stream.is_connect_stream {
                    if stream.headers_processed {
                        if !payload.is_empty() {
                            stream.capsule_buffer.extend_from_slice(&payload);
                        }
                        if !stream.capsule_buffer.is_empty() {
                            events.extend(self.parse_capsules(stream_id, stream)?);
                        }
                    }
                } else if !payload.is_empty() {
                    trace!(
                        "h3_frame receive size={} stream_id={stream_id}",
                        payload.len()
                    );
                }
            }
            H3_FRAME_TYPE_HEADERS => {
                if stream.headers_processed {
                    debug!("h3_headers validate failed stream_id={stream_id}");
                    return Err(WebTransportError::Stream(
                        stream_id,
                        Some(ERR_H3_FRAME_UNEXPECTED),
                        "h3_headers validate failed".into(),
                    ));
                }

                match self.decode_headers(stream_id, frame_data.clone())? {
                    HeaderDecodeStatus::Blocked => {
                        stream.blocked = true;
                        stream.frame_type = Some(H3_FRAME_TYPE_HEADERS);
                        return Ok((events, effects));
                    }
                    HeaderDecodeStatus::Done(headers, decoder_effects) => {
                        effects.extend(decoder_effects);

                        if self.is_client {
                            validate_response_headers(stream_id, &headers)?;
                        } else {
                            validate_request_headers(stream_id, &headers)?;
                        }

                        let is_webtransport_upgrade = utils::find_header(&headers, b":protocol")
                            .is_some_and(|protocol| protocol == WT_UPGRADE_TOKEN);

                        if is_webtransport_upgrade {
                            stream.is_connect_stream = true;
                        }
                        stream.blocked = false;
                        stream.headers_processed = true;

                        let len = frame_data
                            .as_ref()
                            .map(Bytes::len)
                            .unwrap_or_else(|| stream.blocked_frame_size.take().unwrap_or(0));
                        trace!("h3_headers receive size={len} stream_id={stream_id}");

                        events.push(ProtocolEvent::H3HeadersReceived { stream_id, headers });
                    }
                }
            }
            H3_FRAME_TYPE_PUSH_PROMISE => {
                debug!("h3_push_promise validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_FRAME_UNEXPECTED),
                    "h3_push_promise validate invalid".into(),
                ));
            }
            H3_FRAME_TYPE_WT_STREAM => {
                debug!("h3_stream validate invalid actual={frame_type} stream_id={stream_id}");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_FRAME_ERROR),
                    "h3_stream validate invalid".into(),
                ));
            }
            _ => {}
        }

        Ok((events, effects))
    }

    fn handle_stream_opened(&mut self, stream_id: StreamId) -> Vec<Effect> {
        self.streams.insert(stream_id, H3Stream::default());

        vec![Effect::ReceiveQuicData {
            stream_id,
            max_size: 1,
        }]
    }

    fn initialize_local_streams(
        &mut self,
        control_stream_id: StreamId,
        encoder_stream_id: StreamId,
        decoder_stream_id: StreamId,
        settings_frame: &Bytes,
    ) -> Result<[Bytes; 3], WebTransportError> {
        self.set_local_stream_ids(control_stream_id, encoder_stream_id, decoder_stream_id)?;

        let mut control_data = BytesMut::new();
        utils::write_varint(&mut control_data, H3_STREAM_TYPE_CONTROL).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
        control_data.extend_from_slice(settings_frame);

        let mut encoder_data = BytesMut::new();
        utils::write_varint(&mut encoder_data, H3_STREAM_TYPE_QPACK_ENCODER).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        let mut decoder_data = BytesMut::new();
        utils::write_varint(&mut decoder_data, H3_STREAM_TYPE_QPACK_DECODER).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        Ok([
            control_data.freeze(),
            encoder_data.freeze(),
            decoder_data.freeze(),
        ])
    }

    fn initialize_settings(&mut self) -> Result<Bytes, WebTransportError> {
        let settings = H3Settings {
            enable_connect_protocol: Some(1),
            h3_datagram: Some(1),
            max_field_section_size: Some(self.max_field_section_size),
            qpack_blocked_streams: Some(u64::from(self.blocked_streams)),
            qpack_max_table_capacity: Some(u64::from(self.max_table_capacity)),
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

        Ok(frame.freeze())
    }

    fn parse_capsules(
        &mut self,
        stream_id: StreamId,
        stream: &mut H3Stream,
    ) -> Result<Vec<ProtocolEvent>, WebTransportError> {
        let mut events = Vec::new();
        let max_capsule_size = self.max_capsule_size;

        loop {
            if stream.capsule_buffer.is_empty() {
                break;
            }

            let mut cursor = Cursor::new(&stream.capsule_buffer[..]);
            let Ok(capsule_type) = utils::read_varint(&mut cursor) else {
                break;
            };
            let Ok(capsule_length) = utils::read_varint(&mut cursor) else {
                break;
            };

            if capsule_length > max_capsule_size {
                debug!(
                    "wt_capsule validate exceeded actual={capsule_length} limit={max_capsule_size} stream_id={stream_id}"
                );
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "wt_capsule validate exceeded".into(),
                ));
            }

            let needed = usize::try_from(capsule_length).map_err(|e| {
                debug!("wt_capsule convert failed expected=usize stream_id={stream_id} err={e:?}");
                WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "wt_capsule convert failed".into(),
                )
            })?;

            let header_len = usize::try_from(cursor.position()).map_err(|e| {
                debug!("wt_capsule convert failed expected=usize stream_id={stream_id} err={e:?}");
                WebTransportError::Protocol(
                    Some(ERR_H3_INTERNAL_ERROR),
                    "wt_capsule convert failed".into(),
                )
            })?;

            if stream.capsule_buffer.len() < header_len + needed {
                break;
            }

            stream.capsule_buffer.advance(header_len);
            let capsule_value = stream.capsule_buffer.split_to(needed).freeze();

            events.push(ProtocolEvent::H3CapsuleReceived {
                stream_id,
                capsule_type,
                capsule_value,
            });
        }

        Ok(events)
    }

    fn parse_stream_data(
        &mut self,
        stream_id: StreamId,
        stream: &mut H3Stream,
        stream_ended: bool,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut h3_events = Vec::new();
        let mut effects = Vec::new();
        let max_field_section_size =
            usize::try_from(self.max_field_section_size).unwrap_or(usize::MAX);

        if stream.blocked {
            if let Ok(HeaderDecodeStatus::Done(..)) = self.decode_headers(stream_id, None) {
                stream.blocked = false;
            } else {
                return Ok((h3_events, effects));
            }
        }

        loop {
            if stream.blocked {
                break;
            }
            if stream.buffer.is_empty() && !stream_ended {
                break;
            }

            if stream.frame_type.is_none() {
                let mut cursor = Cursor::new(&stream.buffer[..]);
                if let (Ok(frame_type), Ok(frame_size)) = (
                    utils::read_varint(&mut cursor),
                    utils::read_varint(&mut cursor),
                ) {
                    let frame_size = usize::try_from(frame_size).map_err(|e| {
                        debug!(
                            "varint convert failed expected=usize stream_id={stream_id} err={e:?}"
                        );
                        WebTransportError::Protocol(
                            Some(ERR_H3_EXCESSIVE_LOAD),
                            "varint convert failed".into(),
                        )
                    })?;
                    let position = usize::try_from(cursor.position()).map_err(|e| {
                        debug!(
                            "varint convert failed expected=usize stream_id={stream_id} err={e:?}"
                        );
                        WebTransportError::Protocol(
                            Some(ERR_H3_INTERNAL_ERROR),
                            "varint convert failed".into(),
                        )
                    })?;

                    if frame_type == H3_FRAME_TYPE_HEADERS && frame_size > max_field_section_size {
                        debug!(
                            "h3_field_section validate exceeded actual={frame_size} limit={max_field_section_size} stream_id={stream_id}"
                        );
                        return Err(WebTransportError::Stream(
                            stream_id,
                            Some(ERR_H3_MESSAGE_ERROR),
                            "h3_field_section validate exceeded".into(),
                        ));
                    }

                    if frame_type != H3_FRAME_TYPE_DATA
                        && frame_type != H3_FRAME_TYPE_HEADERS
                        && frame_size > CONTROL_FRAME_SIZE_LIMIT
                    {
                        debug!(
                            "h3_frame validate exceeded actual={frame_size} expected=control_frame_size_limit stream_id={stream_id}"
                        );
                        return Err(WebTransportError::Protocol(
                            Some(ERR_H3_FRAME_ERROR),
                            "h3_frame validate exceeded".into(),
                        ));
                    }

                    stream.frame_type = Some(frame_type);
                    stream.frame_size = Some(frame_size);
                    if frame_type == H3_FRAME_TYPE_HEADERS {
                        stream.blocked_frame_size = Some(frame_size);
                    }

                    if frame_type == H3_FRAME_TYPE_DATA {
                        trace!("h3_frame receive size={frame_size} stream_id={stream_id}");
                    }

                    stream.buffer.advance(position);
                } else {
                    break;
                }
            }

            let Some(current_type) = stream.frame_type else {
                break;
            };
            let Some(current_size) = stream.frame_size else {
                break;
            };

            let remaining_in_buffer = stream.buffer.len();
            let chunk_size = cmp::min(current_size, remaining_in_buffer);

            if current_type != H3_FRAME_TYPE_DATA && chunk_size < current_size {
                break;
            }

            let frame_data = stream.buffer.split_to(chunk_size).freeze();
            let is_last_chunk = chunk_size == current_size;

            stream.frame_size = Some(current_size - chunk_size);

            let data_to_process = if is_last_chunk {
                Some(frame_data.clone())
            } else {
                None
            };
            let final_data = if current_type == H3_FRAME_TYPE_DATA {
                Some(frame_data)
            } else {
                data_to_process
            };

            if final_data.is_some() {
                let (new_events, new_effects) =
                    self.handle_request_frame(stream_id, stream, current_type, final_data)?;
                h3_events.extend(new_events);
                effects.extend(new_effects);
            }

            if is_last_chunk {
                stream.frame_type = None;
                stream.frame_size = None;
                if current_type == H3_FRAME_TYPE_HEADERS && !stream.blocked {
                    stream.blocked_frame_size = None;
                }
            }

            if chunk_size == 0 && !stream_ended {
                break;
            }
        }

        Ok((h3_events, effects))
    }

    fn parse_stream_header(
        &mut self,
        stream_id: StreamId,
        stream: H3Stream,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut cursor = Cursor::new(&stream.buffer[..]);

        let Ok(session_id) = utils::read_varint(&mut cursor) else {
            if stream.ended {
                return Ok((Vec::new(), self.cleanup_stream(stream_id)));
            }

            self.streams.insert(stream_id, stream);

            return Ok((
                Vec::new(),
                vec![Effect::ReceiveQuicData {
                    stream_id,
                    max_size: 1,
                }],
            ));
        };

        if !utils::is_valid_session_id(session_id) {
            debug!("wt_session validate invalid actual={session_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_ID_ERROR),
                "wt_session validate invalid".into(),
            ));
        }

        Ok((
            vec![ProtocolEvent::H3WebTransportStreamOpened {
                session_id,
                stream_id,
            }],
            Vec::new(),
        ))
    }

    fn recv_datagram(mut data: Bytes) -> Result<Vec<ProtocolEvent>, WebTransportError> {
        let mut cursor = Cursor::new(&data[..]);

        let quarter_stream_id = utils::read_varint(&mut cursor).map_err(|e| {
            debug!("varint decode invalid err={e:?}");
            WebTransportError::Protocol(Some(ERR_H3_DATAGRAM_ERROR), "varint decode invalid".into())
        })?;
        let session_id = quarter_stream_id * 4;

        if session_id > QUIC_VARINT_LIMIT {
            debug!("wt_datagram validate exceeded actual={session_id} expected=quic_varint_limit");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_DATAGRAM_ERROR),
                "wt_datagram validate exceeded".into(),
            ));
        }

        let position = usize::try_from(cursor.position()).map_err(|e| {
            debug!("wt_datagram convert failed expected=usize session_id={session_id} err={e:?}");
            WebTransportError::Protocol(
                Some(ERR_H3_INTERNAL_ERROR),
                "wt_datagram convert failed".into(),
            )
        })?;

        data.advance(position);

        Ok(vec![ProtocolEvent::H3DatagramReceived { session_id, data }])
    }

    fn recv_request_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        stream_ended: bool,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut stream = self.streams.remove(&stream_id).unwrap_or_default();

        if !data.is_empty() {
            stream.buffer.extend_from_slice(data);
        }
        if stream_ended {
            stream.ended = true;
        }

        if !stream.is_request_stream && stream.stream_type.is_none() {
            let mut cursor = Cursor::new(&stream.buffer[..]);

            match utils::read_varint(&mut cursor) {
                Ok(H3_FRAME_TYPE_WT_STREAM) => {
                    stream.buffer.clear();
                    stream.stream_type = Some(H3_STREAM_TYPE_WEBTRANSPORT);
                }
                Ok(_) => {
                    if self.is_client {
                        debug!("h3_stream validate invalid stream_id={stream_id}");
                        return Err(WebTransportError::Protocol(
                            Some(ERR_H3_STREAM_CREATION_ERROR),
                            "h3_stream validate invalid".into(),
                        ));
                    }
                    stream.is_request_stream = true;
                }
                Err(_) => {
                    if stream.ended {
                        return Ok((Vec::new(), self.cleanup_stream(stream_id)));
                    }

                    self.streams.insert(stream_id, stream);

                    return Ok((
                        Vec::new(),
                        vec![Effect::ReceiveQuicData {
                            stream_id,
                            max_size: 1,
                        }],
                    ));
                }
            }
        }

        if stream.stream_type == Some(H3_STREAM_TYPE_WEBTRANSPORT) {
            return self.parse_stream_header(stream_id, stream);
        }

        let was_headers_processed = stream.headers_processed;
        let is_ended = stream.ended;
        let (mut events, mut effects) = self.parse_stream_data(stream_id, &mut stream, is_ended)?;

        if stream.ended && stream.buffer.is_empty() {
            if stream.is_connect_stream {
                debug!("wt_stream close stream_id={stream_id}");
                events.push(ProtocolEvent::H3ConnectStreamClosed { stream_id });
            }
            effects.extend(self.cleanup_stream(stream_id));

            return Ok((events, effects));
        }

        let is_headers_received = !was_headers_processed && stream.headers_processed;

        if !stream.blocked && !stream.ended && !is_headers_received {
            effects.push(Effect::ReceiveQuicData {
                stream_id,
                max_size: u64::MAX,
            });
        }
        self.streams.insert(stream_id, stream);

        Ok((events, effects))
    }

    fn recv_stop_sending(
        &self,
        stream_id: StreamId,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let is_critical_stream = [
            self.local_control_stream_id,
            self.local_decoder_stream_id,
            self.local_encoder_stream_id,
        ]
        .contains(&Some(stream_id));

        if is_critical_stream {
            debug!("h3_control_stream validate invalid stream_id={stream_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_CLOSED_CRITICAL_STREAM),
                "h3_control_stream validate invalid".into(),
            ));
        }

        Ok((Vec::new(), Vec::new()))
    }

    fn recv_stream_reset(
        &mut self,
        stream_id: StreamId,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let is_critical_stream = [
            self.peer_control_stream_id,
            self.peer_decoder_stream_id,
            self.peer_encoder_stream_id,
        ]
        .contains(&Some(stream_id));

        if is_critical_stream {
            debug!("h3_control_stream validate invalid stream_id={stream_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_CLOSED_CRITICAL_STREAM),
                "h3_control_stream validate invalid".into(),
            ));
        }

        Ok((Vec::new(), self.cleanup_stream(stream_id)))
    }

    fn recv_unidirectional_stream_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut stream = self.streams.remove(&stream_id).unwrap_or_default();

        if !data.is_empty() {
            stream.buffer.extend_from_slice(data);
        }
        if stream_ended {
            stream.ended = true;
        }

        let mut events = Vec::new();
        let mut effects = Vec::new();

        let stream_type = if let Some(stream_type) = stream.stream_type {
            stream_type
        } else {
            let mut cursor = Cursor::new(&stream.buffer[..]);
            let Ok(stream_type) = utils::read_varint(&mut cursor) else {
                if stream.ended {
                    return Ok((Vec::new(), self.cleanup_stream(stream_id)));
                }

                self.streams.insert(stream_id, stream);

                return Ok((
                    Vec::new(),
                    vec![Effect::ReceiveQuicData {
                        stream_id,
                        max_size: 1,
                    }],
                ));
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
                    return Err(WebTransportError::Protocol(
                        Some(error_code),
                        "h3_push_stream validate invalid".into(),
                    ));
                }
                H3_STREAM_TYPE_WEBTRANSPORT => {}
                _ => {
                    debug!("h3_stream validate invalid actual={stream_type} stream_id={stream_id}");
                    return Ok((
                        Vec::new(),
                        vec![Effect::StopQuicStream {
                            stream_id,
                            error_code: ERR_H3_STREAM_CREATION_ERROR,
                        }],
                    ));
                }
            }

            stream.stream_type = Some(stream_type);
            stream_type
        };

        match stream_type {
            H3_STREAM_TYPE_CONTROL => {
                if stream.ended {
                    debug!("h3_control_stream validate invalid stream_id={stream_id}");
                    return Err(WebTransportError::Protocol(
                        Some(ERR_H3_CLOSED_CRITICAL_STREAM),
                        "h3_control_stream validate invalid".into(),
                    ));
                }

                loop {
                    if stream.buffer.is_empty() {
                        break;
                    }
                    let mut cursor = Cursor::new(&stream.buffer[..]);
                    let Ok(frame_type) = utils::read_varint(&mut cursor) else {
                        break;
                    };
                    let Ok(frame_size) = utils::read_varint(&mut cursor) else {
                        break;
                    };

                    if frame_type == H3_FRAME_TYPE_DATA {
                        debug!(
                            "h3_control_stream validate invalid actual={frame_type} stream_id={stream_id}"
                        );
                        return Err(WebTransportError::Protocol(
                            Some(ERR_H3_FRAME_UNEXPECTED),
                            "h3_control_stream validate invalid".into(),
                        ));
                    }

                    let needed = usize::try_from(frame_size).map_err(|e| {
                        debug!(
                            "varint convert failed expected=usize stream_id={stream_id} err={e:?}"
                        );
                        WebTransportError::Protocol(
                            Some(ERR_H3_EXCESSIVE_LOAD),
                            "varint convert failed".into(),
                        )
                    })?;
                    let header_len = usize::try_from(cursor.position()).map_err(|e| {
                        debug!(
                            "varint convert failed expected=usize stream_id={stream_id} err={e:?}"
                        );
                        WebTransportError::Protocol(
                            Some(ERR_H3_INTERNAL_ERROR),
                            "varint convert failed".into(),
                        )
                    })?;

                    if frame_type != H3_FRAME_TYPE_HEADERS && needed > CONTROL_FRAME_SIZE_LIMIT {
                        debug!(
                            "h3_frame validate exceeded actual={needed} expected=control_frame_size_limit stream_id={stream_id}"
                        );
                        return Err(WebTransportError::Protocol(
                            Some(ERR_H3_FRAME_ERROR),
                            "h3_frame validate exceeded".into(),
                        ));
                    }

                    if stream.buffer.len() < header_len + needed {
                        break;
                    }

                    stream.buffer.advance(header_len);
                    let frame_data = stream.buffer.split_to(needed).freeze();

                    let (new_events, new_effects) =
                        self.handle_control_frame(frame_type, &frame_data, connection)?;
                    events.extend(new_events);
                    effects.extend(new_effects);
                }
            }
            H3_STREAM_TYPE_QPACK_DECODER => {
                if !stream.buffer.is_empty() {
                    let decoder_instructions = stream.buffer.split_to(stream.buffer.len()).freeze();
                    self.encoder.feed_decoder(&decoder_instructions).map_err(|e| {
                        debug!(
                            "qpack_decoder_stream decode invalid stream_id={stream_id} err={e:?}"
                        );
                        WebTransportError::Protocol(
                            Some(ERR_QPACK_DECODER_STREAM_ERROR),
                            "qpack_decoder_stream decode invalid".into(),
                        )
                    })?;
                }
            }
            H3_STREAM_TYPE_QPACK_ENCODER => {
                if !stream.buffer.is_empty() {
                    let encoder_instructions = stream.buffer.split_to(stream.buffer.len()).freeze();
                    match self.decoder.feed_encoder(&encoder_instructions) {
                        Ok(unblocked) => {
                            for unblocked_stream_id in unblocked {
                                let should_resume = {
                                    if let Some(unblocked_stream) =
                                        self.streams.get_mut(&unblocked_stream_id)
                                    {
                                        if unblocked_stream.blocked {
                                            unblocked_stream.blocked = false;
                                            Some(unblocked_stream.ended)
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                };

                                if let Some(ended) = should_resume {
                                    let (new_events, new_effects) =
                                        self.recv_request_data(unblocked_stream_id, &[], ended)?;
                                    events.extend(new_events);
                                    effects.extend(new_effects);
                                }
                            }
                        }
                        Err(e) => {
                            debug!(
                                "qpack_encoder_stream decode invalid stream_id={stream_id} err={e:?}"
                            );
                            return Err(WebTransportError::Protocol(
                                Some(ERR_QPACK_ENCODER_STREAM_ERROR),
                                "qpack_encoder_stream decode invalid".into(),
                            ));
                        }
                    }
                }
            }
            H3_STREAM_TYPE_WEBTRANSPORT => {
                return self.parse_stream_header(stream_id, stream);
            }
            _ => {}
        }

        if stream.ended {
            debug!("h3_control_stream validate invalid stream_id={stream_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_CLOSED_CRITICAL_STREAM),
                "h3_control_stream validate invalid".into(),
            ));
        }

        effects.push(Effect::ReceiveQuicData {
            stream_id,
            max_size: u64::MAX,
        });
        self.streams.insert(stream_id, stream);

        Ok((events, effects))
    }

    fn register_peer_infrastructure_stream(
        &mut self,
        stream_id: StreamId,
        stream_type: u64,
    ) -> Result<(), WebTransportError> {
        let is_id_clash = [
            self.peer_control_stream_id,
            self.peer_decoder_stream_id,
            self.peer_encoder_stream_id,
        ]
        .contains(&Some(stream_id));

        if is_id_clash {
            debug!("h3_stream validate failed stream_id={stream_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_STREAM_CREATION_ERROR),
                "h3_stream validate failed".into(),
            ));
        }

        let role_occupied = match stream_type {
            H3_STREAM_TYPE_CONTROL => self.peer_control_stream_id.is_some(),
            H3_STREAM_TYPE_QPACK_DECODER => self.peer_decoder_stream_id.is_some(),
            H3_STREAM_TYPE_QPACK_ENCODER => self.peer_encoder_stream_id.is_some(),
            _ => false,
        };

        if role_occupied {
            debug!("h3_stream validate failed actual={stream_type} stream_id={stream_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_STREAM_CREATION_ERROR),
                "h3_stream validate failed".into(),
            ));
        }

        match stream_type {
            H3_STREAM_TYPE_CONTROL => self.peer_control_stream_id = Some(stream_id),
            H3_STREAM_TYPE_QPACK_DECODER => self.peer_decoder_stream_id = Some(stream_id),
            H3_STREAM_TYPE_QPACK_ENCODER => self.peer_encoder_stream_id = Some(stream_id),
            _ => {}
        }

        Ok(())
    }

    fn set_local_stream_ids(
        &mut self,
        control_stream_id: StreamId,
        encoder_stream_id: StreamId,
        decoder_stream_id: StreamId,
    ) -> Result<(), WebTransportError> {
        if !utils::is_unidirectional_stream(control_stream_id) {
            debug!(
                "h3_control_stream validate invalid actual={control_stream_id} expected=unidirectional"
            );
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_ID_ERROR),
                "h3_control_stream validate invalid".into(),
            ));
        }

        if !utils::is_unidirectional_stream(encoder_stream_id) {
            debug!(
                "qpack_encoder_stream validate invalid actual={encoder_stream_id} expected=unidirectional"
            );
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_ID_ERROR),
                "qpack_encoder_stream validate invalid".into(),
            ));
        }

        if !utils::is_unidirectional_stream(decoder_stream_id) {
            debug!(
                "qpack_decoder_stream validate invalid actual={decoder_stream_id} expected=unidirectional"
            );
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_ID_ERROR),
                "qpack_decoder_stream validate invalid".into(),
            ));
        }

        if control_stream_id == encoder_stream_id
            || control_stream_id == decoder_stream_id
            || encoder_stream_id == decoder_stream_id
        {
            debug!("h3_stream validate invalid");
            return Err(WebTransportError::Protocol(
                Some(ERR_LIB_INTERNAL_ERROR),
                "h3_stream validate invalid".into(),
            ));
        }

        if self.local_control_stream_id.is_some()
            || self.local_encoder_stream_id.is_some()
            || self.local_decoder_stream_id.is_some()
        {
            debug!("h3_stream validate failed");
            return Err(WebTransportError::Protocol(
                Some(ERR_LIB_INTERNAL_ERROR),
                "h3_stream validate failed".into(),
            ));
        }

        self.local_control_stream_id = Some(control_stream_id);
        self.local_encoder_stream_id = Some(encoder_stream_id);
        self.local_decoder_stream_id = Some(decoder_stream_id);

        debug!("h3_control_stream create stream_id={control_stream_id}");
        debug!("qpack_encoder_stream create stream_id={encoder_stream_id}");
        debug!("qpack_decoder_stream create stream_id={decoder_stream_id}");

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

#[derive(Debug, Default)]
struct H3Stream {
    blocked: bool,
    blocked_frame_size: Option<usize>,
    buffer: BytesMut,
    capsule_buffer: BytesMut,
    ended: bool,
    frame_size: Option<usize>,
    frame_type: Option<u64>,
    headers_processed: bool,
    is_connect_stream: bool,
    is_request_stream: bool,
    stream_type: Option<u64>,
}

enum HeaderDecodeStatus {
    Blocked,
    Done(Headers, Vec<Effect>),
}

fn encode_frame_header(frame_type: u64, payload_length: usize) -> Result<Bytes, WebTransportError> {
    let mut buffer = BytesMut::with_capacity(16);

    utils::write_varint(&mut buffer, frame_type).map_err(|e| {
        debug!("varint encode failed err={e:?}");
        WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
    })?;

    utils::write_varint(&mut buffer, payload_length as u64).map_err(|e| {
        debug!("varint encode failed err={e:?}");
        WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
    })?;

    Ok(buffer.freeze())
}

fn encode_settings(settings: &H3Settings) -> Result<Bytes, WebTransportError> {
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
        utils::write_varint(&mut buffer, identifier).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
        utils::write_varint(&mut buffer, value).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
    }

    for &(identifier, value) in &settings.unknown {
        utils::write_varint(&mut buffer, identifier).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
        utils::write_varint(&mut buffer, value).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
    }

    Ok(buffer.freeze())
}

fn parse_push_id(data: &[u8]) -> Result<u64, WebTransportError> {
    let mut cursor = Cursor::new(data);

    let push_id = utils::read_varint(&mut cursor).map_err(|e| {
        debug!("varint decode invalid err={e:?}");
        WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), "varint decode invalid".into())
    })?;

    if cursor.has_remaining() {
        debug!("h3_frame validate invalid");
        return Err(WebTransportError::Protocol(
            Some(ERR_H3_FRAME_ERROR),
            "h3_frame validate invalid".into(),
        ));
    }

    Ok(push_id)
}

fn parse_settings(data: &[u8]) -> Result<H3Settings, WebTransportError> {
    let mut settings = H3Settings::default();
    let mut cursor = Cursor::new(data);
    let mut count = 0usize;

    while cursor.has_remaining() {
        if count >= SETTINGS_ENTRIES_LIMIT {
            debug!("h3_settings validate exceeded actual={count} expected=settings_entries_limit");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_SETTINGS_ERROR),
                "h3_settings validate exceeded".into(),
            ));
        }
        let identifier = utils::read_varint(&mut cursor).map_err(|e| {
            debug!("varint decode invalid err={e:?}");
            WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), "varint decode invalid".into())
        })?;
        let value = utils::read_varint(&mut cursor).map_err(|e| {
            debug!("varint decode invalid err={e:?}");
            WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), "varint decode invalid".into())
        })?;

        if matches!(
            identifier,
            SETTINGS_RESERVED_ENABLE_PUSH
                | SETTINGS_RESERVED_INITIAL_WINDOW_SIZE
                | SETTINGS_RESERVED_MAX_CONCURRENT_STREAMS
                | SETTINGS_RESERVED_MAX_FRAME_SIZE
        ) {
            debug!("h3_settings validate invalid actual={identifier}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_SETTINGS_ERROR),
                "h3_settings validate invalid".into(),
            ));
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
                    return Err(WebTransportError::Protocol(
                        Some(ERR_H3_SETTINGS_ERROR),
                        "h3_settings validate invalid".into(),
                    ));
                }
                settings.unknown.push((identifier, value));
                count += 1;
                continue;
            }
        };

        if slot.is_some() {
            debug!("h3_settings validate invalid actual={identifier}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_SETTINGS_ERROR),
                "h3_settings validate invalid".into(),
            ));
        }
        *slot = Some(value);
        count += 1;
    }

    Ok(settings)
}

fn validate_request_headers(
    stream_id: StreamId,
    headers: &Headers,
) -> Result<(), WebTransportError> {
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
            return Err(WebTransportError::Stream(
                stream_id,
                Some(ERR_H3_MESSAGE_ERROR),
                "h3_headers validate invalid".into(),
            ));
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
                    return Err(WebTransportError::Stream(
                        stream_id,
                        Some(ERR_H3_MESSAGE_ERROR),
                        "h3_headers validate invalid".into(),
                    ));
                }
            };
            if after_pseudo || pseudo_header.replace(value).is_some() {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
                ));
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
        return Err(WebTransportError::Stream(
            stream_id,
            Some(ERR_H3_MESSAGE_ERROR),
            "h3_headers validate invalid".into(),
        ));
    }

    if matches!(scheme, Some(b"http" | b"https")) {
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
            return Err(WebTransportError::Stream(
                stream_id,
                Some(ERR_H3_MESSAGE_ERROR),
                "h3_headers validate invalid".into(),
            ));
        }
    }

    Ok(())
}

fn validate_response_headers(
    stream_id: StreamId,
    headers: &Headers,
) -> Result<(), WebTransportError> {
    let mut after_pseudo = false;
    let mut status: Option<&[u8]> = None;

    for (key, value) in headers {
        let key = key.as_ref();
        let value = value.as_ref();

        if !utils::is_valid_header(key, value)
            || utils::is_connection_specific_header(key, value, false)
        {
            debug!("h3_headers validate invalid stream_id={stream_id}");
            return Err(WebTransportError::Stream(
                stream_id,
                Some(ERR_H3_MESSAGE_ERROR),
                "h3_headers validate invalid".into(),
            ));
        }

        if key.starts_with(b":") {
            if after_pseudo || key != b":status" || status.replace(value).is_some() {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
                ));
            }
        } else {
            after_pseudo = true;
        }
    }

    if status.is_none() {
        debug!("h3_headers validate invalid stream_id={stream_id}");
        return Err(WebTransportError::Stream(
            stream_id,
            Some(ERR_H3_MESSAGE_ERROR),
            "h3_headers validate invalid".into(),
        ));
    }

    Ok(())
}

fn validate_settings(
    settings: &H3Settings,
    connection: &Connection,
) -> Result<(), WebTransportError> {
    if settings
        .enable_connect_protocol
        .is_some_and(|value| value != 1)
    {
        debug!("h3_settings validate invalid expected=enable_connect_protocol");
        return Err(WebTransportError::Protocol(
            Some(ERR_H3_SETTINGS_ERROR),
            "h3_settings validate invalid".into(),
        ));
    }

    let quic_supports_datagrams = connection
        .peer_max_datagram_frame_size()
        .unwrap_or_default()
        > 0;

    if !quic_supports_datagrams && settings.h3_datagram == Some(1) {
        debug!("h3_settings validate invalid expected=max_datagram_frame_size");
        return Err(WebTransportError::Protocol(
            Some(ERR_H3_SETTINGS_ERROR),
            "h3_settings validate invalid".into(),
        ));
    }

    if connection.is_client() && settings.wt_enabled.is_some_and(|value| value > 1) {
        debug!("h3_settings validate invalid expected=wt_enabled");
        return Err(WebTransportError::Protocol(
            Some(ERR_H3_SETTINGS_ERROR),
            "h3_settings validate invalid".into(),
        ));
    }

    if settings.wt_enabled.unwrap_or(0) == 0 {
        debug!("h3_settings validate invalid expected=wt_enabled");
        return Err(WebTransportError::Protocol(
            Some(ERR_WT_REQUIREMENTS_NOT_MET),
            "h3_settings validate invalid".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests;
