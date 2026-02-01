//! Internal specialized H3 protocol engine logic.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde_json::{Value, json};
use tracing::{debug, error, warn};

use crate::common::constants::{
    ERR_H3_CLOSED_CRITICAL_STREAM, ERR_H3_DATAGRAM_ERROR, ERR_H3_EXCESSIVE_LOAD,
    ERR_H3_FRAME_ERROR, ERR_H3_FRAME_UNEXPECTED, ERR_H3_ID_ERROR, ERR_H3_INTERNAL_ERROR,
    ERR_H3_MESSAGE_ERROR, ERR_H3_MISSING_SETTINGS, ERR_H3_SETTINGS_ERROR,
    ERR_H3_STREAM_CREATION_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_QPACK_DECOMPRESSION_FAILED,
    ERR_QPACK_ENCODER_STREAM_ERROR, H3_FRAME_TYPE_DATA, H3_FRAME_TYPE_GOAWAY,
    H3_FRAME_TYPE_HEADERS, H3_FRAME_TYPE_SETTINGS, H3_FRAME_TYPE_WEBTRANSPORT_STREAM,
    H3_STREAM_TYPE_CONTROL, H3_STREAM_TYPE_PUSH, H3_STREAM_TYPE_QPACK_DECODER,
    H3_STREAM_TYPE_QPACK_ENCODER, H3_STREAM_TYPE_WEBTRANSPORT, QPACK_DECODER_MAX_BLOCKED_STREAMS,
    QPACK_DECODER_MAX_TABLE_CAPACITY, SETTINGS_ENABLE_CONNECT_PROTOCOL, SETTINGS_H3_DATAGRAM,
    SETTINGS_QPACK_BLOCKED_STREAMS, SETTINGS_QPACK_MAX_TABLE_CAPACITY,
    SETTINGS_WT_INITIAL_MAX_DATA, SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI,
    SETTINGS_WT_INITIAL_MAX_STREAMS_UNI,
};
use crate::common::error::WebTransportError;
use crate::common::types::{Headers, StreamId};
use crate::protocol::connection::Connection;
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::qpack::{DecodeStatus, Decoder, Encoder};
use crate::protocol::utils::{
    is_bidirectional_stream, is_request_response_stream, is_unidirectional_stream, read_varint,
    validate_control_stream_id, validate_unidirectional_stream_id, write_varint,
};

// Header value colon constant.
const COLON: u8 = 0x3A;
// Header value tab constant.
const HTAB: u8 = 0x09;
// Header value space constant.
const SP: u8 = 0x20;
// Header value whitespace set.
const WHITESPACE: &[u8] = &[SP, HTAB];
// Reserved settings identifier list.
const RESERVED_SETTINGS: &[u64] = &[0x0, 0x2, 0x3, 0x4, 0x5];

// Internal HTTP/3 protocol state machine.
pub(super) struct H3 {
    is_client: bool,
    config: H3Config,
    max_table_capacity: u32,
    blocked_streams: u32,
    decoder: Decoder,
    encoder: Encoder,
    settings_received: bool,
    local_control_stream_id: Option<StreamId>,
    local_decoder_stream_id: Option<StreamId>,
    local_encoder_stream_id: Option<StreamId>,
    peer_control_stream_id: Option<StreamId>,
    peer_decoder_stream_id: Option<StreamId>,
    peer_encoder_stream_id: Option<StreamId>,
    partial_frames: HashMap<StreamId, PartialFrameInfo>,
}

impl H3 {
    // H3 engine initialization.
    pub(super) fn new(
        is_client: bool,
        initial_max_data: u64,
        initial_max_streams_bidi: u64,
        initial_max_streams_uni: u64,
        max_capsule_size: u64,
    ) -> Result<Self, WebTransportError> {
        let max_table_capacity = u32::try_from(QPACK_DECODER_MAX_TABLE_CAPACITY).map_err(|_e| {
            WebTransportError::H3(
                Some(ERR_H3_INTERNAL_ERROR),
                "Invalid QPACK capacity".to_owned(),
            )
        })?;
        let blocked_streams = u32::try_from(QPACK_DECODER_MAX_BLOCKED_STREAMS).map_err(|_e| {
            WebTransportError::H3(
                Some(ERR_H3_INTERNAL_ERROR),
                "Invalid QPACK blocked streams".to_owned(),
            )
        })?;

        Ok(Self {
            is_client,
            config: H3Config {
                initial_max_data,
                initial_max_streams_bidi,
                initial_max_streams_uni,
                max_capsule_size,
            },
            max_table_capacity,
            blocked_streams,
            decoder: Decoder::new(max_table_capacity, blocked_streams),
            encoder: Encoder::new(),
            settings_received: false,
            local_control_stream_id: None,
            local_decoder_stream_id: None,
            local_encoder_stream_id: None,
            peer_control_stream_id: None,
            peer_decoder_stream_id: None,
            peer_encoder_stream_id: None,
            partial_frames: HashMap::new(),
        })
    }

    // Stream state cleanup.
    pub(super) fn cleanup_stream(&mut self, stream_id: StreamId) {
        self.partial_frames.remove(&stream_id);
    }

    // Capsule encoding to HTTP/3 DATA frame.
    pub(super) fn encode_capsule(
        stream_id: StreamId,
        capsule_type: u64,
        capsule_data: Bytes,
    ) -> Result<Bytes, WebTransportError> {
        if !is_request_response_stream(stream_id) {
            return Err(WebTransportError::H3(
                Some(ERR_H3_STREAM_CREATION_ERROR),
                "Capsules can only be encoded for client-initiated bidirectional streams."
                    .to_owned(),
            ));
        }

        let mut capsule_buf = BytesMut::with_capacity(capsule_data.len() + 16);
        write_varint(&mut capsule_buf, capsule_type).map_err(|e| {
            WebTransportError::H3(
                Some(ERR_LIB_INTERNAL_ERROR),
                format!("Failed to encode capsule type: {e}"),
            )
        })?;
        write_varint(&mut capsule_buf, capsule_data.len() as u64).map_err(|e| {
            WebTransportError::H3(
                Some(ERR_LIB_INTERNAL_ERROR),
                format!("Failed to encode capsule length: {e}"),
            )
        })?;
        capsule_buf.put(capsule_data);

        encode_frame(H3_FRAME_TYPE_DATA, capsule_buf.freeze())
    }

    // Datagram payload encoding.
    pub(super) fn encode_datagram(
        stream_id: StreamId,
        data: Bytes,
    ) -> Result<Vec<Bytes>, WebTransportError> {
        if !is_request_response_stream(stream_id) {
            return Err(WebTransportError::H3(
                Some(ERR_H3_STREAM_CREATION_ERROR),
                "Datagrams can only be encoded for client-initiated bidirectional streams"
                    .to_owned(),
            ));
        }

        let mut header = BytesMut::new();
        write_varint(&mut header, stream_id / 4).map_err(|e| {
            WebTransportError::H3(
                Some(ERR_LIB_INTERNAL_ERROR),
                format!("Failed to encode datagram stream ID: {e}"),
            )
        })?;

        Ok(vec![header.freeze(), data])
    }

    // H3 GOAWAY frame encoding.
    pub(super) fn encode_goaway(last_stream_id: StreamId) -> Result<Bytes, WebTransportError> {
        let mut buf = BytesMut::with_capacity(8);
        write_varint(&mut buf, last_stream_id).map_err(|e| {
            WebTransportError::H3(
                Some(ERR_LIB_INTERNAL_ERROR),
                format!("Failed to encode GOAWAY stream ID: {e}"),
            )
        })?;
        encode_frame(H3_FRAME_TYPE_GOAWAY, buf.freeze())
    }

    // Headers encoding.
    pub(super) fn encode_headers(
        &mut self,
        stream_id: StreamId,
        headers: &Headers,
        end_stream: bool,
    ) -> Result<Vec<Effect>, WebTransportError> {
        let mut effects = Vec::new();

        let (frame_payload, encoder_instructions) =
            self.encoder.encode(stream_id, headers).map_err(|e| {
                WebTransportError::H3(
                    Some(ERR_QPACK_ENCODER_STREAM_ERROR),
                    format!("Encoder error: {e:?}"),
                )
            })?;

        if let Some(id) = self
            .local_encoder_stream_id
            .filter(|_| !encoder_instructions.is_empty())
        {
            effects.push(Effect::SendQuicData {
                stream_id: id,
                data: Bytes::from(encoder_instructions),
                end_stream: false,
            });
        }

        let frame_payload_bytes = Bytes::from(frame_payload);
        let frame_len = frame_payload_bytes.len();
        let frame_data = encode_frame(H3_FRAME_TYPE_HEADERS, frame_payload_bytes)?;

        effects.push(Effect::SendQuicData {
            stream_id,
            data: frame_data,
            end_stream,
        });

        effects.push(Effect::LogH3Frame {
            category: "http".to_owned(),
            event: "frame_created".to_owned(),
            data: json!({
                "frame_type": H3_FRAME_TYPE_HEADERS,
                "length": frame_len,
                "headers": headers_to_json(headers),
                "stream_id": stream_id
            })
            .to_string(),
        });

        Ok(effects)
    }

    // Stream creation preamble encoding.
    pub(super) fn encode_stream_creation(
        &mut self,
        stream_id: StreamId,
        control_stream_id: StreamId,
        is_unidirectional: bool,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        if is_unidirectional {
            let mut buf = BytesMut::new();
            if let Err(e) = write_varint(&mut buf, H3_STREAM_TYPE_WEBTRANSPORT) {
                error!("Failed to encode WT stream type: {e:?}");
                return Vec::new();
            }
            effects.push(Effect::SendQuicData {
                stream_id,
                data: buf.freeze(),
                end_stream: false,
            });

            let mut buf2 = BytesMut::new();
            if let Err(e) = write_varint(&mut buf2, control_stream_id) {
                error!("Failed to encode WT control stream ID: {e:?}");
                return Vec::new();
            }
            effects.push(Effect::SendQuicData {
                stream_id,
                data: buf2.freeze(),
                end_stream: false,
            });
        } else {
            let mut buf = BytesMut::new();
            if let Err(e) = write_varint(&mut buf, H3_FRAME_TYPE_WEBTRANSPORT_STREAM) {
                error!("Failed to encode WT stream frame type: {e:?}");
                return Vec::new();
            }
            if let Err(e) = write_varint(&mut buf, control_stream_id) {
                error!("Failed to encode WT control stream ID: {e:?}");
                return Vec::new();
            }
            effects.push(Effect::SendQuicData {
                stream_id,
                data: buf.freeze(),
                end_stream: false,
            });
        }

        effects.push(Effect::LogH3Frame {
            category: "http".to_owned(),
            event: "stream_type_set".to_owned(),
            data: json!({
                "new": "webtransport",
                "stream_id": stream_id
            })
            .to_string(),
        });

        if self.is_client {
            let partial = self.ensure_partial_frame(stream_id);
            partial.stream_type = Some(H3_STREAM_TYPE_WEBTRANSPORT);
            partial.control_stream_id = Some(control_stream_id);
        }

        effects
    }

    // Transport event handling.
    pub(super) fn handle_transport_event(
        &mut self,
        event: &ProtocolEvent,
        connection: &Connection,
    ) -> (Vec<ProtocolEvent>, Vec<Effect>) {
        let mut h3_events = Vec::new();
        let mut effects = Vec::new();

        let result = match event {
            ProtocolEvent::TransportStreamDataReceived {
                data,
                end_stream,
                stream_id,
            } => {
                if is_unidirectional_stream(*stream_id) {
                    self.recv_uni_stream_data(*stream_id, data.clone(), *end_stream, connection)
                } else {
                    self.recv_request_data(*stream_id, data.clone(), *end_stream, connection)
                }
            }
            ProtocolEvent::TransportDatagramFrameReceived { data } => {
                match Self::recv_datagram(data) {
                    Ok(evts) => Ok((evts, Vec::new())),
                    Err(e) => Err(e),
                }
            }
            _ => Ok((Vec::new(), Vec::new())),
        };

        match result {
            Ok((new_evts, new_fx)) => {
                h3_events.extend(new_evts);
                effects.extend(new_fx);
            }
            Err(e) => {
                let (code, reason) = match e {
                    WebTransportError::H3(c, msg) => (c.unwrap_or(ERR_H3_INTERNAL_ERROR), msg),
                    _ => (ERR_H3_INTERNAL_ERROR, e.to_string()),
                };
                effects.push(Effect::CloseQuicConnection {
                    error_code: code,
                    reason: Some(reason),
                });
            }
        }

        (h3_events, effects)
    }

    // Encoded SETTINGS frame generation.
    pub(super) fn initialize_settings(&mut self) -> Result<Bytes, WebTransportError> {
        let settings_payload = encode_settings(&self.local_settings_snapshot())?;
        encode_frame(H3_FRAME_TYPE_SETTINGS, settings_payload)
    }

    // Settings received status check.
    pub(super) fn is_settings_received(&self) -> bool {
        self.settings_received
    }

    // Local control stream ID accessor.
    pub(super) fn local_control_stream_id(&self) -> Option<StreamId> {
        self.local_control_stream_id
    }

    // Local settings snapshot retrieval.
    pub(super) fn local_settings_snapshot(&self) -> HashMap<u64, u64> {
        let mut settings = HashMap::new();
        settings.insert(SETTINGS_ENABLE_CONNECT_PROTOCOL, 1);
        settings.insert(SETTINGS_H3_DATAGRAM, 1);
        settings.insert(
            SETTINGS_QPACK_BLOCKED_STREAMS,
            u64::from(self.blocked_streams),
        );
        settings.insert(
            SETTINGS_QPACK_MAX_TABLE_CAPACITY,
            u64::from(self.max_table_capacity),
        );
        settings.insert(SETTINGS_WT_INITIAL_MAX_DATA, self.config.initial_max_data);
        settings.insert(
            SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI,
            self.config.initial_max_streams_bidi,
        );
        settings.insert(
            SETTINGS_WT_INITIAL_MAX_STREAMS_UNI,
            self.config.initial_max_streams_uni,
        );
        settings
    }

    // Local stream ID assignment.
    pub(super) fn set_local_stream_ids(
        &mut self,
        control_stream_id: StreamId,
        encoder_stream_id: StreamId,
        decoder_stream_id: StreamId,
    ) -> Result<(), WebTransportError> {
        validate_unidirectional_stream_id(control_stream_id, "Control")
            .map_err(|e| WebTransportError::H3(Some(ERR_H3_ID_ERROR), e))?;
        validate_unidirectional_stream_id(encoder_stream_id, "Encoder")
            .map_err(|e| WebTransportError::H3(Some(ERR_H3_ID_ERROR), e))?;
        validate_unidirectional_stream_id(decoder_stream_id, "Decoder")
            .map_err(|e| WebTransportError::H3(Some(ERR_H3_ID_ERROR), e))?;

        self.local_control_stream_id = Some(control_stream_id);
        self.local_encoder_stream_id = Some(encoder_stream_id);
        self.local_decoder_stream_id = Some(decoder_stream_id);
        Ok(())
    }

    // Header block decoding and effect generation.
    fn decode_headers(
        &mut self,
        stream_id: StreamId,
        frame_data: Option<Bytes>,
    ) -> Result<HeaderDecodeResult, WebTransportError> {
        let mut effects = Vec::new();

        let (decoder_instructions, raw_headers) = if let Some(data) = frame_data {
            let (instr, status) = self.decoder.decode_header(stream_id, data).map_err(|e| {
                WebTransportError::H3(
                    Some(ERR_QPACK_DECOMPRESSION_FAILED),
                    format!("QPACK decompression failed: {e:?}"),
                )
            })?;
            (instr, Some(status))
        } else {
            match self.decoder.resume_header(stream_id) {
                Ok(Some(h)) => (Vec::new(), Some(DecodeStatus::Complete(h))),
                Ok(None) => (Vec::new(), Some(DecodeStatus::Blocked)),
                Err(e) => {
                    return Err(WebTransportError::H3(
                        Some(ERR_QPACK_DECOMPRESSION_FAILED),
                        format!("QPACK resume failed: {e:?}"),
                    ));
                }
            }
        };

        if let Some(id) = self
            .local_decoder_stream_id
            .filter(|_| !decoder_instructions.is_empty())
        {
            effects.push(Effect::SendQuicData {
                stream_id: id,
                data: Bytes::from(decoder_instructions),
                end_stream: false,
            });
        }

        match raw_headers {
            Some(DecodeStatus::Complete(h)) => Ok(HeaderDecodeResult::Done(h, effects)),
            Some(DecodeStatus::Blocked) => Ok(HeaderDecodeResult::Blocked),
            None => Err(WebTransportError::H3(
                Some(ERR_H3_INTERNAL_ERROR),
                "Stream blocked".to_owned(),
            )),
        }
    }

    // Partial frame state initialization or retrieval.
    fn ensure_partial_frame(&mut self, stream_id: StreamId) -> &mut PartialFrameInfo {
        self.partial_frames
            .entry(stream_id)
            .or_insert_with(|| PartialFrameInfo::new(stream_id))
    }

    // Control stream frame processing.
    fn handle_control_frame(
        &mut self,
        frame_type: u64,
        frame_data: &Bytes,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut effects = Vec::new();
        let mut events = Vec::new();

        if frame_type != H3_FRAME_TYPE_SETTINGS && !self.settings_received {
            return Err(WebTransportError::H3(
                Some(ERR_H3_MISSING_SETTINGS),
                "First frame on control stream must be SETTINGS".to_owned(),
            ));
        }

        match frame_type {
            H3_FRAME_TYPE_SETTINGS => {
                if self.settings_received {
                    return Err(WebTransportError::H3(
                        Some(ERR_H3_FRAME_UNEXPECTED),
                        "SETTINGS frame received twice".to_owned(),
                    ));
                }
                let settings = parse_settings(frame_data)?;
                validate_settings(&settings, connection)?;

                let enc_instr = self
                    .encoder
                    .apply_settings(
                        *settings
                            .get(&SETTINGS_QPACK_MAX_TABLE_CAPACITY)
                            .unwrap_or(&0),
                        *settings.get(&SETTINGS_QPACK_BLOCKED_STREAMS).unwrap_or(&0),
                    )
                    .map_err(|_e| {
                        WebTransportError::H3(
                            Some(ERR_H3_INTERNAL_ERROR),
                            "Failed to apply QPACK settings".to_owned(),
                        )
                    })?;

                if let Some(id) = self
                    .local_encoder_stream_id
                    .filter(|_| !enc_instr.is_empty())
                {
                    effects.push(Effect::SendQuicData {
                        stream_id: id,
                        data: Bytes::from(enc_instr),
                        end_stream: false,
                    });
                }
                self.settings_received = true;
                events.push(ProtocolEvent::SettingsReceived { settings });
            }
            H3_FRAME_TYPE_GOAWAY => {
                debug!("H3 GOAWAY frame received.");
                events.push(ProtocolEvent::GoawayReceived);
            }
            H3_FRAME_TYPE_HEADERS => {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_FRAME_UNEXPECTED),
                    "Invalid frame type on control stream".to_owned(),
                ));
            }
            _ => {}
        }

        Ok((events, effects))
    }

    // Request stream frame processing.
    fn handle_request_frame(
        &mut self,
        frame_type: u64,
        frame_data: Option<Bytes>,
        stream_id: StreamId,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut events = Vec::new();
        let mut effects = Vec::new();

        let is_wt_control = {
            if let Some(p) = self.partial_frames.get(&stream_id) {
                if p.is_webtransport_control {
                    true
                } else {
                    is_control_stream(stream_id, connection)
                }
            } else {
                is_control_stream(stream_id, connection)
            }
        };

        if is_wt_control && !self.ensure_partial_frame(stream_id).headers_processed {
            self.ensure_partial_frame(stream_id).is_webtransport_control = true;
        }

        match frame_type {
            H3_FRAME_TYPE_DATA => {
                let payload = frame_data.unwrap_or_default();

                if is_wt_control {
                    let p = self.ensure_partial_frame(stream_id);
                    if p.headers_processed {
                        if !payload.is_empty() {
                            p.capsule_buffer.extend_from_slice(&payload);
                        }
                        if !p.capsule_buffer.is_empty() {
                            events.extend(self.parse_capsules(stream_id)?);
                        }
                    }
                } else if connection.sessions.contains_key(&stream_id) {
                    if !payload.is_empty() {
                        events.push(ProtocolEvent::WebTransportStreamDataReceived {
                            data: payload,
                            session_id: stream_id,
                            stream_id,
                            stream_ended,
                        });
                    }
                } else if !payload.is_empty() {
                    debug!(
                        "Ignored DATA frame on non-WebTransport stream {stream_id} (len={})",
                        payload.len()
                    );
                }
            }
            H3_FRAME_TYPE_HEADERS => {
                if !self.settings_received {
                    return Err(WebTransportError::H3(
                        Some(ERR_H3_MISSING_SETTINGS),
                        "Stream Blocked: Missing Settings".to_owned(),
                    ));
                }

                let p_check = self.ensure_partial_frame(stream_id);
                if p_check.headers_processed {
                    return Err(WebTransportError::H3(
                        Some(ERR_H3_FRAME_UNEXPECTED),
                        "HEADERS frame received after initial headers".to_owned(),
                    ));
                }

                match self.decode_headers(stream_id, frame_data.clone())? {
                    HeaderDecodeResult::Blocked => {
                        let p = self.ensure_partial_frame(stream_id);
                        p.blocked = true;
                        p.frame_type = Some(H3_FRAME_TYPE_HEADERS);
                        return Ok((events, effects));
                    }
                    HeaderDecodeResult::Done(raw_headers, decoder_effects) => {
                        effects.extend(decoder_effects);

                        let mut is_wt = false;
                        for (k, v) in &raw_headers {
                            if k.as_ref() == b":protocol" && v.as_ref() == b"webtransport" {
                                is_wt = true;
                                break;
                            }
                        }

                        let p_update = self.ensure_partial_frame(stream_id);
                        if is_wt {
                            p_update.is_webtransport_control = true;
                        }
                        p_update.blocked = false;

                        if self.is_client {
                            validate_response_headers(&raw_headers)?;
                        } else {
                            validate_request_headers(&raw_headers)?;
                        }

                        self.ensure_partial_frame(stream_id).headers_processed = true;

                        let length = if let Some(d) = &frame_data {
                            d.len()
                        } else {
                            let p = self.ensure_partial_frame(stream_id);
                            p.blocked_frame_size.take().unwrap_or(0)
                        };

                        effects.push(Effect::LogH3Frame {
                            category: "http".to_owned(),
                            event: "frame_parsed".to_owned(),
                            data: json!({
                                "frame_type": H3_FRAME_TYPE_HEADERS,
                                "length": length,
                                "headers": headers_to_json(&raw_headers),
                                "stream_id": stream_id
                            })
                            .to_string(),
                        });

                        events.push(ProtocolEvent::HeadersReceived {
                            headers: raw_headers,
                            stream_id,
                            stream_ended,
                        });
                    }
                }
            }
            H3_FRAME_TYPE_SETTINGS => {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_FRAME_UNEXPECTED),
                    "Invalid frame type on request stream".to_owned(),
                ));
            }
            H3_FRAME_TYPE_WEBTRANSPORT_STREAM => {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_FRAME_ERROR),
                    "WT_STREAM frame (0x41) received in unexpected location".to_owned(),
                ));
            }
            _ => {}
        }

        Ok((events, effects))
    }

    // HTTP/3 capsule parsing loop.
    fn parse_capsules(
        &mut self,
        stream_id: StreamId,
    ) -> Result<Vec<ProtocolEvent>, WebTransportError> {
        let mut events = Vec::new();
        let working_buffer = {
            let p = self.ensure_partial_frame(stream_id);
            p.capsule_buffer.split().freeze()
        };

        let max_size = self.config.max_capsule_size;
        let mut consumed = 0;

        let mut buf = Cursor::new(&working_buffer[..]);

        loop {
            let start_pos = buf.position();
            if buf.remaining() == 0 {
                break;
            }

            let Ok(capsule_type) = read_varint(&mut buf) else {
                buf.set_position(start_pos);
                break;
            };

            let Ok(capsule_length) = read_varint(&mut buf) else {
                buf.set_position(start_pos);
                break;
            };

            if capsule_length > max_size {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_EXCESSIVE_LOAD),
                    format!("Capsule length {capsule_length} exceeds limit"),
                ));
            }

            if (buf.remaining() as u64) < capsule_length {
                buf.set_position(start_pos);
                break;
            }

            let start_val = usize::try_from(buf.position()).map_err(|e| {
                WebTransportError::H3(
                    Some(ERR_H3_EXCESSIVE_LOAD),
                    format!("Capsule start position exceeds memory limit: {e}"),
                )
            })?;

            let end_val = start_val
                + usize::try_from(capsule_length).map_err(|e| {
                    WebTransportError::H3(
                        Some(ERR_H3_EXCESSIVE_LOAD),
                        format!("Capsule length exceeds memory limit: {e}"),
                    )
                })?;

            if end_val > buf.get_ref().len() {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_INTERNAL_ERROR),
                    "Buffer underflow reading capsule".to_owned(),
                ));
            }

            let capsule_value =
                Bytes::copy_from_slice(buf.get_ref().get(start_val..end_val).ok_or(
                    WebTransportError::H3(
                        Some(ERR_H3_INTERNAL_ERROR),
                        "Buffer underflow reading capsule".to_owned(),
                    ),
                )?);
            buf.advance(end_val - start_val);

            consumed = usize::try_from(buf.position()).unwrap_or(consumed);

            events.push(ProtocolEvent::CapsuleReceived {
                stream_id,
                capsule_type,
                capsule_data: capsule_value,
            });
        }

        let p = self.ensure_partial_frame(stream_id);
        if consumed < working_buffer.len() {
            p.capsule_buffer
                .extend_from_slice(working_buffer.get(consumed..).unwrap_or_default());
        }

        Ok(events)
    }

    // Generic stream data parsing loop.
    fn parse_stream_data(
        &mut self,
        stream_id: StreamId,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut h3_events = Vec::new();
        let mut effects = Vec::new();

        let p_blocked = self.ensure_partial_frame(stream_id);
        if p_blocked.blocked {
            if let Ok(HeaderDecodeResult::Done(..)) = self.decode_headers(stream_id, None) {
                let p = self.ensure_partial_frame(stream_id);
                p.blocked = false;
            } else {
                return Ok((h3_events, effects));
            }
        }

        let p = self.ensure_partial_frame(stream_id);
        let mut temp_data = BytesMut::new();
        for chunk in &p.buffer {
            temp_data.extend_from_slice(chunk);
        }

        let mut buf = Cursor::new(&temp_data[..]);
        let mut consumed = 0;

        loop {
            if buf.remaining() == 0 && !(stream_ended && consumed == temp_data.len()) {
                break;
            }
            if self.ensure_partial_frame(stream_id).blocked {
                break;
            }

            let original_consumed = consumed;

            let mut check_wt = false;
            {
                let p_state = self.ensure_partial_frame(stream_id);
                if p_state.stream_type.is_none()
                    && !p_state.headers_processed
                    && p_state.frame_type.is_none()
                    && is_bidirectional_stream(stream_id)
                {
                    check_wt = true;
                }
            }

            if check_wt {
                let pos = buf.position();
                match (read_varint(&mut buf), read_varint(&mut buf)) {
                    (Ok(H3_FRAME_TYPE_WEBTRANSPORT_STREAM), Ok(control_id)) => {
                        validate_control_stream_id(control_id)
                            .map_err(|e| WebTransportError::H3(Some(ERR_H3_ID_ERROR), e))?;

                        let p = self.ensure_partial_frame(stream_id);
                        p.stream_type = Some(H3_STREAM_TYPE_WEBTRANSPORT);
                        p.control_stream_id = Some(control_id);

                        effects.push(Effect::LogH3Frame {
                            category: "http".to_owned(),
                            event: "stream_type_set".to_owned(),
                            data: json!({
                                "new": "webtransport",
                                "stream_id": stream_id
                            })
                            .to_string(),
                        });

                        h3_events.push(ProtocolEvent::WebTransportStreamDataReceived {
                            data: Bytes::new(),
                            session_id: control_id,
                            stream_id,
                            stream_ended: false,
                        });

                        consumed = usize::try_from(buf.position()).unwrap_or(consumed);
                        continue;
                    }
                    _ => {
                        buf.set_position(pos);
                    }
                }
            }

            let mut is_wt_data = false;
            let mut wt_control_id = None;
            {
                let p_state = self.ensure_partial_frame(stream_id);
                if p_state.stream_type == Some(H3_STREAM_TYPE_WEBTRANSPORT) {
                    is_wt_data = true;
                    wt_control_id = p_state.control_stream_id;
                }
            }

            if is_wt_data {
                let pos = usize::try_from(buf.position()).map_err(|e| {
                    WebTransportError::H3(
                        Some(ERR_H3_EXCESSIVE_LOAD),
                        format!("Buffer position exceeds memory limit: {e}"),
                    )
                })?;
                if pos > buf.get_ref().len() {
                    return Err(WebTransportError::H3(
                        Some(ERR_H3_INTERNAL_ERROR),
                        "Buffer underflow".to_owned(),
                    ));
                }
                let payload = buf.get_ref().get(pos..).unwrap_or_default().to_vec();
                if !payload.is_empty() || (stream_ended && pos == temp_data.len()) {
                    let cid = wt_control_id.ok_or(WebTransportError::H3(
                        Some(ERR_H3_INTERNAL_ERROR),
                        "Cannot process WT stream data without control stream ID.".to_owned(),
                    ))?;

                    h3_events.push(ProtocolEvent::WebTransportStreamDataReceived {
                        data: Bytes::from(payload),
                        session_id: cid,
                        stream_id,
                        stream_ended,
                    });
                }
                consumed = temp_data.len();
                break;
            }

            let (current_type, current_size) = {
                let p = self.ensure_partial_frame(stream_id);
                if let (Some(t), Some(s)) = (p.frame_type, p.frame_size) {
                    (t, s)
                } else {
                    let pos = buf.position();
                    let Ok(ft) = read_varint(&mut buf) else {
                        buf.set_position(pos);
                        break;
                    };
                    let Ok(fs) = read_varint(&mut buf) else {
                        buf.set_position(pos);
                        break;
                    };

                    let p = self.ensure_partial_frame(stream_id);
                    p.frame_type = Some(ft);
                    let frame_sz = usize::try_from(fs).map_err(|e| {
                        WebTransportError::H3(
                            Some(ERR_H3_EXCESSIVE_LOAD),
                            format!("Frame size exceeds memory limit: {e}"),
                        )
                    })?;
                    p.frame_size = Some(frame_sz);

                    if ft == H3_FRAME_TYPE_HEADERS {
                        p.blocked_frame_size = Some(frame_sz);
                    }

                    if ft == H3_FRAME_TYPE_DATA {
                        effects.push(Effect::LogH3Frame {
                            category: "http".to_owned(),
                            event: "frame_parsed".to_owned(),
                            data: json!({
                                "frame_type": ft,
                                "length": fs,
                                "stream_id": stream_id
                            })
                            .to_string(),
                        });
                    }
                    consumed = usize::try_from(buf.position()).unwrap_or(consumed);
                    (ft, frame_sz)
                }
            };

            let remaining_in_buf = buf.remaining();
            let chunk_size = std::cmp::min(current_size, remaining_in_buf);

            if current_type != H3_FRAME_TYPE_DATA && chunk_size < current_size {
                break;
            }

            let pos = usize::try_from(buf.position()).map_err(|e| {
                WebTransportError::H3(
                    Some(ERR_H3_EXCESSIVE_LOAD),
                    format!("Buffer position exceeds memory limit: {e}"),
                )
            })?;
            if pos + chunk_size > buf.get_ref().len() {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_INTERNAL_ERROR),
                    "Buffer underflow reading frame chunk".to_owned(),
                ));
            }
            let frame_data = Bytes::copy_from_slice(
                buf.get_ref().get(pos..pos + chunk_size).unwrap_or_default(),
            );
            buf.advance(chunk_size);
            consumed = usize::try_from(buf.position()).unwrap_or(consumed);

            let is_last_chunk = {
                let p = self.ensure_partial_frame(stream_id);
                let new_rem = current_size - chunk_size;
                p.frame_size = Some(new_rem);
                new_rem == 0
            };

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
                let (new_evts, new_fx) = self.handle_request_frame(
                    current_type,
                    final_data,
                    stream_id,
                    stream_ended && is_last_chunk && buf.remaining() == 0,
                    connection,
                )?;
                h3_events.extend(new_evts);
                effects.extend(new_fx);
            }

            if is_last_chunk {
                let p = self.ensure_partial_frame(stream_id);
                p.frame_type = None;
                p.frame_size = None;
                if current_type == H3_FRAME_TYPE_HEADERS && !p.blocked {
                    p.blocked_frame_size = None;
                }
            }

            if consumed == original_consumed {
                if !stream_ended {
                    warn!("H3 parsing stuck on stream {stream_id}");
                }
                break;
            }
        }

        let p = self.ensure_partial_frame(stream_id);
        p.buffer.clear();
        if consumed < temp_data.len() {
            p.buffer
                .push_back(Bytes::copy_from_slice(temp_data.get(consumed..).ok_or(
                    WebTransportError::H3(
                        Some(ERR_H3_INTERNAL_ERROR),
                        "Buffer underflow when caching remaining data".to_owned(),
                    ),
                )?));
        }

        Ok((h3_events, effects))
    }

    // Datagram frame parsing.
    fn recv_datagram(data: &Bytes) -> Result<Vec<ProtocolEvent>, WebTransportError> {
        let mut buf = Cursor::new(&data[..]);
        let quarter_id = read_varint(&mut buf).map_err(|e| {
            WebTransportError::H3(
                Some(ERR_H3_DATAGRAM_ERROR),
                format!("Could not parse quarter stream ID: {e}"),
            )
        })?;
        let stream_id = quarter_id * 4;

        if !is_request_response_stream(stream_id) {
            return Err(WebTransportError::H3(
                Some(ERR_H3_ID_ERROR),
                format!("Datagram received on invalid Session ID {stream_id}"),
            ));
        }

        let pos = usize::try_from(buf.position()).map_err(|e| {
            WebTransportError::H3(
                Some(ERR_H3_EXCESSIVE_LOAD),
                format!("Datagram position exceeds memory limit: {e}"),
            )
        })?;

        let payload = if pos < data.len() {
            Bytes::copy_from_slice(data.get(pos..).ok_or_else(|| {
                WebTransportError::H3(
                    Some(ERR_H3_INTERNAL_ERROR),
                    "Buffer underflow reading datagram payload".to_owned(),
                )
            })?)
        } else {
            Bytes::new()
        };

        Ok(vec![ProtocolEvent::DatagramReceived {
            stream_id,
            data: payload,
        }])
    }

    // Request stream data ingestion.
    fn recv_request_data(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let p = self.ensure_partial_frame(stream_id);
        if !data.is_empty() {
            p.buffer.push_back(data);
        }
        let mut p_ended = p.ended;
        if stream_ended {
            p.ended = true;
            p_ended = true;
        }

        if !p.blocked && p.buffer.is_empty() && !p_ended {
            return Ok((Vec::new(), Vec::new()));
        }

        let (mut events, effects) = self.parse_stream_data(stream_id, p_ended, connection)?;

        let p = self.ensure_partial_frame(stream_id);
        if p.ended && p.buffer.is_empty() {
            if connection.sessions.contains_key(&stream_id) {
                debug!("CONNECT stream {stream_id} cleanly closed (FIN received)");
                events.push(ProtocolEvent::ConnectStreamClosed { stream_id });
            }
            self.partial_frames.remove(&stream_id);
        }

        Ok((events, effects))
    }

    // Unidirectional stream data ingestion.
    fn recv_uni_stream_data(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let p = self.ensure_partial_frame(stream_id);
        if !data.is_empty() {
            p.buffer.push_back(data);
        }
        if stream_ended {
            p.ended = true;
        }

        if p.blocked || (p.buffer.is_empty() && !p.ended) {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut events = Vec::new();
        let mut effects = Vec::new();

        let mut temp_data = BytesMut::new();
        for chunk in &p.buffer {
            temp_data.extend_from_slice(chunk);
        }
        let mut buf = Cursor::new(&temp_data[..]);
        let mut consumed = 0;

        {
            let p = self.ensure_partial_frame(stream_id);
            if p.stream_type.is_none() {
                if let Ok(st) = read_varint(&mut buf) {
                    p.stream_type = Some(st);
                    consumed = usize::try_from(buf.position()).unwrap_or(consumed);

                    match st {
                        H3_STREAM_TYPE_CONTROL => {
                            if self.peer_control_stream_id.is_none() {
                                self.peer_control_stream_id = Some(stream_id);
                            }
                        }
                        H3_STREAM_TYPE_QPACK_DECODER => {
                            if self.peer_decoder_stream_id.is_none() {
                                self.peer_decoder_stream_id = Some(stream_id);
                            }
                        }
                        H3_STREAM_TYPE_QPACK_ENCODER => {
                            if self.peer_encoder_stream_id.is_none() {
                                self.peer_encoder_stream_id = Some(stream_id);
                            }
                        }
                        _ => {}
                    }

                    let type_name = match st {
                        H3_STREAM_TYPE_CONTROL => "control",
                        H3_STREAM_TYPE_QPACK_ENCODER => "qpack_encoder",
                        H3_STREAM_TYPE_QPACK_DECODER => "qpack_decoder",
                        H3_STREAM_TYPE_WEBTRANSPORT => "webtransport",
                        H3_STREAM_TYPE_PUSH => "push",
                        _ => "unknown",
                    };

                    if type_name == "unknown" {
                        warn!(
                            "Received unknown unidirectional stream type {st} on stream {stream_id}"
                        );
                    }

                    effects.push(Effect::LogH3Frame {
                        category: "http".to_owned(),
                        event: "stream_type_set".to_owned(),
                        data: json!({
                            "new": type_name,
                            "stream_id": stream_id
                        })
                        .to_string(),
                    });
                } else {
                    return Ok((events, effects));
                }
            }
        }

        let st = self
            .ensure_partial_frame(stream_id)
            .stream_type
            .unwrap_or(0);

        match st {
            H3_STREAM_TYPE_WEBTRANSPORT => {
                let mut cid_opt = self.ensure_partial_frame(stream_id).control_stream_id;

                if cid_opt.is_none() {
                    if let Ok(cid) = read_varint(&mut buf) {
                        validate_control_stream_id(cid)
                            .map_err(|e| WebTransportError::H3(Some(ERR_H3_ID_ERROR), e))?;
                        let p = self.ensure_partial_frame(stream_id);
                        p.control_stream_id = Some(cid);
                        cid_opt = Some(cid);
                        consumed = usize::try_from(buf.position()).unwrap_or(consumed);
                    } else {
                        let p = self.ensure_partial_frame(stream_id);
                        p.buffer.clear();
                        if consumed < temp_data.len() {
                            p.buffer.push_back(Bytes::copy_from_slice(
                                temp_data.get(consumed..).unwrap_or_default(),
                            ));
                        }
                        return Ok((events, effects));
                    }
                }

                let p = self.ensure_partial_frame(stream_id);
                if consumed > temp_data.len() {
                    return Err(WebTransportError::H3(
                        Some(ERR_H3_INTERNAL_ERROR),
                        "Buffer underflow reading WT uni payload".to_owned(),
                    ));
                }
                let payload = Bytes::copy_from_slice(temp_data.get(consumed..).unwrap_or_default());

                if let Some(cid) = cid_opt.filter(|_| !payload.is_empty() || p.ended) {
                    events.push(ProtocolEvent::WebTransportStreamDataReceived {
                        data: payload,
                        session_id: cid,
                        stream_id,
                        stream_ended: p.ended,
                    });
                }
                self.ensure_partial_frame(stream_id).buffer.clear();
                return Ok((events, effects));
            }
            H3_STREAM_TYPE_CONTROL => {
                let p = self.ensure_partial_frame(stream_id);
                if p.ended {
                    return Err(WebTransportError::H3(
                        Some(ERR_H3_CLOSED_CRITICAL_STREAM),
                        "Closing control stream is not allowed".to_owned(),
                    ));
                }

                loop {
                    let start_pos = buf.position();
                    let remaining = buf.remaining();
                    if remaining == 0 {
                        break;
                    }

                    if let (Ok(ft), Ok(fs)) = (read_varint(&mut buf), read_varint(&mut buf)) {
                        let needed = usize::try_from(fs).map_err(|_e| {
                            WebTransportError::H3(
                                Some(ERR_H3_EXCESSIVE_LOAD),
                                "Control frame size exceeds memory limit".to_owned(),
                            )
                        })?;

                        if buf.remaining() < needed {
                            buf.set_position(start_pos);
                            break;
                        }
                        let pos = usize::try_from(buf.position()).map_err(|_e| {
                            WebTransportError::H3(
                                Some(ERR_H3_EXCESSIVE_LOAD),
                                "Buffer position exceeds memory limit".to_owned(),
                            )
                        })?;
                        if pos + needed > buf.get_ref().len() {
                            return Err(WebTransportError::H3(
                                Some(ERR_H3_INTERNAL_ERROR),
                                "Buffer underflow reading control frame".to_owned(),
                            ));
                        }
                        let frame_data =
                            Bytes::copy_from_slice(buf.get_ref().get(pos..pos + needed).ok_or(
                                WebTransportError::H3(
                                    Some(ERR_H3_INTERNAL_ERROR),
                                    "Buffer underflow reading control frame".to_owned(),
                                ),
                            )?);
                        buf.advance(needed);

                        let (new_evts, new_fx) =
                            self.handle_control_frame(ft, &frame_data, connection)?;
                        events.extend(new_evts);
                        effects.extend(new_fx);
                    } else {
                        buf.set_position(start_pos);
                        break;
                    }
                }
                consumed = usize::try_from(buf.position()).unwrap_or(consumed);
            }
            H3_STREAM_TYPE_QPACK_DECODER => {
                let slice = temp_data.get(consumed..).unwrap_or(&[]);
                if !slice.is_empty() {
                    self.encoder.feed_decoder(slice);
                    consumed += slice.len();
                }
            }
            H3_STREAM_TYPE_QPACK_ENCODER => {
                let slice = temp_data.get(consumed..).unwrap_or(&[]);
                if !slice.is_empty() {
                    if let Ok(unblocked) = self.decoder.feed_encoder(slice) {
                        for sid in unblocked {
                            let should_resume = {
                                let p = self.ensure_partial_frame(sid);
                                if p.blocked {
                                    p.blocked = false;
                                    Some(p.ended)
                                } else {
                                    None
                                }
                            };

                            if let Some(ended) = should_resume {
                                let (new_evts, new_fx) =
                                    self.recv_request_data(sid, Bytes::new(), ended, connection)?;
                                events.extend(new_evts);
                                effects.extend(new_fx);
                            }
                        }
                    }
                    consumed += slice.len();
                }
            }
            _ => {
                consumed = temp_data.len();
            }
        }

        let p = self.ensure_partial_frame(stream_id);
        p.buffer.clear();
        if consumed < temp_data.len() {
            p.buffer.push_back(Bytes::copy_from_slice(
                temp_data.get(consumed..).unwrap_or_default(),
            ));
        }

        if p.ended && p.buffer.is_empty() {
            self.partial_frames.remove(&stream_id);
        }

        Ok((events, effects))
    }
}

// HTTP/3 configuration parameters.
struct H3Config {
    initial_max_data: u64,
    initial_max_streams_bidi: u64,
    initial_max_streams_uni: u64,
    max_capsule_size: u64,
}

// Header decoding operation result.
enum HeaderDecodeResult {
    Blocked,
    Done(Headers, Vec<Effect>),
}

// Stream-specific partial frame buffering state.
#[derive(Debug)]
struct PartialFrameInfo {
    _stream_id: StreamId,
    blocked: bool,
    blocked_frame_size: Option<usize>,
    buffer: VecDeque<Bytes>,
    capsule_buffer: BytesMut,
    control_stream_id: Option<StreamId>,
    ended: bool,
    frame_size: Option<usize>,
    frame_type: Option<u64>,
    headers_processed: bool,
    is_webtransport_control: bool,
    stream_type: Option<u64>,
}

impl PartialFrameInfo {
    // Partial frame info constructor.
    fn new(stream_id: StreamId) -> Self {
        Self {
            _stream_id: stream_id,
            buffer: VecDeque::new(),
            capsule_buffer: BytesMut::new(),
            ended: false,
            blocked: false,
            blocked_frame_size: None,
            frame_size: None,
            frame_type: None,
            stream_type: None,
            control_stream_id: None,
            headers_processed: false,
            is_webtransport_control: false,
        }
    }
}

// Generic H3 frame encoding.
fn encode_frame(frame_type: u64, frame_data: Bytes) -> Result<Bytes, WebTransportError> {
    let mut buf = BytesMut::with_capacity(frame_data.len() + 16);
    write_varint(&mut buf, frame_type).map_err(|e| {
        WebTransportError::H3(
            Some(ERR_LIB_INTERNAL_ERROR),
            format!("Failed to encode frame type: {e}"),
        )
    })?;
    write_varint(&mut buf, frame_data.len() as u64).map_err(|e| {
        WebTransportError::H3(
            Some(ERR_LIB_INTERNAL_ERROR),
            format!("Failed to encode frame length: {e}"),
        )
    })?;
    buf.put(frame_data);
    Ok(buf.freeze())
}

// SETTINGS frame payload encoding.
fn encode_settings(settings: &HashMap<u64, u64>) -> Result<Bytes, WebTransportError> {
    let mut buf = BytesMut::with_capacity(1024);
    let mut keys: Vec<_> = settings.keys().collect();
    keys.sort_unstable();

    for id in keys {
        if let Some(val) = settings.get(id) {
            write_varint(&mut buf, *id).map_err(|e| {
                WebTransportError::H3(
                    Some(ERR_LIB_INTERNAL_ERROR),
                    format!("Failed to encode setting ID: {e}"),
                )
            })?;
            write_varint(&mut buf, *val).map_err(|e| {
                WebTransportError::H3(
                    Some(ERR_LIB_INTERNAL_ERROR),
                    format!("Failed to encode setting value: {e}"),
                )
            })?;
        }
    }
    Ok(buf.freeze())
}

// Header diagnostics serialization.
fn headers_to_json(headers: &Headers) -> Value {
    let mut sorted_headers: Vec<_> = headers.iter().collect();
    sorted_headers.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let converted: Vec<Vec<String>> = sorted_headers
        .iter()
        .map(|(k, v)| {
            vec![
                String::from_utf8_lossy(k).into_owned(),
                String::from_utf8_lossy(v).into_owned(),
            ]
        })
        .collect();
    json!(converted)
}

// Control stream identification.
fn is_control_stream(stream_id: StreamId, connection: &Connection) -> bool {
    connection.sessions.contains_key(&stream_id)
}

// SETTINGS payload parsing.
fn parse_settings(data: &Bytes) -> Result<HashMap<u64, u64>, WebTransportError> {
    let mut settings = HashMap::new();
    let mut buf = Cursor::new(&data[..]);

    while buf.has_remaining() {
        let id = read_varint(&mut buf).map_err(|e| {
            WebTransportError::H3(
                Some(ERR_H3_FRAME_ERROR),
                format!("Malformed settings ID: {e}"),
            )
        })?;
        let val = read_varint(&mut buf).map_err(|e| {
            WebTransportError::H3(
                Some(ERR_H3_FRAME_ERROR),
                format!("Malformed settings value: {e}"),
            )
        })?;

        if RESERVED_SETTINGS.contains(&id) {
            return Err(WebTransportError::H3(
                Some(ERR_H3_SETTINGS_ERROR),
                format!("Setting identifier 0x{id:x} is reserved"),
            ));
        }
        if settings.insert(id, val).is_some() {
            return Err(WebTransportError::H3(
                Some(ERR_H3_SETTINGS_ERROR),
                format!("Setting identifier 0x{id:x} is included twice"),
            ));
        }
    }
    Ok(settings)
}

// Header name syntax validation.
fn validate_header_name(key: &[u8]) -> Result<(), WebTransportError> {
    if key.is_empty() {
        return Err(WebTransportError::H3(
            Some(ERR_H3_MESSAGE_ERROR),
            "Header name empty".to_owned(),
        ));
    }
    for (i, &b) in key.iter().enumerate() {
        if b == COLON {
            if i != 0 {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_MESSAGE_ERROR),
                    "Non-initial colon".to_owned(),
                ));
            }
            continue;
        }

        let is_valid_char = b.is_ascii_lowercase()
            || b.is_ascii_digit()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            );

        if !is_valid_char {
            if b.is_ascii_uppercase() {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_MESSAGE_ERROR),
                    "Header name contains uppercase".to_owned(),
                ));
            }
            return Err(WebTransportError::H3(
                Some(ERR_H3_MESSAGE_ERROR),
                "Header name contains invalid characters".to_owned(),
            ));
        }
    }
    Ok(())
}

// Header value syntax validation.
fn validate_header_value(value: &[u8]) -> Result<(), WebTransportError> {
    if let (Some(first), Some(last)) = (value.first(), value.last()) {
        if WHITESPACE.contains(first) || WHITESPACE.contains(last) {
            return Err(WebTransportError::H3(
                Some(ERR_H3_MESSAGE_ERROR),
                "Leading/trailing whitespace".to_owned(),
            ));
        }
        for &b in value {
            if b == HTAB || (SP..=0x7E).contains(&b) {
                continue;
            }
            return Err(WebTransportError::H3(
                Some(ERR_H3_MESSAGE_ERROR),
                "Header value contains illegal character".to_owned(),
            ));
        }
    }
    Ok(())
}

// Request header semantic validation.
fn validate_request_headers(headers: &Headers) -> Result<(), WebTransportError> {
    let mut seen_pseudo = HashSet::new();
    let mut after_pseudo = false;
    let mut scheme: Option<&[u8]> = None;
    let mut authority: Option<&[u8]> = None;
    let mut path: Option<&[u8]> = None;
    let mut _method: Option<&[u8]> = None;

    let allowed: Vec<&[u8]> = vec![
        b":method",
        b":scheme",
        b":authority",
        b":path",
        b":protocol",
    ];
    let required: Vec<&[u8]> = vec![b":method", b":scheme", b":authority", b":path"];

    for (k, v) in headers {
        let k_slice = k.as_ref();
        let v_slice = v.as_ref();

        validate_header_name(k_slice)?;
        validate_header_value(v_slice)?;

        if k_slice.starts_with(b":") {
            if after_pseudo {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_MESSAGE_ERROR),
                    "Pseudo-header after regular header".to_owned(),
                ));
            }
            if !allowed.iter().any(|x| x[..] == k_slice[..]) {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_MESSAGE_ERROR),
                    "Unknown pseudo-header".to_owned(),
                ));
            }
            if seen_pseudo.contains(k_slice) {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_MESSAGE_ERROR),
                    "Duplicate pseudo-header".to_owned(),
                ));
            }
            seen_pseudo.insert(k_slice);

            if k_slice == b":method" {
                _method = Some(v_slice);
            }
            if k_slice == b":scheme" {
                scheme = Some(v_slice);
            }
            if k_slice == b":authority" {
                authority = Some(v_slice);
            }
            if k_slice == b":path" {
                path = Some(v_slice);
            }
        } else {
            after_pseudo = true;
        }
    }

    for req in required {
        if !seen_pseudo.contains(req) {
            return Err(WebTransportError::H3(
                Some(ERR_H3_MESSAGE_ERROR),
                "Missing required pseudo-header".to_owned(),
            ));
        }
    }

    if matches!(scheme, Some(b"http" | b"https")) {
        if authority.unwrap_or_default().is_empty() {
            return Err(WebTransportError::H3(
                Some(ERR_H3_MESSAGE_ERROR),
                ":authority cannot be empty for http/https".to_owned(),
            ));
        }
        if path.unwrap_or_default().is_empty() {
            return Err(WebTransportError::H3(
                Some(ERR_H3_MESSAGE_ERROR),
                ":path cannot be empty for http/https".to_owned(),
            ));
        }
    }

    Ok(())
}

// Response header semantic validation.
fn validate_response_headers(headers: &Headers) -> Result<(), WebTransportError> {
    let mut seen_pseudo = HashSet::new();
    let mut after_pseudo = false;

    for (k, v) in headers {
        let k_slice = k.as_ref();
        validate_header_name(k_slice)?;
        validate_header_value(v.as_ref())?;

        if k_slice.starts_with(b":") {
            if after_pseudo {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_MESSAGE_ERROR),
                    "Pseudo-header after regular".to_owned(),
                ));
            }
            if k_slice != b":status" {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_MESSAGE_ERROR),
                    "Invalid response pseudo-header".to_owned(),
                ));
            }
            if seen_pseudo.contains(k_slice) {
                return Err(WebTransportError::H3(
                    Some(ERR_H3_MESSAGE_ERROR),
                    "Duplicate :status".to_owned(),
                ));
            }
            seen_pseudo.insert(k_slice);
        } else {
            after_pseudo = true;
        }
    }

    if !seen_pseudo.contains(b":status".as_slice()) {
        return Err(WebTransportError::H3(
            Some(ERR_H3_MESSAGE_ERROR),
            "Missing :status".to_owned(),
        ));
    }
    Ok(())
}

// SETTINGS semantic validation.
fn validate_settings(
    settings: &HashMap<u64, u64>,
    connection: &Connection,
) -> Result<(), WebTransportError> {
    if settings
        .get(&SETTINGS_ENABLE_CONNECT_PROTOCOL)
        .is_some_and(|&val| val != 1)
    {
        return Err(WebTransportError::H3(
            Some(ERR_H3_SETTINGS_ERROR),
            "ENABLE_CONNECT_PROTOCOL setting must be 1 if present".to_owned(),
        ));
    }

    let quic_supports_datagrams = connection
        .remote_max_datagram_frame_size
        .unwrap_or_default()
        > 0;
    if !quic_supports_datagrams && settings.get(&SETTINGS_H3_DATAGRAM) == Some(&1) {
        return Err(WebTransportError::H3(
            Some(ERR_H3_SETTINGS_ERROR),
            "H3_DATAGRAM requires max_datagram_frame_size".to_owned(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests;
