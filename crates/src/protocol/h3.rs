//! Internal specialized H3 protocol engine logic.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::{debug, trace};

use crate::common::constants::{
    ERR_H3_CLOSED_CRITICAL_STREAM, ERR_H3_DATAGRAM_ERROR, ERR_H3_EXCESSIVE_LOAD,
    ERR_H3_FRAME_ERROR, ERR_H3_FRAME_UNEXPECTED, ERR_H3_ID_ERROR, ERR_H3_INTERNAL_ERROR,
    ERR_H3_MESSAGE_ERROR, ERR_H3_MISSING_SETTINGS, ERR_H3_SETTINGS_ERROR,
    ERR_H3_STREAM_CREATION_ERROR, ERR_LIB_INTERNAL_ERROR, ERR_QPACK_DECOMPRESSION_FAILED,
    ERR_QPACK_ENCODER_STREAM_ERROR, ERR_WT_REQUIREMENTS_NOT_MET, H3_FRAME_TYPE_CANCEL_PUSH,
    H3_FRAME_TYPE_DATA, H3_FRAME_TYPE_GOAWAY, H3_FRAME_TYPE_HEADERS, H3_FRAME_TYPE_MAX_PUSH_ID,
    H3_FRAME_TYPE_PUSH_PROMISE, H3_FRAME_TYPE_SETTINGS, H3_FRAME_TYPE_WT_STREAM,
    H3_STREAM_TYPE_CONTROL, H3_STREAM_TYPE_PUSH, H3_STREAM_TYPE_QPACK_DECODER,
    H3_STREAM_TYPE_QPACK_ENCODER, H3_STREAM_TYPE_WEBTRANSPORT, SETTINGS_ENABLE_CONNECT_PROTOCOL,
    SETTINGS_H3_DATAGRAM, SETTINGS_MAX_FIELD_SECTION_SIZE, SETTINGS_QPACK_BLOCKED_STREAMS,
    SETTINGS_QPACK_MAX_TABLE_CAPACITY, SETTINGS_WT_ENABLED, SETTINGS_WT_INITIAL_MAX_DATA,
    SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, WT_UPGRADE_TOKEN,
};
use crate::common::error::WebTransportError;
use crate::common::types::{Headers, StreamId};
use crate::protocol::connection::Connection;
use crate::protocol::events::{Effect, ProtocolEvent};
use crate::protocol::qpack::{DecodeStatus, Decoder, Encoder};
use crate::protocol::utils::{
    is_bidirectional_stream, is_peer_initiated_stream, is_request_response_stream,
    is_unidirectional_stream, read_varint, write_varint,
};

// Header value colon constant.
const COLON: u8 = 0x3A;
// Maximum permitted HTTP/3 control frame size.
const CONTROL_FRAME_SIZE_LIMIT: usize = 1024 * 1024;
// Header value tab constant.
const HTAB: u8 = 0x09;
// QPACK decoder blocked stream capacity.
const QPACK_DECODER_BLOCKED_STREAM_CAPACITY: u64 = 16;
// QPACK decoder table size.
const QPACK_DECODER_TABLE_SIZE: u64 = 65536;
// Reserved settings identifier list.
const RESERVED_SETTINGS: &[u64] = &[0x0, 0x2, 0x3, 0x4, 0x5];
// Settings frame entries limit.
const SETTINGS_ENTRIES_LIMIT: usize = 64;
// Header value space constant.
const SP: u8 = 0x20;
// Header value whitespace set.
const WHITESPACE: &[u8] = &[SP, HTAB];

// HTTP/3 settings parameters.
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

// Internal HTTP/3 protocol engine.
pub(super) struct H3 {
    blocked_streams: u32,
    decoder: Decoder,
    encoder: Encoder,
    is_client: bool,
    local_control_stream_id: Option<StreamId>,
    local_decoder_stream_id: Option<StreamId>,
    local_encoder_stream_id: Option<StreamId>,
    max_table_capacity: u32,
    params: H3Params,
    partial_frames: HashMap<StreamId, PartialFrameInfo>,
    peer_control_stream_id: Option<StreamId>,
    peer_decoder_stream_id: Option<StreamId>,
    peer_encoder_stream_id: Option<StreamId>,
    settings_received: bool,
}

impl H3 {
    // H3 engine initialization.
    pub(super) fn new(is_client: bool, params: H3Params) -> Result<Self, WebTransportError> {
        let max_table_capacity = u32::try_from(QPACK_DECODER_TABLE_SIZE).map_err(|e| {
            debug!("qpack_max_table_capacity convert failed expected=u32 err={e:?}");
            WebTransportError::Protocol(
                Some(ERR_H3_INTERNAL_ERROR),
                "qpack_max_table_capacity convert failed".into(),
            )
        })?;

        let blocked_streams =
            u32::try_from(QPACK_DECODER_BLOCKED_STREAM_CAPACITY).map_err(|e| {
                debug!("qpack_blocked_streams convert failed expected=u32 err={e:?}");
                WebTransportError::Protocol(
                    Some(ERR_H3_INTERNAL_ERROR),
                    "qpack_blocked_streams convert failed".into(),
                )
            })?;

        Ok(Self {
            blocked_streams,
            decoder: Decoder::new(max_table_capacity, blocked_streams),
            encoder: Encoder::new(),
            is_client,
            local_control_stream_id: None,
            local_decoder_stream_id: None,
            local_encoder_stream_id: None,
            max_table_capacity,
            params,
            partial_frames: HashMap::new(),
            peer_control_stream_id: None,
            peer_decoder_stream_id: None,
            peer_encoder_stream_id: None,
            settings_received: false,
        })
    }

    // Stream state cleanup.
    pub(super) fn cleanup_stream(&mut self, stream_id: StreamId) -> Vec<Effect> {
        let mut effects = Vec::new();
        let cancel_instr = self.decoder.abandon_header_block(stream_id);

        if !cancel_instr.is_empty()
            && let Some(id) = self.local_decoder_stream_id
        {
            effects.push(Effect::SendQuicData {
                stream_id: id,
                data: Bytes::from(cancel_instr),
                end_stream: false,
            });
        }

        self.partial_frames.remove(&stream_id);
        effects
    }

    // Capsule encoding to HTTP/3 DATA frame.
    pub(super) fn encode_capsule(
        stream_id: StreamId,
        capsule_type: u64,
        capsule_data: Bytes,
    ) -> Result<Vec<Bytes>, WebTransportError> {
        if !is_request_response_stream(stream_id) {
            debug!("h3_stream validate invalid actual={stream_id} expected=request_response");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_STREAM_CREATION_ERROR),
                "h3_stream validate invalid".into(),
            ));
        }

        let mut capsule_hdr = BytesMut::with_capacity(16);

        write_varint(&mut capsule_hdr, capsule_type).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        write_varint(&mut capsule_hdr, capsule_data.len() as u64).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        let capsule_header = capsule_hdr.freeze();
        let payload_len = capsule_header.len() + capsule_data.len();
        let frame_header = encode_frame_header(H3_FRAME_TYPE_DATA, payload_len)?;

        Ok(vec![frame_header, capsule_header, capsule_data])
    }

    // Datagram payload encoding.
    pub(super) fn encode_datagram(
        stream_id: StreamId,
        data: Bytes,
    ) -> Result<Vec<Bytes>, WebTransportError> {
        if !is_request_response_stream(stream_id) {
            debug!("h3_stream validate invalid actual={stream_id} expected=request_response");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_STREAM_CREATION_ERROR),
                "h3_stream validate invalid".into(),
            ));
        }

        let mut header = BytesMut::new();

        write_varint(&mut header, stream_id / 4).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        Ok(vec![header.freeze(), data])
    }

    // H3 GOAWAY frame encoding.
    pub(super) fn encode_goaway(last_stream_id: StreamId) -> Result<Bytes, WebTransportError> {
        let mut buf = BytesMut::with_capacity(8);

        write_varint(&mut buf, last_stream_id).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;

        let payload = buf.freeze();
        let header = encode_frame_header(H3_FRAME_TYPE_GOAWAY, payload.len())?;

        let mut frame = BytesMut::with_capacity(header.len() + payload.len());
        frame.put(header);
        frame.put(payload);
        Ok(frame.freeze())
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
                debug!("qpack_encoder encode failed err={e:?}");
                WebTransportError::Protocol(
                    Some(ERR_QPACK_ENCODER_STREAM_ERROR),
                    "qpack_encoder encode failed".into(),
                )
            })?;

        if !encoder_instructions.is_empty()
            && let Some(id) = self.local_encoder_stream_id
        {
            effects.push(Effect::SendQuicData {
                stream_id: id,
                data: Bytes::from(encoder_instructions),
                end_stream: false,
            });
        }

        let frame_payload_bytes = Bytes::from(frame_payload);
        let frame_len = frame_payload_bytes.len();
        let frame_header = encode_frame_header(H3_FRAME_TYPE_HEADERS, frame_len)?;

        effects.push(Effect::SendQuicData {
            stream_id,
            data: frame_header,
            end_stream: false,
        });
        effects.push(Effect::SendQuicData {
            stream_id,
            data: frame_payload_bytes,
            end_stream,
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
        let mut buf = BytesMut::with_capacity(16);

        let type_val = if is_unidirectional {
            H3_STREAM_TYPE_WEBTRANSPORT
        } else {
            H3_FRAME_TYPE_WT_STREAM
        };

        if let Err(e) = write_varint(&mut buf, type_val) {
            debug!("varint encode failed err={e:?}");
            return Vec::new();
        }

        if let Err(e) = write_varint(&mut buf, control_stream_id) {
            debug!("varint encode failed err={e:?}");
            return Vec::new();
        }

        effects.push(Effect::SendQuicData {
            stream_id,
            data: buf.freeze(),
            end_stream: false,
        });

        let mut partial_frame = self
            .partial_frames
            .remove(&stream_id)
            .unwrap_or_else(|| PartialFrameInfo::new(stream_id));
        partial_frame.stream_type = Some(H3_STREAM_TYPE_WEBTRANSPORT);
        partial_frame.control_stream_id = Some(control_stream_id);
        self.partial_frames.insert(stream_id, partial_frame);

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
                stream_id,
                data,
                end_stream,
            } => {
                if is_unidirectional_stream(*stream_id) {
                    self.recv_uni_stream_data(*stream_id, data.clone(), *end_stream, connection)
                } else {
                    self.recv_request_data(*stream_id, data.clone(), *end_stream, connection)
                }
            }
            ProtocolEvent::TransportDatagramFrameReceived { data } => {
                match Self::recv_datagram(data.clone()) {
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
                WebTransportError::Configuration(c, msg)
                | WebTransportError::Connection(c, msg)
                | WebTransportError::Protocol(c, msg)
                | WebTransportError::Unknown(c, msg) => {
                    let error_code = c.unwrap_or(ERR_H3_INTERNAL_ERROR);
                    effects.push(Effect::CloseQuicConnection {
                        error_code,
                        reason: Some(msg),
                    });
                }
            },
        }

        (h3_events, effects)
    }

    // Local settings initialization.
    pub(super) fn initialize_settings(&mut self) -> Result<Bytes, WebTransportError> {
        let settings = H3Settings {
            enable_connect_protocol: Some(1),
            h3_datagram: Some(1),
            max_field_section_size: Some(self.params.max_field_section_size),
            qpack_blocked_streams: Some(u64::from(self.blocked_streams)),
            qpack_max_table_capacity: Some(u64::from(self.max_table_capacity)),
            wt_enabled: Some(1),
            wt_initial_max_data: Some(self.params.initial_max_data),
            wt_initial_max_streams_bidi: Some(self.params.initial_max_streams_bidi),
            wt_initial_max_streams_uni: Some(self.params.initial_max_streams_uni),
            ..Default::default()
        };
        let payload = encode_settings(&settings)?;
        let header = encode_frame_header(H3_FRAME_TYPE_SETTINGS, payload.len())?;

        let mut frame = BytesMut::with_capacity(header.len() + payload.len());
        frame.put(header);
        frame.put(payload);
        Ok(frame.freeze())
    }

    // Local control stream ID accessor.
    pub(super) fn local_control_stream_id(&self) -> Option<StreamId> {
        self.local_control_stream_id
    }

    // Local stream ID assignment.
    pub(super) fn set_local_stream_ids(
        &mut self,
        control_stream_id: StreamId,
        encoder_stream_id: StreamId,
        decoder_stream_id: StreamId,
    ) -> Result<(), WebTransportError> {
        if !is_unidirectional_stream(control_stream_id) {
            debug!(
                "h3_control_stream validate invalid actual={control_stream_id} expected=unidirectional"
            );
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_ID_ERROR),
                "h3_control_stream validate invalid".into(),
            ));
        }

        if !is_unidirectional_stream(encoder_stream_id) {
            debug!(
                "qpack_encoder_stream validate invalid actual={encoder_stream_id} expected=unidirectional"
            );
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_ID_ERROR),
                "qpack_encoder_stream validate invalid".into(),
            ));
        }

        if !is_unidirectional_stream(decoder_stream_id) {
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

    // Active WebTransport session ID retrieval.
    fn active_session_id(&self, stream_id: StreamId) -> Option<StreamId> {
        let p = self.partial_frames.get(&stream_id)?;

        if p.stream_type == Some(H3_STREAM_TYPE_WEBTRANSPORT) && !p.blocked && p.buffer.is_empty() {
            p.control_stream_id
        } else {
            None
        }
    }

    // Header block decoding and effect generation.
    fn decode_headers(
        &mut self,
        stream_id: StreamId,
        frame_data: Option<Bytes>,
    ) -> Result<HeaderDecodeResult, WebTransportError> {
        let mut effects = Vec::new();

        let (decoder_instructions, raw_headers) = if let Some(data) = frame_data {
            let (instructions, status) =
                self.decoder.decode_header(stream_id, data).map_err(|e| {
                    debug!("h3_headers decode invalid stream_id={stream_id} err={e:?}");
                    WebTransportError::Protocol(
                        Some(ERR_QPACK_DECOMPRESSION_FAILED),
                        "h3_headers decode invalid".into(),
                    )
                })?;
            (instructions, Some(status))
        } else {
            match self.decoder.resume_header(stream_id) {
                Ok((instructions, Some(h))) => (instructions, Some(DecodeStatus::Complete(h))),
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
            && let Some(id) = self.local_decoder_stream_id
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
            None => {
                debug!("h3_headers decode invalid stream_id={stream_id}");
                Err(WebTransportError::Protocol(
                    Some(ERR_H3_INTERNAL_ERROR),
                    "h3_headers decode invalid".into(),
                ))
            }
        }
    }

    // Control stream frame processing.
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
                debug!("h3_cancel_push validate invalid");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_ID_ERROR),
                    "h3_cancel_push validate invalid".into(),
                ));
            }
            H3_FRAME_TYPE_DATA | H3_FRAME_TYPE_HEADERS | H3_FRAME_TYPE_PUSH_PROMISE => {
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
                    && let Some(id) = self.local_encoder_stream_id
                {
                    effects.push(Effect::SendQuicData {
                        stream_id: id,
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

    // Request stream frame processing.
    fn handle_request_frame(
        &mut self,
        stream_id: StreamId,
        partial_frame: &mut PartialFrameInfo,
        frame_type: u64,
        frame_data: Option<Bytes>,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut events = Vec::new();
        let mut effects = Vec::new();

        let is_wt_control =
            partial_frame.is_webtransport_control || is_control_stream(stream_id, connection);

        if is_wt_control && !partial_frame.headers_processed {
            partial_frame.is_webtransport_control = true;
        }

        match frame_type {
            H3_FRAME_TYPE_CANCEL_PUSH
            | H3_FRAME_TYPE_GOAWAY
            | H3_FRAME_TYPE_MAX_PUSH_ID
            | H3_FRAME_TYPE_SETTINGS => {
                debug!("h3_stream validate invalid actual={frame_type} stream_id={stream_id}");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_FRAME_UNEXPECTED),
                    "h3_stream validate invalid".into(),
                ));
            }
            H3_FRAME_TYPE_DATA => {
                let payload = frame_data.unwrap_or_default();

                if is_wt_control {
                    if partial_frame.headers_processed {
                        if !payload.is_empty() {
                            if !connection.is_session_stream(stream_id) {
                                debug!("h3_stream validate invalid stream_id={stream_id}");
                                return Err(WebTransportError::Stream(
                                    stream_id,
                                    Some(ERR_H3_MESSAGE_ERROR),
                                    "h3_stream validate invalid".into(),
                                ));
                            }
                            partial_frame.capsule_buffer.extend_from_slice(&payload);
                        }
                        if !partial_frame.capsule_buffer.is_empty() {
                            events.extend(self.parse_capsules(stream_id, partial_frame)?);
                        }
                    }
                } else if connection.is_session_stream(stream_id) {
                    if !payload.is_empty() {
                        events.push(ProtocolEvent::WebTransportStreamDataReceived {
                            session_id: stream_id,
                            stream_id,
                            data: payload,
                            stream_ended,
                        });
                    }
                } else if !payload.is_empty() {
                    trace!(
                        "h3_frame receive size={} stream_id={stream_id}",
                        payload.len()
                    );
                }
            }
            H3_FRAME_TYPE_HEADERS => {
                if partial_frame.headers_processed {
                    debug!("h3_headers validate failed stream_id={stream_id}");
                    return Err(WebTransportError::Stream(
                        stream_id,
                        Some(ERR_H3_FRAME_UNEXPECTED),
                        "h3_headers validate failed".into(),
                    ));
                }

                match self.decode_headers(stream_id, frame_data.clone())? {
                    HeaderDecodeResult::Blocked => {
                        partial_frame.blocked = true;
                        partial_frame.frame_type = Some(H3_FRAME_TYPE_HEADERS);
                        return Ok((events, effects));
                    }
                    HeaderDecodeResult::Done(raw_headers, decoder_effects) => {
                        effects.extend(decoder_effects);

                        let mut is_wt = false;
                        for (k, v) in &raw_headers {
                            if k.as_ref() == b":protocol" && v.as_ref() == WT_UPGRADE_TOKEN {
                                is_wt = true;
                                break;
                            }
                        }

                        if is_wt {
                            partial_frame.is_webtransport_control = true;
                        }
                        partial_frame.blocked = false;

                        if self.is_client {
                            validate_response_headers(stream_id, &raw_headers)?;
                        } else {
                            validate_request_headers(stream_id, &raw_headers)?;
                        }

                        partial_frame.headers_processed = true;

                        let length = frame_data.as_ref().map(Bytes::len).unwrap_or_else(|| {
                            partial_frame.blocked_frame_size.take().unwrap_or(0)
                        });
                        trace!("h3_headers receive size={length} stream_id={stream_id}");

                        events.push(ProtocolEvent::H3HeadersReceived {
                            stream_id,
                            headers: raw_headers,
                            stream_ended,
                        });
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

    // HTTP/3 capsule parsing loop.
    fn parse_capsules(
        &mut self,
        stream_id: StreamId,
        partial_frame: &mut PartialFrameInfo,
    ) -> Result<Vec<ProtocolEvent>, WebTransportError> {
        let mut events = Vec::new();
        let max_capsule_size = self.params.max_capsule_size;

        loop {
            if partial_frame.capsule_buffer.is_empty() {
                break;
            }

            let mut buf = Cursor::new(&partial_frame.capsule_buffer[..]);
            let Ok(capsule_type) = read_varint(&mut buf) else {
                break;
            };
            let Ok(capsule_length) = read_varint(&mut buf) else {
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

            let header_len = usize::try_from(buf.position()).map_err(|e| {
                debug!("wt_capsule convert failed expected=usize stream_id={stream_id} err={e:?}");
                WebTransportError::Protocol(
                    Some(ERR_H3_INTERNAL_ERROR),
                    "wt_capsule convert failed".into(),
                )
            })?;

            if partial_frame.capsule_buffer.len() < header_len + needed {
                break;
            }

            partial_frame.capsule_buffer.advance(header_len);
            let capsule_value = partial_frame.capsule_buffer.split_to(needed).freeze();

            events.push(ProtocolEvent::H3CapsuleReceived {
                stream_id,
                capsule_type,
                capsule_data: capsule_value,
            });
        }

        Ok(events)
    }

    // Generic stream data parsing loop.
    fn parse_stream_data(
        &mut self,
        stream_id: StreamId,
        partial_frame: &mut PartialFrameInfo,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        let mut h3_events = Vec::new();
        let mut effects = Vec::new();
        let max_field_section_size =
            usize::try_from(self.params.max_field_section_size).unwrap_or(usize::MAX);

        if partial_frame.blocked {
            if let Ok(HeaderDecodeResult::Done(..)) = self.decode_headers(stream_id, None) {
                partial_frame.blocked = false;
            } else {
                return Ok((h3_events, effects));
            }
        }

        loop {
            if partial_frame.blocked {
                break;
            }
            if partial_frame.buffer.is_empty() && !stream_ended {
                break;
            }

            let mut check_wt = false;
            if partial_frame.stream_type.is_none()
                && !partial_frame.headers_processed
                && partial_frame.frame_type.is_none()
                && is_bidirectional_stream(stream_id)
            {
                check_wt = true;
            }

            if check_wt {
                let mut buf = Cursor::new(&partial_frame.buffer[..]);
                let pos = buf.position();
                match read_varint(&mut buf) {
                    Ok(H3_FRAME_TYPE_WT_STREAM) => {
                        if let Ok(control_id) = read_varint(&mut buf) {
                            let new_pos = usize::try_from(buf.position()).map_err(|e| {
                                debug!("varint convert failed expected=usize stream_id={stream_id} err={e:?}");
                                WebTransportError::Protocol(
                                    Some(ERR_H3_INTERNAL_ERROR),
                                    "varint convert failed".into(),
                                )
                            })?;

                            if !is_request_response_stream(control_id) {
                                debug!(
                                    "wt_session validate invalid actual={control_id} expected=request_response"
                                );
                                return Err(WebTransportError::Protocol(
                                    Some(ERR_H3_ID_ERROR),
                                    "wt_session validate invalid".into(),
                                ));
                            }

                            partial_frame.stream_type = Some(H3_STREAM_TYPE_WEBTRANSPORT);
                            partial_frame.control_stream_id = Some(control_id);
                            partial_frame.buffer.advance(new_pos);

                            h3_events.push(ProtocolEvent::WebTransportStreamDataReceived {
                                session_id: control_id,
                                stream_id,
                                data: Bytes::new(),
                                stream_ended: false,
                            });
                            continue;
                        }
                        break;
                    }
                    Ok(_) => {
                        buf.set_position(pos);
                        if self.is_client && is_peer_initiated_stream(stream_id, self.is_client) {
                            debug!("h3_stream validate invalid stream_id={stream_id}");
                            return Err(WebTransportError::Protocol(
                                Some(ERR_H3_STREAM_CREATION_ERROR),
                                "h3_stream validate invalid".into(),
                            ));
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }

            let mut is_wt_data = false;
            let mut wt_control_id = None;
            if partial_frame.stream_type == Some(H3_STREAM_TYPE_WEBTRANSPORT) {
                is_wt_data = true;
                wt_control_id = partial_frame.control_stream_id;
            }

            if is_wt_data {
                let payload = partial_frame
                    .buffer
                    .split_to(partial_frame.buffer.len())
                    .freeze();

                if !payload.is_empty() || stream_ended {
                    let Some(control_id) = wt_control_id else {
                        debug!("wt_stream resolve failed stream_id={stream_id}");
                        return Err(WebTransportError::Protocol(
                            Some(ERR_H3_INTERNAL_ERROR),
                            "wt_stream resolve failed".into(),
                        ));
                    };

                    h3_events.push(ProtocolEvent::WebTransportStreamDataReceived {
                        session_id: control_id,
                        stream_id,
                        data: payload,
                        stream_ended,
                    });
                }
                break;
            }

            if partial_frame.frame_type.is_none() {
                let mut buf = Cursor::new(&partial_frame.buffer[..]);
                if let (Ok(frame_type), Ok(frame_size)) =
                    (read_varint(&mut buf), read_varint(&mut buf))
                {
                    let frame_size = usize::try_from(frame_size).map_err(|e| {
                        debug!(
                            "varint convert failed expected=usize stream_id={stream_id} err={e:?}"
                        );
                        WebTransportError::Protocol(
                            Some(ERR_H3_EXCESSIVE_LOAD),
                            "varint convert failed".into(),
                        )
                    })?;
                    let pos = usize::try_from(buf.position()).map_err(|e| {
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

                    partial_frame.frame_type = Some(frame_type);
                    partial_frame.frame_size = Some(frame_size);
                    if frame_type == H3_FRAME_TYPE_HEADERS {
                        partial_frame.blocked_frame_size = Some(frame_size);
                    }

                    if frame_type == H3_FRAME_TYPE_DATA {
                        trace!("h3_frame receive size={frame_size} stream_id={stream_id}");
                    }

                    partial_frame.buffer.advance(pos);
                } else {
                    break;
                }
            }

            let Some(current_type) = partial_frame.frame_type else {
                break;
            };
            let Some(current_size) = partial_frame.frame_size else {
                break;
            };

            let remaining_in_buf = partial_frame.buffer.len();
            let chunk_size = std::cmp::min(current_size, remaining_in_buf);

            if current_type != H3_FRAME_TYPE_DATA && chunk_size < current_size {
                break;
            }

            let frame_data = partial_frame.buffer.split_to(chunk_size).freeze();
            let is_last_chunk = chunk_size == current_size;

            partial_frame.frame_size = Some(current_size - chunk_size);

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
                let is_end_condition =
                    stream_ended && is_last_chunk && partial_frame.buffer.is_empty();

                let (new_evts, new_fx) = self.handle_request_frame(
                    stream_id,
                    partial_frame,
                    current_type,
                    final_data,
                    is_end_condition,
                    connection,
                )?;
                h3_events.extend(new_evts);
                effects.extend(new_fx);
            }

            if is_last_chunk {
                partial_frame.frame_type = None;
                partial_frame.frame_size = None;
                if current_type == H3_FRAME_TYPE_HEADERS && !partial_frame.blocked {
                    partial_frame.blocked_frame_size = None;
                }
            }

            if chunk_size == 0 && !stream_ended {
                break;
            }
        }

        Ok((h3_events, effects))
    }

    // Datagram frame parsing.
    fn recv_datagram(mut data: Bytes) -> Result<Vec<ProtocolEvent>, WebTransportError> {
        let mut buf = Cursor::new(&data[..]);

        let quarter_id = read_varint(&mut buf).map_err(|e| {
            debug!("varint decode invalid err={e:?}");
            WebTransportError::Protocol(Some(ERR_H3_DATAGRAM_ERROR), "varint decode invalid".into())
        })?;
        let stream_id = quarter_id * 4;

        if !is_request_response_stream(stream_id) {
            debug!("wt_datagram validate invalid actual={stream_id} expected=request_response");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_ID_ERROR),
                "wt_datagram validate invalid".into(),
            ));
        }

        let pos = usize::try_from(buf.position()).map_err(|e| {
            debug!("wt_datagram convert failed expected=usize stream_id={stream_id} err={e:?}");
            WebTransportError::Protocol(
                Some(ERR_H3_EXCESSIVE_LOAD),
                "wt_datagram convert failed".into(),
            )
        })?;

        if pos > data.len() {
            debug!("wt_datagram validate invalid stream_id={stream_id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_INTERNAL_ERROR),
                "wt_datagram validate invalid".into(),
            ));
        }

        data.advance(pos);

        Ok(vec![ProtocolEvent::H3DatagramReceived { stream_id, data }])
    }

    // Request stream data ingestion.
    fn recv_request_data(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        stream_ended: bool,
        connection: &Connection,
    ) -> Result<(Vec<ProtocolEvent>, Vec<Effect>), WebTransportError> {
        if let Some(session_id) = self.active_session_id(stream_id) {
            let mut events = Vec::new();
            let mut effects = Vec::new();

            if !data.is_empty() || stream_ended {
                events.push(ProtocolEvent::WebTransportStreamDataReceived {
                    session_id,
                    stream_id,
                    data,
                    stream_ended,
                });
            }

            if stream_ended {
                if connection.is_session_stream(stream_id) {
                    debug!("wt_stream close stream_id={stream_id}");
                    events.push(ProtocolEvent::H3ConnectStreamClosed { stream_id });
                }
                effects.extend(self.cleanup_stream(stream_id));
            }

            return Ok((events, effects));
        }

        let mut partial_frame = self
            .partial_frames
            .remove(&stream_id)
            .unwrap_or_else(|| PartialFrameInfo::new(stream_id));

        if !data.is_empty() {
            partial_frame.buffer.extend_from_slice(&data);
        }
        if stream_ended {
            partial_frame.ended = true;
        }

        if !partial_frame.blocked && partial_frame.buffer.is_empty() && !partial_frame.ended {
            self.partial_frames.insert(stream_id, partial_frame);
            return Ok((Vec::new(), Vec::new()));
        }

        let is_ended = partial_frame.ended;
        let (mut events, mut effects) =
            self.parse_stream_data(stream_id, &mut partial_frame, is_ended, connection)?;

        let should_cleanup = partial_frame.ended && partial_frame.buffer.is_empty();
        if !should_cleanup {
            self.partial_frames.insert(stream_id, partial_frame);
        }

        if should_cleanup {
            if connection.is_session_stream(stream_id) {
                debug!("wt_stream close stream_id={stream_id}");
                events.push(ProtocolEvent::H3ConnectStreamClosed { stream_id });
            }
            effects.extend(self.cleanup_stream(stream_id));
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
        if let Some(session_id) = self.active_session_id(stream_id) {
            let mut events = Vec::new();
            let mut effects = Vec::new();

            if !data.is_empty() || stream_ended {
                events.push(ProtocolEvent::WebTransportStreamDataReceived {
                    session_id,
                    stream_id,
                    data,
                    stream_ended,
                });
            }

            if stream_ended {
                effects.extend(self.cleanup_stream(stream_id));
            }

            return Ok((events, effects));
        }

        let mut partial_frame = self
            .partial_frames
            .remove(&stream_id)
            .unwrap_or_else(|| PartialFrameInfo::new(stream_id));

        if !data.is_empty() {
            partial_frame.buffer.extend_from_slice(&data);
        }
        if stream_ended {
            partial_frame.ended = true;
        }

        if partial_frame.blocked || (partial_frame.buffer.is_empty() && !partial_frame.ended) {
            self.partial_frames.insert(stream_id, partial_frame);
            return Ok((Vec::new(), Vec::new()));
        }

        let mut events = Vec::new();
        let mut effects = Vec::new();

        if partial_frame.stream_type.is_none() {
            let mut buf = Cursor::new(&partial_frame.buffer[..]);
            if let Ok(stream_type) = read_varint(&mut buf) {
                let pos = usize::try_from(buf.position()).map_err(|e| {
                    debug!("varint convert failed expected=usize stream_id={stream_id} err={e:?}");
                    WebTransportError::Protocol(
                        Some(ERR_H3_INTERNAL_ERROR),
                        "varint convert failed".into(),
                    )
                })?;
                partial_frame.stream_type = Some(stream_type);
                partial_frame.buffer.advance(pos);

                match stream_type {
                    H3_STREAM_TYPE_CONTROL
                    | H3_STREAM_TYPE_QPACK_DECODER
                    | H3_STREAM_TYPE_QPACK_ENCODER => {
                        self.register_peer_infrastructure_stream(stream_id, stream_type)?;
                    }
                    H3_STREAM_TYPE_PUSH => {
                        debug!("h3_push_stream validate invalid stream_id={stream_id}");
                        return Err(WebTransportError::Protocol(
                            Some(ERR_H3_ID_ERROR),
                            "h3_push_stream validate invalid".into(),
                        ));
                    }
                    H3_STREAM_TYPE_WEBTRANSPORT => {}
                    _ => {
                        debug!(
                            "h3_stream validate invalid actual={stream_type} stream_id={stream_id}"
                        );
                    }
                }
            }
        }

        let Some(stream_type) = partial_frame.stream_type else {
            let is_ended = partial_frame.ended;
            if is_ended {
                effects.extend(self.cleanup_stream(stream_id));
            } else {
                self.partial_frames.insert(stream_id, partial_frame);
            }
            return Ok((events, effects));
        };

        match stream_type {
            H3_STREAM_TYPE_CONTROL => {
                if partial_frame.ended {
                    debug!("h3_control_stream validate invalid stream_id={stream_id}");
                    return Err(WebTransportError::Protocol(
                        Some(ERR_H3_CLOSED_CRITICAL_STREAM),
                        "h3_control_stream validate invalid".into(),
                    ));
                }

                loop {
                    if partial_frame.buffer.is_empty() {
                        break;
                    }
                    let mut buf = Cursor::new(&partial_frame.buffer[..]);
                    let Ok(frame_type) = read_varint(&mut buf) else {
                        break;
                    };
                    let Ok(frame_size) = read_varint(&mut buf) else {
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
                    let header_len = usize::try_from(buf.position()).map_err(|e| {
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

                    if partial_frame.buffer.len() < header_len + needed {
                        break;
                    }

                    partial_frame.buffer.advance(header_len);
                    let frame_data = partial_frame.buffer.split_to(needed).freeze();

                    let (new_evts, new_fx) =
                        self.handle_control_frame(frame_type, &frame_data, connection)?;
                    events.extend(new_evts);
                    effects.extend(new_fx);
                }
            }
            H3_STREAM_TYPE_QPACK_DECODER => {
                if !partial_frame.buffer.is_empty() {
                    let slice_data = partial_frame
                        .buffer
                        .split_to(partial_frame.buffer.len())
                        .freeze();
                    self.encoder.feed_decoder(&slice_data);
                }
            }
            H3_STREAM_TYPE_QPACK_ENCODER => {
                if !partial_frame.buffer.is_empty() {
                    let slice_data = partial_frame
                        .buffer
                        .split_to(partial_frame.buffer.len())
                        .freeze();
                    match self.decoder.feed_encoder(&slice_data) {
                        Ok(unblocked) => {
                            for sid in unblocked {
                                let should_resume = {
                                    if let Some(pu) = self.partial_frames.get_mut(&sid) {
                                        if pu.blocked {
                                            pu.blocked = false;
                                            Some(pu.ended)
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                };

                                if let Some(ended) = should_resume {
                                    let (new_evts, new_fx) = self.recv_request_data(
                                        sid,
                                        Bytes::new(),
                                        ended,
                                        connection,
                                    )?;
                                    events.extend(new_evts);
                                    effects.extend(new_fx);
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
                if partial_frame.control_stream_id.is_none() {
                    let mut buf = Cursor::new(&partial_frame.buffer[..]);
                    if let Ok(control_id) = read_varint(&mut buf) {
                        let pos = usize::try_from(buf.position()).map_err(|e| {
                            debug!("varint convert failed expected=usize stream_id={stream_id} err={e:?}");
                            WebTransportError::Protocol(
                                Some(ERR_H3_INTERNAL_ERROR),
                                "varint convert failed".into(),
                            )
                        })?;

                        if !is_request_response_stream(control_id) {
                            debug!(
                                "wt_session validate invalid actual={control_id} expected=request_response"
                            );
                            return Err(WebTransportError::Protocol(
                                Some(ERR_H3_ID_ERROR),
                                "wt_session validate invalid".into(),
                            ));
                        }

                        partial_frame.control_stream_id = Some(control_id);
                        partial_frame.buffer.advance(pos);
                    } else {
                        let is_ended = partial_frame.ended;
                        if is_ended {
                            effects.extend(self.cleanup_stream(stream_id));
                        } else {
                            self.partial_frames.insert(stream_id, partial_frame);
                        }
                        return Ok((events, effects));
                    }
                }

                let payload = partial_frame
                    .buffer
                    .split_to(partial_frame.buffer.len())
                    .freeze();
                let ended = partial_frame.ended;

                if !payload.is_empty() || ended {
                    let Some(control_id) = partial_frame.control_stream_id else {
                        debug!("wt_stream resolve failed stream_id={stream_id}");
                        return Err(WebTransportError::Protocol(
                            Some(ERR_H3_INTERNAL_ERROR),
                            "wt_stream resolve failed".into(),
                        ));
                    };
                    events.push(ProtocolEvent::WebTransportStreamDataReceived {
                        session_id: control_id,
                        stream_id,
                        data: payload,
                        stream_ended: ended,
                    });
                }

                if ended {
                    effects.extend(self.cleanup_stream(stream_id));
                } else {
                    self.partial_frames.insert(stream_id, partial_frame);
                }

                return Ok((events, effects));
            }
            _ => {
                partial_frame.buffer.clear();
            }
        }

        let should_cleanup = partial_frame.ended && partial_frame.buffer.is_empty();
        if should_cleanup {
            if matches!(
                stream_type,
                H3_STREAM_TYPE_CONTROL
                    | H3_STREAM_TYPE_QPACK_DECODER
                    | H3_STREAM_TYPE_QPACK_ENCODER
            ) {
                debug!("h3_control_stream validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Protocol(
                    Some(ERR_H3_CLOSED_CRITICAL_STREAM),
                    "h3_control_stream validate invalid".into(),
                ));
            }
            effects.extend(self.cleanup_stream(stream_id));
        } else {
            self.partial_frames.insert(stream_id, partial_frame);
        }

        Ok((events, effects))
    }

    // Register peer infrastructure stream with role/id checks.
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
}

// HTTP/3 configuration parameters.
#[derive(Clone, Copy, Debug)]
pub(super) struct H3Params {
    pub(super) initial_max_data: u64,
    pub(super) initial_max_streams_bidi: u64,
    pub(super) initial_max_streams_uni: u64,
    pub(super) max_capsule_size: u64,
    pub(super) max_field_section_size: u64,
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
    buffer: BytesMut,
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
            blocked: false,
            blocked_frame_size: None,
            buffer: BytesMut::new(),
            capsule_buffer: BytesMut::new(),
            control_stream_id: None,
            ended: false,
            frame_size: None,
            frame_type: None,
            headers_processed: false,
            is_webtransport_control: false,
            stream_type: None,
        }
    }
}

// Generic H3 frame header encoding.
fn encode_frame_header(frame_type: u64, payload_length: usize) -> Result<Bytes, WebTransportError> {
    let mut buf = BytesMut::with_capacity(16);

    write_varint(&mut buf, frame_type).map_err(|e| {
        debug!("varint encode failed err={e:?}");
        WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
    })?;

    write_varint(&mut buf, payload_length as u64).map_err(|e| {
        debug!("varint encode failed err={e:?}");
        WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
    })?;

    Ok(buf.freeze())
}

// SETTINGS frame payload encoding.
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

    let mut buf = BytesMut::with_capacity(128);

    for &(id, val) in known {
        let Some(val) = val else { continue };
        write_varint(&mut buf, id).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
        write_varint(&mut buf, val).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
    }

    for &(id, val) in &settings.unknown {
        write_varint(&mut buf, id).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
        write_varint(&mut buf, val).map_err(|e| {
            debug!("varint encode failed err={e:?}");
            WebTransportError::Protocol(Some(ERR_LIB_INTERNAL_ERROR), "varint encode failed".into())
        })?;
    }

    Ok(buf.freeze())
}

// Control stream identification.
fn is_control_stream(stream_id: StreamId, connection: &Connection) -> bool {
    connection.is_session_stream(stream_id)
}

// SETTINGS payload parsing.
fn parse_settings(data: &[u8]) -> Result<H3Settings, WebTransportError> {
    let mut out = H3Settings::default();
    let mut buf = Cursor::new(data);
    let mut count = 0usize;

    while buf.has_remaining() {
        if count >= SETTINGS_ENTRIES_LIMIT {
            debug!("h3_settings validate exceeded actual={count} expected=settings_entries_limit");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_SETTINGS_ERROR),
                "h3_settings validate exceeded".into(),
            ));
        }
        let id = read_varint(&mut buf).map_err(|e| {
            debug!("varint decode invalid err={e:?}");
            WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), "varint decode invalid".into())
        })?;
        let val = read_varint(&mut buf).map_err(|e| {
            debug!("varint decode invalid err={e:?}");
            WebTransportError::Protocol(Some(ERR_H3_FRAME_ERROR), "varint decode invalid".into())
        })?;

        if RESERVED_SETTINGS.contains(&id) {
            debug!("h3_settings validate invalid actual={id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_SETTINGS_ERROR),
                "h3_settings validate invalid".into(),
            ));
        }

        let slot = match id {
            SETTINGS_ENABLE_CONNECT_PROTOCOL => &mut out.enable_connect_protocol,
            SETTINGS_H3_DATAGRAM => &mut out.h3_datagram,
            SETTINGS_MAX_FIELD_SECTION_SIZE => &mut out.max_field_section_size,
            SETTINGS_QPACK_BLOCKED_STREAMS => &mut out.qpack_blocked_streams,
            SETTINGS_QPACK_MAX_TABLE_CAPACITY => &mut out.qpack_max_table_capacity,
            SETTINGS_WT_ENABLED => &mut out.wt_enabled,
            SETTINGS_WT_INITIAL_MAX_DATA => &mut out.wt_initial_max_data,
            SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI => &mut out.wt_initial_max_streams_bidi,
            SETTINGS_WT_INITIAL_MAX_STREAMS_UNI => &mut out.wt_initial_max_streams_uni,
            _ => {
                if out.unknown.iter().any(|&(k, _)| k == id) {
                    debug!("h3_settings validate invalid actual={id}");
                    return Err(WebTransportError::Protocol(
                        Some(ERR_H3_SETTINGS_ERROR),
                        "h3_settings validate invalid".into(),
                    ));
                }
                out.unknown.push((id, val));
                count += 1;
                continue;
            }
        };

        if slot.is_some() {
            debug!("h3_settings validate invalid actual={id}");
            return Err(WebTransportError::Protocol(
                Some(ERR_H3_SETTINGS_ERROR),
                "h3_settings validate invalid".into(),
            ));
        }
        *slot = Some(val);
        count += 1;
    }

    Ok(out)
}

// Header name syntax validation.
fn validate_header_name(stream_id: StreamId, key: &[u8]) -> Result<(), WebTransportError> {
    if key.is_empty() {
        debug!("h3_headers validate invalid stream_id={stream_id}");
        return Err(WebTransportError::Stream(
            stream_id,
            Some(ERR_H3_MESSAGE_ERROR),
            "h3_headers validate invalid".into(),
        ));
    }
    for (i, &b) in key.iter().enumerate() {
        if b == COLON {
            if i != 0 {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
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

// Header value syntax validation.
fn validate_header_value(stream_id: StreamId, value: &[u8]) -> Result<(), WebTransportError> {
    if let (Some(first), Some(last)) = (value.first(), value.last()) {
        if WHITESPACE.contains(first) || WHITESPACE.contains(last) {
            debug!("h3_headers validate invalid stream_id={stream_id}");
            return Err(WebTransportError::Stream(
                stream_id,
                Some(ERR_H3_MESSAGE_ERROR),
                "h3_headers validate invalid".into(),
            ));
        }
        for &b in value {
            if b == HTAB || (SP..=0x7E).contains(&b) {
                continue;
            }
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

// Request header semantic validation.
fn validate_request_headers(
    stream_id: StreamId,
    headers: &Headers,
) -> Result<(), WebTransportError> {
    let mut seen_pseudo = HashSet::new();
    let mut after_pseudo = false;
    let mut scheme: Option<&[u8]> = None;
    let mut authority: Option<&[u8]> = None;
    let mut path: Option<&[u8]> = None;

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

        validate_header_name(stream_id, k_slice)?;
        validate_header_value(stream_id, v_slice)?;

        if k_slice.starts_with(b":") {
            if after_pseudo {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
                ));
            }
            if !allowed.iter().any(|x| x[..] == k_slice[..]) {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
                ));
            }
            if seen_pseudo.contains(k_slice) {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
                ));
            }
            seen_pseudo.insert(k_slice);

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
            debug!("h3_headers validate invalid stream_id={stream_id}");
            return Err(WebTransportError::Stream(
                stream_id,
                Some(ERR_H3_MESSAGE_ERROR),
                "h3_headers validate invalid".into(),
            ));
        }
    }

    if matches!(scheme, Some(b"http" | b"https")) {
        if authority.unwrap_or_default().is_empty() {
            debug!("h3_headers validate invalid stream_id={stream_id}");
            return Err(WebTransportError::Stream(
                stream_id,
                Some(ERR_H3_MESSAGE_ERROR),
                "h3_headers validate invalid".into(),
            ));
        }
        if path.unwrap_or_default().is_empty() {
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

// Response header semantic validation.
fn validate_response_headers(
    stream_id: StreamId,
    headers: &Headers,
) -> Result<(), WebTransportError> {
    let mut seen_pseudo = HashSet::new();
    let mut after_pseudo = false;

    for (k, v) in headers {
        let k_slice = k.as_ref();
        validate_header_name(stream_id, k_slice)?;
        validate_header_value(stream_id, v.as_ref())?;

        if k_slice.starts_with(b":") {
            if after_pseudo {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
                ));
            }
            if k_slice != b":status" {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
                ));
            }
            if seen_pseudo.contains(k_slice) {
                debug!("h3_headers validate invalid stream_id={stream_id}");
                return Err(WebTransportError::Stream(
                    stream_id,
                    Some(ERR_H3_MESSAGE_ERROR),
                    "h3_headers validate invalid".into(),
                ));
            }
            seen_pseudo.insert(k_slice);
        } else {
            after_pseudo = true;
        }
    }

    if !seen_pseudo.contains(b":status".as_slice()) {
        debug!("h3_headers validate invalid stream_id={stream_id}");
        return Err(WebTransportError::Stream(
            stream_id,
            Some(ERR_H3_MESSAGE_ERROR),
            "h3_headers validate invalid".into(),
        ));
    }

    Ok(())
}

// SETTINGS semantic validation.
fn validate_settings(
    settings: &H3Settings,
    connection: &Connection,
) -> Result<(), WebTransportError> {
    if settings.enable_connect_protocol.is_some_and(|val| val != 1) {
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
