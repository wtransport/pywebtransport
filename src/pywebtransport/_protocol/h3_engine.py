"""Internal specialized H3 protocol engine logic."""

from __future__ import annotations

import functools
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, cast

import pylsqpack
from aioquic._buffer import Buffer as QuicBuffer
from aioquic._buffer import BufferReadError
from aioquic.buffer import UINT_VAR_MAX_SIZE, encode_uint_var

from pywebtransport import constants
from pywebtransport._protocol.events import (
    CapsuleReceived,
    CloseQuicConnection,
    ConnectStreamClosed,
    DatagramReceived,
    Effect,
    GoawayReceived,
    H3Event,
    HeadersReceived,
    LogH3Frame,
    SendQuicData,
    SettingsReceived,
    TransportDatagramFrameReceived,
    TransportStreamDataReceived,
    WebTransportStreamDataReceived,
)
from pywebtransport._protocol.utils import (
    is_bidirectional_stream,
    is_request_response_stream,
    is_unidirectional_stream,
    validate_control_stream_id,
    validate_unidirectional_stream_id,
)
from pywebtransport.constants import ErrorCodes
from pywebtransport.exceptions import ProtocolError
from pywebtransport.types import Buffer, Headers, StreamId
from pywebtransport.utils import get_logger

if TYPE_CHECKING:
    from pywebtransport._protocol.state import ProtocolState
    from pywebtransport.config import ClientConfig, ServerConfig


__all__: list[str] = []


type _RawHeaders = list[tuple[bytes, bytes]]

COLON = 0x3A
CR = 0x0D
HTAB = 0x09
LF = 0x0A
NUL = 0x0
RESERVED_SETTINGS = (0x0, 0x2, 0x3, 0x4, 0x5)
SP = 0x20
WHITESPACE = (SP, HTAB)

logger = get_logger(name=__name__)


class WebTransportH3Engine:
    """Handles WebTransport over HTTP/3 protocol parsing and encoding."""

    def __init__(self, *, is_client: bool, config: ClientConfig | ServerConfig) -> None:
        """Initialize the WebTransportH3Engine."""
        self._config = config
        self._is_client = is_client

        self._max_table_capacity = 4096
        self._blocked_streams = 16
        self._decoder = pylsqpack.Decoder(self._max_table_capacity, self._blocked_streams)
        self._encoder = pylsqpack.Encoder()

        self._settings_received = False
        self._local_control_stream_id: int | None = None
        self._local_decoder_stream_id: int | None = None
        self._local_encoder_stream_id: int | None = None
        self._peer_control_stream_id: int | None = None
        self._peer_decoder_stream_id: int | None = None
        self._peer_encoder_stream_id: int | None = None

        self._partial_frames: dict[StreamId, _PartialFrameInfo] = {}

    def cleanup_stream(self, *, stream_id: StreamId) -> None:
        """Remove any partial state associated with a closed stream."""
        self._partial_frames.pop(stream_id, None)

    def encode_capsule(self, *, stream_id: StreamId, capsule_type: int, capsule_data: bytes) -> bytes:
        """Encode a capsule into an HTTP/3 DATA frame."""
        if not is_request_response_stream(stream_id=stream_id):
            raise ProtocolError(
                message="Capsules can only be encoded for client-initiated bidirectional streams.",
                error_code=ErrorCodes.H3_STREAM_CREATION_ERROR,
            )

        capsule_buf = QuicBuffer(capacity=len(capsule_data) + 2 * UINT_VAR_MAX_SIZE)
        capsule_buf.push_uint_var(capsule_type)
        capsule_buf.push_uint_var(len(capsule_data))
        capsule_buf.push_bytes(capsule_data)

        return _encode_frame(frame_type=constants.H3_FRAME_TYPE_DATA, frame_data=capsule_buf.data)

    def encode_datagram(self, *, stream_id: StreamId, data: Buffer | list[Buffer]) -> list[Buffer]:
        """Encode a datagram payload."""
        if not is_request_response_stream(stream_id=stream_id):
            raise ProtocolError(
                message="Datagrams can only be encoded for client-initiated bidirectional streams",
                error_code=ErrorCodes.H3_STREAM_CREATION_ERROR,
            )

        header = encode_uint_var(stream_id // 4)

        if isinstance(data, list):
            return [header, *data]
        return [header, data]

    def encode_goaway_frame(self, *, last_stream_id: int = 0) -> bytes:
        """Encode an H3 GOAWAY frame."""
        buf = QuicBuffer(capacity=UINT_VAR_MAX_SIZE)
        buf.push_uint_var(last_stream_id)
        return _encode_frame(frame_type=constants.H3_FRAME_TYPE_GOAWAY, frame_data=buf.data)

    def encode_headers(self, *, stream_id: StreamId, headers: Headers, end_stream: bool = False) -> list[Effect]:
        """Encode headers and return effects to send them."""
        effects: list[Effect] = []
        raw_items = headers.items() if isinstance(headers, dict) else headers
        raw_headers: _RawHeaders = []

        for k, v in raw_items:
            key_bytes = k if isinstance(k, bytes) else k.encode("utf-8")
            val_bytes = v if isinstance(v, bytes) else v.encode("utf-8")
            raw_headers.append((key_bytes, val_bytes))

        encoder_instructions, frame_payload = self._encoder.encode(stream_id, raw_headers)
        if self._local_encoder_stream_id is not None and encoder_instructions:
            effects.append(
                SendQuicData(stream_id=self._local_encoder_stream_id, data=encoder_instructions, end_stream=False)
            )

        frame_data = _encode_frame(frame_type=constants.H3_FRAME_TYPE_HEADERS, frame_data=frame_payload)
        effects.append(SendQuicData(stream_id=stream_id, data=frame_data, end_stream=end_stream))

        effects.append(
            LogH3Frame(
                category="http",
                event="frame_created",
                data={
                    "frame_type": constants.H3_FRAME_TYPE_HEADERS,
                    "length": len(frame_payload),
                    "headers": raw_headers,
                    "stream_id": stream_id,
                },
            )
        )
        return effects

    def encode_webtransport_stream_creation(
        self, *, stream_id: StreamId, control_stream_id: StreamId, is_unidirectional: bool
    ) -> list[Effect]:
        """Return effects to signal stream creation."""
        effects: list[Effect] = []

        if is_unidirectional:
            effects.append(
                SendQuicData(
                    stream_id=stream_id, data=encode_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT), end_stream=False
                )
            )
            effects.append(SendQuicData(stream_id=stream_id, data=encode_uint_var(control_stream_id), end_stream=False))
        else:
            frame_data = encode_uint_var(constants.H3_FRAME_TYPE_WEBTRANSPORT_STREAM) + encode_uint_var(
                control_stream_id
            )
            effects.append(SendQuicData(stream_id=stream_id, data=frame_data, end_stream=False))

        effects.append(
            LogH3Frame(category="http", event="stream_type_set", data={"new": "webtransport", "stream_id": stream_id})
        )

        if self._is_client:
            partial_info = self._get_or_create_partial_frame_info(stream_id=stream_id)
            partial_info.stream_type = constants.H3_STREAM_TYPE_WEBTRANSPORT
            partial_info.control_stream_id = control_stream_id
        return effects

    def handle_transport_event(
        self, *, event: TransportStreamDataReceived | TransportDatagramFrameReceived, state: ProtocolState
    ) -> tuple[list[H3Event], list[Effect]]:
        """Handle a translated transport event and return H3 events and effects."""
        h3_events: list[H3Event] = []
        effects: list[Effect] = []

        try:
            match event:
                case TransportStreamDataReceived(data=data, end_stream=end_stream, stream_id=stream_id):
                    if is_unidirectional_stream(stream_id=stream_id):
                        new_h3_events, new_effects = self._receive_stream_data_uni(
                            stream_id=stream_id, data=data, stream_ended=end_stream, state=state
                        )
                        h3_events.extend(new_h3_events)
                        effects.extend(new_effects)
                    else:
                        new_h3_events, new_effects = self._receive_request_data(
                            stream_id=stream_id, data=data, stream_ended=end_stream, state=state
                        )
                        h3_events.extend(new_h3_events)
                        effects.extend(new_effects)

                case TransportDatagramFrameReceived(data=data):
                    h3_events.extend(self._receive_datagram(data=data))

        except ProtocolError as exc:
            effects.append(CloseQuicConnection(error_code=exc.error_code, reason=str(exc)))
        except pylsqpack.StreamBlocked as exc:
            stream_id = int(str(exc))
            partial_info = self._partial_frames.get(stream_id)
            if partial_info is not None:
                partial_info.blocked = True
        except Exception as exc:
            effects.append(CloseQuicConnection(error_code=ErrorCodes.INTERNAL_ERROR, reason=f"H3 Engine Error: {exc}"))

        return h3_events, effects

    def initialize_connection(self) -> bytes:
        """Get the encoded SETTINGS frame payload."""
        return _encode_frame(
            frame_type=constants.H3_FRAME_TYPE_SETTINGS,
            frame_data=_encode_settings(settings=self._get_local_settings()),
        )

    def set_local_stream_ids(
        self, *, control_stream_id: StreamId, encoder_stream_id: StreamId, decoder_stream_id: StreamId
    ) -> None:
        """Set the local unidirectional stream IDs (called by Engine)."""
        validate_unidirectional_stream_id(stream_id=control_stream_id, context="Control")
        validate_unidirectional_stream_id(stream_id=encoder_stream_id, context="Encoder")
        validate_unidirectional_stream_id(stream_id=decoder_stream_id, context="Decoder")

        self._local_control_stream_id = control_stream_id
        self._local_encoder_stream_id = encoder_stream_id
        self._local_decoder_stream_id = decoder_stream_id

    def _decode_headers(self, *, stream_id: int, frame_data: bytes | None) -> tuple[_RawHeaders, list[Effect]]:
        """Decode a HEADERS frame, return headers and decoder instruction effects."""
        effects: list[Effect] = []
        decoder_instructions = b""
        try:
            if frame_data is None:
                decoder_instructions, raw_headers = self._decoder.resume_header(stream_id)
            else:
                decoder_instructions, raw_headers = self._decoder.feed_header(stream_id, frame_data)
        except pylsqpack.DecompressionFailed as exc:
            raise ProtocolError(
                message="QPACK decompression failed", error_code=ErrorCodes.QPACK_DECOMPRESSION_FAILED
            ) from exc

        if self._local_decoder_stream_id is not None and decoder_instructions:
            effects.append(
                SendQuicData(stream_id=self._local_decoder_stream_id, data=decoder_instructions, end_stream=False)
            )

        return raw_headers, effects

    @functools.cache
    def _get_local_settings(self) -> dict[int, int]:
        """Get the local HTTP/3 settings."""
        settings: dict[int, int] = {
            constants.SETTINGS_ENABLE_CONNECT_PROTOCOL: 1,
            constants.SETTINGS_H3_DATAGRAM: 1,
            constants.SETTINGS_QPACK_BLOCKED_STREAMS: self._blocked_streams,
            constants.SETTINGS_QPACK_MAX_TABLE_CAPACITY: self._max_table_capacity,
            constants.SETTINGS_WT_INITIAL_MAX_DATA: self._config.initial_max_data,
            constants.SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI: self._config.initial_max_streams_bidi,
            constants.SETTINGS_WT_INITIAL_MAX_STREAMS_UNI: self._config.initial_max_streams_uni,
        }
        return settings

    def _get_or_create_partial_frame_info(self, *, stream_id: int) -> _PartialFrameInfo:
        """Get or create the partial frame info for a stream."""
        if stream_id not in self._partial_frames:
            self._partial_frames[stream_id] = _PartialFrameInfo(stream_id=stream_id)
        return self._partial_frames[stream_id]

    def _handle_control_frame(
        self, *, frame_type: int, frame_data: bytes, state: ProtocolState
    ) -> tuple[list[H3Event], list[Effect]]:
        """Handle a frame received on the control stream, return events and effects."""
        effects: list[Effect] = []
        h3_events: list[H3Event] = []
        if frame_type != constants.H3_FRAME_TYPE_SETTINGS and not self._settings_received:
            raise ProtocolError(
                message="First frame on control stream must be SETTINGS", error_code=ErrorCodes.H3_MISSING_SETTINGS
            )

        match frame_type:
            case constants.H3_FRAME_TYPE_SETTINGS:
                if self._settings_received:
                    raise ProtocolError(
                        message="SETTINGS frame received twice", error_code=ErrorCodes.H3_FRAME_UNEXPECTED
                    )
                settings = _parse_settings(data=frame_data)
                self._validate_settings(settings=settings, state=state)

                encoder_instructions = self._encoder.apply_settings(
                    max_table_capacity=settings.get(constants.SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0),
                    blocked_streams=settings.get(constants.SETTINGS_QPACK_BLOCKED_STREAMS, 0),
                )

                if self._local_encoder_stream_id is not None and encoder_instructions:
                    effects.append(
                        SendQuicData(
                            stream_id=self._local_encoder_stream_id, data=encoder_instructions, end_stream=False
                        )
                    )
                self._settings_received = True
                h3_events.append(SettingsReceived(settings=settings))

            case constants.H3_FRAME_TYPE_GOAWAY:
                logger.debug("H3 GOAWAY frame received.")
                h3_events.append(GoawayReceived())

            case constants.H3_FRAME_TYPE_HEADERS:
                raise ProtocolError(
                    message="Invalid frame type on control stream", error_code=ErrorCodes.H3_FRAME_UNEXPECTED
                )

            case _:
                pass

        return h3_events, effects

    def _handle_request_frame(
        self, *, frame_type: int, frame_data: bytes | None, stream_id: int, stream_ended: bool, state: ProtocolState
    ) -> tuple[list[H3Event], list[Effect]]:
        """Handle a frame received on a request stream using a routing pattern."""
        h3_events: list[H3Event] = []
        effects: list[Effect] = []
        partial_info = self._get_or_create_partial_frame_info(stream_id=stream_id)

        match frame_type:
            case constants.H3_FRAME_TYPE_DATA:
                payload = frame_data if frame_data is not None else b""

                is_webtransport_control = partial_info.is_webtransport_control

                if not is_webtransport_control:
                    is_webtransport_control = self._is_control_stream(stream_id=stream_id, state=state)

                if is_webtransport_control:
                    if partial_info.headers_processed:
                        if payload:
                            partial_info.capsule_buffer.extend(payload)
                        if partial_info.capsule_buffer:
                            new_h3_events = self._parse_capsules(stream_id=stream_id, partial_info=partial_info)
                            h3_events.extend(new_h3_events)

                elif stream_id in state.streams:
                    session_id = state.streams[stream_id].session_id
                    session_data = state.sessions.get(session_id)
                    control_stream_id = session_id if session_data is not None else None

                    if payload:
                        if control_stream_id is not None:
                            h3_events.append(
                                WebTransportStreamDataReceived(
                                    data=payload,
                                    session_id=control_stream_id,
                                    stream_id=stream_id,
                                    stream_ended=stream_ended,
                                )
                            )
                        else:
                            raise ProtocolError(
                                message=f"Data stream {stream_id} orphaned (no control stream)",
                                error_code=ErrorCodes.INTERNAL_ERROR,
                            )
                else:
                    if payload:
                        logger.debug(
                            "Ignored DATA frame on non-WebTransport stream %d (len=%d)", stream_id, len(payload)
                        )

            case constants.H3_FRAME_TYPE_HEADERS:
                if not self._settings_received:
                    raise pylsqpack.StreamBlocked(stream_id)

                if partial_info.headers_processed:
                    raise ProtocolError(
                        message="HEADERS frame received after initial headers",
                        error_code=ErrorCodes.H3_FRAME_UNEXPECTED,
                    )

                (raw_headers, decoder_effects) = self._decode_headers(stream_id=stream_id, frame_data=frame_data)
                effects.extend(decoder_effects)

                for k, v in raw_headers:
                    if k == b":protocol" and v == b"webtransport":
                        partial_info.is_webtransport_control = True
                        break

                if self._is_client:
                    _validate_response_headers(headers=raw_headers)
                else:
                    _validate_request_headers(headers=raw_headers)

                partial_info.headers_processed = True

                frame_info = self._partial_frames.get(stream_id)
                length = 0
                if frame_data is not None:
                    length = len(frame_data)
                elif frame_info is not None and frame_info.blocked_frame_size is not None:
                    length = frame_info.blocked_frame_size
                effects.append(
                    LogH3Frame(
                        category="http",
                        event="frame_parsed",
                        data={
                            "frame_type": constants.H3_FRAME_TYPE_HEADERS,
                            "length": length,
                            "headers": raw_headers,
                            "stream_id": stream_id,
                        },
                    )
                )

                h3_events.append(
                    HeadersReceived(headers=cast(Headers, raw_headers), stream_id=stream_id, stream_ended=stream_ended)
                )

            case constants.H3_FRAME_TYPE_SETTINGS:
                raise ProtocolError(
                    message="Invalid frame type on request stream", error_code=ErrorCodes.H3_FRAME_UNEXPECTED
                )

            case (
                constants.H3_FRAME_TYPE_CANCEL_PUSH
                | constants.H3_FRAME_TYPE_MAX_PUSH_ID
                | constants.H3_FRAME_TYPE_PUSH_PROMISE
            ):
                pass

            case constants.H3_FRAME_TYPE_WEBTRANSPORT_STREAM:
                raise ProtocolError(
                    message="WT_STREAM frame (0x41) received in unexpected location",
                    error_code=ErrorCodes.H3_FRAME_ERROR,
                )

        return h3_events, effects

    def _is_control_stream(self, *, stream_id: int, state: ProtocolState) -> bool:
        """Check if the given stream ID is a WebTransport control stream (CONNECT stream)."""
        return stream_id in state.sessions

    def _parse_capsules(self, *, stream_id: int, partial_info: _PartialFrameInfo) -> list[H3Event]:
        """Parse Capsules from the accumulated data buffer in partial_info."""
        h3_events: list[H3Event] = []
        buf = QuicBuffer(data=bytes(partial_info.capsule_buffer))
        consumed = 0

        while not buf.eof():
            start_pos = buf.tell()
            try:
                capsule_type = buf.pull_uint_var()
                capsule_length = buf.pull_uint_var()

                if buf.capacity - buf.tell() < capsule_length:
                    buf.seek(start_pos)
                    break

                capsule_value = buf.pull_bytes(capsule_length)
                consumed = buf.tell()

                h3_events.append(
                    CapsuleReceived(stream_id=stream_id, capsule_type=capsule_type, capsule_data=capsule_value)
                )

            except BufferReadError:
                buf.seek(start_pos)
                break

        if consumed > 0:
            remaining_data = partial_info.capsule_buffer[consumed:]
            partial_info.capsule_buffer = bytearray(remaining_data)

        return h3_events

    def _parse_stream_data(
        self, *, stream_id: int, data_buffer: deque[Buffer], stream_ended: bool, state: ProtocolState
    ) -> tuple[list[H3Event], list[Effect]]:
        """Parse buffered data for a stream, returning events and effects."""
        h3_events: list[H3Event] = []
        effects: list[Effect] = []
        partial_info = self._get_or_create_partial_frame_info(stream_id=stream_id)

        if partial_info.blocked and partial_info.frame_type == constants.H3_FRAME_TYPE_HEADERS:
            new_h3_events, new_effects = self._handle_request_frame(
                frame_type=constants.H3_FRAME_TYPE_HEADERS,
                frame_data=None,
                stream_id=stream_id,
                stream_ended=stream_ended and not data_buffer,
                state=state,
            )
            h3_events.extend(new_h3_events)
            effects.extend(new_effects)
            partial_info.blocked = False
            partial_info.blocked_frame_size = None

        temp_data = b"".join(bytes(chunk) for chunk in data_buffer)
        consumed = 0
        buf = QuicBuffer(data=temp_data)

        while consumed < len(temp_data) or (stream_ended and consumed == len(temp_data)):
            original_consumed = consumed

            if (
                partial_info.stream_type is None
                and not partial_info.headers_processed
                and partial_info.frame_type is None
                and is_bidirectional_stream(stream_id=stream_id)
            ):
                try:
                    pos = buf.tell()
                    frame_type_check = buf.pull_uint_var()
                    if frame_type_check == constants.H3_FRAME_TYPE_WEBTRANSPORT_STREAM:
                        control_stream_id = buf.pull_uint_var()
                        validate_control_stream_id(stream_id=control_stream_id)
                        partial_info.stream_type = constants.H3_STREAM_TYPE_WEBTRANSPORT
                        partial_info.control_stream_id = control_stream_id
                        effects.append(
                            LogH3Frame(
                                category="http",
                                event="stream_type_set",
                                data={"new": "webtransport", "stream_id": stream_id},
                            )
                        )

                        h3_events.append(
                            WebTransportStreamDataReceived(
                                data=b"", session_id=control_stream_id, stream_id=stream_id, stream_ended=False
                            )
                        )
                        consumed = buf.tell()
                        continue
                    else:
                        buf.seek(pos)
                except BufferReadError:
                    buf.seek(pos)
                    break

            if partial_info.stream_type == constants.H3_STREAM_TYPE_WEBTRANSPORT:
                payload = temp_data[consumed:]
                if payload or (stream_ended and consumed == len(temp_data) and not payload):
                    control_id: int | None = partial_info.control_stream_id
                    if control_id is None:
                        stream_state = state.streams.get(stream_id)
                        session_id_fallback = stream_state.session_id if stream_state is not None else None
                        session_data_fallback = (
                            state.sessions.get(session_id_fallback) if session_id_fallback is not None else None
                        )
                        control_id = session_id_fallback if session_data_fallback is not None else None

                    if control_id is None:
                        raise ProtocolError(
                            message=f"Cannot process WT stream data for stream {stream_id} without control stream ID.",
                            error_code=ErrorCodes.INTERNAL_ERROR,
                        )

                    h3_events.append(
                        WebTransportStreamDataReceived(
                            data=payload, session_id=control_id, stream_id=stream_id, stream_ended=stream_ended
                        )
                    )
                consumed = len(temp_data)
                break
            else:
                if partial_info.frame_size is None:
                    try:
                        pos = buf.tell()
                        partial_info.frame_type = buf.pull_uint_var()
                        partial_info.frame_size = buf.pull_uint_var()
                        consumed = buf.tell()
                    except BufferReadError:
                        buf.seek(pos)
                        break

                    log_frame_size = partial_info.frame_size if partial_info.frame_size is not None else 0
                    if partial_info.frame_type == constants.H3_FRAME_TYPE_DATA:
                        effects.append(
                            LogH3Frame(
                                category="http",
                                event="frame_parsed",
                                data={
                                    "frame_type": constants.H3_FRAME_TYPE_DATA,
                                    "length": log_frame_size,
                                    "stream_id": stream_id,
                                },
                            )
                        )

                current_frame_type = partial_info.frame_type
                current_frame_size = partial_info.frame_size

                if current_frame_type is None or current_frame_size is None:
                    break

                chunk_size = min(current_frame_size, len(temp_data) - consumed)
                if current_frame_type != constants.H3_FRAME_TYPE_DATA and chunk_size < current_frame_size:
                    break

                frame_data = buf.pull_bytes(chunk_size)
                consumed = buf.tell()
                partial_info.frame_size = current_frame_size - chunk_size

                is_last_chunk = partial_info.frame_size == 0
                frame_data_to_process = frame_data if is_last_chunk else None
                if current_frame_type == constants.H3_FRAME_TYPE_DATA:
                    frame_data_to_process = frame_data

                if frame_data_to_process is not None or partial_info.blocked:
                    new_h3_events, new_effects = self._handle_request_frame(
                        frame_type=current_frame_type,
                        frame_data=frame_data_to_process,
                        stream_id=stream_id,
                        stream_ended=stream_ended and is_last_chunk and buf.eof(),
                        state=state,
                    )
                    h3_events.extend(new_h3_events)
                    effects.extend(new_effects)

                if is_last_chunk:
                    partial_info.frame_type = None
                    partial_info.frame_size = None

            if consumed == original_consumed:
                if not stream_ended:
                    logger.warning("H3 parsing stuck on stream %d", stream_id)
                break

        data_buffer.clear()
        if consumed < len(temp_data):
            data_buffer.append(temp_data[consumed:])

        return h3_events, effects

    def _receive_datagram(self, *, data: Buffer) -> list[H3Event]:
        """Parse an incoming datagram."""
        buf = QuicBuffer(data=bytes(data))
        try:
            quarter_stream_id = buf.pull_uint_var()
        except BufferReadError:
            raise ProtocolError(
                message="Could not parse quarter stream ID from datagram", error_code=ErrorCodes.H3_DATAGRAM_ERROR
            )

        stream_id = quarter_stream_id * 4
        if not is_request_response_stream(stream_id=stream_id):
            raise ProtocolError(
                message=f"Datagram received on invalid Session ID {stream_id}", error_code=ErrorCodes.H3_ID_ERROR
            )

        return [DatagramReceived(data=data[buf.tell() :], stream_id=stream_id)]

    def _receive_request_data(
        self, *, stream_id: int, data: Buffer, stream_ended: bool, state: ProtocolState
    ) -> tuple[list[H3Event], list[Effect]]:
        """Handle incoming data on a bidirectional request stream."""
        partial_info = self._get_or_create_partial_frame_info(stream_id=stream_id)
        if data:
            partial_info.buffer.append(data)
        if stream_ended:
            partial_info.ended = True

        if partial_info.blocked or (not partial_info.buffer and not partial_info.ended):
            return [], []

        h3_events, effects = self._parse_stream_data(
            stream_id=stream_id, data_buffer=partial_info.buffer, stream_ended=partial_info.ended, state=state
        )

        if partial_info.ended and not partial_info.buffer:
            stream_state = state.streams.get(stream_id)
            session_id = stream_state.session_id if stream_state is not None else None
            if session_id is not None:
                if session_id == stream_id:
                    logger.debug("CONNECT stream %d cleanly closed (FIN received)", stream_id)
                    h3_events.append(ConnectStreamClosed(stream_id=stream_id))
            self._partial_frames.pop(stream_id, None)

        return h3_events, effects

    def _receive_stream_data_uni(
        self, *, stream_id: int, data: Buffer, stream_ended: bool, state: ProtocolState
    ) -> tuple[list[H3Event], list[Effect]]:
        """Handle incoming data on a unidirectional stream."""
        partial_info = self._get_or_create_partial_frame_info(stream_id=stream_id)
        if data:
            partial_info.buffer.append(data)
        if stream_ended:
            partial_info.ended = True

        if partial_info.blocked or (not partial_info.buffer and not partial_info.ended):
            return [], []

        h3_events: list[H3Event] = []
        effects: list[Effect] = []
        temp_data = b"".join(bytes(chunk) for chunk in partial_info.buffer)
        consumed = 0
        buf = QuicBuffer(data=temp_data)

        if partial_info.stream_type is None:
            try:
                partial_info.stream_type = buf.pull_uint_var()
                consumed = buf.tell()
                stream_type = partial_info.stream_type
                uni_stream_types = (
                    constants.H3_STREAM_TYPE_CONTROL,
                    constants.H3_STREAM_TYPE_PUSH,
                    constants.H3_STREAM_TYPE_QPACK_DECODER,
                    constants.H3_STREAM_TYPE_QPACK_ENCODER,
                    constants.H3_STREAM_TYPE_WEBTRANSPORT,
                )
                if stream_type not in uni_stream_types:
                    partial_info.buffer.clear()
                    logger.warning(
                        "Received unknown unidirectional stream type %d on stream %d", stream_type, stream_id
                    )
                    return [], []

                if stream_type == constants.H3_STREAM_TYPE_CONTROL and self._peer_control_stream_id is None:
                    self._peer_control_stream_id = stream_id
                elif stream_type == constants.H3_STREAM_TYPE_QPACK_DECODER and self._peer_decoder_stream_id is None:
                    self._peer_decoder_stream_id = stream_id
                elif stream_type == constants.H3_STREAM_TYPE_QPACK_ENCODER and self._peer_encoder_stream_id is None:
                    self._peer_encoder_stream_id = stream_id

                effects.append(
                    LogH3Frame(
                        category="http",
                        event="stream_type_set",
                        data={
                            "new": {
                                constants.H3_STREAM_TYPE_CONTROL: "control",
                                constants.H3_STREAM_TYPE_PUSH: "push",
                                constants.H3_STREAM_TYPE_QPACK_ENCODER: "qpack_encoder",
                                constants.H3_STREAM_TYPE_QPACK_DECODER: "qpack_decoder",
                                constants.H3_STREAM_TYPE_WEBTRANSPORT: "webtransport",
                            }.get(stream_type, "unknown"),
                            "stream_id": stream_id,
                        },
                    )
                )
            except BufferReadError:
                return [], []

        stream_type = partial_info.stream_type

        match stream_type:
            case constants.H3_STREAM_TYPE_WEBTRANSPORT:
                if partial_info.control_stream_id is None:
                    try:
                        control_stream_id = buf.pull_uint_var()
                        validate_control_stream_id(stream_id=control_stream_id)
                        partial_info.control_stream_id = control_stream_id
                        consumed = buf.tell()
                    except BufferReadError:
                        partial_info.buffer.clear()
                        partial_info.buffer.append(temp_data[consumed:])
                        return [], []

                payload = temp_data[consumed:]
                if payload or partial_info.ended:
                    control_id = partial_info.control_stream_id
                    if control_id is None:
                        raise ProtocolError(
                            message="Cannot process WT uni stream data without control stream ID.",
                            error_code=ErrorCodes.INTERNAL_ERROR,
                        )

                    h3_events.append(
                        WebTransportStreamDataReceived(
                            data=payload, session_id=control_id, stream_ended=partial_info.ended, stream_id=stream_id
                        )
                    )
                partial_info.buffer.clear()

            case constants.H3_STREAM_TYPE_CONTROL:
                if partial_info.ended:
                    raise ProtocolError(
                        message="Closing control stream is not allowed", error_code=ErrorCodes.H3_CLOSED_CRITICAL_STREAM
                    )
                buf = QuicBuffer(data=temp_data[consumed:])
                start_pos = buf.tell()

                while not buf.eof():
                    if partial_info.frame_type is None:
                        try:
                            partial_info.frame_type = buf.pull_uint_var()
                            partial_info.frame_size = buf.pull_uint_var()
                        except BufferReadError:
                            buf.seek(start_pos)
                            break

                    needed = partial_info.frame_size
                    if needed is None:
                        break

                    if buf.capacity - buf.tell() < needed:
                        buf.seek(start_pos)
                        break

                    frame_data = buf.pull_bytes(needed)
                    frame_type = partial_info.frame_type

                    new_h3_events, new_effects = self._handle_control_frame(
                        frame_type=frame_type, frame_data=frame_data, state=state
                    )
                    h3_events.extend(new_h3_events)
                    effects.extend(new_effects)

                    partial_info.frame_type = None
                    partial_info.frame_size = None
                    start_pos = buf.tell()

                consumed += start_pos
                partial_info.buffer.clear()
                if consumed < len(temp_data):
                    partial_info.buffer.append(temp_data[consumed:])

            case constants.H3_STREAM_TYPE_QPACK_DECODER:
                if self._peer_decoder_stream_id != stream_id:
                    raise ProtocolError(
                        message="Data on unexpected QPACK decoder stream",
                        error_code=ErrorCodes.H3_STREAM_CREATION_ERROR,
                    )
                data_to_feed = temp_data[consumed:]
                try:
                    self._encoder.feed_decoder(data_to_feed)
                except pylsqpack.DecoderStreamError as exc:
                    raise ProtocolError(
                        message="QPACK decoder stream error", error_code=ErrorCodes.QPACK_DECODER_STREAM_ERROR
                    ) from exc
                partial_info.buffer.clear()

            case constants.H3_STREAM_TYPE_QPACK_ENCODER:
                if self._peer_encoder_stream_id != stream_id:
                    raise ProtocolError(
                        message="Data on unexpected QPACK encoder stream",
                        error_code=ErrorCodes.H3_STREAM_CREATION_ERROR,
                    )
                data_to_feed = temp_data[consumed:]
                unblocked_streams: set[int] = set()
                try:
                    unblocked_streams.update(self._decoder.feed_encoder(data_to_feed))
                except pylsqpack.EncoderStreamError as exc:
                    raise ProtocolError(
                        message="QPACK encoder stream error", error_code=ErrorCodes.QPACK_ENCODER_STREAM_ERROR
                    ) from exc
                partial_info.buffer.clear()

                for unblocked_stream_id in unblocked_streams:
                    unblocked_partial_info = self._partial_frames.get(unblocked_stream_id)
                    if unblocked_partial_info is not None and unblocked_partial_info.blocked:
                        unblocked_partial_info.blocked = False
                        unblocked_partial_info.blocked_frame_size = None
                        (new_h3_events, new_effects) = self._receive_request_data(
                            stream_id=unblocked_stream_id,
                            data=b"",
                            stream_ended=unblocked_partial_info.ended,
                            state=state,
                        )
                        h3_events.extend(new_h3_events)
                        effects.extend(new_effects)

        if partial_info.ended and not partial_info.buffer:
            self._partial_frames.pop(stream_id, None)

        return h3_events, effects

    def _validate_settings(self, *, settings: dict[int, int], state: ProtocolState) -> None:
        """Validate the peer's HTTP/3 settings."""
        if settings.get(constants.SETTINGS_ENABLE_CONNECT_PROTOCOL) not in (None, 1):
            raise ProtocolError(
                message="ENABLE_CONNECT_PROTOCOL setting must be 1 if present", error_code=ErrorCodes.H3_SETTINGS_ERROR
            )

        quic_supports_datagrams = state.remote_max_datagram_frame_size > 0
        if not quic_supports_datagrams and settings.get(constants.SETTINGS_H3_DATAGRAM) == 1:
            raise ProtocolError(
                message="H3_DATAGRAM requires max_datagram_frame_size transport parameter",
                error_code=ErrorCodes.H3_SETTINGS_ERROR,
            )

        webtransport_indicated = settings.get(constants.SETTINGS_ENABLE_CONNECT_PROTOCOL) == 1
        if webtransport_indicated and settings.get(constants.SETTINGS_H3_DATAGRAM) != 1:
            raise ProtocolError(
                message="WebTransport requires the H3_DATAGRAM setting", error_code=ErrorCodes.H3_SETTINGS_ERROR
            )


class _HeadersState(Enum):
    """Represent the state for tracking header frames on a stream."""

    INITIAL = 0
    AFTER_HEADERS = 1


@dataclass(kw_only=True, slots=True)
class _PartialFrameInfo:
    """Stores state for partially received H3 frames or stream data."""

    stream_id: int
    buffer: deque[Buffer] = field(default_factory=deque)
    capsule_buffer: bytearray = field(default_factory=bytearray)
    ended: bool = False
    blocked: bool = False
    blocked_frame_size: int | None = None
    frame_size: int | None = None
    frame_type: int | None = None
    stream_type: int | None = None
    control_stream_id: int | None = None
    headers_processed: bool = False
    is_webtransport_control: bool = False


def _encode_frame(*, frame_type: int, frame_data: bytes) -> bytes:
    """Encode an HTTP/3 frame."""
    frame_length = len(frame_data)
    buf = QuicBuffer(capacity=frame_length + 2 * UINT_VAR_MAX_SIZE)

    buf.push_uint_var(frame_type)
    buf.push_uint_var(frame_length)
    buf.push_bytes(frame_data)

    return buf.data


def _encode_settings(*, settings: dict[int, int]) -> bytes:
    """Encode an HTTP/3 SETTINGS frame."""
    buf = QuicBuffer(capacity=1024)
    for setting, value in settings.items():
        buf.push_uint_var(setting)
        buf.push_uint_var(value)
    return buf.data


def _parse_settings(*, data: bytes) -> dict[int, int]:
    """Parse an HTTP/3 SETTINGS frame."""
    buf = QuicBuffer(data=data)
    settings: dict[int, int] = {}
    try:
        while not buf.eof():
            setting = buf.pull_uint_var()
            value = buf.pull_uint_var()
            if setting in RESERVED_SETTINGS:
                raise ProtocolError(
                    message=f"Setting identifier 0x{setting:x} is reserved", error_code=ErrorCodes.H3_SETTINGS_ERROR
                )
            if setting in settings:
                raise ProtocolError(
                    message=f"Setting identifier 0x{setting:x} is included twice",
                    error_code=ErrorCodes.H3_SETTINGS_ERROR,
                )
            settings[setting] = value
    except BufferReadError as exc:
        raise ProtocolError(message="Malformed SETTINGS frame payload", error_code=ErrorCodes.H3_FRAME_ERROR) from exc
    return dict(settings)


def _validate_header_name(*, key: bytes) -> None:
    """Validate an HTTP header name."""
    if not key or key != key.lower():
        raise ProtocolError(
            message=f"Header name {key!r} must be lowercase and non-empty", error_code=ErrorCodes.H3_MESSAGE_ERROR
        )

    for i, c_byte in enumerate(key):
        if c_byte == COLON:
            if i == 0:
                continue
            raise ProtocolError(
                message=f"Header name {key!r} contains a non-initial colon", error_code=ErrorCodes.H3_MESSAGE_ERROR
            )

        is_tchar = (0x61 <= c_byte <= 0x7A) or (0x30 <= c_byte <= 0x39) or c_byte in b"!#$%&'*+-.^_`|~"
        if not is_tchar:
            raise ProtocolError(
                message=f"Header name {key!r} contains invalid characters", error_code=ErrorCodes.H3_MESSAGE_ERROR
            )


def _validate_header_value(*, key: bytes, value: bytes) -> None:
    """Validate an HTTP header value."""
    if not all(0x21 <= c <= 0x7E or c == SP or c == HTAB for c in value):
        raise ProtocolError(
            message=f"Header {key!r} value has forbidden characters", error_code=ErrorCodes.H3_MESSAGE_ERROR
        )
    if value and (value[0] in WHITESPACE or value[-1] in WHITESPACE):
        raise ProtocolError(
            message=f"Header {key!r} value has leading/trailing whitespace", error_code=ErrorCodes.H3_MESSAGE_ERROR
        )


def _validate_headers(
    *, headers: _RawHeaders, allowed_pseudo_headers: frozenset[bytes], required_pseudo_headers: frozenset[bytes]
) -> None:
    """Validate a list of raw HTTP headers."""
    after_pseudo_headers = False
    authority: bytes | None = None
    path: bytes | None = None
    scheme: bytes | None = None
    seen_pseudo_headers: set[bytes] = set()

    for key, value in headers:
        _validate_header_name(key=key)
        _validate_header_value(key=key, value=value)

        if key.startswith(b":"):
            if after_pseudo_headers:
                raise ProtocolError(
                    message=f"Pseudo-header {key!r} after regular headers", error_code=ErrorCodes.H3_MESSAGE_ERROR
                )
            if key not in allowed_pseudo_headers:
                raise ProtocolError(
                    message=f"Pseudo-header {key!r} not allowed", error_code=ErrorCodes.H3_MESSAGE_ERROR
                )
            if key in seen_pseudo_headers:
                raise ProtocolError(message=f"Duplicate pseudo-header {key!r}", error_code=ErrorCodes.H3_MESSAGE_ERROR)
            seen_pseudo_headers.add(key)

            match key:
                case b":authority":
                    authority = value
                case b":path":
                    path = value
                case b":scheme":
                    scheme = value
                case _:
                    pass
        else:
            after_pseudo_headers = True

    missing = required_pseudo_headers.difference(seen_pseudo_headers)
    if missing:
        missing_str = ", ".join(repr(h) for h in sorted(list(missing)))
        raise ProtocolError(message=f"Missing pseudo-headers: {missing_str}", error_code=ErrorCodes.H3_MESSAGE_ERROR)

    if scheme in (b"http", b"https"):
        if not authority:
            raise ProtocolError(
                message="Pseudo-header b':authority' cannot be empty", error_code=ErrorCodes.H3_MESSAGE_ERROR
            )
        if not path:
            raise ProtocolError(
                message="Pseudo-header b':path' cannot be empty", error_code=ErrorCodes.H3_MESSAGE_ERROR
            )


def _validate_request_headers(*, headers: _RawHeaders) -> None:
    """Validate HTTP request headers (specific for WebTransport CONNECT)."""
    _validate_headers(
        headers=headers,
        allowed_pseudo_headers=frozenset((b":method", b":scheme", b":authority", b":path", b":protocol")),
        required_pseudo_headers=frozenset((b":method", b":scheme", b":authority", b":path")),
    )


def _validate_response_headers(*, headers: _RawHeaders) -> None:
    """Validate HTTP response headers (specific for WebTransport CONNECT response)."""
    _validate_headers(
        headers=headers,
        allowed_pseudo_headers=frozenset((b":status",)),
        required_pseudo_headers=frozenset((b":status",)),
    )
