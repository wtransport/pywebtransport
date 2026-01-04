"""Unit tests for the pywebtransport._protocol.h3_engine module."""

from typing import cast
from unittest.mock import MagicMock

import pylsqpack
import pytest
from aioquic._buffer import Buffer
from aioquic.buffer import encode_uint_var
from pytest_mock import MockerFixture

from pywebtransport import ClientConfig, ErrorCodes, ProtocolError, constants
from pywebtransport._protocol.events import (
    CapsuleReceived,
    CloseQuicConnection,
    ConnectStreamClosed,
    DatagramReceived,
    GoawayReceived,
    HeadersReceived,
    LogH3Frame,
    SendQuicData,
    SettingsReceived,
    TransportDatagramFrameReceived,
    TransportStreamDataReceived,
    WebTransportStreamDataReceived,
)
from pywebtransport._protocol.h3_engine import (
    WebTransportH3Engine,
    _parse_settings,
    _validate_header_name,
    _validate_header_value,
    _validate_headers,
    _validate_request_headers,
    _validate_response_headers,
)
from pywebtransport._protocol.state import ProtocolState, SessionStateData, StreamStateData
from pywebtransport.types import Buffer as BufferType
from pywebtransport.types import Headers


class TestWebTransportH3Engine:

    @pytest.fixture
    def client_config(self, mocker: MockerFixture) -> ClientConfig:
        conf = mocker.Mock(spec=ClientConfig)
        conf.initial_max_data = 10000
        conf.initial_max_streams_bidi = 10
        conf.initial_max_streams_uni = 10
        conf.max_capsule_size = 65536
        return cast(ClientConfig, conf)

    @pytest.fixture
    def engine(
        self, client_config: ClientConfig, mock_decoder: MagicMock, mock_encoder: MagicMock
    ) -> WebTransportH3Engine:
        return WebTransportH3Engine(is_client=True, config=client_config)

    @pytest.fixture
    def mock_decoder(self, mock_decoder_cls: MagicMock) -> MagicMock:
        instance = mock_decoder_cls.return_value
        instance.feed_header.return_value = (b"", [])
        instance.resume_header.return_value = (b"", [])
        return cast(MagicMock, instance)

    @pytest.fixture
    def mock_decoder_cls(self, mocker: MockerFixture) -> MagicMock:
        return cast(MagicMock, mocker.patch("pylsqpack.Decoder", autospec=True))

    @pytest.fixture
    def mock_encoder(self, mock_encoder_cls: MagicMock) -> MagicMock:
        instance = mock_encoder_cls.return_value
        instance.encode.return_value = (b"", b"encoded_headers")
        return cast(MagicMock, instance)

    @pytest.fixture
    def mock_encoder_cls(self, mocker: MockerFixture) -> MagicMock:
        return cast(MagicMock, mocker.patch("pylsqpack.Encoder", autospec=True))

    @pytest.fixture
    def protocol_state(self, mocker: MockerFixture) -> ProtocolState:
        state = mocker.Mock(spec=ProtocolState)
        state.streams = {}
        state.sessions = {}
        state.remote_max_datagram_frame_size = 1000
        return cast(ProtocolState, state)

    def test_buffer_read_error_during_frame_parsing(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        session_data = MagicMock(spec=SessionStateData)
        session_data.session_id = 1
        session_data.control_stream_id = 4
        protocol_state.sessions[1] = session_data
        stream_state = MagicMock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 1
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        buf = Buffer(capacity=10)
        buf.push_bytes(b"\xc0")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)

        assert len(info.buffer) > 0

    def test_buffer_read_error_during_stream_type_parsing(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        engine.cleanup_stream(stream_id=stream_id)
        buf = Buffer(capacity=10)
        buf.push_bytes(b"\x40")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        assert info is not None
        assert len(info.buffer) > 0
        assert info.stream_type is None

    def test_buffer_read_error_in_capsule_header(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        session_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = session_id
        session_data.control_stream_id = stream_id
        protocol_state.sessions[session_id] = session_data
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        frame_buf = Buffer(capacity=1024)
        frame_buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        frame_buf.push_uint_var(10)
        frame_buf.push_bytes(b"\x00")
        event = TransportStreamDataReceived(stream_id=stream_id, data=frame_buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 0
        assert len(info.capsule_buffer) > 0

    def test_buffer_read_error_in_uni_stream_header(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        buf = Buffer(capacity=10)
        buf.push_bytes(b"\x80")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        assert info is not None
        assert len(info.buffer) > 0
        assert info.stream_type is None

    def test_capsule_payload_truncated(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        session_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = session_id
        session_data.control_stream_id = stream_id
        protocol_state.sessions[session_id] = session_data
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        info.is_webtransport_control = True
        frame_buf = Buffer(capacity=1024)
        frame_buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        frame_buf.push_uint_var(10)
        capsule_header = Buffer(capacity=10)
        capsule_header.push_uint_var(0x00)
        capsule_header.push_uint_var(5)
        frame_buf.push_bytes(capsule_header.data)
        frame_buf.push_bytes(b"x")
        event = TransportStreamDataReceived(stream_id=stream_id, data=frame_buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)
        errors = [e for e in effects if isinstance(e, CloseQuicConnection)]

        assert not errors
        assert len(h3_events) == 0
        assert len(info.capsule_buffer) > 0

    def test_cleanup_stream(self, engine: WebTransportH3Engine) -> None:
        stream_id = 1
        engine._get_or_create_partial_frame_info(stream_id=stream_id)

        engine.cleanup_stream(stream_id=stream_id)

        assert stream_id not in engine._partial_frames

    def test_cleanup_stream_nonexistent(self, engine: WebTransportH3Engine) -> None:
        engine.cleanup_stream(stream_id=999)

    def test_cleanup_stream_with_partial_data(self, engine: WebTransportH3Engine) -> None:
        stream_id = 1
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.buffer.append(b"data")

        engine.cleanup_stream(stream_id=stream_id)

        assert stream_id not in engine._partial_frames

    def test_decode_headers_decompression_failed(self, engine: WebTransportH3Engine, mock_decoder: MagicMock) -> None:
        mock_decoder.feed_header.side_effect = pylsqpack.DecompressionFailed()

        with pytest.raises(ProtocolError) as exc:
            engine._decode_headers(stream_id=0, frame_data=b"header")

        assert exc.value.error_code == ErrorCodes.QPACK_DECOMPRESSION_FAILED

    def test_decode_headers_no_local_stream_instructions(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mock_decoder: MagicMock
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        engine._local_decoder_stream_id = None
        mock_decoder.feed_header.return_value = (b"decoder_instruction", [(b":status", b"200")])
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(5)
        buf.push_bytes(b"xxxxx")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        decoder_effect = next(
            (e for e in effects if isinstance(e, SendQuicData) and e.data == b"decoder_instruction"), None
        )

        assert decoder_effect is None

    def test_decode_headers_sends_instructions(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mock_decoder: MagicMock
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        engine._local_decoder_stream_id = 10
        mock_decoder.feed_header.return_value = (b"decoder_instruction", [(b":status", b"200")])
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(5)
        buf.push_bytes(b"xxxxx")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        decoder_effect = next((e for e in effects if isinstance(e, SendQuicData) and e.stream_id == 10), None)

        assert decoder_effect is not None
        assert decoder_effect.data == b"decoder_instruction"

    def test_encode_capsule_invalid_stream(self, engine: WebTransportH3Engine) -> None:
        stream_id = 2

        with pytest.raises(ProtocolError) as exc:
            engine.encode_capsule(stream_id=stream_id, capsule_type=1, capsule_data=b"")

        assert exc.value.error_code == ErrorCodes.H3_STREAM_CREATION_ERROR

    def test_encode_capsule_success(self, engine: WebTransportH3Engine) -> None:
        stream_id = 0
        capsule_type = 0x01
        capsule_data = b"test"

        result = engine.encode_capsule(stream_id=stream_id, capsule_type=capsule_type, capsule_data=capsule_data)
        buf = Buffer(data=result)

        assert buf.pull_uint_var() == constants.H3_FRAME_TYPE_DATA
        length = buf.pull_uint_var()
        payload = buf.pull_bytes(length)
        c_buf = Buffer(data=payload)
        assert c_buf.pull_uint_var() == capsule_type
        assert c_buf.pull_uint_var() == len(capsule_data)
        assert c_buf.pull_bytes(len(capsule_data)) == capsule_data

    def test_encode_datagram_invalid_stream(self, engine: WebTransportH3Engine) -> None:
        stream_id = 2

        with pytest.raises(ProtocolError):
            engine.encode_datagram(stream_id=stream_id, data=b"")

    def test_encode_datagram_success(self, engine: WebTransportH3Engine) -> None:
        stream_id = 4
        data = b"payload"

        result = engine.encode_datagram(stream_id=stream_id, data=data)
        quarter_id = stream_id // 4

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == encode_uint_var(quarter_id)
        assert result[1] == data

    def test_encode_datagram_with_list(self, engine: WebTransportH3Engine) -> None:
        stream_id = 4
        data = cast(list[BufferType], [b"chunk1", b"chunk2"])

        result = engine.encode_datagram(stream_id=stream_id, data=data)
        quarter_id = stream_id // 4

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0] == encode_uint_var(quarter_id)
        assert result[1] == b"chunk1"
        assert result[2] == b"chunk2"

    def test_encode_goaway_frame(self, engine: WebTransportH3Engine) -> None:
        data = engine.encode_goaway_frame(last_stream_id=10)
        buf = Buffer(data=data)

        assert buf.pull_uint_var() == constants.H3_FRAME_TYPE_GOAWAY
        length = buf.pull_uint_var()
        payload = buf.pull_bytes(length)
        p_buf = Buffer(data=payload)
        assert p_buf.pull_uint_var() == 10

    def test_encode_headers(self, engine: WebTransportH3Engine, mock_encoder: MagicMock) -> None:
        stream_id = 0
        headers = {":method": "CONNECT"}
        mock_encoder.encode.return_value = (b"instruction", b"payload")
        engine._local_encoder_stream_id = 2

        effects = engine.encode_headers(stream_id=stream_id, headers=cast(Headers, headers), end_stream=False)

        assert len(effects) == 3
        assert isinstance(effects[0], SendQuicData)
        assert effects[0].stream_id == 2
        assert effects[0].data == b"instruction"
        assert isinstance(effects[1], SendQuicData)
        assert effects[1].stream_id == stream_id
        assert b"payload" in effects[1].data
        assert isinstance(effects[2], LogH3Frame)

    def test_encode_headers_no_instructions(self, engine: WebTransportH3Engine, mock_encoder: MagicMock) -> None:
        stream_id = 0
        headers = {":method": "CONNECT"}
        mock_encoder.encode.return_value = (b"", b"payload")
        engine._local_encoder_stream_id = 2

        effects = engine.encode_headers(stream_id=stream_id, headers=cast(Headers, headers), end_stream=False)

        assert len(effects) == 2
        assert isinstance(effects[0], SendQuicData)
        assert effects[0].stream_id == stream_id
        assert isinstance(effects[1], LogH3Frame)

    def test_encode_webtransport_stream_creation_bidi(self, engine: WebTransportH3Engine) -> None:
        stream_id = 4
        control_stream_id = 0

        effects = engine.encode_webtransport_stream_creation(
            stream_id=stream_id, control_stream_id=control_stream_id, is_unidirectional=False
        )

        assert len(effects) == 2
        assert isinstance(effects[0], SendQuicData)
        expected_payload = encode_uint_var(constants.H3_FRAME_TYPE_WEBTRANSPORT_STREAM) + encode_uint_var(
            control_stream_id
        )
        assert effects[0].data == expected_payload
        assert isinstance(effects[1], LogH3Frame)

    def test_encode_webtransport_stream_creation_uni(self, engine: WebTransportH3Engine) -> None:
        stream_id = 2
        control_stream_id = 0

        effects = engine.encode_webtransport_stream_creation(
            stream_id=stream_id, control_stream_id=control_stream_id, is_unidirectional=True
        )
        info = engine._partial_frames[stream_id]

        assert len(effects) >= 2
        assert isinstance(effects[0], SendQuicData)
        assert effects[0].data == encode_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        assert isinstance(effects[1], SendQuicData)
        assert effects[1].data == encode_uint_var(control_stream_id)
        assert isinstance(effects[-1], LogH3Frame)
        assert info.stream_type == constants.H3_STREAM_TYPE_WEBTRANSPORT

    def test_fragmented_capsule_parsing(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        session_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = session_id
        session_data.control_stream_id = stream_id
        protocol_state.sessions[session_id] = session_data
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        capsule_type = 0x1234
        capsule_data = b"complete_payload"
        capsule_len = len(capsule_data)
        capsule_head = Buffer(capacity=128)
        capsule_head.push_uint_var(capsule_type)
        capsule_head.push_uint_var(capsule_len)
        capsule_head.push_bytes(capsule_data[:5])
        frame_buf = Buffer(capacity=1024)
        frame_buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        frame_buf.push_uint_var(len(capsule_head.data))
        frame_buf.push_bytes(capsule_head.data)
        event1 = TransportStreamDataReceived(stream_id=stream_id, data=frame_buf.data, end_stream=False)
        h3_events, _ = engine.handle_transport_event(event=event1, state=protocol_state)
        assert len(h3_events) == 0
        remaining_data = capsule_data[5:]
        frame_buf2 = Buffer(capacity=1024)
        frame_buf2.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        frame_buf2.push_uint_var(len(remaining_data))
        frame_buf2.push_bytes(remaining_data)
        event2 = TransportStreamDataReceived(stream_id=stream_id, data=frame_buf2.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event2, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], CapsuleReceived)
        assert h3_events[0].capsule_data == capsule_data

    def test_fragmented_frame_header_parsing(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 1
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        buf = Buffer(capacity=10)
        buf.push_bytes(b"\x40")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        assert len(h3_events) == 0
        assert info is not None
        assert len(info.buffer) == 1
        assert info.frame_type is None

    def test_fragmented_parsing(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_decoder: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        headers_payload = b"payload"
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(len(headers_payload))
        buf.push_bytes(headers_payload)
        chunk1 = buf.data[:2]
        chunk2 = buf.data[2:]
        mock_decoder.feed_header.return_value = (b"", [(b":status", b"200")])
        event1 = TransportStreamDataReceived(stream_id=stream_id, data=chunk1, end_stream=False)
        h3_events, _ = engine.handle_transport_event(event=event1, state=protocol_state)
        assert len(h3_events) == 0
        event2 = TransportStreamDataReceived(stream_id=stream_id, data=chunk2, end_stream=True)

        h3_events, _ = engine.handle_transport_event(event=event2, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], HeadersReceived)

    def test_fragmented_uni_stream_payload(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 2
        control_stream_id = 4
        buf = Buffer(capacity=100)
        buf.push_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        buf.push_uint_var(control_stream_id)
        payload = b"helloworld"
        chunk1 = buf.data + payload[:5]
        event1 = TransportStreamDataReceived(stream_id=stream_id, data=chunk1, end_stream=False)
        h3_events, _ = engine.handle_transport_event(event=event1, state=protocol_state)
        assert len(h3_events) == 1
        assert isinstance(h3_events[0], WebTransportStreamDataReceived)
        assert h3_events[0].data == payload[:5]
        chunk2 = payload[5:]
        event2 = TransportStreamDataReceived(stream_id=stream_id, data=chunk2, end_stream=False)

        h3_events_2, _ = engine.handle_transport_event(event=event2, state=protocol_state)

        assert len(h3_events_2) == 1
        assert isinstance(h3_events_2[0], WebTransportStreamDataReceived)
        assert h3_events_2[0].data == chunk2

    def test_handle_capsule_excessive_load(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        session_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = session_id
        session_data.control_stream_id = stream_id
        protocol_state.sessions[session_id] = session_data
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        capsule_buf = Buffer(capacity=1024)
        capsule_buf.push_uint_var(0x1234)
        capsule_buf.push_uint_var(65537)
        frame_buf = Buffer(capacity=1024)
        frame_buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        frame_buf.push_uint_var(len(capsule_buf.data))
        frame_buf.push_bytes(capsule_buf.data)
        event = TransportStreamDataReceived(stream_id=stream_id, data=frame_buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_EXCESSIVE_LOAD

    def test_handle_capsule_on_request_stream(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        session_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = session_id
        session_data.control_stream_id = stream_id
        protocol_state.sessions[session_id] = session_data
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        capsule_buf = Buffer(capacity=1024)
        capsule_buf.push_uint_var(0x1234)
        capsule_buf.push_uint_var(4)
        capsule_buf.push_bytes(b"data")
        frame_buf = Buffer(capacity=1024)
        frame_buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        frame_buf.push_uint_var(len(capsule_buf.data))
        frame_buf.push_bytes(capsule_buf.data)
        event = TransportStreamDataReceived(stream_id=stream_id, data=frame_buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], CapsuleReceived)
        assert h3_events[0].capsule_type == 0x1234
        assert h3_events[0].capsule_data == b"data"

    def test_handle_capsule_reserved_type_error(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        session_id = 1
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = session_id
        session_data.control_stream_id = stream_id
        protocol_state.sessions[session_id] = session_data
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_FRAME_UNEXPECTED

    def test_handle_control_frame_fallthrough(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        engine._settings_received = True
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(0x55)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 0
        assert len([e for e in effects if not isinstance(e, LogH3Frame)]) == 0

    def test_handle_control_stream_duplicate_settings(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        engine._settings_received = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_FRAME_UNEXPECTED

    def test_handle_control_stream_goaway(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        engine._settings_received = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_GOAWAY)
        buf.push_uint_var(1)
        buf.push_uint_var(100)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], GoawayReceived)

    def test_handle_control_stream_headers_forbidden(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        engine._settings_received = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_FRAME_UNEXPECTED

    def test_handle_control_stream_missing_settings_first(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_GOAWAY)
        buf.push_uint_var(1)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_MISSING_SETTINGS

    def test_handle_control_stream_settings(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mock_decoder: MagicMock
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        settings_data = Buffer(capacity=1024)
        settings_data.push_uint_var(constants.SETTINGS_H3_DATAGRAM)
        settings_data.push_uint_var(1)
        settings_data.push_uint_var(constants.SETTINGS_ENABLE_CONNECT_PROTOCOL)
        settings_data.push_uint_var(1)
        buf.push_uint_var(len(settings_data.data))
        buf.push_bytes(settings_data.data)
        protocol_state.remote_max_datagram_frame_size = 1500
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert engine._settings_received is True
        assert len(h3_events) == 1
        assert isinstance(h3_events[0], SettingsReceived)
        assert h3_events[0].settings[constants.SETTINGS_H3_DATAGRAM] == 1

    def test_handle_control_stream_unexpected_close(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        info = engine._get_or_create_partial_frame_info(stream_id=control_stream_id)
        info.stream_type = constants.H3_STREAM_TYPE_CONTROL
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=b"", end_stream=True)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_CLOSED_CRITICAL_STREAM

    def test_handle_control_stream_unknown_frame(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        engine._settings_received = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        unknown_type = 0x1234
        payload = b"ignore_me"
        buf.push_uint_var(unknown_type)
        buf.push_uint_var(len(payload))
        buf.push_bytes(payload)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)
        non_log_effects = [e for e in effects if not isinstance(e, LogH3Frame)]

        assert len(h3_events) == 0
        assert len(non_log_effects) == 0

    def test_handle_data_on_unknown_stream(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 100
        buf = Buffer(capacity=1024)
        buf.push_bytes(b"some_data")
        mocker.patch("pywebtransport._protocol.h3_engine.is_unidirectional_stream", return_value=True)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 0
        assert len(engine._partial_frames[stream_id].buffer) == 0

    def test_handle_ignored_frames_on_request_stream(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 1
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)

        for frame_type in (
            constants.H3_FRAME_TYPE_CANCEL_PUSH,
            constants.H3_FRAME_TYPE_MAX_PUSH_ID,
            constants.H3_FRAME_TYPE_PUSH_PROMISE,
        ):
            buf = Buffer(capacity=1024)
            buf.push_uint_var(frame_type)
            buf.push_uint_var(0)
            event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

            h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

            assert len(h3_events) == 0
            assert len(effects) == 0

    def test_handle_internal_error(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(engine, "_receive_datagram", side_effect=Exception("Boom"))
        event = TransportDatagramFrameReceived(data=b"")

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.INTERNAL_ERROR

    def test_handle_request_data_orphaned_session(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 999
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        protocol_state.sessions = {}
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        payload = b"orphan"
        buf.push_uint_var(len(payload))
        buf.push_bytes(payload)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        close_effects = [e for e in effects if isinstance(e, CloseQuicConnection)]

        assert len(close_effects) == 1
        assert close_effects[0].error_code == ErrorCodes.INTERNAL_ERROR

    def test_handle_request_frame_data_no_control_stream(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 1
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        payload = b"trigger_error"
        buf.push_uint_var(len(payload))
        buf.push_bytes(payload)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        close_effects = [e for e in effects if isinstance(e, CloseQuicConnection)]

        assert len(close_effects) == 1
        assert close_effects[0].error_code == ErrorCodes.INTERNAL_ERROR

    def test_handle_request_frame_data_on_unassociated_stream(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        close_effects = [e for e in effects if isinstance(e, CloseQuicConnection)]

        assert len(close_effects) == 0

    def test_handle_stream_blocked(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_decoder: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        mock_decoder.feed_header.side_effect = pylsqpack.StreamBlocked(stream_id)
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(5)
        buf.push_bytes(b"xxxxx")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)

        assert engine._partial_frames[stream_id].blocked is True

    def test_handle_stream_blocked_no_partial_info(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mock_decoder: MagicMock
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        mock_decoder.feed_header.side_effect = pylsqpack.StreamBlocked(999)
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(5)
        buf.push_bytes(b"xxxxx")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)

        assert 999 not in engine._partial_frames

    def test_handle_unknown_capsule(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        session_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = session_id
        session_data.control_stream_id = stream_id
        protocol_state.sessions[session_id] = session_data
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        capsule_buf = Buffer(capacity=128)
        capsule_buf.push_uint_var(0x9999)
        capsule_buf.push_uint_var(0)
        frame_buf = Buffer(capacity=1024)
        frame_buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        frame_buf.push_uint_var(len(capsule_buf.data))
        frame_buf.push_bytes(capsule_buf.data)
        event = TransportStreamDataReceived(stream_id=stream_id, data=frame_buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], CapsuleReceived)
        assert h3_events[0].capsule_type == 0x9999

    def test_handle_unknown_frame_on_request_stream(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 1
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        buf = Buffer(capacity=1024)
        buf.push_uint_var(0x55)
        buf.push_uint_var(3)
        buf.push_bytes(b"ign")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 0
        assert len(effects) == 0

    def test_handle_wt_stream_frame_on_request_stream_invalid(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 1
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.control_stream_id = 4
        protocol_state.sessions[1] = session_data
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_WEBTRANSPORT_STREAM)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_FRAME_ERROR

    def test_handle_wt_stream_frame_valid_switch(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_WEBTRANSPORT_STREAM)
        buf.push_uint_var(4)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert any(
            isinstance(e, LogH3Frame) and e.event == "stream_type_set" and e.data["new"] == "webtransport"
            for e in effects
        )
        assert len(h3_events) == 1
        assert isinstance(h3_events[0], WebTransportStreamDataReceived)
        assert h3_events[0].session_id == 4

    def test_header_validation_comprehensive(self, engine: WebTransportH3Engine) -> None:
        invalid_names = [b"Name", b"name:bad", b"name\x00", b"name\r", b"name\n"]
        for name in invalid_names:
            with pytest.raises(ProtocolError):
                _validate_header_name(key=name)

        invalid_values = [b"val\x00ue", b"val\rue", b"val\nue", b"val\x7fue"]
        for value in invalid_values:
            with pytest.raises(ProtocolError):
                _validate_header_value(key=b"key", value=value)

        _validate_header_value(key=b"k", value=b"val\tue")

    def test_init(self, client_config: ClientConfig, mock_decoder_cls: MagicMock, mock_encoder_cls: MagicMock) -> None:
        engine = WebTransportH3Engine(is_client=True, config=client_config)

        assert engine._is_client is True
        assert engine._config == client_config
        assert engine._settings_received is False
        mock_decoder_cls.assert_called_once()
        mock_encoder_cls.assert_called_once()

    def test_initialize_connection(self, engine: WebTransportH3Engine) -> None:
        data = engine.initialize_connection()
        buf = Buffer(data=data)
        frame_type = buf.pull_uint_var()
        length = buf.pull_uint_var()

        assert frame_type == constants.H3_FRAME_TYPE_SETTINGS
        assert len(data) >= 2 + length

    def test_parse_settings_buffer_error(self, engine: WebTransportH3Engine) -> None:
        buf = Buffer(capacity=10)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        buf.push_uint_var(2)
        buf.push_bytes(b"\x01")
        malformed_payload = b"\x06\x40"

        with pytest.raises(ProtocolError) as exc:
            _parse_settings(data=malformed_payload)

        assert exc.value.error_code == ErrorCodes.H3_FRAME_ERROR

    def test_parse_settings_duplicate_key(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        settings_payload = Buffer(capacity=128)
        settings_payload.push_uint_var(constants.SETTINGS_H3_DATAGRAM)
        settings_payload.push_uint_var(1)
        settings_payload.push_uint_var(constants.SETTINGS_H3_DATAGRAM)
        settings_payload.push_uint_var(1)
        buf.push_uint_var(len(settings_payload.data))
        buf.push_bytes(settings_payload.data)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_SETTINGS_ERROR

    def test_parse_settings_malformed(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        buf.push_uint_var(1)
        buf.push_bytes(b"\x01")
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_FRAME_ERROR

    def test_parse_settings_reserved_key(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        settings_payload = Buffer(capacity=128)
        settings_payload.push_uint_var(0x02)
        settings_payload.push_uint_var(1)
        buf.push_uint_var(len(settings_payload.data))
        buf.push_bytes(settings_payload.data)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_SETTINGS_ERROR

    def test_parse_stream_data_stuck(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        stream_id = 10
        engine._peer_control_stream_id = 3
        buf = Buffer(capacity=10)
        buf.push_bytes(b"\xc0")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        assert info is not None
        assert len(info.buffer) == 1

    def test_qpack_decoder_stream_error(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mock_encoder: MagicMock
    ) -> None:
        stream_id = 6
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_QPACK_DECODER)
        buf.push_bytes(b"bad_data")
        mock_encoder.feed_decoder.side_effect = pylsqpack.DecoderStreamError()
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.QPACK_DECODER_STREAM_ERROR

    def test_qpack_decoder_stream_wrong_id(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        engine._peer_decoder_stream_id = 6
        new_stream_id = 10
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_QPACK_DECODER)
        event = TransportStreamDataReceived(stream_id=new_stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_STREAM_CREATION_ERROR

    def test_qpack_encoder_process_blocked_stream(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_decoder: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        blocked_stream_id = 4
        engine._settings_received = True
        blocked_info = engine._get_or_create_partial_frame_info(stream_id=blocked_stream_id)
        blocked_info.blocked = True
        valid_frame_buf = Buffer(capacity=64)
        valid_frame_buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        valid_frame_buf.push_uint_var(3)
        valid_frame_buf.push_bytes(b"foo")
        blocked_info.buffer.append(valid_frame_buf.data)
        stream_state = MagicMock(spec=StreamStateData)
        stream_state.stream_id = blocked_stream_id
        stream_state.session_id = 1
        protocol_state.streams[blocked_stream_id] = cast(StreamStateData, stream_state)
        session_data = MagicMock(spec=SessionStateData)
        session_data.session_id = 1
        session_data.control_stream_id = 4
        protocol_state.sessions[1] = session_data
        encoder_stream_id = 6
        engine._peer_encoder_stream_id = encoder_stream_id
        mock_decoder.feed_encoder.return_value = {blocked_stream_id}
        buf = Buffer(capacity=100)
        buf.push_uint_var(constants.H3_STREAM_TYPE_QPACK_ENCODER)
        buf.push_bytes(b"instructions")
        event = TransportStreamDataReceived(stream_id=encoder_stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)

        assert blocked_info.blocked is False
        assert len(blocked_info.buffer) == 0

    def test_qpack_encoder_stream_error(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mock_decoder: MagicMock
    ) -> None:
        stream_id = 2
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_QPACK_ENCODER)
        buf.push_bytes(b"bad_data")
        mock_decoder.feed_encoder.side_effect = pylsqpack.EncoderStreamError()
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.QPACK_ENCODER_STREAM_ERROR

    def test_qpack_encoder_stream_wrong_id(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        engine._peer_encoder_stream_id = 2
        new_stream_id = 10
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_QPACK_ENCODER)
        event = TransportStreamDataReceived(stream_id=new_stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_STREAM_CREATION_ERROR

    def test_qpack_spurious_unblock(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_decoder: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        encoder_stream_id = 6
        engine._peer_encoder_stream_id = encoder_stream_id
        mock_decoder.feed_encoder.return_value = {999}
        buf = Buffer(capacity=100)
        buf.push_uint_var(constants.H3_STREAM_TYPE_QPACK_ENCODER)
        buf.push_bytes(b"instructions")
        event = TransportStreamDataReceived(stream_id=encoder_stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)

    def test_receive_connect_stream_closed(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 4
        session_id = 4
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.control_stream_id = stream_id
        session_data.session_id = session_id
        protocol_state.sessions[session_id] = session_data
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        event = TransportStreamDataReceived(stream_id=stream_id, data=b"", end_stream=True)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], ConnectStreamClosed)
        assert h3_events[0].stream_id == stream_id

    def test_receive_control_stream_closed(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        stream_id = 3
        engine._peer_control_stream_id = stream_id
        buf = Buffer(capacity=10)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)
        engine.handle_transport_event(event=event, state=protocol_state)
        event_fin = TransportStreamDataReceived(stream_id=stream_id, data=b"", end_stream=True)

        _, effects = engine.handle_transport_event(event=event_fin, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_CLOSED_CRITICAL_STREAM

    def test_receive_control_stream_multiple_frames_truncated(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        engine._settings_received = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_GOAWAY)
        buf.push_uint_var(1)
        buf.push_uint_var(0)
        buf.push_bytes(b"\x40")
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(control_stream_id)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], GoawayReceived)
        assert info is not None
        assert len(info.buffer) > 0
        assert info.frame_type is None

    def test_receive_control_stream_truncated_frame(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(control_stream_id)

        assert info is not None
        assert len(info.buffer) > 0

    def test_receive_datagram_buffer_error(self, engine: WebTransportH3Engine) -> None:
        data = b"\x40"

        with pytest.raises(ProtocolError) as exc:
            engine._receive_datagram(data=data)

        assert exc.value.error_code == ErrorCodes.H3_DATAGRAM_ERROR

    def test_receive_datagram_invalid_session_id(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        mocker.patch("pywebtransport._protocol.h3_engine.is_request_response_stream", return_value=False)
        quarter_id = 1
        payload = encode_uint_var(quarter_id) + b"data"
        event = TransportDatagramFrameReceived(data=payload)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_ID_ERROR

    def test_receive_datagram_malformed_varint(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        event = TransportDatagramFrameReceived(data=b"\x80")

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_DATAGRAM_ERROR

    def test_receive_datagram_success(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        stream_id = 4
        quarter_id = stream_id // 4
        payload = encode_uint_var(quarter_id) + b"mydata"
        event = TransportDatagramFrameReceived(data=payload)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], DatagramReceived)
        assert len(effects) == 0

    def test_receive_datagram_truncated(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        event = TransportDatagramFrameReceived(data=b"")

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_DATAGRAM_ERROR

    def test_receive_empty_data_frame(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 1
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        non_log_effects = [e for e in effects if not isinstance(e, LogH3Frame)]

        assert len(non_log_effects) == 0

    def test_receive_multiple_control_frames(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        control_stream_id = 3
        engine._peer_control_stream_id = control_stream_id
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_CONTROL)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        buf.push_uint_var(0)
        buf.push_uint_var(constants.H3_FRAME_TYPE_GOAWAY)
        buf.push_uint_var(1)
        buf.push_uint_var(100)
        event = TransportStreamDataReceived(stream_id=control_stream_id, data=buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 2
        assert isinstance(h3_events[0], SettingsReceived)
        assert isinstance(h3_events[1], GoawayReceived)

    def test_receive_push_stream(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        stream_id = 10
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_PUSH)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert any(
            isinstance(e, LogH3Frame) and e.event == "stream_type_set" and e.data["new"] == "push" for e in effects
        )

    def test_receive_qpack_streams(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_encoder: MagicMock,
        mock_decoder: MagicMock,
    ) -> None:
        encoder_id = 2
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_QPACK_ENCODER)
        buf.push_bytes(b"inst")
        event_enc = TransportStreamDataReceived(stream_id=encoder_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event_enc, state=protocol_state)

        assert engine._peer_encoder_stream_id == encoder_id
        decoder_id = 6
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_QPACK_DECODER)
        buf.push_bytes(b"inst")
        event_dec = TransportStreamDataReceived(stream_id=decoder_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event_dec, state=protocol_state)

        assert engine._peer_decoder_stream_id == decoder_id

    def test_receive_request_data_before_headers(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_DATA)
        buf.push_uint_var(5)
        buf.push_bytes(b"12345")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        close_effects = [e for e in effects if isinstance(e, CloseQuicConnection)]

        assert len(close_effects) == 0

    def test_receive_request_data_no_session_fallback(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 1
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        partial_info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        partial_info.stream_type = constants.H3_STREAM_TYPE_WEBTRANSPORT
        buf = Buffer(capacity=1024)
        buf.push_bytes(b"data")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        close_effects = [e for e in effects if isinstance(e, CloseQuicConnection)]

        assert len(close_effects) == 1
        assert close_effects[0].error_code == ErrorCodes.INTERNAL_ERROR

    def test_receive_request_data_when_blocked(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.blocked = True
        buf = Buffer(capacity=1024)
        buf.push_bytes(b"data")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 0
        assert len(effects) == 0
        assert info.buffer

    def test_receive_request_headers(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_decoder: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        headers_payload = b"encoded_headers"
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(len(headers_payload))
        buf.push_bytes(headers_payload)
        mock_decoder.feed_header.return_value = (b"", [(b":status", b"200")])
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)
        log_effects = [e for e in effects if isinstance(e, LogH3Frame)]

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], HeadersReceived)
        assert h3_events[0].headers == [(b":status", b"200")]
        assert len(log_effects) == 1

    def test_receive_request_headers_blocked(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        stream_id = 0
        engine._settings_received = False
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)

        assert engine._partial_frames[stream_id].blocked is True

    def test_receive_request_headers_fin(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_decoder: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(0)
        mock_decoder.feed_header.return_value = (b"", [(b":status", b"200")])
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=True)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], HeadersReceived)
        assert h3_events[0].stream_ended is True

    def test_receive_request_headers_multiple_values(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_decoder: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        headers_payload = b"encoded_headers"
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(len(headers_payload))
        buf.push_bytes(headers_payload)
        mock_headers = [(b":status", b"200"), (b"set-cookie", b"a=1"), (b"set-cookie", b"b=2")]
        mock_decoder.feed_header.return_value = (b"", mock_headers)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], HeadersReceived)
        assert h3_events[0].headers == [(b":status", b"200"), (b"set-cookie", b"a=1"), (b"set-cookie", b"b=2")]

    def test_receive_request_headers_trailing(
        self,
        engine: WebTransportH3Engine,
        protocol_state: ProtocolState,
        mock_decoder: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        info.headers_processed = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_HEADERS)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_FRAME_UNEXPECTED

    def test_receive_settings_on_request_stream(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 0
        engine._settings_received = True
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_FRAME_TYPE_SETTINGS)
        buf.push_uint_var(0)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(effects) == 1
        assert isinstance(effects[0], CloseQuicConnection)
        assert effects[0].error_code == ErrorCodes.H3_FRAME_UNEXPECTED

    def test_receive_uni_stream_buffer_read_error_type(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        buf = Buffer(capacity=1)
        buf.push_bytes(b"\x40")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        assert info is not None
        assert len(info.buffer) == 1

    def test_receive_uni_stream_ended_cleanly(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        control_stream_id = 0
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        buf.push_uint_var(control_stream_id)
        buf.push_bytes(b"data")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=True)

        engine.handle_transport_event(event=event, state=protocol_state)

        assert stream_id not in engine._partial_frames

    def test_receive_uni_stream_ended_partial(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        buf = Buffer(capacity=10)
        buf.push_bytes(b"\x40")
        event1 = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)
        engine.handle_transport_event(event=event1, state=protocol_state)
        event2 = TransportStreamDataReceived(stream_id=stream_id, data=b"", end_stream=True)

        engine.handle_transport_event(event=event2, state=protocol_state)

        assert stream_id in engine._partial_frames

    def test_receive_uni_stream_payload_parsing(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        control_stream_id = 0
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        buf.push_uint_var(control_stream_id)
        payload = b"12345"
        buf.push_bytes(payload)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)
        event0 = h3_events[0]

        assert len(h3_events) == 1
        assert isinstance(event0, WebTransportStreamDataReceived)
        assert event0.data == payload

    def test_receive_uni_stream_truncated_session_id(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        buf.push_bytes(b"\x40")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        assert info is not None
        assert len(info.buffer) > 0

    def test_receive_uni_stream_truncated_type(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        buf = Buffer(capacity=128)
        buf.push_bytes(b"\x80")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        assert info is not None
        assert len(info.buffer) > 0

    def test_receive_uni_stream_wt_truncated_control_id(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        buf = Buffer(capacity=128)
        buf.push_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        buf.push_bytes(b"\x40")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        assert info is not None
        assert len(info.buffer) > 0

    def test_receive_unknown_uni_stream_type(self, engine: WebTransportH3Engine, protocol_state: ProtocolState) -> None:
        stream_id = 18
        buf = Buffer(capacity=128)
        buf.push_uint_var(0x999)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert not any(isinstance(e, CloseQuicConnection) for e in effects)

    def test_receive_unknown_uni_stream_type_logging(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 10
        buf = Buffer(capacity=128)
        buf.push_uint_var(0x1F)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        _, effects = engine.handle_transport_event(event=event, state=protocol_state)
        info = engine._partial_frames.get(stream_id)

        if info:
            assert len(info.buffer) == 0

    def test_receive_webtransport_data_bidi(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        control_stream_id = 4
        engine._settings_received = True
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = 0
        session_data.control_stream_id = control_stream_id
        protocol_state.sessions[0] = session_data
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = 0
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        partial_info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        partial_info.stream_type = constants.H3_STREAM_TYPE_WEBTRANSPORT
        partial_info.control_stream_id = control_stream_id
        payload = b"wt_data"
        buf = Buffer(capacity=1024)
        buf.push_bytes(payload)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)
        event0 = h3_events[0]

        assert len(h3_events) == 1
        assert isinstance(event0, WebTransportStreamDataReceived)
        assert event0.data == payload

    def test_receive_webtransport_data_bidi_session_zero(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState, mocker: MockerFixture
    ) -> None:
        stream_id = 0
        control_stream_id = 4
        engine._settings_received = True
        session_id = 0
        session_data = mocker.Mock(spec=SessionStateData)
        session_data.session_id = session_id
        session_data.control_stream_id = control_stream_id
        protocol_state.sessions[session_id] = session_data
        stream_state = mocker.Mock(spec=StreamStateData)
        stream_state.stream_id = stream_id
        stream_state.session_id = session_id
        protocol_state.streams[stream_id] = cast(StreamStateData, stream_state)
        partial_info = engine._get_or_create_partial_frame_info(stream_id=stream_id)
        partial_info.stream_type = constants.H3_STREAM_TYPE_WEBTRANSPORT
        payload = b"wt_data"
        buf = Buffer(capacity=1024)
        buf.push_bytes(payload)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], WebTransportStreamDataReceived)

    def test_receive_webtransport_uni_stream_init(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        control_stream_id = 4
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        buf.push_uint_var(control_stream_id)
        buf.push_bytes(b"actual_data")
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, effects = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], WebTransportStreamDataReceived)

    def test_receive_webtransport_uni_stream_no_control_id(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        buf = Buffer(capacity=1024)
        buf.push_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        event = TransportStreamDataReceived(stream_id=stream_id, data=buf.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event, state=protocol_state)

        assert len(h3_events) == 0
        assert engine._partial_frames[stream_id].buffer

    def test_receive_webtransport_uni_stream_split_header(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        stream_id = 2
        buf1 = Buffer(capacity=1024)
        buf1.push_uint_var(constants.H3_STREAM_TYPE_WEBTRANSPORT)
        event1 = TransportStreamDataReceived(stream_id=stream_id, data=buf1.data, end_stream=False)
        engine.handle_transport_event(event=event1, state=protocol_state)
        buf2 = Buffer(capacity=1024)
        buf2.push_bytes(b"\x40")
        event2 = TransportStreamDataReceived(stream_id=stream_id, data=buf2.data, end_stream=False)
        engine.handle_transport_event(event=event2, state=protocol_state)
        buf3 = Buffer(capacity=1024)
        buf3.push_bytes(b"\x04")
        buf3.push_bytes(b"data")
        event3 = TransportStreamDataReceived(stream_id=stream_id, data=buf3.data, end_stream=False)

        h3_events, _ = engine.handle_transport_event(event=event3, state=protocol_state)

        assert len(h3_events) == 1
        assert isinstance(h3_events[0], WebTransportStreamDataReceived)
        assert h3_events[0].data == b"data"
        assert h3_events[0].session_id == 4

    def test_server_mode_encode_webtransport_stream_creation(self, client_config: ClientConfig) -> None:
        engine = WebTransportH3Engine(is_client=False, config=client_config)
        stream_id = 4
        control_stream_id = 0

        effects = engine.encode_webtransport_stream_creation(
            stream_id=stream_id, control_stream_id=control_stream_id, is_unidirectional=False
        )

        assert len(effects) == 2
        assert isinstance(effects[0], SendQuicData)
        assert stream_id not in engine._partial_frames

    def test_server_mode_validate_request_headers(self, client_config: ClientConfig) -> None:
        headers = [
            (b":method", b"CONNECT"),
            (b":scheme", b"https"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b":protocol", b"webtransport"),
        ]
        _validate_request_headers(headers=headers)
        invalid_headers = [
            (b":method", b"CONNECT"),
            (b":scheme", b"https"),
            (b":path", b"/"),
            (b":protocol", b"webtransport"),
        ]

        with pytest.raises(ProtocolError):
            _validate_request_headers(headers=invalid_headers)

    def test_set_local_stream_ids(self, engine: WebTransportH3Engine) -> None:
        engine.set_local_stream_ids(control_stream_id=2, encoder_stream_id=6, decoder_stream_id=10)

        assert engine._local_control_stream_id == 2
        assert engine._local_encoder_stream_id == 6
        assert engine._local_decoder_stream_id == 10

    def test_validate_header_name_colon_middle(self, engine: WebTransportH3Engine) -> None:
        with pytest.raises(ProtocolError) as exc:
            _validate_header_name(key=b"my:header")

        assert ErrorCodes.H3_MESSAGE_ERROR == exc.value.error_code

    def test_validate_header_name_format(self, engine: WebTransportH3Engine) -> None:
        _validate_header_name(key=b"content-type")

        with pytest.raises(ProtocolError):
            _validate_header_name(key=b"Content-Type")
        with pytest.raises(ProtocolError):
            _validate_header_name(key=b"key@")

    def test_validate_header_name_invalid_chars(self, engine: WebTransportH3Engine) -> None:
        _validate_header_name(key=b"a!#$%&'*+-.^_`|~")
        try:
            _validate_header_name(key=b"!#$%&'*+-.^_`|~")
        except ProtocolError:
            pytest.fail("Source code rejects valid symbol-only header names")

        with pytest.raises(ProtocolError) as exc:
            _validate_header_name(key=b"name:bad")

        assert ErrorCodes.H3_MESSAGE_ERROR == exc.value.error_code
        with pytest.raises(ProtocolError):
            _validate_header_name(key=b"Name")

    def test_validate_header_value_del_char(self, engine: WebTransportH3Engine) -> None:
        with pytest.raises(ProtocolError) as exc:
            _validate_header_value(key=b"k", value=b"\x7f")

        assert ErrorCodes.H3_MESSAGE_ERROR == exc.value.error_code

    def test_validate_header_value_htab(self, engine: WebTransportH3Engine) -> None:
        _validate_header_value(key=b"k", value=b"val\tue")

    def test_validate_header_value_invalid(self, engine: WebTransportH3Engine) -> None:
        with pytest.raises(ProtocolError):
            _validate_header_value(key=b"k", value=b"\x00")
        with pytest.raises(ProtocolError):
            _validate_header_value(key=b"k", value=b" v")
        with pytest.raises(ProtocolError):
            _validate_header_value(key=b"k", value=b"v ")

    def test_validate_headers_duplicate_pseudo(self, engine: WebTransportH3Engine) -> None:
        headers = [(b":path", b"/"), (b":path", b"/2")]

        with pytest.raises(ProtocolError):
            _validate_request_headers(headers=headers)

    def test_validate_headers_empty_pseudo(self, engine: WebTransportH3Engine) -> None:
        headers = [
            (b":scheme", b"https"),
            (b":authority", b""),
            (b":path", b"/"),
            (b":method", b"GET"),
            (b":protocol", b"webtransport"),
        ]

        with pytest.raises(ProtocolError):
            _validate_request_headers(headers=headers)

    def test_validate_headers_missing_authority_http(self, engine: WebTransportH3Engine) -> None:
        headers = [(b":method", b"CONNECT"), (b":scheme", b"https"), (b":path", b"/"), (b":authority", b"")]

        with pytest.raises(ProtocolError) as exc:
            _validate_request_headers(headers=headers)

        assert "Pseudo-header b':authority' cannot be empty" in str(exc.value)

    def test_validate_headers_missing_pseudo(self, engine: WebTransportH3Engine) -> None:
        headers = [(b"custom", b"val")]

        with pytest.raises(ProtocolError) as exc:
            _validate_request_headers(headers=headers)

        assert "Missing pseudo-headers" in str(exc.value)

    def test_validate_headers_pass_through(self, engine: WebTransportH3Engine) -> None:
        headers = [(b":method", b"CONNECT")]

        _validate_headers(
            headers=headers, allowed_pseudo_headers=frozenset([b":method"]), required_pseudo_headers=frozenset()
        )

    def test_validate_headers_pseudo_order(self, engine: WebTransportH3Engine) -> None:
        headers = [(b"custom", b"val"), (b":path", b"/")]

        with pytest.raises(ProtocolError):
            _validate_request_headers(headers=headers)

    def test_validate_request_headers_optional_protocol(self, client_config: ClientConfig) -> None:
        headers = [(b":method", b"CONNECT"), (b":scheme", b"https"), (b":authority", b"example.com"), (b":path", b"/")]

        _validate_request_headers(headers=headers)

    def test_validate_response_headers(self, engine: WebTransportH3Engine) -> None:
        headers = [(b":status", b"200")]
        _validate_response_headers(headers=headers)
        invalid = [(b":status", b"200"), (b":other", b"bad")]

        with pytest.raises(ProtocolError):
            _validate_response_headers(headers=invalid)

    def test_validate_settings_error_connect_protocol(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        settings = {constants.SETTINGS_ENABLE_CONNECT_PROTOCOL: 0, constants.SETTINGS_H3_DATAGRAM: 1}

        with pytest.raises(ProtocolError) as exc:
            engine._validate_settings(settings=settings, state=protocol_state)

        assert exc.value.error_code == ErrorCodes.H3_SETTINGS_ERROR

    def test_validate_settings_error_missing_datagram_support(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        settings = {constants.SETTINGS_H3_DATAGRAM: 1}
        protocol_state.remote_max_datagram_frame_size = 0

        with pytest.raises(ProtocolError) as exc:
            engine._validate_settings(settings=settings, state=protocol_state)

        assert exc.value.error_code == ErrorCodes.H3_SETTINGS_ERROR

    def test_validate_settings_wt_without_datagram(
        self, engine: WebTransportH3Engine, protocol_state: ProtocolState
    ) -> None:
        settings = {constants.SETTINGS_ENABLE_CONNECT_PROTOCOL: 1}
        protocol_state.remote_max_datagram_frame_size = 1000

        with pytest.raises(ProtocolError) as exc:
            engine._validate_settings(settings=settings, state=protocol_state)

        assert exc.value.error_code == ErrorCodes.H3_SETTINGS_ERROR
