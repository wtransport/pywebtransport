"""Unit tests for the pywebtransport._protocol.stream_processor module."""

from collections import deque
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ClientConfig, ErrorCodes, ServerConfig, SessionError, StreamError, constants
from pywebtransport._protocol.events import (
    EmitStreamEvent,
    InternalBindQuicStream,
    InternalFailQuicStream,
    InternalReturnStreamData,
    NotifyRequestDone,
    NotifyRequestFailed,
    ResetQuicStream,
    SendH3Capsule,
    SendQuicData,
    StopQuicStream,
    TransportStreamReset,
    UserGetStreamDiagnostics,
    UserResetStream,
    UserSendStreamData,
    UserStopStream,
    UserStreamRead,
    WebTransportStreamDataReceived,
)
from pywebtransport._protocol.state import ProtocolState, SessionStateData, StreamStateData
from pywebtransport._protocol.stream_processor import StreamProcessor
from pywebtransport.types import EventType, SessionState, StreamDirection, StreamState


class TestStreamProcessor:

    @pytest.fixture
    def client_processor(self, mock_client_config: MagicMock) -> StreamProcessor:
        return StreamProcessor(is_client=True, config=mock_client_config)

    @pytest.fixture
    def mock_buffer_cls(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("pywebtransport._protocol.stream_processor.QuicBuffer", autospec=True)

    @pytest.fixture
    def mock_calc_data_limit(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("pywebtransport._protocol.stream_processor.calculate_new_data_limit")

    @pytest.fixture
    def mock_calc_stream_limit(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("pywebtransport._protocol.stream_processor.calculate_new_stream_limit")

    @pytest.fixture
    def mock_client_config(self, mocker: MockerFixture) -> MagicMock:
        config = mocker.create_autospec(ClientConfig, instance=True)
        config.max_stream_write_buffer = 1000
        config.max_stream_read_buffer = 1000
        config.max_total_pending_events = 100
        config.max_pending_events_per_session = 10
        config.flow_control_window_auto_scale = True
        config.flow_control_window_size = 100
        config.initial_max_streams_uni = 10
        config.initial_max_streams_bidi = 10
        return config

    @pytest.fixture
    def mock_ensure_buffer(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch(
            "pywebtransport._protocol.stream_processor.ensure_buffer",
            side_effect=lambda data: (
                data if isinstance(data, (bytes, bytearray, memoryview)) else memoryview(b"mock")
            ),
        )

    @pytest.fixture
    def mock_get_timestamp(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("pywebtransport._protocol.stream_processor.get_timestamp", return_value=123456.0)

    @pytest.fixture
    def mock_http_to_wt_code(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch(
            "pywebtransport._protocol.stream_processor.http_code_to_webtransport_code", return_value=1234
        )

    @pytest.fixture
    def mock_is_peer_initiated(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("pywebtransport._protocol.stream_processor.is_peer_initiated_stream")

    @pytest.fixture
    def mock_is_unidirectional(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("pywebtransport._protocol.stream_processor.is_unidirectional_stream")

    @pytest.fixture
    def mock_server_config(self, mocker: MockerFixture) -> MagicMock:
        config = mocker.create_autospec(ServerConfig, instance=True)
        config.max_stream_write_buffer = 1000
        config.max_stream_read_buffer = 1000
        config.max_total_pending_events = 100
        config.max_pending_events_per_session = 10
        config.flow_control_window_auto_scale = True
        config.flow_control_window_size = 100
        config.initial_max_streams_uni = 10
        config.initial_max_streams_bidi = 10
        return config

    @pytest.fixture
    def mock_session_data(self, mocker: MockerFixture, mock_state: MagicMock) -> SessionStateData:
        session = mocker.create_autospec(SessionStateData, instance=True)
        session.session_id = 0
        session.state = SessionState.CONNECTED
        session.peer_max_data = 1000
        session.local_data_sent = 0
        session.local_data_consumed = 0
        session.local_max_data = 100
        session.peer_data_sent = 0
        session.peer_streams_uni_opened = 0
        session.peer_streams_uni_closed = 0
        session.local_max_streams_uni = 10
        session.peer_streams_bidi_opened = 8
        session.peer_streams_bidi_closed = 0
        session.local_max_streams_bidi = 10
        session.active_streams = set()
        session.blocked_streams = set()
        mock_state.sessions.get.return_value = session
        return session

    @pytest.fixture
    def mock_state(self, mocker: MockerFixture) -> MagicMock:
        state = mocker.create_autospec(ProtocolState, instance=True)
        state.sessions = MagicMock()
        state.streams = {}
        state.early_event_buffer = MagicMock()
        state.early_event_count = 0
        return state

    @pytest.fixture
    def mock_stream_data(
        self, mocker: MockerFixture, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> StreamStateData:
        stream = mocker.create_autospec(StreamStateData, instance=True)
        stream.stream_id = 4
        stream.session_id = 0
        stream.state = StreamState.OPEN
        stream.bytes_sent = 0
        stream.bytes_received = 0
        stream.read_buffer = deque()
        stream.read_buffer_size = 0
        stream.pending_read_requests = deque()
        stream.write_buffer = deque()
        stream.write_buffer_size = 0
        mock_state.streams = {4: stream}
        mock_session_data.active_streams.add(4)
        return stream

    @pytest.fixture
    def mock_wt_to_http_code(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch(
            "pywebtransport._protocol.stream_processor.webtransport_code_to_http_code", return_value=0x52E0
        )

    @pytest.fixture
    def server_processor(self, mock_server_config: MagicMock) -> StreamProcessor:
        return StreamProcessor(is_client=False, config=mock_server_config)

    def test_check_and_send_data_credit_closed_session(
        self, client_processor: StreamProcessor, mock_session_data: SessionStateData, mock_calc_data_limit: MagicMock
    ) -> None:
        mock_session_data.state = SessionState.CLOSED

        effect = client_processor._check_and_send_data_credit(session_data=mock_session_data)

        assert effect is None
        mock_calc_data_limit.assert_not_called()

    def test_check_and_send_data_credit_no_update(
        self,
        client_processor: StreamProcessor,
        mock_session_data: SessionStateData,
        mock_client_config: MagicMock,
        mock_calc_data_limit: MagicMock,
    ) -> None:
        mock_calc_data_limit.return_value = None

        effect = client_processor._check_and_send_data_credit(session_data=mock_session_data)

        assert effect is None
        mock_calc_data_limit.assert_called_once_with(
            current_limit=100, consumed=0, window_size=100, auto_scale=True, force_update=False
        )

    def test_check_and_send_data_credit_sends_update(
        self,
        client_processor: StreamProcessor,
        mock_session_data: SessionStateData,
        mock_calc_data_limit: MagicMock,
        mock_buffer_cls: MagicMock,
    ) -> None:
        mock_calc_data_limit.return_value = 200

        effect = client_processor._check_and_send_data_credit(session_data=mock_session_data)

        assert isinstance(effect, SendH3Capsule)
        assert effect.capsule_type == constants.WT_MAX_DATA_TYPE
        assert mock_session_data.local_max_data == 200

    def test_check_and_send_stream_credit_closed_session(
        self, client_processor: StreamProcessor, mock_session_data: SessionStateData, mock_calc_stream_limit: MagicMock
    ) -> None:
        mock_session_data.state = SessionState.CLOSED

        effect = client_processor._check_and_send_stream_credit(session_data=mock_session_data, is_unidirectional=True)

        assert effect is None
        mock_calc_stream_limit.assert_not_called()

    def test_check_and_send_stream_credit_no_update(
        self, client_processor: StreamProcessor, mock_session_data: SessionStateData, mock_calc_stream_limit: MagicMock
    ) -> None:
        mock_calc_stream_limit.return_value = None

        effect = client_processor._check_and_send_stream_credit(session_data=mock_session_data, is_unidirectional=True)

        assert effect is None

    def test_check_and_send_stream_credit_sends_update_bidi(
        self,
        client_processor: StreamProcessor,
        mock_session_data: SessionStateData,
        mock_calc_stream_limit: MagicMock,
        mock_buffer_cls: MagicMock,
    ) -> None:
        mock_calc_stream_limit.return_value = 20
        mock_session_data.local_max_streams_bidi = 10

        effect = client_processor._check_and_send_stream_credit(session_data=mock_session_data, is_unidirectional=False)

        assert isinstance(effect, SendH3Capsule)
        assert effect.capsule_type == constants.WT_MAX_STREAMS_BIDI_TYPE
        assert mock_session_data.local_max_streams_bidi == 20

    def test_check_and_send_stream_credit_sends_update_uni(
        self,
        client_processor: StreamProcessor,
        mock_session_data: SessionStateData,
        mock_calc_stream_limit: MagicMock,
        mock_buffer_cls: MagicMock,
    ) -> None:
        mock_calc_stream_limit.return_value = 20
        mock_session_data.local_max_streams_uni = 10

        effect = client_processor._check_and_send_stream_credit(session_data=mock_session_data, is_unidirectional=True)

        assert isinstance(effect, SendH3Capsule)
        assert effect.capsule_type == constants.WT_MAX_STREAMS_UNI_TYPE
        assert mock_session_data.local_max_streams_uni == 20

    def test_handle_get_stream_diagnostics_not_found(
        self, client_processor: StreamProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.streams = {}
        event = UserGetStreamDiagnostics(request_id=1, stream_id=4)

        effects = client_processor.handle_get_stream_diagnostics(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, StreamError)

    def test_handle_get_stream_diagnostics_success(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"sensitive", b"data"])
        mock_stream_data.read_buffer_size = 13
        test_dict = {
            "stream_id": 4,
            "state": StreamState.OPEN,
            "read_buffer": deque([b"sensitive", b"data"]),
            "read_buffer_size": 13,
        }

        with patch(
            "pywebtransport._protocol.stream_processor.dataclasses.asdict", return_value=test_dict
        ) as mock_asdict:
            event = UserGetStreamDiagnostics(request_id=1, stream_id=4)

            effects = client_processor.handle_get_stream_diagnostics(event=event, state=mock_state)

            mock_asdict.assert_called_once_with(mock_stream_data)
            assert len(effects) == 1
            effect = effects[0]
            assert isinstance(effect, NotifyRequestDone)
            assert effect.request_id == 1
            result_value = effect.result
            assert result_value["read_buffer"] == b""
            assert result_value["read_buffer_size"] == 13

    def test_handle_internal_bind_quic_stream_session_not_found(
        self, client_processor: StreamProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions.get.return_value = None
        event = InternalBindQuicStream(request_id=1, session_id=999, stream_id=1, is_unidirectional=False)

        effects = client_processor.handle_internal_bind_quic_stream(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert effects[0].request_id == 1
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_internal_bind_quic_stream_success(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
    ) -> None:
        event = InternalBindQuicStream(request_id=1, session_id=0, stream_id=5, is_unidirectional=False)

        effects = client_processor.handle_internal_bind_quic_stream(event=event, state=mock_state)

        assert 5 in mock_state.streams
        assert 5 in mock_session_data.active_streams
        assert mock_state.streams[5].direction == StreamDirection.BIDIRECTIONAL
        assert mock_state.streams[5].state == StreamState.OPEN
        assert len(effects) == 2
        assert isinstance(effects[0], NotifyRequestDone)
        assert effects[0].request_id == 1
        assert effects[0].result == 5
        assert isinstance(effects[1], EmitStreamEvent)
        assert effects[1].event_type == EventType.STREAM_OPENED

    def test_handle_internal_fail_quic_stream_decrements_bidi(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.local_streams_bidi_opened = 5
        event = InternalFailQuicStream(
            request_id=1, session_id=0, exception=ValueError("fail"), is_unidirectional=False
        )

        effects = client_processor.handle_internal_fail_quic_stream(event=event, state=mock_state)

        assert mock_session_data.local_streams_bidi_opened == 4
        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert effects[0].request_id == 1
        assert effects[0].exception == event.exception

    def test_handle_internal_fail_quic_stream_decrements_uni(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.local_streams_uni_opened = 5
        event = InternalFailQuicStream(request_id=1, session_id=0, exception=ValueError("fail"), is_unidirectional=True)

        effects = client_processor.handle_internal_fail_quic_stream(event=event, state=mock_state)

        assert mock_session_data.local_streams_uni_opened == 4
        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert effects[0].request_id == 1

    def test_handle_internal_fail_quic_stream_session_closed(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = SessionState.CLOSED
        mock_session_data.local_streams_uni_opened = 5
        event = InternalFailQuicStream(request_id=1, session_id=0, exception=ValueError("fail"), is_unidirectional=True)

        effects = client_processor.handle_internal_fail_quic_stream(event=event, state=mock_state)

        assert mock_session_data.local_streams_uni_opened == 4
        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)

    def test_handle_internal_fail_quic_stream_session_not_found(
        self, client_processor: StreamProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions.get.return_value = None
        event = InternalFailQuicStream(
            request_id=1, session_id=999, exception=ValueError("fail"), is_unidirectional=False
        )

        effects = client_processor.handle_internal_fail_quic_stream(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert effects[0].request_id == 1

    def test_handle_internal_fail_quic_stream_zero_counters(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.local_streams_bidi_opened = 0
        mock_session_data.local_streams_uni_opened = 0

        event_bidi = InternalFailQuicStream(request_id=1, session_id=0, exception=ValueError(), is_unidirectional=False)
        client_processor.handle_internal_fail_quic_stream(event=event_bidi, state=mock_state)
        assert mock_session_data.local_streams_bidi_opened == 0

        event_uni = InternalFailQuicStream(request_id=2, session_id=0, exception=ValueError(), is_unidirectional=True)
        client_processor.handle_internal_fail_quic_stream(event=event_uni, state=mock_state)
        assert mock_session_data.local_streams_uni_opened == 0

    def test_handle_reset_stream_active_streams_inconsistency(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_is_peer_initiated: MagicMock,
    ) -> None:
        mock_stream_data.state = StreamState.OPEN
        mock_session_data.active_streams = set()
        mock_is_peer_initiated.return_value = True
        event = UserResetStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_reset_stream(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.RESET_SENT
        assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) not in effects

    def test_handle_reset_stream_empty_buffer(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_wt_to_http_code: MagicMock,
        mock_get_timestamp: MagicMock,
    ) -> None:
        mock_stream_data.state = StreamState.OPEN
        mock_stream_data.write_buffer = deque()
        event = UserResetStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_reset_stream(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.RESET_SENT
        assert ResetQuicStream(stream_id=4, error_code=0x52E0) in effects
        assert NotifyRequestDone(request_id=1, result=None) in effects

    def test_handle_reset_stream_fails_pending_writes(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_wt_to_http_code: MagicMock,
    ) -> None:
        mock_stream_data.write_buffer = deque([(b"data", 2, False)])
        mock_stream_data.write_buffer_size = 4
        event = UserResetStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_reset_stream(event=event, state=mock_state)

        assert len(mock_stream_data.write_buffer) == 0
        assert mock_stream_data.write_buffer_size == 0
        fail_effect = next(e for e in effects if isinstance(e, NotifyRequestFailed) and e.request_id == 2)
        assert isinstance(fail_effect.exception, StreamError)
        assert fail_effect.exception.error_code == 100

    def test_handle_reset_stream_not_found(self, client_processor: StreamProcessor, mock_state: MagicMock) -> None:
        mock_state.streams = {}
        event = UserResetStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_reset_stream(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, StreamError)

    def test_handle_reset_stream_peer_initiated_no_credit_update(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_is_peer_initiated: MagicMock,
        mock_calc_stream_limit: MagicMock,
        mock_wt_to_http_code: MagicMock,
    ) -> None:
        mock_stream_data.state = StreamState.HALF_CLOSED_REMOTE
        mock_session_data.blocked_streams.add(4)
        mock_is_peer_initiated.return_value = True
        mock_calc_stream_limit.return_value = None
        event = UserResetStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_reset_stream(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.CLOSED
        assert not any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_reset_stream_session_missing(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_state.sessions.get.return_value = None
        mock_stream_data.state = StreamState.HALF_CLOSED_REMOTE
        event = UserResetStream(request_id=1, stream_id=4, error_code=0)

        effects = client_processor.handle_reset_stream(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.CLOSED
        assert NotifyRequestDone(request_id=1, result=None) in effects

    @pytest.mark.parametrize(
        "state_from, state_to",
        [
            (StreamState.OPEN, StreamState.RESET_SENT),
            (StreamState.HALF_CLOSED_REMOTE, StreamState.CLOSED),
            (StreamState.RESET_RECEIVED, StreamState.CLOSED),
        ],
    )
    def test_handle_reset_stream_state_transitions_and_credit_update(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_wt_to_http_code: MagicMock,
        mock_get_timestamp: MagicMock,
        mock_is_peer_initiated: MagicMock,
        mock_is_unidirectional: MagicMock,
        mock_calc_stream_limit: MagicMock,
        state_from: StreamState,
        state_to: StreamState,
    ) -> None:
        mock_stream_data.state = state_from
        mock_session_data.blocked_streams.add(4)
        mock_is_peer_initiated.return_value = True
        mock_is_unidirectional.return_value = False
        mock_calc_stream_limit.return_value = 50
        event = UserResetStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_reset_stream(event=event, state=mock_state)

        mock_wt_to_http_code.assert_called_once_with(app_error_code=100)
        assert mock_stream_data.state == state_to
        assert mock_stream_data.closed_at == 123456.0
        assert mock_stream_data.close_code == 100
        assert ResetQuicStream(stream_id=4, error_code=0x52E0) in effects
        assert NotifyRequestDone(request_id=1, result=None) in effects
        assert 4 not in mock_session_data.blocked_streams

        if state_to == StreamState.CLOSED:
            assert 4 not in mock_session_data.active_streams
            assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects
            assert mock_session_data.peer_streams_bidi_closed == 1
            assert any(
                isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_STREAMS_BIDI_TYPE for e in effects
            )

    @pytest.mark.parametrize("state", [StreamState.HALF_CLOSED_LOCAL, StreamState.CLOSED, StreamState.RESET_SENT])
    def test_handle_reset_stream_wrong_state_no_op(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        state: StreamState,
    ) -> None:
        mock_stream_data.state = state
        event = UserResetStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_reset_stream(event=event, state=mock_state)

        assert effects == [NotifyRequestDone(request_id=1, result=None)]

    def test_handle_return_stream_data(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"existing"])
        mock_stream_data.read_buffer_size = 8
        event = InternalReturnStreamData(stream_id=4, data=b"returned")

        client_processor.handle_return_stream_data(event=event, state=mock_state)

        assert list(mock_stream_data.read_buffer) == [b"returned", b"existing"]
        assert mock_stream_data.read_buffer_size == 16

    def test_handle_return_stream_data_not_found(
        self, client_processor: StreamProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.streams = {}
        event = InternalReturnStreamData(stream_id=999, data=b"data")

        client_processor.handle_return_stream_data(event=event, state=mock_state)

    def test_handle_send_stream_data_appends_to_write_buffer(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
    ) -> None:
        mock_stream_data.write_buffer = deque([(b"first", 2, False)])
        mock_stream_data.write_buffer_size = 5
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"second", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert effects == []
        assert len(mock_stream_data.write_buffer) == 2
        assert mock_stream_data.write_buffer[1] == (b"second", 1, False)
        assert mock_stream_data.write_buffer_size == 11
        assert 4 in mock_session_data.blocked_streams

    def test_handle_send_stream_data_buffer_boundary(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_client_config: MagicMock,
    ) -> None:
        mock_client_config.max_stream_write_buffer = 100
        mock_stream_data.write_buffer = deque([(b"a" * 90, 2, False)])
        mock_stream_data.write_buffer_size = 90
        mock_session_data.peer_max_data = 1000
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"a" * 10, end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert not any(isinstance(e, NotifyRequestFailed) for e in effects)

    def test_handle_send_stream_data_buffer_full(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_client_config: MagicMock,
    ) -> None:
        mock_client_config.max_stream_write_buffer = 100
        mock_stream_data.write_buffer = deque([(b"a" * 90, 2, False)])
        mock_stream_data.write_buffer_size = 90
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"a" * 11, end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert "write buffer full" in str(fail_effect.exception)

    def test_handle_send_stream_data_dummy_state(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.state = "DUMMY_STATE"  # type: ignore[assignment]
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=True)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert not any(isinstance(e, EmitStreamEvent) for e in effects)

    def test_handle_send_stream_data_empty_payload_no_fin(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
    ) -> None:
        mock_session_data.peer_max_data = 1000
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert effects == [
            SendQuicData(stream_id=4, data=b"", end_stream=False),
            NotifyRequestDone(request_id=1, result=None),
        ]

    def test_handle_send_stream_data_ensure_buffer_type_error(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_ensure_buffer: MagicMock,
    ) -> None:
        mock_ensure_buffer.side_effect = TypeError("Invalid type")
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"data", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert isinstance(fail_effect.exception, TypeError)

    def test_handle_send_stream_data_fully_blocked(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_buffer_cls: MagicMock,
    ) -> None:
        mock_session_data.peer_max_data = 100
        mock_session_data.local_data_sent = 100
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert len(mock_stream_data.write_buffer) == 1
        assert mock_stream_data.write_buffer_size == 5
        assert 4 in mock_session_data.blocked_streams
        assert any(isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_DATA_BLOCKED_TYPE for e in effects)

    def test_handle_send_stream_data_not_found(self, client_processor: StreamProcessor, mock_state: MagicMock) -> None:
        mock_state.streams = {}
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)

    def test_handle_send_stream_data_partial_send_blocked(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_buffer_cls: MagicMock,
    ) -> None:
        mock_session_data.peer_max_data = 100
        mock_session_data.local_data_sent = 98
        available_credit = 2
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert SendQuicData(stream_id=4, data=b"he", end_stream=False) in effects
        assert any(isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_DATA_BLOCKED_TYPE for e in effects)
        assert mock_session_data.local_data_sent == 100
        assert mock_stream_data.bytes_sent == available_credit
        assert len(mock_stream_data.write_buffer) == 1
        assert mock_stream_data.write_buffer_size == 3
        assert mock_stream_data.write_buffer[0] == (b"llo", 1, False)
        assert 4 in mock_session_data.blocked_streams

    def test_handle_send_stream_data_session_not_found(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_state.sessions.get.return_value = None
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, StreamError)
        assert "Session not found" in fail_effect.exception.args[0]

    def test_handle_send_stream_data_success_end_stream_half_closed_with_credit_update(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_is_peer_initiated: MagicMock,
        mock_calc_stream_limit: MagicMock,
    ) -> None:
        mock_stream_data.state = StreamState.HALF_CLOSED_REMOTE
        mock_session_data.blocked_streams.add(4)
        mock_is_peer_initiated.return_value = True
        mock_calc_stream_limit.return_value = 50
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=True)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.CLOSED
        assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects
        assert NotifyRequestDone(request_id=1, result=None) in effects
        assert 4 not in mock_session_data.active_streams
        assert 4 not in mock_session_data.blocked_streams
        assert any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_send_stream_data_success_end_stream_open(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.state = StreamState.OPEN
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=True)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.HALF_CLOSED_LOCAL
        assert not any(isinstance(e, EmitStreamEvent) for e in effects)

    def test_handle_send_stream_data_success_fits_credit(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
    ) -> None:
        mock_session_data.peer_max_data = 1000
        mock_session_data.local_data_sent = 0
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert effects == [
            SendQuicData(stream_id=4, data=b"hello", end_stream=False),
            NotifyRequestDone(request_id=1, result=None),
        ]
        assert mock_session_data.local_data_sent == 5
        assert mock_stream_data.bytes_sent == 5
        assert len(mock_stream_data.write_buffer) == 0
        assert mock_stream_data.write_buffer_size == 0

    @pytest.mark.parametrize("state", [StreamState.HALF_CLOSED_LOCAL, StreamState.CLOSED, StreamState.RESET_SENT])
    def test_handle_send_stream_data_wrong_state(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        state: StreamState,
    ) -> None:
        mock_stream_data.state = state
        event = UserSendStreamData(request_id=1, stream_id=4, data=b"hello", end_stream=False)

        effects = client_processor.handle_send_stream_data(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, StreamError)
        assert "is not writable" in fail_effect.exception.args[0]

    def test_handle_stop_stream_active_streams_inconsistency(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_is_peer_initiated: MagicMock,
    ) -> None:
        mock_stream_data.state = StreamState.OPEN
        mock_session_data.active_streams = set()
        mock_is_peer_initiated.return_value = True
        event = UserStopStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_stop_stream(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.RESET_RECEIVED
        assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) not in effects

    def test_handle_stop_stream_fails_pending_reads(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_wt_to_http_code: MagicMock,
    ) -> None:
        mock_stream_data.pending_read_requests = deque([(2, 100), (3, 200)])
        event = UserStopStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_stop_stream(event=event, state=mock_state)

        assert len(mock_stream_data.pending_read_requests) == 0
        failed_ids = {e.request_id for e in effects if isinstance(e, NotifyRequestFailed)}
        assert {2, 3}.issubset(failed_ids)

    def test_handle_stop_stream_no_pending_reads(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_wt_to_http_code: MagicMock,
        mock_get_timestamp: MagicMock,
    ) -> None:
        mock_stream_data.state = StreamState.OPEN
        mock_stream_data.pending_read_requests = deque()
        event = UserStopStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_stop_stream(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.RESET_RECEIVED
        assert NotifyRequestDone(request_id=1, result=None) in effects

    def test_handle_stop_stream_not_found(self, client_processor: StreamProcessor, mock_state: MagicMock) -> None:
        mock_state.streams = {}
        event = UserStopStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_stop_stream(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, StreamError)

    def test_handle_stop_stream_session_missing(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_state.sessions.get.return_value = None
        mock_stream_data.state = StreamState.HALF_CLOSED_LOCAL
        event = UserStopStream(request_id=1, stream_id=4, error_code=0)

        client_processor.handle_stop_stream(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.CLOSED

    @pytest.mark.parametrize(
        "state_from, state_to",
        [
            (StreamState.OPEN, StreamState.RESET_RECEIVED),
            (StreamState.HALF_CLOSED_LOCAL, StreamState.CLOSED),
            (StreamState.RESET_SENT, StreamState.CLOSED),
        ],
    )
    def test_handle_stop_stream_state_transitions_and_credit_update(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_wt_to_http_code: MagicMock,
        mock_get_timestamp: MagicMock,
        mock_is_peer_initiated: MagicMock,
        mock_calc_stream_limit: MagicMock,
        state_from: StreamState,
        state_to: StreamState,
    ) -> None:
        mock_stream_data.state = state_from
        mock_session_data.blocked_streams.add(4)
        mock_is_peer_initiated.return_value = True
        mock_calc_stream_limit.return_value = 50
        event = UserStopStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_stop_stream(event=event, state=mock_state)

        mock_wt_to_http_code.assert_called_once_with(app_error_code=100)
        assert mock_stream_data.state == state_to
        assert mock_stream_data.closed_at == 123456.0
        assert mock_stream_data.close_code == 100
        assert StopQuicStream(stream_id=4, error_code=0x52E0) in effects
        assert NotifyRequestDone(request_id=1, result=None) in effects
        if state_to == StreamState.CLOSED:
            assert 4 not in mock_session_data.active_streams
            assert 4 not in mock_session_data.blocked_streams
            assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects
            assert any(isinstance(e, SendH3Capsule) for e in effects)

    @pytest.mark.parametrize("state", [StreamState.HALF_CLOSED_REMOTE, StreamState.CLOSED, StreamState.RESET_RECEIVED])
    def test_handle_stop_stream_wrong_state_no_op(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        state: StreamState,
    ) -> None:
        mock_stream_data.state = state
        event = UserStopStream(request_id=1, stream_id=4, error_code=100)

        effects = client_processor.handle_stop_stream(event=event, state=mock_state)

        assert effects == [NotifyRequestDone(request_id=1, result=None)]

    def test_handle_stream_read_appends_pending_request(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque()
        mock_stream_data.read_buffer_size = 0
        mock_stream_data.state = StreamState.OPEN
        event = UserStreamRead(request_id=1, stream_id=4, max_bytes=100)

        effects = client_processor.handle_stream_read(event=event, state=mock_state)

        assert effects == []
        assert len(mock_stream_data.pending_read_requests) == 1
        assert mock_stream_data.pending_read_requests[0] == (1, 100)

    def test_handle_stream_read_eof(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque()
        mock_stream_data.read_buffer_size = 0
        mock_stream_data.state = StreamState.HALF_CLOSED_REMOTE
        event = UserStreamRead(request_id=1, stream_id=4, max_bytes=100)

        effects = client_processor.handle_stream_read(event=event, state=mock_state)

        assert effects == [NotifyRequestDone(request_id=1, result=b"")]

    def test_handle_stream_read_fails_on_reset(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque()
        mock_stream_data.read_buffer_size = 0
        mock_stream_data.state = StreamState.RESET_RECEIVED
        event = UserStreamRead(request_id=1, stream_id=4, max_bytes=100)

        effects = client_processor.handle_stream_read(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, StreamError)
        assert "receive side closed" in fail_effect.exception.args[0]

    def test_handle_stream_read_not_found(self, client_processor: StreamProcessor, mock_state: MagicMock) -> None:
        mock_state.streams = {}
        event = UserStreamRead(request_id=1, stream_id=4, max_bytes=100)

        effects = client_processor.handle_stream_read(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, StreamError)

    def test_handle_stream_read_reads_from_buffer_all(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"hello", b" world"])
        mock_stream_data.read_buffer_size = 11
        event = UserStreamRead(request_id=1, stream_id=4, max_bytes=0)

        effects = client_processor.handle_stream_read(event=event, state=mock_state)

        assert isinstance(effects[0], NotifyRequestDone)
        assert effects[0].result == b"hello world"
        assert len(mock_stream_data.read_buffer) == 0
        assert mock_stream_data.read_buffer_size == 0

    def test_handle_stream_read_reads_from_buffer_and_updates_credit(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_session_data: SessionStateData,
        mock_calc_data_limit: MagicMock,
        mock_buffer_cls: MagicMock,
    ) -> None:
        mock_stream_data.read_buffer = deque([b"hello", b" world"])
        mock_stream_data.read_buffer_size = 11
        mock_calc_data_limit.return_value = 200
        event = UserStreamRead(request_id=1, stream_id=4, max_bytes=5)

        effects = client_processor.handle_stream_read(event=event, state=mock_state)

        assert NotifyRequestDone(request_id=1, result=b"hello") in effects
        assert any(isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_DATA_TYPE for e in effects)
        assert mock_session_data.local_data_consumed == 5
        assert list(mock_stream_data.read_buffer) == [b" world"]
        assert mock_stream_data.read_buffer_size == 6

    def test_handle_stream_read_session_gone(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"data"])
        mock_stream_data.read_buffer_size = 4
        mock_state.sessions.get.return_value = None
        event = UserStreamRead(request_id=1, stream_id=4, max_bytes=100)

        effects = client_processor.handle_stream_read(event=event, state=mock_state)

        assert isinstance(effects[0], NotifyRequestDone)
        assert effects[0].result == b"data"
        assert not any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_transport_stream_reset_active_streams_inconsistency(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_is_peer_initiated: MagicMock,
    ) -> None:
        mock_stream_data.state = StreamState.OPEN
        mock_session_data.active_streams = set()
        mock_is_peer_initiated.return_value = True
        event = TransportStreamReset(stream_id=4, error_code=ErrorCodes.WT_APPLICATION_ERROR_FIRST)

        effects = client_processor.handle_transport_stream_reset(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.CLOSED
        assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects

    def test_handle_transport_stream_reset_fails_pending_futures(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_http_to_wt_code: MagicMock,
    ) -> None:
        mock_stream_data.pending_read_requests = deque([(1, 100), (2, 200)])
        mock_stream_data.write_buffer = deque([(b"data", 2, False)])
        mock_stream_data.write_buffer_size = 4
        event = TransportStreamReset(stream_id=4, error_code=ErrorCodes.H3_NO_ERROR)

        effects = client_processor.handle_transport_stream_reset(event=event, state=mock_state)

        mock_http_to_wt_code.assert_not_called()
        assert mock_stream_data.close_code == ErrorCodes.H3_NO_ERROR
        assert len(mock_stream_data.pending_read_requests) == 0
        assert len(mock_stream_data.write_buffer) == 0
        assert mock_stream_data.write_buffer_size == 0
        failed_ids = {e.request_id for e in effects if isinstance(e, NotifyRequestFailed)}
        assert {1, 2}.issubset(failed_ids)

    def test_handle_transport_stream_reset_maps_app_error_and_updates_credit(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_http_to_wt_code: MagicMock,
        mock_session_data: SessionStateData,
        mock_is_peer_initiated: MagicMock,
        mock_calc_stream_limit: MagicMock,
    ) -> None:
        mock_session_data.blocked_streams.add(4)
        mock_is_peer_initiated.return_value = True
        mock_calc_stream_limit.return_value = 50
        event = TransportStreamReset(stream_id=4, error_code=ErrorCodes.WT_APPLICATION_ERROR_FIRST)

        effects = client_processor.handle_transport_stream_reset(event=event, state=mock_state)

        mock_http_to_wt_code.assert_called_once_with(http_error_code=ErrorCodes.WT_APPLICATION_ERROR_FIRST)
        assert mock_stream_data.close_code == 1234
        assert mock_stream_data.state == StreamState.CLOSED
        assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects
        assert 4 not in mock_session_data.active_streams
        assert 4 not in mock_session_data.blocked_streams
        assert any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_transport_stream_reset_maps_http_error_value_error(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_http_to_wt_code: MagicMock,
    ) -> None:
        mock_http_to_wt_code.side_effect = ValueError("Invalid code")
        event = TransportStreamReset(stream_id=4, error_code=ErrorCodes.WT_APPLICATION_ERROR_FIRST)

        client_processor.handle_transport_stream_reset(event=event, state=mock_state)

        assert mock_stream_data.close_code == ErrorCodes.WT_APPLICATION_ERROR_FIRST

    def test_handle_transport_stream_reset_maps_reserved_http_error(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_http_to_wt_code: MagicMock,
    ) -> None:
        mock_http_to_wt_code.side_effect = ValueError("test")
        event = TransportStreamReset(stream_id=4, error_code=ErrorCodes.WT_APPLICATION_ERROR_FIRST)

        client_processor.handle_transport_stream_reset(event=event, state=mock_state)

        assert mock_stream_data.close_code == ErrorCodes.WT_APPLICATION_ERROR_FIRST

    def test_handle_transport_stream_reset_no_op_if_closed(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.state = StreamState.CLOSED
        event = TransportStreamReset(stream_id=4, error_code=100)

        effects = client_processor.handle_transport_stream_reset(event=event, state=mock_state)

        assert effects == []

    def test_handle_transport_stream_reset_session_missing(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_state.sessions.get.return_value = None
        mock_stream_data.state = StreamState.OPEN
        event = TransportStreamReset(stream_id=4, error_code=0)

        client_processor.handle_transport_stream_reset(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.CLOSED

    def test_handle_transport_stream_reset_uni_stream_credit(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_stream_data: StreamStateData,
        mock_session_data: SessionStateData,
        mock_is_peer_initiated: MagicMock,
        mock_is_unidirectional: MagicMock,
        mock_calc_stream_limit: MagicMock,
        mock_buffer_cls: MagicMock,
    ) -> None:
        mock_is_peer_initiated.return_value = True
        mock_is_unidirectional.return_value = True
        mock_calc_stream_limit.return_value = 25
        event = TransportStreamReset(stream_id=4, error_code=0)

        effects = client_processor.handle_transport_stream_reset(event=event, state=mock_state)

        assert mock_session_data.peer_streams_uni_closed == 1
        assert any(
            isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_STREAMS_UNI_TYPE for e in effects
        )

    def test_handle_webtransport_stream_data_buffers_data(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
    ) -> None:
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"hello", stream_ended=False)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert mock_session_data.peer_data_sent == 5
        assert list(mock_stream_data.read_buffer) == [b"hello"]
        assert mock_stream_data.read_buffer_size == 5
        assert not any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_webtransport_stream_data_client_no_session_data(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_state.sessions.get.return_value = None
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"hello", stream_ended=False)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert mock_stream_data.bytes_received == 0
        assert effects == []

    def test_handle_webtransport_stream_data_client_unknown_stream(
        self, client_processor: StreamProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.streams = {}
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"hello", stream_ended=False)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []

    def test_handle_webtransport_stream_data_end_stream_closes_half_local_updates_credit(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_get_timestamp: MagicMock,
        mock_is_peer_initiated: MagicMock,
        mock_calc_stream_limit: MagicMock,
    ) -> None:
        mock_stream_data.state = StreamState.HALF_CLOSED_LOCAL
        mock_stream_data.read_buffer = deque()
        mock_stream_data.read_buffer_size = 0
        mock_session_data.blocked_streams.add(4)
        mock_is_peer_initiated.return_value = True
        mock_calc_stream_limit.return_value = 50
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"", stream_ended=True)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.CLOSED
        assert mock_stream_data.closed_at == 123456.0
        assert 4 not in mock_session_data.active_streams
        assert 4 not in mock_session_data.blocked_streams
        assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects
        assert any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_webtransport_stream_data_end_stream_data_pending_transition(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
    ) -> None:
        mock_stream_data.state = StreamState.HALF_CLOSED_LOCAL
        mock_stream_data.read_buffer = deque([b"pending"])
        mock_stream_data.read_buffer_size = 7
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"", stream_ended=True)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.HALF_CLOSED_REMOTE
        assert not any(isinstance(e, EmitStreamEvent) for e in effects)

    def test_handle_webtransport_stream_data_end_stream_duplicate_fin(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.state = StreamState.HALF_CLOSED_REMOTE
        mock_stream_data.read_buffer_size = 0
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"", stream_ended=True)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.HALF_CLOSED_REMOTE
        assert not any(isinstance(e, EmitStreamEvent) for e in effects)

    def test_handle_webtransport_stream_data_end_stream_open_transition(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
    ) -> None:
        mock_stream_data.state = StreamState.OPEN
        mock_stream_data.read_buffer_size = 0
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"", stream_ended=True)

        client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.HALF_CLOSED_REMOTE

    def test_handle_webtransport_stream_data_end_stream_reset_sent(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
    ) -> None:
        mock_stream_data.state = StreamState.RESET_SENT
        mock_stream_data.read_buffer = deque()
        mock_stream_data.read_buffer_size = 0
        mock_session_data.blocked_streams.add(4)
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"", stream_ended=True)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert mock_stream_data.state == StreamState.CLOSED
        assert EmitStreamEvent(stream_id=4, event_type=EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects

    def test_handle_webtransport_stream_data_end_stream_wakes_all_pending_reads(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
    ) -> None:
        mock_stream_data.state = StreamState.OPEN
        mock_stream_data.pending_read_requests = deque([(1, 100), (2, 200)])
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"", stream_ended=True)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert NotifyRequestDone(request_id=1, result=b"") in effects
        assert NotifyRequestDone(request_id=2, result=b"") in effects
        assert len(mock_stream_data.pending_read_requests) == 0

    def test_handle_webtransport_stream_data_fulfills_multiple_reads_updates_credit(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_calc_data_limit: MagicMock,
    ) -> None:
        mock_stream_data.pending_read_requests = deque([(1, 2), (2, 2)])
        mock_calc_data_limit.side_effect = [None, 200]
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"hello", stream_ended=False)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert NotifyRequestDone(request_id=1, result=b"he") in effects
        assert NotifyRequestDone(request_id=2, result=b"ll") in effects
        assert list(mock_stream_data.read_buffer) == [b"o"]
        assert mock_session_data.local_data_consumed == 4
        assert any(isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_DATA_TYPE for e in effects)

    def test_handle_webtransport_stream_data_ignore_if_closed(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.state = StreamState.CLOSED
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"hello", stream_ended=False)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []

    def test_handle_webtransport_stream_data_internal_state_error(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_state.sessions.get.return_value = None
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"data", stream_ended=False)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []
        assert mock_stream_data.bytes_received == 0

    def test_handle_webtransport_stream_data_no_pending_reads(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.pending_read_requests = deque()
        mock_stream_data.read_buffer = deque([b"existing"])
        mock_stream_data.read_buffer_size = 8
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"hello", stream_ended=False)

        client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert len(mock_stream_data.read_buffer) == 2
        assert mock_stream_data.read_buffer_size == 13

    def test_handle_webtransport_stream_data_orphaned_stream_mid_processing(
        self, client_processor: StreamProcessor, mock_state: MagicMock, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.session_id = 999
        mock_state.sessions.get.return_value = None
        event = WebTransportStreamDataReceived(stream_id=4, session_id=999, data=b"data", stream_ended=False)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []

    def test_handle_webtransport_stream_data_server_creates_new_stream_bidi(
        self,
        server_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
        mock_is_unidirectional: MagicMock,
    ) -> None:
        mock_state.streams = {}
        mock_is_unidirectional.return_value = False
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        with patch(
            "pywebtransport._protocol.stream_processor.get_stream_direction_from_id",
            return_value=StreamDirection.BIDIRECTIONAL,
        ) as mock_get_dir:
            effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)
            mock_get_dir.assert_called_once_with(stream_id=8, is_client=False)

        assert 8 in mock_state.streams
        new_stream = mock_state.streams[8]
        assert isinstance(new_stream, StreamStateData)
        assert new_stream.session_id == 0
        assert new_stream.direction == StreamDirection.BIDIRECTIONAL
        assert new_stream.bytes_received == 5
        assert list(new_stream.read_buffer) == [b"hello"]
        assert new_stream.read_buffer_size == 5
        assert mock_session_data.peer_streams_bidi_opened == 9
        assert 8 in mock_session_data.active_streams
        assert (
            EmitStreamEvent(
                stream_id=8,
                event_type=EventType.STREAM_OPENED,
                data={"stream_id": 8, "session_id": 0, "direction": StreamDirection.BIDIRECTIONAL},
            )
            in effects
        )

    def test_handle_webtransport_stream_data_server_creates_new_stream_uni(
        self,
        server_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
        mock_is_unidirectional: MagicMock,
    ) -> None:
        mock_state.streams = {}
        mock_is_unidirectional.return_value = True
        event = WebTransportStreamDataReceived(stream_id=10, session_id=0, data=b"hello", stream_ended=False)

        with patch(
            "pywebtransport._protocol.stream_processor.get_stream_direction_from_id",
            return_value=StreamDirection.RECEIVE_ONLY,
        ) as mock_get_dir:
            effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)
            mock_get_dir.assert_called_once_with(stream_id=10, is_client=False)

        assert 10 in mock_state.streams
        new_stream = mock_state.streams[10]
        assert isinstance(new_stream, StreamStateData)
        assert new_stream.session_id == 0
        assert new_stream.direction == StreamDirection.RECEIVE_ONLY
        assert new_stream.bytes_received == 5
        assert list(new_stream.read_buffer) == [b"hello"]
        assert new_stream.read_buffer_size == 5
        assert mock_session_data.peer_streams_uni_opened == 1
        assert 10 in mock_session_data.active_streams
        assert (
            EmitStreamEvent(
                stream_id=10,
                event_type=EventType.STREAM_OPENED,
                data={"stream_id": 10, "session_id": 0, "direction": StreamDirection.RECEIVE_ONLY},
            )
            in effects
        )

    def test_handle_webtransport_stream_data_server_creates_stream_no_credit_update(
        self,
        server_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
        mock_is_unidirectional: MagicMock,
        mock_calc_stream_limit: MagicMock,
        mock_buffer_cls: MagicMock,
    ) -> None:
        mock_state.streams = {}
        mock_is_unidirectional.return_value = True
        mock_calc_stream_limit.return_value = 20
        event = WebTransportStreamDataReceived(stream_id=10, session_id=0, data=b"data", stream_ended=False)

        with patch(
            "pywebtransport._protocol.stream_processor.get_stream_direction_from_id",
            return_value=StreamDirection.RECEIVE_ONLY,
        ):
            effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert not any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_webtransport_stream_data_server_early_buffering(
        self, server_processor: StreamProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        mock_state.sessions.get.return_value = None
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []
        mock_state.early_event_buffer.setdefault.assert_called()

    def test_handle_webtransport_stream_data_server_early_buffering_global_full(
        self, server_processor: StreamProcessor, mock_state: MagicMock, mock_server_config: MagicMock
    ) -> None:
        mock_server_config.max_total_pending_events = 10
        mock_state.sessions.get.return_value = None
        mock_state.early_event_count = 10
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == [ResetQuicStream(stream_id=8, error_code=constants.ErrorCodes.WT_BUFFERED_STREAM_REJECTED)]

    def test_handle_webtransport_stream_data_server_early_buffering_session_full(
        self, server_processor: StreamProcessor, mock_state: MagicMock, mock_server_config: MagicMock
    ) -> None:
        mock_server_config.max_total_pending_events = 100
        mock_server_config.max_pending_events_per_session = 1
        mock_state.sessions.get.return_value = None
        mock_state.early_event_buffer = {0: ["existing_event"]}
        mock_state.early_event_count = 1
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == [ResetQuicStream(stream_id=8, error_code=constants.ErrorCodes.WT_BUFFERED_STREAM_REJECTED)]

    def test_handle_webtransport_stream_data_server_race_condition_session_deleted(
        self, server_processor: StreamProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.sessions.get.side_effect = [mock_session_data, None]
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []

    def test_handle_webtransport_stream_data_server_rejects_send_only(
        self, server_processor: StreamProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.streams = {}
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"hello", stream_ended=False)
        with patch(
            "pywebtransport._protocol.stream_processor.get_stream_direction_from_id",
            return_value=StreamDirection.SEND_ONLY,
        ):
            effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []
        assert 4 not in mock_state.streams

    @pytest.mark.parametrize(
        "direction, current_opened, limit",
        [(StreamDirection.BIDIRECTIONAL, 10, 10), (StreamDirection.RECEIVE_ONLY, 10, 10)],
    )
    def test_handle_webtransport_stream_data_server_stream_limit_reached(
        self,
        server_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        direction: StreamDirection,
        current_opened: int,
        limit: int,
    ) -> None:
        mock_state.streams = {}
        if direction == StreamDirection.BIDIRECTIONAL:
            mock_session_data.peer_streams_bidi_opened = current_opened
            mock_session_data.local_max_streams_bidi = limit
        else:
            mock_session_data.peer_streams_uni_opened = current_opened
            mock_session_data.local_max_streams_uni = limit
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        with patch("pywebtransport._protocol.stream_processor.get_stream_direction_from_id", return_value=direction):
            effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []
        assert 8 not in mock_state.streams

    def test_handle_webtransport_stream_data_server_unknown_direction(
        self, server_processor: StreamProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.streams = {}
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        with patch(
            "pywebtransport._protocol.stream_processor.get_stream_direction_from_id", return_value="UNKNOWN_DIRECTION"
        ):
            with pytest.raises(AssertionError, match="Unreachable code: Unhandled stream direction UNKNOWN_DIRECTION"):
                server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert 8 not in mock_state.streams

    def test_handle_webtransport_stream_data_server_unknown_session(
        self, server_processor: StreamProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions.get.return_value = None
        mock_state.early_event_count = 0
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []

    def test_handle_webtransport_stream_data_server_wrong_session_state(
        self, server_processor: StreamProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.streams = {}
        mock_session_data.state = SessionState.CONNECTING
        event = WebTransportStreamDataReceived(stream_id=8, session_id=0, data=b"hello", stream_ended=False)

        effects = server_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert effects == []
        assert 8 not in mock_state.streams

    def test_handle_webtransport_stream_data_stream_buffer_overflow(
        self,
        client_processor: StreamProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateData,
        mock_client_config: MagicMock,
    ) -> None:
        mock_client_config.max_stream_read_buffer = 10
        mock_stream_data.read_buffer_size = 8
        event = WebTransportStreamDataReceived(stream_id=4, session_id=0, data=b"overflow", stream_ended=False)

        effects = client_processor.handle_webtransport_stream_data(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], StopQuicStream)
        assert effects[0].stream_id == 4
        assert effects[0].error_code == ErrorCodes.WT_FLOW_CONTROL_ERROR
        assert mock_stream_data.read_buffer_size == 8

    def test_read_from_buffer_aggregation(
        self, client_processor: StreamProcessor, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"a", b"b", b"c"])
        mock_stream_data.read_buffer_size = 3

        data = client_processor._read_from_buffer(stream_data=mock_stream_data, max_bytes=2)

        assert data == b"ab"
        assert list(mock_stream_data.read_buffer) == [b"c"]
        assert mock_stream_data.read_buffer_size == 1

    def test_read_from_buffer_empty(self, client_processor: StreamProcessor, mock_stream_data: StreamStateData) -> None:
        mock_stream_data.read_buffer = deque()
        mock_stream_data.read_buffer_size = 0

        data = client_processor._read_from_buffer(stream_data=mock_stream_data, max_bytes=100)

        assert data == b""

    def test_read_from_buffer_exact_chunk_match(
        self, client_processor: StreamProcessor, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"exact", b"rest"])
        mock_stream_data.read_buffer_size = 9

        data = client_processor._read_from_buffer(stream_data=mock_stream_data, max_bytes=5)

        assert data == b"exact"
        assert list(mock_stream_data.read_buffer) == [b"rest"]
        assert mock_stream_data.read_buffer_size == 4

    def test_read_from_buffer_large_chunk_split(
        self, client_processor: StreamProcessor, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"large_chunk"])
        mock_stream_data.read_buffer_size = 11

        with patch("pywebtransport._protocol.stream_processor.OPTIMIZED_READ_SLICE_THRESHOLD", new=5):
            data = client_processor._read_from_buffer(stream_data=mock_stream_data, max_bytes=2)

        assert data == b"la"
        assert list(mock_stream_data.read_buffer) == [b"rge_chunk"]
        assert mock_stream_data.read_buffer_size == 9

    def test_read_from_buffer_optimized_head(
        self, client_processor: StreamProcessor, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"hello", b"world"])
        mock_stream_data.read_buffer_size = 10

        data = client_processor._read_from_buffer(stream_data=mock_stream_data, max_bytes=5)

        assert data == b"hello"
        assert list(mock_stream_data.read_buffer) == [b"world"]
        assert mock_stream_data.read_buffer_size == 5

    def test_read_from_buffer_partial_small(
        self, client_processor: StreamProcessor, mock_stream_data: StreamStateData
    ) -> None:
        mock_stream_data.read_buffer = deque([b"hello"])
        mock_stream_data.read_buffer_size = 5

        data = client_processor._read_from_buffer(stream_data=mock_stream_data, max_bytes=2)

        assert data == b"he"
        assert list(mock_stream_data.read_buffer) == [b"llo"]
        assert mock_stream_data.read_buffer_size == 3

    def test_read_from_buffer_split(self, client_processor: StreamProcessor, mock_stream_data: StreamStateData) -> None:
        mock_stream_data.read_buffer = deque([b"hello"])
        mock_stream_data.read_buffer_size = 5

        data = client_processor._read_from_buffer(stream_data=mock_stream_data, max_bytes=2)

        assert data == b"he"
        assert list(mock_stream_data.read_buffer) == [b"llo"]
        assert mock_stream_data.read_buffer_size == 3
