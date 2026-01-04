"""Unit tests for the pywebtransport._protocol.session_processor module."""

from collections import deque
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from aioquic._buffer import BufferReadError
from pytest_mock import MockerFixture

from pywebtransport import ClientConfig, ErrorCodes, ProtocolError, ServerConfig, SessionError, constants, types
from pywebtransport._protocol.events import (
    CapsuleReceived,
    CloseQuicConnection,
    ConnectStreamClosed,
    CreateQuicStream,
    DatagramReceived,
    EmitSessionEvent,
    EmitStreamEvent,
    NotifyRequestDone,
    NotifyRequestFailed,
    ResetQuicStream,
    SendH3Capsule,
    SendH3Datagram,
    SendH3Headers,
    SendQuicData,
    StopQuicStream,
    UserAcceptSession,
    UserCloseSession,
    UserCreateStream,
    UserGetSessionDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserRejectSession,
    UserSendDatagram,
)
from pywebtransport._protocol.session_processor import SessionProcessor
from pywebtransport._protocol.session_processor import logger as module_logger
from pywebtransport._protocol.state import ProtocolState, SessionStateData
from pywebtransport._protocol.state import StreamStateData as StreamStateDataInternal
from pywebtransport.exceptions import FlowControlError


class TestSessionProcessor:

    @pytest.fixture
    def client_processor(self, mock_config: MagicMock) -> SessionProcessor:
        return SessionProcessor(is_client=True, config=mock_config)

    @pytest.fixture
    def mock_buffer_instance(self, mocker: MockerFixture) -> MagicMock:
        mock_cls = mocker.patch("pywebtransport._protocol.session_processor.QuicBuffer", autospec=True)
        instance = mock_cls.return_value
        instance.tell.return_value = 0
        return cast(MagicMock, instance)

    @pytest.fixture
    def mock_config(self, mocker: MockerFixture) -> MagicMock:
        config = mocker.create_autospec(ClientConfig, instance=True)
        config.max_total_pending_events = 100
        config.max_pending_events_per_session = 10
        config.flow_control_window_auto_scale = True
        config.flow_control_window_size = 1000
        config.initial_max_streams_uni = 10
        config.initial_max_streams_bidi = 10
        return config

    @pytest.fixture
    def mock_get_timestamp(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("pywebtransport._protocol.session_processor.get_timestamp", return_value=123456.0)

    @pytest.fixture
    def mock_server_config(self, mocker: MockerFixture) -> MagicMock:
        config = mocker.create_autospec(ServerConfig, instance=True)
        config.max_total_pending_events = 100
        config.max_pending_events_per_session = 10
        config.flow_control_window_auto_scale = True
        return config

    @pytest.fixture
    def mock_session_data(self, mocker: MockerFixture, mock_state: MagicMock) -> SessionStateData:
        session = mocker.create_autospec(SessionStateData, instance=True)
        session.session_id = 1
        session.state = types.SessionState.CONNECTED
        session.peer_max_data = 1000
        session.local_data_sent = 0
        session.local_data_consumed = 0
        session.local_max_data = 1000
        session.peer_max_streams_bidi = 10
        session.local_streams_bidi_opened = 0
        session.local_max_streams_bidi = 10
        session.peer_max_streams_uni = 10
        session.local_streams_uni_opened = 0
        session.local_max_streams_uni = 10
        session.peer_streams_bidi_closed = 0
        session.peer_streams_uni_closed = 0
        session.pending_bidi_stream_requests = deque()
        session.pending_uni_stream_requests = deque()
        session.datagrams_received = 0
        session.datagram_bytes_received = 0
        session.datagrams_sent = 0
        session.datagram_bytes_sent = 0
        session.active_streams = set()
        session.blocked_streams = set()
        mock_state.sessions = {1: session}
        return session

    @pytest.fixture
    def mock_state(self, mocker: MockerFixture) -> MagicMock:
        state = mocker.create_autospec(ProtocolState, instance=True)
        state.sessions = {}
        state.streams = {}
        state.stream_to_session_map = {}
        state.early_event_buffer = {}
        state.early_event_count = 0
        state.max_datagram_size = 1200
        state.remote_max_datagram_frame_size = 1200
        return state

    @pytest.fixture
    def mock_stream_data(self, mocker: MockerFixture, mock_state: MagicMock) -> StreamStateDataInternal:
        stream = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream.stream_id = 4
        stream.session_id = 1
        stream.state = types.StreamState.OPEN
        stream.bytes_sent = 0
        stream.write_buffer_size = 0
        stream.pending_read_requests = deque()
        stream.write_buffer = deque()
        mock_state.streams = {4: stream}
        return stream

    @pytest.fixture
    def server_processor(self, mock_server_config: MagicMock) -> SessionProcessor:
        return SessionProcessor(is_client=False, config=mock_server_config)

    def test_check_and_send_data_credit_session_closed(
        self, client_processor: SessionProcessor, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CLOSED

        effect = client_processor._check_and_send_data_credit(session_data=mock_session_data)

        assert effect is None

    def test_check_and_send_stream_credit_session_closed(
        self, client_processor: SessionProcessor, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CLOSED

        effect = client_processor._check_and_send_stream_credit(session_data=mock_session_data, is_unidirectional=True)

        assert effect is None

    def test_drain_session_write_buffers_break_logic(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
    ) -> None:
        mock_session_data.peer_max_data = 10
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams.add(4)
        mock_stream_data.write_buffer = deque([(b"a" * 10, 1, False), (b"b" * 5, 2, False)])

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert mock_session_data.local_data_sent == 10
        assert any(isinstance(e, SendQuicData) and e.data == b"aaaaaaaaaa" for e in effects)
        assert len(mock_stream_data.write_buffer) == 1
        assert mock_stream_data.write_buffer[0][0] == b"bbbbb"

    def test_drain_session_write_buffers_end_stream_half_closed(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.peer_max_data = 100
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams.add(4)
        mock_stream_data.state = types.StreamState.HALF_CLOSED_REMOTE
        mock_stream_data.write_buffer = deque([(b"hello", 1, True)])
        mocker.patch("pywebtransport._protocol.session_processor.is_peer_initiated_stream", return_value=True)
        mocker.patch("pywebtransport._protocol.session_processor.is_unidirectional_stream", return_value=True)
        mock_calc_stream = mocker.patch(
            "pywebtransport._protocol.session_processor.calculate_new_stream_limit", return_value=None
        )

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert mock_stream_data.state == types.StreamState.CLOSED
        assert EmitStreamEvent(stream_id=4, event_type=types.EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects
        assert SendQuicData(stream_id=4, data=b"hello", end_stream=True) in effects
        assert NotifyRequestDone(request_id=1, result=None) in effects
        assert mock_session_data.peer_streams_uni_closed == 1
        mock_calc_stream.assert_called()

    def test_drain_session_write_buffers_end_stream_open(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
    ) -> None:
        mock_session_data.peer_max_data = 100
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams.add(4)
        mock_stream_data.state = types.StreamState.OPEN
        mock_stream_data.write_buffer = deque([(b"hello", 1, True)])

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert mock_stream_data.state == types.StreamState.HALF_CLOSED_LOCAL
        assert SendQuicData(stream_id=4, data=b"hello", end_stream=True) in effects
        assert NotifyRequestDone(request_id=1, result=None) in effects

    def test_drain_session_write_buffers_exact_send_no_end(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
    ) -> None:
        mock_session_data.peer_max_data = 5
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams.add(4)
        mock_stream_data.write_buffer = deque([(b"12345", 1, False)])

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert SendQuicData(stream_id=4, data=b"12345", end_stream=False) in effects
        assert mock_session_data.local_data_sent == 5
        assert len(mock_stream_data.write_buffer) == 0
        assert 4 not in mock_session_data.blocked_streams
        assert NotifyRequestDone(request_id=1, result=None) in effects

    def test_drain_session_write_buffers_fallthrough(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
        mocker: MockerFixture,
    ) -> None:
        mock_stream_data.state = types.StreamState.HALF_CLOSED_LOCAL
        mock_stream_data.write_buffer = deque([(b"data", 1, True)])
        mock_session_data.peer_max_data = 1000
        mock_session_data.blocked_streams.add(4)
        mocker.patch("pywebtransport._protocol.session_processor.is_peer_initiated_stream", return_value=False)
        mock_debug = mocker.patch.object(module_logger, "debug")

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert any("send side closed (from buffer drain)" in call.args[0] for call in mock_debug.call_args_list)
        assert SendQuicData(stream_id=4, data=b"data", end_stream=True) in effects

    def test_drain_session_write_buffers_fin_on_open(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.peer_max_data = 1000
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams.add(4)
        mock_stream_data.state = types.StreamState.OPEN
        mock_stream_data.write_buffer = deque([(b"data", 1, True)])
        mocker.patch("pywebtransport._protocol.session_processor.is_peer_initiated_stream", return_value=False)

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert mock_stream_data.state == types.StreamState.HALF_CLOSED_LOCAL
        assert any(isinstance(e, SendQuicData) and e.end_stream is True for e in effects)

    def test_drain_session_write_buffers_logging(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.peer_max_data = 100
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams.add(4)
        mock_stream_data.write_buffer = deque([(b"data", 1, False)])
        mock_debug = mocker.patch.object(module_logger, "debug")

        client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert mock_debug.call_count >= 1
        assert any("Draining write buffer for stream" in call.args[0] for call in mock_debug.call_args_list)

    def test_drain_session_write_buffers_malformed_buffer(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.blocked_streams.add(4)
        mock_deque = mocker.MagicMock(spec=deque)
        mock_deque.__len__.return_value = 1
        mock_pop = cast(MagicMock, mock_deque.popleft)
        mock_pop.side_effect = IndexError
        mock_stream_data.write_buffer = mock_deque

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert effects == []

    def test_drain_session_write_buffers_multi_stream_break(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.peer_max_data = 10
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams = {1, 2}
        s1 = mocker.create_autospec(StreamStateDataInternal, instance=True)
        s1.stream_id = 1
        s1.session_id = 1
        s1.state = types.StreamState.OPEN
        s1.write_buffer = deque([(b"a" * 10, 101, False)])
        s2 = mocker.create_autospec(StreamStateDataInternal, instance=True)
        s2.stream_id = 2
        s2.session_id = 1
        s2.state = types.StreamState.OPEN
        s2.write_buffer = deque([(b"b" * 5, 102, False)])
        mock_state.streams = {1: s1, 2: s2}

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert mock_session_data.local_data_sent == 10
        send_events = [e for e in effects if isinstance(e, SendQuicData)]
        assert len(send_events) == 1
        s1_empty = len(s1.write_buffer) == 0
        s2_empty = len(s2.write_buffer) == 0
        assert s1_empty != s2_empty

    def test_drain_session_write_buffers_no_credit(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.peer_max_data = 100
        mock_session_data.local_data_sent = 100

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert effects == []

    def test_drain_session_write_buffers_no_session(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert effects == []

    def test_drain_session_write_buffers_no_streams(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.streams = {}
        mock_session_data.blocked_streams.add(4)

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert effects == []
        assert 4 not in mock_session_data.blocked_streams

    def test_drain_session_write_buffers_partial_send(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
    ) -> None:
        mock_session_data.peer_max_data = 10
        mock_session_data.local_data_sent = 5
        mock_session_data.blocked_streams.add(4)
        available_credit = 5
        mock_stream_data.write_buffer = deque([(b"long data", 1, False)])

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert SendQuicData(stream_id=4, data=b"long ", end_stream=False) in effects
        assert mock_session_data.local_data_sent == 10
        assert mock_stream_data.bytes_sent == available_credit
        assert len(mock_stream_data.write_buffer) == 1
        assert mock_stream_data.write_buffer[0][0] == b"data"
        assert not any(isinstance(e, NotifyRequestDone) for e in effects)

    @pytest.mark.parametrize(
        "initial_stream_state, end_stream, expected_final_state, expect_event",
        [
            (types.StreamState.OPEN, True, types.StreamState.HALF_CLOSED_LOCAL, False),
            (types.StreamState.HALF_CLOSED_REMOTE, True, types.StreamState.CLOSED, True),
            (types.StreamState.OPEN, False, types.StreamState.OPEN, False),
        ],
    )
    def test_drain_session_write_buffers_scenarios(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
        mocker: MockerFixture,
        initial_stream_state: types.StreamState,
        end_stream: bool,
        expected_final_state: types.StreamState,
        expect_event: bool,
    ) -> None:
        mock_session_data.peer_max_data = 100
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams.add(4)
        mock_stream_data.state = initial_stream_state
        mock_stream_data.write_buffer = deque([(b"hello", 1, end_stream)])
        mocker.patch("pywebtransport._protocol.session_processor.is_peer_initiated_stream", return_value=False)

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert mock_stream_data.state == expected_final_state
        assert SendQuicData(stream_id=4, data=b"hello", end_stream=end_stream) in effects
        assert NotifyRequestDone(request_id=1, result=None) in effects
        event_present = (
            EmitStreamEvent(stream_id=4, event_type=types.EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects
        )
        assert event_present == expect_event

    def test_drain_session_write_buffers_stream_skipped(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.peer_max_data = 2000
        mock_session_data.local_data_sent = 0
        mock_session_data.blocked_streams = {1, 2, 3}
        s2 = mocker.create_autospec(StreamStateDataInternal, instance=True)
        s2.state = types.StreamState.CLOSED
        s2.write_buffer = deque([(b"data", 2, False)])
        s3 = mocker.create_autospec(StreamStateDataInternal, instance=True)
        s3.state = types.StreamState.OPEN
        s3.write_buffer = deque()
        mock_state.streams = {2: s2, 3: s3}

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert 1 not in mock_session_data.blocked_streams
        assert 2 not in mock_session_data.blocked_streams
        assert 3 not in mock_session_data.blocked_streams
        assert effects == []

    def test_drain_session_write_buffers_value_error(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.blocked_streams.add(4)
        mock_deque = mocker.MagicMock(spec=deque)
        mock_deque.__len__.return_value = 1
        mock_pop = cast(MagicMock, mock_deque.popleft)
        mock_pop.side_effect = ValueError("Malformed buffer item")
        mock_stream_data.write_buffer = mock_deque

        effects = client_processor._drain_session_write_buffers(session_id=1, state=mock_state)

        assert effects == []

    def test_handle_accept_session_client_fails(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        event = UserAcceptSession(request_id=1, session_id=1)

        effects = client_processor.handle_accept_session(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, ProtocolError)

    def test_handle_accept_session_not_found(self, server_processor: SessionProcessor, mock_state: MagicMock) -> None:
        event = UserAcceptSession(request_id=1, session_id=1)
        mock_state.sessions = {}

        effects = server_processor.handle_accept_session(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_accept_session_success(
        self,
        server_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
    ) -> None:
        mock_session_data.state = types.SessionState.CONNECTING
        event = UserAcceptSession(request_id=1, session_id=1)

        effects = server_processor.handle_accept_session(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CONNECTED
        assert mock_session_data.ready_at == 123456.0
        assert effects == [
            SendH3Headers(stream_id=1, status=200, end_stream=False),
            EmitSessionEvent(
                session_id=1, event_type=types.EventType.SESSION_READY, data={"session_id": 1, "ready_at": 123456.0}
            ),
            NotifyRequestDone(request_id=1, result=None),
        ]

    def test_handle_accept_session_wrong_state(
        self, server_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CONNECTED
        event = UserAcceptSession(request_id=1, session_id=1)

        effects = server_processor.handle_accept_session(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)
        assert "not in connecting state" in effects[0].exception.args[0]

    def test_handle_capsule_received_cleanup_error(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_reset = mocker.patch.object(client_processor, "_reset_all_session_streams")
        mock_reset.side_effect = RuntimeError("Double fault")

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.side_effect = ProtocolError("Initial Error")
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(isinstance(e, CloseQuicConnection) for e in effects)

    def test_handle_capsule_received_close_session(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_instance = mock_qb.return_value
            mock_instance.pull_uint32.return_value = 1001
            mock_instance.pull_bytes.return_value = b"Test Close"
            mock_instance.tell.return_value = 4
            event = CapsuleReceived(
                stream_id=1, capsule_type=constants.CLOSE_WEBTRANSPORT_SESSION_TYPE, capsule_data=b"....Test Close"
            )
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert mock_session_data.closed_at == 123456.0
        assert mock_session_data.close_code == 1001
        assert mock_session_data.close_reason == "Test Close"
        assert (
            EmitSessionEvent(
                session_id=1,
                event_type=types.EventType.SESSION_CLOSED,
                data={"session_id": 1, "code": 1001, "reason": "Test Close"},
            )
            in effects
        )

    def test_handle_capsule_received_data_blocked_autoscale_off(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        client_processor._config.flow_control_window_auto_scale = False
        event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_DATA_BLOCKED_TYPE, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert effects == [
            EmitSessionEvent(session_id=1, event_type=types.EventType.SESSION_DATA_BLOCKED, data={"session_id": 1})
        ]

    def test_handle_capsule_received_data_blocked_autoscale_on(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        client_processor._config.flow_control_window_auto_scale = True
        client_processor._config.flow_control_window_size = 5000
        mock_session_data.local_max_data = 1000
        mock_session_data.peer_data_sent = 0
        event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_DATA_BLOCKED_TYPE, capsule_data=b"")
        mocker.patch("pywebtransport._protocol.session_processor.calculate_new_data_limit", return_value=5000)

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.local_max_data == 5000
        assert any(isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_DATA_TYPE for e in effects)

    def test_handle_capsule_received_data_blocked_no_increase(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        client_processor._config.flow_control_window_auto_scale = True
        client_processor._config.flow_control_window_size = 1000
        mock_session_data.local_max_data = 2000
        mock_session_data.peer_data_sent = 0
        event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_DATA_BLOCKED_TYPE, capsule_data=b"")
        mocker.patch("pywebtransport._protocol.session_processor.calculate_new_data_limit", return_value=None)

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert not any(isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_DATA_TYPE for e in effects)

    def test_handle_capsule_received_drain_session(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.state = types.SessionState.CONNECTED
        event = CapsuleReceived(stream_id=1, capsule_type=constants.DRAIN_WEBTRANSPORT_SESSION_TYPE, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.DRAINING
        assert effects == [
            EmitSessionEvent(session_id=1, event_type=types.EventType.SESSION_DRAINING, data={"session_id": 1})
        ]

    def test_handle_capsule_received_drain_session_wrong_state(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.state = types.SessionState.DRAINING
        event = CapsuleReceived(stream_id=1, capsule_type=constants.DRAIN_WEBTRANSPORT_SESSION_TYPE, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert effects == []

    def test_handle_capsule_received_exception_already_closed(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        real_session_data = SessionStateData(
            session_id=1,
            state=types.SessionState.CONNECTED,
            peer_max_data=1000,
            local_data_sent=0,
            local_max_data=1000,
            peer_max_streams_bidi=10,
            local_streams_bidi_opened=0,
            local_max_streams_bidi=10,
            peer_max_streams_uni=10,
            local_streams_uni_opened=0,
            local_max_streams_uni=10,
            pending_bidi_stream_requests=deque(),
            pending_uni_stream_requests=deque(),
            datagrams_received=0,
            datagram_bytes_received=0,
            datagrams_sent=0,
            datagram_bytes_sent=0,
            active_streams=set(),
            blocked_streams=set(),
            ready_at=None,
            closed_at=None,
            close_code=0,
            close_reason="",
            peer_data_sent=0,
            peer_streams_bidi_opened=0,
            peer_streams_uni_opened=0,
            peer_streams_bidi_closed=0,
            peer_streams_uni_closed=0,
            local_data_consumed=0,
            path="/",
            headers={},
            created_at=123456.0,
        )
        mock_state.sessions = {1: real_session_data}

        def side_effect(*args: Any, **kwargs: Any) -> int:
            mock_state.sessions[1].state = types.SessionState.CLOSED
            raise BufferReadError("Trigger")

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.side_effect = side_effect
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert any(isinstance(e, CloseQuicConnection) for e in effects)

    @pytest.mark.parametrize("capsule_type", [constants.WT_MAX_STREAM_DATA_TYPE, constants.WT_STREAM_DATA_BLOCKED_TYPE])
    def test_handle_capsule_received_forbidden_capsule(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        capsule_type: int,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        event = CapsuleReceived(stream_id=1, capsule_type=capsule_type, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(isinstance(e, ResetQuicStream) and e.error_code == ErrorCodes.H3_FRAME_UNEXPECTED for e in effects)
        assert any(isinstance(e, EmitSessionEvent) and e.event_type == types.EventType.SESSION_CLOSED for e in effects)

    def test_handle_capsule_received_max_data_decrease_fails(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_data = 1000
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 500
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(isinstance(e, ResetQuicStream) and e.error_code == ErrorCodes.WT_FLOW_CONTROL_ERROR for e in effects)

    def test_handle_capsule_received_max_data_same_value(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_data = 1000

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 1000
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert not any(isinstance(e, (EmitSessionEvent, ResetQuicStream)) for e in effects)
        assert mock_session_data.peer_max_data == 1000

    def test_handle_capsule_received_max_data_success_and_drain(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_stream_data: StreamStateDataInternal,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_data = 1000
        mock_session_data.local_data_sent = 1000
        mock_session_data.blocked_streams.add(4)
        mock_stream_data.write_buffer = deque([(b"hello", 1, True)])

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 2000
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.peer_max_data == 2000
        assert (
            EmitSessionEvent(
                session_id=1,
                event_type=types.EventType.SESSION_MAX_DATA_UPDATED,
                data={"session_id": 1, "max_data": 2000},
            )
            in effects
        )
        assert SendQuicData(stream_id=4, data=b"hello", end_stream=True) in effects

    def test_handle_capsule_received_max_streams_bidi_client_pending(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_bidi = 0
        mock_session_data.local_streams_bidi_opened = 0
        mock_session_data.pending_bidi_stream_requests = deque([1])

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_BIDI_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.peer_max_streams_bidi == 5
        assert mock_session_data.local_streams_bidi_opened == 1
        assert not mock_session_data.pending_bidi_stream_requests
        assert CreateQuicStream(session_id=1, is_unidirectional=False, request_id=1) in effects

    def test_handle_capsule_received_max_streams_bidi_decrease_fails(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_bidi = 10
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_BIDI_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(isinstance(e, ResetQuicStream) and e.error_code == ErrorCodes.WT_FLOW_CONTROL_ERROR for e in effects)

    def test_handle_capsule_received_max_streams_bidi_same_value(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_bidi = 10

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 10
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_BIDI_TYPE, capsule_data=b"")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert not any(isinstance(e, (EmitSessionEvent, ResetQuicStream)) for e in effects)
        assert mock_session_data.peer_max_streams_bidi == 10

    @pytest.mark.parametrize(
        "capsule_type, field_limit",
        [
            (constants.WT_MAX_STREAMS_BIDI_TYPE, "peer_max_streams_bidi"),
            (constants.WT_MAX_STREAMS_UNI_TYPE, "peer_max_streams_uni"),
        ],
    )
    def test_handle_capsule_received_max_streams_decrease_fails(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        capsule_type: int,
        field_limit: str,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        setattr(mock_session_data, field_limit, 10)
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=capsule_type, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(isinstance(e, ResetQuicStream) and e.error_code == ErrorCodes.WT_FLOW_CONTROL_ERROR for e in effects)

    @pytest.mark.parametrize("capsule_type", [constants.WT_MAX_STREAMS_BIDI_TYPE, constants.WT_MAX_STREAMS_UNI_TYPE])
    def test_handle_capsule_received_max_streams_protocol_limit(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        capsule_type: int,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = constants.MAX_PROTOCOL_STREAMS_LIMIT + 1
            event = CapsuleReceived(stream_id=1, capsule_type=capsule_type, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert any(
            isinstance(e, CloseQuicConnection) and e.error_code == ErrorCodes.FRAME_ENCODING_ERROR for e in effects
        )

    def test_handle_capsule_received_max_streams_server_no_pending(
        self,
        server_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_bidi = 0
        mock_session_data.pending_bidi_stream_requests = deque([1])
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_BIDI_TYPE, capsule_data=b"...")
            effects = server_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.peer_max_streams_bidi == 5
        assert len(mock_session_data.pending_bidi_stream_requests) == 1
        assert not any(isinstance(e, CreateQuicStream) for e in effects)

    def test_handle_capsule_received_max_streams_uni_client_no_pending(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_uni = 0
        mock_session_data.local_streams_uni_opened = 0
        mock_session_data.pending_uni_stream_requests = deque()
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_UNI_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.peer_max_streams_uni == 5
        assert mock_session_data.local_streams_uni_opened == 0
        assert not any(isinstance(e, CreateQuicStream) for e in effects)

    def test_handle_capsule_received_max_streams_uni_client_pending(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_uni = 0
        mock_session_data.local_streams_uni_opened = 0
        mock_session_data.pending_uni_stream_requests = deque([1])

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_UNI_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.peer_max_streams_uni == 5
        assert mock_session_data.local_streams_uni_opened == 1
        assert CreateQuicStream(session_id=1, is_unidirectional=True, request_id=1) in effects

    def test_handle_capsule_received_max_streams_uni_decrease_fails(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_uni = 10
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_UNI_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(isinstance(e, ResetQuicStream) and e.error_code == ErrorCodes.WT_FLOW_CONTROL_ERROR for e in effects)

    def test_handle_capsule_received_max_streams_uni_protocol_limit(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = constants.MAX_PROTOCOL_STREAMS_LIMIT + 1
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_UNI_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert any(
            isinstance(e, CloseQuicConnection) and e.error_code == ErrorCodes.FRAME_ENCODING_ERROR for e in effects
        )

    def test_handle_capsule_received_max_streams_uni_same_value(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_uni = 10

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 10
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_UNI_TYPE, capsule_data=b"")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert not any(isinstance(e, (EmitSessionEvent, ResetQuicStream)) for e in effects)
        assert mock_session_data.peer_max_streams_uni == 10

    def test_handle_capsule_received_max_streams_uni_server(
        self, server_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_uni = 0
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_UNI_TYPE, capsule_data=b"")
            effects = server_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.peer_max_streams_uni == 5
        assert any(
            isinstance(e, EmitSessionEvent) and e.event_type == types.EventType.SESSION_MAX_STREAMS_UNI_UPDATED
            for e in effects
        )

    def test_handle_capsule_received_max_streams_uni_server_explicit(
        self, server_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.peer_max_streams_uni = 0

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = 5
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_STREAMS_UNI_TYPE, capsule_data=b"")
            effects = server_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.peer_max_streams_uni == 5
        assert any(
            e.event_type == types.EventType.SESSION_MAX_STREAMS_UNI_UPDATED
            for e in effects
            if isinstance(e, EmitSessionEvent)
        )
        assert not any(isinstance(e, CreateQuicStream) for e in effects)

    @pytest.mark.parametrize(
        "capsule_type, field_limit, field_pending, field_opened, limit_val, protocol_max, expect_create",
        [
            (
                constants.WT_MAX_STREAMS_BIDI_TYPE,
                "peer_max_streams_bidi",
                "pending_bidi_stream_requests",
                "local_streams_bidi_opened",
                5,
                constants.MAX_PROTOCOL_STREAMS_LIMIT,
                True,
            ),
            (
                constants.WT_MAX_STREAMS_UNI_TYPE,
                "peer_max_streams_uni",
                "pending_uni_stream_requests",
                "local_streams_uni_opened",
                5,
                constants.MAX_PROTOCOL_STREAMS_LIMIT,
                True,
            ),
        ],
    )
    def test_handle_capsule_received_max_streams_updates(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        capsule_type: int,
        field_limit: str,
        field_pending: str,
        field_opened: str,
        limit_val: int,
        protocol_max: int,
        expect_create: bool,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        setattr(mock_session_data, field_limit, 0)
        setattr(mock_session_data, field_opened, 0)
        getattr(mock_session_data, field_pending).append(1)

        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.return_value = limit_val
            event = CapsuleReceived(stream_id=1, capsule_type=capsule_type, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert getattr(mock_session_data, field_limit) == limit_val
        if expect_create:
            assert getattr(mock_session_data, field_opened) == 1
            assert len(getattr(mock_session_data, field_pending)) == 0
            assert any(isinstance(e, CreateQuicStream) for e in effects)

    def test_handle_capsule_received_parsing_errors_close_session(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint32.side_effect = BufferReadError("test")
            event = CapsuleReceived(
                stream_id=1, capsule_type=constants.CLOSE_WEBTRANSPORT_SESSION_TYPE, capsule_data=b"malformed"
            )
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(isinstance(e, CloseQuicConnection) for e in effects)

    @pytest.mark.parametrize(
        "capsule_type",
        [constants.WT_MAX_DATA_TYPE, constants.WT_MAX_STREAMS_BIDI_TYPE, constants.WT_MAX_STREAMS_UNI_TYPE],
    )
    def test_handle_capsule_received_parsing_errors_pull_var(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        capsule_type: int,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.return_value.pull_uint_var.side_effect = BufferReadError("test")
            event = CapsuleReceived(stream_id=1, capsule_type=capsule_type, capsule_data=b"malformed")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(isinstance(e, CloseQuicConnection) for e in effects)

    def test_handle_capsule_received_protocol_error(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        with patch("pywebtransport._protocol.session_processor.QuicBuffer") as mock_qb:
            mock_qb.side_effect = ProtocolError("Protocol Violation")
            event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"...")
            effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert any(
            isinstance(e, CloseQuicConnection) and e.error_code == ErrorCodes.PROTOCOL_VIOLATION for e in effects
        )

    def test_handle_capsule_received_session_closed_raises(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.state = types.SessionState.CLOSED
        event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"...")

        with pytest.raises(ProtocolError) as exc_info:
            client_processor.handle_capsule_received(event=event, state=mock_state)

        assert exc_info.value.error_code == ErrorCodes.H3_MESSAGE_ERROR
        assert "Data received on closed session" in str(exc_info.value)

    def test_handle_capsule_received_session_missing(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}
        mock_state.stream_to_session_map = {}
        event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert effects == []

    @pytest.mark.parametrize(
        "capsule_type, is_uni, config_attr, expected_capsule_type",
        [
            (
                constants.WT_STREAMS_BLOCKED_BIDI_TYPE,
                False,
                "initial_max_streams_bidi",
                constants.WT_MAX_STREAMS_BIDI_TYPE,
            ),
            (constants.WT_STREAMS_BLOCKED_UNI_TYPE, True, "initial_max_streams_uni", constants.WT_MAX_STREAMS_UNI_TYPE),
        ],
    )
    def test_handle_capsule_received_streams_blocked_autoscale_off(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        capsule_type: int,
        is_uni: bool,
        config_attr: str,
        expected_capsule_type: int,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        client_processor._config.flow_control_window_auto_scale = False
        event = CapsuleReceived(stream_id=1, capsule_type=capsule_type, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert effects == [
            EmitSessionEvent(
                session_id=1,
                event_type=types.EventType.SESSION_STREAMS_BLOCKED,
                data={"session_id": 1, "is_unidirectional": is_uni},
            )
        ]

    @pytest.mark.parametrize(
        (
            "capsule_type, is_uni, config_attr, expected_capsule_type, "
            "current_limit, peer_opened, target_initial, should_send"
        ),
        [
            (
                constants.WT_STREAMS_BLOCKED_BIDI_TYPE,
                False,
                "initial_max_streams_bidi",
                constants.WT_MAX_STREAMS_BIDI_TYPE,
                10,
                10,
                15,
                True,
            ),
            (
                constants.WT_STREAMS_BLOCKED_UNI_TYPE,
                True,
                "initial_max_streams_uni",
                constants.WT_MAX_STREAMS_UNI_TYPE,
                10,
                10,
                15,
                True,
            ),
            (
                constants.WT_STREAMS_BLOCKED_BIDI_TYPE,
                False,
                "initial_max_streams_bidi",
                constants.WT_MAX_STREAMS_BIDI_TYPE,
                20,
                10,
                15,
                False,
            ),
            (
                constants.WT_STREAMS_BLOCKED_UNI_TYPE,
                True,
                "initial_max_streams_uni",
                constants.WT_MAX_STREAMS_UNI_TYPE,
                20,
                10,
                15,
                False,
            ),
        ],
    )
    def test_handle_capsule_received_streams_blocked_autoscale_on(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
        capsule_type: int,
        is_uni: bool,
        config_attr: str,
        expected_capsule_type: int,
        current_limit: int,
        peer_opened: int,
        target_initial: int,
        should_send: bool,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        client_processor._config.flow_control_window_auto_scale = True
        setattr(client_processor._config, config_attr, target_initial)

        if is_uni:
            mock_session_data.local_max_streams_uni = current_limit
            mock_session_data.peer_streams_uni_opened = peer_opened
        else:
            mock_session_data.local_max_streams_bidi = current_limit
            mock_session_data.peer_streams_bidi_opened = peer_opened

        calculated_limit = target_initial

        if should_send:
            mocker.patch(
                "pywebtransport._protocol.session_processor.calculate_new_stream_limit", return_value=calculated_limit
            )
        else:
            mocker.patch("pywebtransport._protocol.session_processor.calculate_new_stream_limit", return_value=None)

        event = CapsuleReceived(stream_id=1, capsule_type=capsule_type, capsule_data=b"")
        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        if should_send:
            expected_new_limit = calculated_limit
            if is_uni:
                assert mock_session_data.local_max_streams_uni == expected_new_limit
            else:
                assert mock_session_data.local_max_streams_bidi == expected_new_limit
            assert any(isinstance(e, SendH3Capsule) and e.capsule_type == expected_capsule_type for e in effects)
        else:
            if is_uni:
                assert mock_session_data.local_max_streams_uni == current_limit
            else:
                assert mock_session_data.local_max_streams_bidi == current_limit
            assert not any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_capsule_received_streams_blocked_autoscale_on_no_increase_bidi(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        client_processor._config.flow_control_window_auto_scale = True
        client_processor._config.initial_max_streams_bidi = 20
        mock_session_data.local_max_streams_bidi = 20
        mock_session_data.peer_streams_bidi_opened = 10
        event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_STREAMS_BLOCKED_BIDI_TYPE, capsule_data=b"")

        mocker.patch("pywebtransport._protocol.session_processor.calculate_new_stream_limit", return_value=None)

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.local_max_streams_bidi == 20
        assert not any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_capsule_received_streams_blocked_autoscale_on_no_increase_uni(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        client_processor._config.flow_control_window_auto_scale = True
        client_processor._config.initial_max_streams_uni = 20
        mock_session_data.local_max_streams_uni = 20
        mock_session_data.peer_streams_uni_opened = 10
        event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_STREAMS_BLOCKED_UNI_TYPE, capsule_data=b"")

        mocker.patch("pywebtransport._protocol.session_processor.calculate_new_stream_limit", return_value=None)

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert mock_session_data.local_max_streams_uni == 20
        assert not any(isinstance(e, SendH3Capsule) for e in effects)

    def test_handle_capsule_received_streams_blocked_uni(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        client_processor._config.flow_control_window_auto_scale = True
        client_processor._config.initial_max_streams_uni = 20
        mock_session_data.local_max_streams_uni = 10

        mock_calc = mocker.patch(
            "pywebtransport._protocol.session_processor.calculate_new_stream_limit", return_value=20
        )
        event = CapsuleReceived(stream_id=1, capsule_type=constants.WT_STREAMS_BLOCKED_UNI_TYPE, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        mock_calc.assert_called_with(
            current_limit=10, closed_count=0, initial_window=20, auto_scale=True, force_update=True
        )
        assert mock_session_data.local_max_streams_uni == 20
        assert any(
            isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_STREAMS_UNI_TYPE for e in effects
        )

    def test_handle_capsule_received_unknown_capsule(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        event = CapsuleReceived(stream_id=1, capsule_type=0xFEEDBEEF, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert effects == []

    def test_handle_capsule_received_unknown_session(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}
        mock_state.stream_to_session_map = {}
        event = CapsuleReceived(stream_id=99, capsule_type=constants.WT_MAX_DATA_TYPE, capsule_data=b"")

        effects = client_processor.handle_capsule_received(event=event, state=mock_state)

        assert effects == []

    def test_handle_close_session_already_closed(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CLOSED
        event = UserCloseSession(request_id=1, session_id=1, error_code=1, reason="Test")

        effects = client_processor.handle_close_session(event=event, state=mock_state)

        assert effects == [NotifyRequestDone(request_id=1, result=None)]

    def test_handle_close_session_not_found(self, client_processor: SessionProcessor, mock_state: MagicMock) -> None:
        mock_state.sessions = {}
        event = UserCloseSession(request_id=1, session_id=1, error_code=1, reason="Test")

        effects = client_processor.handle_close_session(event=event, state=mock_state)

        assert effects == [NotifyRequestDone(request_id=1, result=None)]

    def test_handle_close_session_reason_truncation(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        long_reason = "a" * (constants.MAX_CLOSE_REASON_BYTES + 10)
        truncated_reason_bytes = ("a" * constants.MAX_CLOSE_REASON_BYTES).encode("utf-8")
        event = UserCloseSession(request_id=1, session_id=1, error_code=1, reason=long_reason)

        effects = client_processor.handle_close_session(event=event, state=mock_state)

        sent_capsule = next(
            e
            for e in effects
            if isinstance(e, SendH3Capsule) and e.capsule_type == constants.CLOSE_WEBTRANSPORT_SESSION_TYPE
        )
        assert sent_capsule.capsule_data[4:] == truncated_reason_bytes

    def test_handle_close_session_success(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
    ) -> None:
        event = UserCloseSession(request_id=1, session_id=1, error_code=1001, reason="Test")

        effects = client_processor.handle_close_session(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert mock_session_data.closed_at == 123456.0
        assert mock_session_data.close_code == 1001
        assert mock_session_data.close_reason == "Test"
        assert (
            EmitSessionEvent(
                session_id=1,
                event_type=types.EventType.SESSION_CLOSED,
                data={"session_id": 1, "code": 1001, "reason": "Test"},
            )
            in effects
        )
        assert any(
            isinstance(e, SendH3Capsule)
            and e.capsule_type == constants.CLOSE_WEBTRANSPORT_SESSION_TYPE
            and e.end_stream is True
            for e in effects
        )
        assert not any(isinstance(e, SendQuicData) for e in effects)
        assert NotifyRequestDone(request_id=1, result=None) in effects

    def test_handle_connect_stream_closed_already_closed(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.state = types.SessionState.CLOSED
        event = ConnectStreamClosed(stream_id=1)

        effects = client_processor.handle_connect_stream_closed(event=event, state=mock_state)

        assert effects == []

    def test_handle_connect_stream_closed_clean(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        event = ConnectStreamClosed(stream_id=1)

        effects = client_processor.handle_connect_stream_closed(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert mock_session_data.close_code == ErrorCodes.NO_ERROR
        assert any(isinstance(e, ResetQuicStream) for e in effects)

    def test_handle_connect_stream_closed_not_found(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}
        mock_state.stream_to_session_map = {}
        event = ConnectStreamClosed(stream_id=0)

        effects = client_processor.handle_connect_stream_closed(event=event, state=mock_state)

        assert effects == []

    def test_handle_connect_stream_closed_success(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        event = ConnectStreamClosed(stream_id=1)

        effects = client_processor.handle_connect_stream_closed(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert mock_session_data.close_reason == "CONNECT stream cleanly closed"
        assert any(isinstance(e, ResetQuicStream) and e.error_code == ErrorCodes.NO_ERROR for e in effects)

    @pytest.mark.parametrize(
        "is_uni, field_pending, field_opened, field_max, capsule_type",
        [
            (
                True,
                "pending_uni_stream_requests",
                "local_streams_uni_opened",
                "peer_max_streams_uni",
                constants.WT_STREAMS_BLOCKED_UNI_TYPE,
            ),
            (
                False,
                "pending_bidi_stream_requests",
                "local_streams_bidi_opened",
                "peer_max_streams_bidi",
                constants.WT_STREAMS_BLOCKED_BIDI_TYPE,
            ),
        ],
    )
    def test_handle_create_stream_flow_control_client_blocked(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        is_uni: bool,
        field_pending: str,
        field_opened: str,
        field_max: str,
        capsule_type: int,
    ) -> None:
        setattr(mock_session_data, field_max, 0)
        setattr(mock_session_data, field_opened, 0)
        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=is_uni)

        effects = client_processor.handle_create_stream(event=event, state=mock_state)

        assert len(getattr(mock_session_data, field_pending)) == 1
        assert any(isinstance(e, SendH3Capsule) and e.capsule_type == capsule_type for e in effects)

    def test_handle_create_stream_flow_control_mixed(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.peer_max_streams_uni = 0
        mock_session_data.local_streams_uni_opened = 0
        mock_session_data.peer_max_streams_bidi = 10
        mock_session_data.local_streams_bidi_opened = 0

        event_uni = UserCreateStream(request_id=1, session_id=1, is_unidirectional=True)
        effects_uni = client_processor.handle_create_stream(event=event_uni, state=mock_state)
        assert len(effects_uni) == 1
        assert isinstance(effects_uni[0], SendH3Capsule)
        assert len(mock_session_data.pending_uni_stream_requests) == 1

        event_bidi = UserCreateStream(request_id=2, session_id=1, is_unidirectional=False)
        effects_bidi = client_processor.handle_create_stream(event=event_bidi, state=mock_state)
        assert any(isinstance(e, CreateQuicStream) for e in effects_bidi)

    @pytest.mark.parametrize("is_uni", [True, False])
    def test_handle_create_stream_flow_control_server_fails(
        self,
        server_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        is_uni: bool,
    ) -> None:
        if is_uni:
            mock_session_data.peer_max_streams_uni = 0
            mock_session_data.local_streams_uni_opened = 0
        else:
            mock_session_data.peer_max_streams_bidi = 0
            mock_session_data.local_streams_bidi_opened = 0

        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=is_uni)

        effects = server_processor.handle_create_stream(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert isinstance(fail_effect.exception, FlowControlError)

    def test_handle_create_stream_blocked_by_limit(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.local_streams_uni_opened = 10
        mock_session_data.peer_max_streams_uni = 10
        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=True)

        effects = client_processor.handle_create_stream(event=event, state=mock_state)

        assert len(mock_session_data.pending_uni_stream_requests) == 1
        assert any(
            isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_STREAMS_BLOCKED_UNI_TYPE for e in effects
        )

    def test_handle_create_stream_limit_exceeded_fallthrough(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.peer_max_streams_uni = 0
        mock_session_data.local_streams_uni_opened = 0
        client_processor._is_client = "INVALID"  # type: ignore[assignment]

        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=True)

        effects = client_processor.handle_create_stream(event=event, state=mock_state)

        assert mock_session_data.local_streams_uni_opened == 1
        assert CreateQuicStream(request_id=1, session_id=1, is_unidirectional=True) in effects

    def test_handle_create_stream_server_bidi_limit_failure(
        self, server_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.peer_max_streams_bidi = 0
        mock_session_data.local_max_streams_bidi = 0
        mock_session_data.local_streams_bidi_opened = 0

        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=False)
        effects = server_processor.handle_create_stream(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, FlowControlError)
        assert effects[0].exception.message == "Bidirectional stream limit reached"

    def test_handle_create_stream_server_bidi_success(
        self, server_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.local_streams_bidi_opened = 0
        mock_session_data.local_max_streams_bidi = 10
        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=False)

        effects = server_processor.handle_create_stream(event=event, state=mock_state)

        assert mock_session_data.local_streams_bidi_opened == 1
        assert effects == [CreateQuicStream(request_id=1, session_id=1, is_unidirectional=False)]

    def test_handle_create_stream_server_uni_success(
        self, server_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.local_streams_uni_opened = 0
        mock_session_data.local_max_streams_uni = 10
        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=True)

        effects = server_processor.handle_create_stream(event=event, state=mock_state)

        assert mock_session_data.local_streams_uni_opened == 1
        assert effects == [CreateQuicStream(request_id=1, session_id=1, is_unidirectional=True)]

    def test_handle_create_stream_session_not_found(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}
        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=False)

        effects = client_processor.handle_create_stream(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_create_stream_success(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=False)

        effects = client_processor.handle_create_stream(event=event, state=mock_state)

        assert mock_session_data.local_streams_bidi_opened == 1
        assert effects == [CreateQuicStream(session_id=1, is_unidirectional=False, request_id=event.request_id)]

    def test_handle_create_stream_wrong_state(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CONNECTING
        event = UserCreateStream(request_id=1, session_id=1, is_unidirectional=False)

        effects = client_processor.handle_create_stream(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)
        assert "not connected or draining" in effects[0].exception.args[0]

    def test_handle_datagram_received_active(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        event = DatagramReceived(stream_id=1, data=b"payload")

        effects = client_processor.handle_datagram_received(event=event, state=mock_state)

        assert mock_session_data.datagrams_received == 1
        assert mock_session_data.datagram_bytes_received == 7
        assert (
            EmitSessionEvent(
                session_id=1, event_type=types.EventType.DATAGRAM_RECEIVED, data={"session_id": 1, "data": b"payload"}
            )
            in effects
        )

    def test_handle_datagram_received_active_session(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        event = DatagramReceived(stream_id=1, data=b"hello")

        effects = client_processor.handle_datagram_received(event=event, state=mock_state)

        assert mock_session_data.datagrams_received == 1
        assert mock_session_data.datagram_bytes_received == 5
        assert effects == [
            EmitSessionEvent(
                session_id=1, event_type=types.EventType.DATAGRAM_RECEIVED, data={"session_id": 1, "data": b"hello"}
            )
        ]

    def test_handle_datagram_received_buffer_early(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}
        event = DatagramReceived(stream_id=1, data=b"early")

        effects = client_processor.handle_datagram_received(event=event, state=mock_state)

        assert effects == []
        assert mock_state.early_event_count == 1
        assert len(mock_state.early_event_buffer[1]) == 1

    def test_handle_datagram_received_early_buffering(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_config: MagicMock,
        mock_get_timestamp: MagicMock,
    ) -> None:
        mock_config.max_total_pending_events = 10
        mock_state.sessions = {}
        mock_state.stream_to_session_map = {}
        event = DatagramReceived(stream_id=99, data=b"hello")

        effects = client_processor.handle_datagram_received(event=event, state=mock_state)

        assert effects == []
        assert mock_state.early_event_count == 1
        assert mock_state.early_event_buffer == {99: [(123456.0, event)]}

    def test_handle_datagram_received_global_buffer_full(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_config: MagicMock
    ) -> None:
        mock_config.max_total_pending_events = 10
        mock_state.sessions = {}
        mock_state.stream_to_session_map = {}
        mock_state.early_event_count = 10
        event = DatagramReceived(stream_id=99, data=b"hello")

        effects = client_processor.handle_datagram_received(event=event, state=mock_state)

        assert effects == []
        assert 99 not in mock_state.early_event_buffer

    def test_handle_datagram_received_inactive_session(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.stream_to_session_map[1] = 1
        mock_session_data.state = types.SessionState.CONNECTING
        event = DatagramReceived(stream_id=1, data=b"hello")

        effects = client_processor.handle_datagram_received(event=event, state=mock_state)

        assert effects == []
        assert mock_session_data.datagrams_received == 0

    def test_handle_datagram_received_session_buffer_full(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_config: MagicMock
    ) -> None:
        mock_config.max_total_pending_events = 10
        mock_config.max_pending_events_per_session = 1
        mock_state.sessions = {}
        mock_state.stream_to_session_map = {}
        mock_state.early_event_buffer = {99: ["existing_event"]}
        mock_state.early_event_count = 1
        event = DatagramReceived(stream_id=99, data=b"hello")

        effects = client_processor.handle_datagram_received(event=event, state=mock_state)

        assert effects == []
        assert len(mock_state.early_event_buffer[99]) == 1
        assert mock_state.early_event_count == 1

    def test_handle_datagram_received_session_wrong_state(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mocker: MockerFixture
    ) -> None:
        real_session = SessionStateData(
            session_id=1,
            state=types.SessionState.CLOSED,
            peer_max_data=1000,
            local_data_sent=0,
            local_max_data=1000,
            peer_max_streams_bidi=10,
            local_streams_bidi_opened=0,
            local_max_streams_bidi=10,
            peer_max_streams_uni=10,
            local_streams_uni_opened=0,
            local_max_streams_uni=10,
            pending_bidi_stream_requests=deque(),
            pending_uni_stream_requests=deque(),
            datagrams_received=0,
            datagram_bytes_received=0,
            datagrams_sent=0,
            datagram_bytes_sent=0,
            active_streams=set(),
            blocked_streams=set(),
            ready_at=None,
            closed_at=None,
            close_code=0,
            close_reason="",
            peer_data_sent=0,
            peer_streams_bidi_opened=0,
            peer_streams_uni_opened=0,
            path="/",
            headers={},
            created_at=123456.0,
        )
        mock_state.sessions = {1: real_session}
        mock_state.stream_to_session_map = {1: 1}
        mock_debug = mocker.patch.object(module_logger, "debug")
        event = DatagramReceived(stream_id=1, data=b"hello")

        effects = client_processor.handle_datagram_received(event=event, state=mock_state)

        assert effects == []
        assert mock_debug.call_count >= 1
        assert any("Ignoring datagram for non-active session" in call.args[0] for call in mock_debug.call_args_list)

    def test_handle_get_session_diagnostics(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        event = UserGetSessionDiagnostics(request_id=1, session_id=1)

        with patch("dataclasses.asdict", return_value={"state": "connected"}):
            effects = client_processor.handle_get_session_diagnostics(event=event, state=mock_state)

        assert isinstance(effects[0], NotifyRequestDone)
        assert effects[0].result["state"] == "connected"

    def test_handle_get_session_diagnostics_not_found(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}
        event = UserGetSessionDiagnostics(request_id=1, session_id=1)

        effects = client_processor.handle_get_session_diagnostics(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_get_session_diagnostics_success(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.session_id = 1
        mock_session_data.state = types.SessionState.CONNECTED
        mock_session_data.active_streams = {1, 2}
        mock_session_data.blocked_streams = {3}
        test_dict: dict[str, Any] = {"id": 1, "state": "connected"}

        with patch(
            "pywebtransport._protocol.session_processor.dataclasses.asdict", return_value=test_dict
        ) as mock_asdict:
            event = UserGetSessionDiagnostics(request_id=1, session_id=1)
            effects = client_processor.handle_get_session_diagnostics(event=event, state=mock_state)

            mock_asdict.assert_called_once_with(mock_session_data)

            assert isinstance(effects[0], NotifyRequestDone)
            result_val = effects[0].result
            assert result_val["id"] == 1
            assert set(result_val["active_streams"]) == {1, 2}
            assert set(result_val["blocked_streams"]) == {3}

    def test_handle_grant_data_credit_not_found(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}
        event = UserGrantDataCredit(request_id=1, session_id=1, max_data=1000)

        effects = client_processor.handle_grant_data_credit(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_grant_data_credit_success(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.local_max_data = 100
        event = UserGrantDataCredit(request_id=1, session_id=1, max_data=200)

        effects = client_processor.handle_grant_data_credit(event=event, state=mock_state)

        assert mock_session_data.local_max_data == 200
        assert any(isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_DATA_TYPE for e in effects)

    @pytest.mark.parametrize(
        "grant_kwargs, no_op", [({"max_data": 2000}, False), ({"max_data": 1000}, True), ({"max_data": 500}, True)]
    )
    def test_handle_grant_data_credit_variants(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        grant_kwargs: dict[str, Any],
        no_op: bool,
    ) -> None:
        mock_session_data.local_max_data = 1000
        event = UserGrantDataCredit(request_id=1, session_id=1, **grant_kwargs)

        effects = client_processor.handle_grant_data_credit(event=event, state=mock_state)

        assert NotifyRequestDone(request_id=1, result=None) in effects
        if no_op:
            assert mock_session_data.local_max_data == 1000
            assert not any(isinstance(e, SendH3Capsule) for e in effects)
        else:
            assert mock_session_data.local_max_data == grant_kwargs["max_data"]
            assert any(isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_DATA_TYPE for e in effects)

    def test_handle_grant_data_credit_wrong_state(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CLOSED
        event = UserGrantDataCredit(request_id=1, session_id=1, max_data=2000)

        effects = client_processor.handle_grant_data_credit(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)
        assert "Cannot grant credit to session in state" in effects[0].exception.args[0]

    def test_handle_grant_streams_credit_not_found(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        mock_state.sessions = {}
        event = UserGrantStreamsCredit(request_id=1, session_id=1, max_streams=10, is_unidirectional=False)

        effects = client_processor.handle_grant_streams_credit(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_grant_streams_credit_success(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.local_max_streams_uni = 10
        event = UserGrantStreamsCredit(request_id=1, session_id=1, max_streams=20, is_unidirectional=True)

        effects = client_processor.handle_grant_streams_credit(event=event, state=mock_state)

        assert mock_session_data.local_max_streams_uni == 20
        assert any(
            isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_STREAMS_UNI_TYPE for e in effects
        )

    @pytest.mark.parametrize(
        "is_unidirectional, max_streams, current_limit, no_op",
        [
            (False, 20, 10, False),
            (False, 10, 10, True),
            (False, 5, 10, True),
            (True, 20, 10, False),
            (True, 10, 10, True),
            (True, 5, 10, True),
        ],
    )
    def test_handle_grant_streams_credit_variants(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        is_unidirectional: bool,
        max_streams: int,
        current_limit: int,
        no_op: bool,
    ) -> None:
        if is_unidirectional:
            mock_session_data.local_max_streams_uni = current_limit
        else:
            mock_session_data.local_max_streams_bidi = current_limit

        event = UserGrantStreamsCredit(
            request_id=1, session_id=1, max_streams=max_streams, is_unidirectional=is_unidirectional
        )

        effects = client_processor.handle_grant_streams_credit(event=event, state=mock_state)

        assert NotifyRequestDone(request_id=1, result=None) in effects

        target_field = "local_max_streams_uni" if is_unidirectional else "local_max_streams_bidi"
        target_capsule = constants.WT_MAX_STREAMS_UNI_TYPE if is_unidirectional else constants.WT_MAX_STREAMS_BIDI_TYPE

        if no_op:
            assert getattr(mock_session_data, target_field) == current_limit
            assert not any(isinstance(e, SendH3Capsule) for e in effects)
        else:
            assert getattr(mock_session_data, target_field) == max_streams
            assert any(isinstance(e, SendH3Capsule) and e.capsule_type == target_capsule for e in effects)

    def test_handle_grant_streams_credit_wrong_state(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CLOSED
        event = UserGrantStreamsCredit(request_id=1, session_id=1, max_streams=20, is_unidirectional=True)

        effects = client_processor.handle_grant_streams_credit(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)
        assert "Cannot grant credit to session in state" in effects[0].exception.args[0]

    def test_handle_reject_session_client_fails(
        self, client_processor: SessionProcessor, mock_state: MagicMock
    ) -> None:
        event = UserRejectSession(request_id=1, session_id=1, status_code=404)

        effects = client_processor.handle_reject_session(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, ProtocolError)

    def test_handle_reject_session_not_found(self, server_processor: SessionProcessor, mock_state: MagicMock) -> None:
        mock_state.sessions = {}
        event = UserRejectSession(request_id=1, session_id=1, status_code=404)

        effects = server_processor.handle_reject_session(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_reject_session_server_only(self, client_processor: SessionProcessor, mock_state: MagicMock) -> None:
        event = UserRejectSession(request_id=1, session_id=1, status_code=403)

        effects = client_processor.handle_reject_session(event=event, state=mock_state)

        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, ProtocolError)

    def test_handle_reject_session_success(
        self,
        server_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mock_get_timestamp: MagicMock,
    ) -> None:
        mock_session_data.state = types.SessionState.CONNECTING
        event = UserRejectSession(request_id=1, session_id=1, status_code=404)

        effects = server_processor.handle_reject_session(event=event, state=mock_state)

        assert mock_session_data.state == types.SessionState.CLOSED
        assert mock_session_data.closed_at == 123456.0
        assert effects == [
            SendH3Headers(stream_id=1, status=404, end_stream=True),
            EmitSessionEvent(
                session_id=1,
                event_type=types.EventType.SESSION_CLOSED,
                data={"session_id": 1, "code": 404, "reason": "Rejected by application"},
            ),
            NotifyRequestDone(request_id=1, result=None),
        ]

    def test_handle_reject_session_wrong_state(
        self, server_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CONNECTED
        event = UserRejectSession(request_id=1, session_id=1, status_code=404)

        effects = server_processor.handle_reject_session(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_send_datagram_datagram_boundary(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.remote_max_datagram_frame_size = 10
        event = UserSendDatagram(request_id=1, session_id=1, data=b"a" * 10)

        effects = client_processor.handle_send_datagram(event=event, state=mock_state)

        assert mock_session_data.datagrams_sent == 1
        assert effects != []
        assert isinstance(effects[0], SendH3Datagram)

    def test_handle_send_datagram_datagram_too_large(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_state.remote_max_datagram_frame_size = 10
        event = UserSendDatagram(request_id=1, session_id=1, data=b"a" * 11)

        effects = client_processor.handle_send_datagram(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, ValueError)
        assert "exceeds maximum 10" in effects[0].exception.args[0]

    def test_handle_send_datagram_list_success(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        event = UserSendDatagram(request_id=1, session_id=1, data=[b"hello", b" world"])

        effects = client_processor.handle_send_datagram(event=event, state=mock_state)

        assert mock_session_data.datagrams_sent == 1
        assert mock_session_data.datagram_bytes_sent == 11
        assert effects == [
            SendH3Datagram(stream_id=1, data=[b"hello", b" world"]),
            NotifyRequestDone(request_id=1, result=None),
        ]

    def test_handle_send_datagram_not_found(self, client_processor: SessionProcessor, mock_state: MagicMock) -> None:
        mock_state.sessions = {}
        event = UserSendDatagram(request_id=1, session_id=1, data=b"hello")

        effects = client_processor.handle_send_datagram(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)

    def test_handle_send_datagram_success(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        event = UserSendDatagram(request_id=1, session_id=1, data=b"test")

        effects = client_processor.handle_send_datagram(event=event, state=mock_state)

        assert mock_session_data.datagrams_sent == 1
        assert mock_session_data.datagram_bytes_sent == 4
        assert SendH3Datagram(stream_id=1, data=b"test") in effects
        assert NotifyRequestDone(request_id=1, result=None) in effects

    def test_handle_send_datagram_wrong_state(
        self, client_processor: SessionProcessor, mock_state: MagicMock, mock_session_data: SessionStateData
    ) -> None:
        mock_session_data.state = types.SessionState.CONNECTING
        event = UserSendDatagram(request_id=1, session_id=1, data=b"hello")

        effects = client_processor.handle_send_datagram(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert isinstance(effects[0].exception, SessionError)
        assert "is not connected" in effects[0].exception.args[0]

    def test_process_stream_closure_counts_bidi(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch("pywebtransport._protocol.session_processor.is_peer_initiated_stream", return_value=True)
        mocker.patch("pywebtransport._protocol.session_processor.is_unidirectional_stream", return_value=False)
        mock_calc = mocker.patch(
            "pywebtransport._protocol.session_processor.calculate_new_stream_limit", return_value=20
        )

        effects = client_processor._process_stream_closure(session_data=mock_session_data, stream_id=2)

        assert mock_session_data.peer_streams_bidi_closed == 1
        assert any(
            isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_STREAMS_BIDI_TYPE for e in effects
        )
        mock_calc.assert_called()

    def test_process_stream_closure_counts_uni(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch("pywebtransport._protocol.session_processor.is_peer_initiated_stream", return_value=True)
        mocker.patch("pywebtransport._protocol.session_processor.is_unidirectional_stream", return_value=True)
        mock_calc = mocker.patch(
            "pywebtransport._protocol.session_processor.calculate_new_stream_limit", return_value=20
        )

        effects = client_processor._process_stream_closure(session_data=mock_session_data, stream_id=2)

        assert mock_session_data.peer_streams_uni_closed == 1
        assert any(
            isinstance(e, SendH3Capsule) and e.capsule_type == constants.WT_MAX_STREAMS_UNI_TYPE for e in effects
        )
        mock_calc.assert_called()

    def test_reset_all_session_streams(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.pending_bidi_stream_requests = deque([1])
        mock_session_data.active_streams = {4}

        stream_1 = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream_1.stream_id = 4
        stream_1.session_id = 1
        stream_1.state = types.StreamState.OPEN
        stream_1.pending_read_requests = deque([(2, None)])
        stream_1.write_buffer = deque([(b"data", 3, False)])

        mock_state.streams = {4: stream_1}

        event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
        effects = client_processor.handle_close_session(event=event, state=mock_state)

        failed_requests = {e.request_id for e in effects if isinstance(e, NotifyRequestFailed)}
        assert failed_requests == {1, 2, 3}

        assert ResetQuicStream(stream_id=4, error_code=ErrorCodes.WT_SESSION_GONE) in effects
        assert StopQuicStream(stream_id=4, error_code=ErrorCodes.WT_SESSION_GONE) in effects
        assert EmitStreamEvent(stream_id=4, event_type=types.EventType.STREAM_CLOSED, data={"stream_id": 4}) in effects
        assert stream_1.state == types.StreamState.CLOSED

    def test_reset_all_session_streams_calls_closure(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.active_streams.add(2)
        stream_data = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream_data.stream_id = 2
        stream_data.state = types.StreamState.OPEN
        stream_data.pending_read_requests = deque()
        stream_data.write_buffer = deque()
        mock_state.streams = {2: stream_data}

        mock_process_closure = mocker.patch.object(client_processor, "_process_stream_closure", return_value=[])

        client_processor._reset_all_session_streams(session_id=1, session_data=mock_session_data, state=mock_state)

        mock_process_closure.assert_called_once_with(session_data=mock_session_data, stream_id=2)

    def test_reset_all_session_streams_complex_states(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.pending_bidi_stream_requests = deque()
        mock_session_data.pending_uni_stream_requests = deque()
        mock_session_data.active_streams = {4, 8}

        stream_1 = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream_1.stream_id = 4
        stream_1.session_id = 1
        stream_1.state = types.StreamState.RESET_SENT
        stream_1.pending_read_requests = deque()
        stream_1.write_buffer = deque()

        stream_2 = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream_2.stream_id = 8
        stream_2.session_id = 1
        stream_2.state = types.StreamState.RESET_RECEIVED
        stream_2.pending_read_requests = deque()
        stream_2.write_buffer = deque()

        mock_state.streams = {4: stream_1, 8: stream_2}

        event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
        effects = client_processor.handle_close_session(event=event, state=mock_state)

        assert not any(isinstance(e, ResetQuicStream) and e.stream_id == 4 for e in effects)
        assert StopQuicStream(stream_id=4, error_code=ErrorCodes.WT_SESSION_GONE) in effects

        assert ResetQuicStream(stream_id=8, error_code=ErrorCodes.WT_SESSION_GONE) in effects
        assert not any(isinstance(e, StopQuicStream) and e.stream_id == 8 for e in effects)

    def test_reset_all_session_streams_missing_or_closed(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.active_streams = {1, 2}

        s2 = mocker.create_autospec(StreamStateDataInternal, instance=True)
        s2.state = types.StreamState.CLOSED
        mock_state.streams = {2: s2}

        event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
        effects = client_processor.handle_close_session(event=event, state=mock_state)

        assert not any(isinstance(e, ResetQuicStream) for e in effects)
        assert mock_session_data.active_streams == set()

    def test_reset_all_session_streams_missing_stream(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.active_streams = {1, 2}
        mock_state.streams = {}

        event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
        effects = client_processor.handle_close_session(event=event, state=mock_state)

        assert not any(isinstance(e, ResetQuicStream) for e in effects)
        assert mock_session_data.active_streams == set()

    def test_reset_all_session_streams_no_stop_sending_forced(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.active_streams = {2}
        stream = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream.stream_id = 2
        stream.session_id = 1
        stream.state = types.StreamState.OPEN
        stream.pending_read_requests = deque()
        stream.write_buffer = deque()
        mock_state.streams = {2: stream}

        with patch("pywebtransport._protocol.session_processor.can_receive_data_on_stream", return_value=False):
            event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
            effects = client_processor.handle_close_session(event=event, state=mock_state)

        assert any(isinstance(e, ResetQuicStream) and e.stream_id == 2 for e in effects)
        assert not any(isinstance(e, StopQuicStream) for e in effects)

    def test_reset_all_session_streams_pending_requests(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.active_streams.add(10)
        stream = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream.stream_id = 10
        stream.session_id = 1
        stream.state = types.StreamState.OPEN
        stream.pending_read_requests = deque([(101, None)])
        stream.write_buffer = deque([(b"data", 102, False)])
        mock_state.streams = {10: stream}

        mock_session_data.pending_uni_stream_requests = deque([201])

        event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
        effects = client_processor.handle_close_session(event=event, state=mock_state)

        failed_requests = {e.request_id for e in effects if isinstance(e, NotifyRequestFailed)}
        assert {101, 102, 201}.issubset(failed_requests)
        assert len(stream.pending_read_requests) == 0
        assert len(stream.write_buffer) == 0

    def test_reset_all_session_streams_pending_uni(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.pending_uni_stream_requests = deque([10])
        mock_session_data.active_streams = set()

        event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
        effects = client_processor.handle_close_session(event=event, state=mock_state)

        failed_requests = {e.request_id for e in effects if isinstance(e, NotifyRequestFailed)}
        assert 10 in failed_requests

    @pytest.mark.parametrize(
        "stream_state, expect_reset, expect_stop",
        [
            (types.StreamState.OPEN, True, True),
            (types.StreamState.RESET_SENT, False, True),
            (types.StreamState.RESET_RECEIVED, True, False),
            (types.StreamState.CLOSED, False, False),
        ],
    )
    def test_reset_all_session_streams_states(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
        stream_state: types.StreamState,
        expect_reset: bool,
        expect_stop: bool,
    ) -> None:
        mock_session_data.pending_bidi_stream_requests = deque([1])
        mock_session_data.active_streams = {4}

        stream = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream.stream_id = 4
        stream.session_id = 1
        stream.state = stream_state
        stream.pending_read_requests = deque()
        stream.write_buffer = deque()
        mock_state.streams = {4: stream}

        with patch("pywebtransport._protocol.session_processor.can_receive_data_on_stream", return_value=True):
            event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
            effects = client_processor.handle_close_session(event=event, state=mock_state)

        reset_present = any(isinstance(e, ResetQuicStream) and e.stream_id == 4 for e in effects)
        stop_present = any(isinstance(e, StopQuicStream) and e.stream_id == 4 for e in effects)

        assert reset_present == expect_reset
        assert stop_present == expect_stop

        if stream_state != types.StreamState.CLOSED:
            assert stream.state == types.StreamState.CLOSED

    def test_reset_all_session_streams_stop_sending_skipped(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.active_streams = {2}
        stream = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream.stream_id = 2
        stream.session_id = 1
        stream.state = types.StreamState.OPEN
        stream.pending_read_requests = deque()
        stream.write_buffer = deque()
        mock_state.streams = {2: stream}

        with patch("pywebtransport._protocol.session_processor.can_receive_data_on_stream", return_value=False):
            event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
            effects = client_processor.handle_close_session(event=event, state=mock_state)

        assert any(isinstance(e, ResetQuicStream) and e.stream_id == 2 for e in effects)
        assert not any(isinstance(e, StopQuicStream) for e in effects)

    def test_reset_all_session_streams_with_pending_io(
        self,
        client_processor: SessionProcessor,
        mock_state: MagicMock,
        mock_session_data: SessionStateData,
        mocker: MockerFixture,
    ) -> None:
        mock_session_data.active_streams.add(10)

        stream = mocker.create_autospec(StreamStateDataInternal, instance=True)
        stream.stream_id = 10
        stream.session_id = 1
        stream.state = types.StreamState.OPEN
        stream.pending_read_requests = deque([(101, None)])
        stream.write_buffer = deque([(b"data", 102, False)])

        mock_state.streams = {10: stream}

        event = UserCloseSession(request_id=99, session_id=1, error_code=1, reason="Test")
        effects = client_processor.handle_close_session(event=event, state=mock_state)

        failed_requests = {e.request_id for e in effects if isinstance(e, NotifyRequestFailed)}
        assert 101 in failed_requests
        assert 102 in failed_requests
        assert len(stream.pending_read_requests) == 0
        assert len(stream.write_buffer) == 0
