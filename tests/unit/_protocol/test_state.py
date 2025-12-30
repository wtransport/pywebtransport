"""Unit tests for the pywebtransport._protocol.state module."""

import time
from collections import deque
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport import Headers
from pywebtransport._protocol.state import ProtocolState, SessionInitData, SessionStateData, StreamStateData
from pywebtransport.types import ConnectionState, SessionState, StreamDirection, StreamState


class TestProtocolState:

    @pytest.fixture
    def mock_connection_state(self, mocker: MockerFixture) -> MagicMock:
        return mocker.create_autospec(ConnectionState, instance=True)

    def test_instantiation(self, mock_connection_state: MagicMock) -> None:
        start_time = time.monotonic()

        state = ProtocolState(
            is_client=True, connection_state=mock_connection_state, max_datagram_size=1200, connected_at=start_time
        )

        assert state.is_client is True
        assert state.connection_state is mock_connection_state
        assert state.max_datagram_size == 1200
        assert state.connected_at == start_time

        assert state.remote_max_datagram_frame_size == 0
        assert state.handshake_complete is False
        assert state.peer_settings_received is False
        assert state.local_goaway_sent is False
        assert state.early_event_count == 0
        assert state.peer_initial_max_data == 0
        assert state.peer_initial_max_streams_bidi == 0
        assert state.peer_initial_max_streams_uni == 0
        assert state.closed_at is None

        assert state.sessions == {}
        assert state.streams == {}
        assert state.pending_requests == {}
        assert state.pending_session_configs == {}
        assert state.early_event_buffer == {}

    def test_mutable_defaults_are_unique(self, mock_connection_state: MagicMock) -> None:
        p1 = ProtocolState(is_client=True, connection_state=mock_connection_state, max_datagram_size=1200)
        p2 = ProtocolState(is_client=False, connection_state=mock_connection_state, max_datagram_size=1200)

        assert p1.sessions is not p2.sessions
        assert p1.streams is not p2.streams
        assert p1.pending_requests is not p2.pending_requests
        assert p1.pending_session_configs is not p2.pending_session_configs
        assert p1.early_event_buffer is not p2.early_event_buffer


class TestSessionInitData:

    def test_instantiation(self) -> None:
        start_time = time.monotonic()
        headers: Headers = {b":path": b"/test"}

        init_data = SessionInitData(path="/test", headers=headers, created_at=start_time)

        assert init_data.path == "/test"
        assert init_data.headers is headers
        assert init_data.created_at == start_time


class TestSessionStateData:

    @pytest.fixture
    def mock_session_state(self, mocker: MockerFixture) -> MagicMock:
        return mocker.create_autospec(SessionState, instance=True)

    def test_instantiation(self, mock_session_state: MagicMock) -> None:
        start_time = time.monotonic()
        headers: Headers = {b":path": b"/test"}

        session = SessionStateData(
            session_id=1,
            state=mock_session_state,
            path="/test",
            headers=headers,
            created_at=start_time,
            local_max_data=1024,
            peer_max_data=2048,
            local_max_streams_bidi=10,
            peer_max_streams_bidi=5,
            local_max_streams_uni=10,
            peer_max_streams_uni=5,
            ready_at=start_time,
        )

        assert session.session_id == 1
        assert session.state is mock_session_state
        assert session.path == "/test"
        assert session.headers is headers
        assert session.created_at == start_time
        assert session.local_max_data == 1024
        assert session.peer_max_data == 2048
        assert session.ready_at == start_time

        assert session.local_data_sent == 0
        assert session.peer_data_sent == 0
        assert session.local_streams_bidi_opened == 0
        assert session.datagrams_sent == 0
        assert session.closed_at is None

        assert session.pending_bidi_stream_requests == deque()
        assert session.pending_uni_stream_requests == deque()
        assert session.active_streams == set()
        assert session.blocked_streams == set()

    def test_mutable_defaults_are_unique(self, mock_session_state: MagicMock) -> None:
        common_session_args: dict[str, Any] = {
            "state": mock_session_state,
            "path": "/",
            "headers": {},
            "created_at": 0.0,
            "local_max_data": 1,
            "peer_max_data": 1,
            "local_max_streams_bidi": 1,
            "peer_max_streams_bidi": 1,
            "local_max_streams_uni": 1,
            "peer_max_streams_uni": 1,
        }
        s1 = SessionStateData(session_id=1, **common_session_args)
        s2 = SessionStateData(session_id=2, **common_session_args)

        assert s1.pending_bidi_stream_requests is not s2.pending_bidi_stream_requests
        assert s1.pending_uni_stream_requests is not s2.pending_uni_stream_requests
        assert s1.active_streams is not s2.active_streams
        assert s1.blocked_streams is not s2.blocked_streams


class TestStreamStateData:

    @pytest.fixture
    def mock_stream_direction(self, mocker: MockerFixture) -> MagicMock:
        return mocker.create_autospec(StreamDirection, instance=True)

    @pytest.fixture
    def mock_stream_state(self, mocker: MockerFixture) -> MagicMock:
        return mocker.create_autospec(StreamState, instance=True)

    def test_instantiation(self, mock_stream_state: MagicMock, mock_stream_direction: MagicMock) -> None:
        start_time = time.monotonic()

        stream = StreamStateData(
            stream_id=4,
            session_id=1,
            direction=mock_stream_direction,
            state=mock_stream_state,
            created_at=start_time,
            close_code=0,
            close_reason="test",
            closed_at=start_time,
        )

        assert stream.stream_id == 4
        assert stream.session_id == 1
        assert stream.direction is mock_stream_direction
        assert stream.state is mock_stream_state
        assert stream.created_at == start_time
        assert stream.close_code == 0
        assert stream.close_reason == "test"
        assert stream.closed_at == start_time

        assert stream.bytes_sent == 0
        assert stream.bytes_received == 0

        assert stream.read_buffer == deque()
        assert stream.read_buffer_size == 0
        assert stream.pending_read_requests == deque()
        assert stream.write_buffer == deque()
        assert stream.write_buffer_size == 0

    def test_mutable_defaults_are_unique(self, mock_stream_state: MagicMock, mock_stream_direction: MagicMock) -> None:
        common_stream_args: dict[str, Any] = {
            "session_id": 1,
            "direction": mock_stream_direction,
            "state": mock_stream_state,
            "created_at": 0.0,
        }
        st1 = StreamStateData(stream_id=0, **common_stream_args)
        st2 = StreamStateData(stream_id=4, **common_stream_args)

        assert st1.read_buffer is not st2.read_buffer
        assert st1.pending_read_requests is not st2.pending_read_requests
        assert st1.write_buffer is not st2.write_buffer
