"""Unit tests for the pywebtransport._protocol.connection_processor module."""

from collections import deque
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ClientConfig, ConnectionError, Headers, ProtocolError, ServerConfig, SessionError, constants
from pywebtransport._protocol.connection_processor import ConnectionProcessor
from pywebtransport._protocol.events import (
    CleanupH3Stream,
    CloseQuicConnection,
    ConnectionClose,
    CreateH3Session,
    EmitConnectionEvent,
    EmitSessionEvent,
    GoawayReceived,
    HeadersReceived,
    InternalBindH3Session,
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalFailH3Session,
    NotifyRequestDone,
    NotifyRequestFailed,
    ProcessProtocolEvent,
    ResetQuicStream,
    SendH3Capsule,
    SendH3Goaway,
    SendH3Headers,
    TransportConnectionTerminated,
    TransportQuicParametersReceived,
    UserConnectionGracefulClose,
    UserCreateSession,
    UserGetConnectionDiagnostics,
)
from pywebtransport._protocol.state import ProtocolState, SessionInitData, SessionStateData, StreamStateData
from pywebtransport.types import ConnectionState, EventType, SessionState, StreamState


class TestConnectionProcessor:

    @pytest.fixture
    def client_processor(self, mock_config: MagicMock) -> ConnectionProcessor:
        return ConnectionProcessor(is_client=True, config=mock_config, connection_id="test-conn-id")

    @pytest.fixture
    def mock_config(self, mocker: MockerFixture) -> MagicMock:
        config = mocker.create_autospec(ClientConfig, instance=True)
        config.initial_max_data = 1024
        config.initial_max_streams_bidi = 10
        config.initial_max_streams_uni = 10
        config.max_sessions = 50
        config.pending_event_ttl = 10.0
        return config

    @pytest.fixture
    def mock_get_timestamp(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("pywebtransport._protocol.connection_processor.get_timestamp", return_value=123456.0)

    @pytest.fixture
    def mock_state(self, mocker: MockerFixture) -> MagicMock:
        state = mocker.create_autospec(ProtocolState, instance=True)
        state.connection_state = ConnectionState.CONNECTED
        state.is_client = True
        state.connected_at = 123456.0
        state.closed_at = None
        state.max_datagram_size = 1200
        state.remote_max_datagram_frame_size = 1100
        state.peer_initial_max_data = 2048
        state.peer_initial_max_streams_bidi = 5
        state.peer_initial_max_streams_uni = 5
        state.sessions = {}
        state.streams = {}
        state.pending_requests = {}
        state.pending_session_configs = {}
        state.early_event_buffer = {}
        state.local_goaway_sent = False
        return state

    @pytest.fixture
    def server_processor(self, mocker: MockerFixture) -> ConnectionProcessor:
        mock_server_config = mocker.create_autospec(ServerConfig, instance=True)
        mock_server_config.initial_max_data = 1024
        mock_server_config.initial_max_streams_bidi = 10
        mock_server_config.initial_max_streams_uni = 10
        mock_server_config.max_sessions = 10
        return ConnectionProcessor(is_client=False, config=mock_server_config, connection_id="test-conn-id")

    def test_handle_cleanup_early_events(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        event = InternalCleanupEarlyEvents()
        mock_state.early_event_buffer = {1: [(123450.0, "event1"), (123440.0, "event2")], 2: [(123440.0, "event3")]}
        mock_state.sessions = {1: "session"}
        mock_state.early_event_count = 3

        effects = client_processor.handle_cleanup_early_events(event=event, state=mock_state)

        assert 1 in mock_state.early_event_buffer
        assert len(mock_state.early_event_buffer[1]) == 1
        assert mock_state.early_event_buffer[1][0] == (123450.0, "event1")
        assert 2 not in mock_state.early_event_buffer
        assert len(effects) == 1
        assert isinstance(effects[0], ResetQuicStream)
        assert effects[0].stream_id == 2
        assert effects[0].error_code == constants.ErrorCodes.WT_BUFFERED_STREAM_REJECTED

    def test_handle_cleanup_early_events_stream_became_session(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        event = InternalCleanupEarlyEvents()
        mock_state.early_event_buffer = {100: [(123.0, "event")]}
        mock_state.sessions = {100: "valid_session"}
        mock_state.early_event_count = 1

        effects = client_processor.handle_cleanup_early_events(event=event, state=mock_state)

        assert 100 not in mock_state.early_event_buffer
        assert effects == []

    def test_handle_cleanup_resources(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mocker: MockerFixture
    ) -> None:
        event = InternalCleanupResources()

        mock_session_closed = mocker.MagicMock(spec=SessionStateData)
        mock_session_closed.state = SessionState.CLOSED
        mock_session_closed.active_streams = {2, 3}

        mock_session_open = mocker.MagicMock(spec=SessionStateData)
        mock_session_open.state = SessionState.CONNECTED
        mock_session_open.active_streams = {1}

        mock_stream_closed = mocker.MagicMock(spec=StreamStateData)
        mock_stream_closed.state = StreamState.CLOSED

        mock_stream_open = mocker.MagicMock(spec=StreamStateData)
        mock_stream_open.state = StreamState.OPEN

        mock_state.sessions = {100: mock_session_closed, 101: mock_session_open}
        mock_state.streams = {1: mock_stream_open, 2: mock_stream_closed, 3: mock_stream_open, 4: mock_stream_closed}

        effects = client_processor.handle_cleanup_resources(event=event, state=mock_state)

        assert 100 not in mock_state.sessions
        assert 101 in mock_state.sessions
        assert 2 not in mock_state.streams
        assert 3 not in mock_state.streams
        assert 4 not in mock_state.streams
        assert 1 in mock_state.streams

        cleanup_ids = sorted([e.stream_id for e in effects if isinstance(e, CleanupH3Stream)])
        assert cleanup_ids == [2, 3, 4, 100]

    def test_handle_cleanup_resources_race_condition_simulation(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mocker: MockerFixture
    ) -> None:
        event = InternalCleanupResources()

        mock_state.sessions = MagicMock()
        mock_session = mocker.MagicMock(spec=SessionStateData)
        mock_session.state = SessionState.CLOSED
        mock_state.sessions.items.return_value = [("race-sid", mock_session)]
        mock_state.sessions.pop.return_value = None

        mock_state.streams = {}

        effects = client_processor.handle_cleanup_resources(event=event, state=mock_state)

        assert not effects
        mock_state.sessions.pop.assert_called_with("race-sid", None)

    def test_handle_cleanup_resources_stream_missing_from_state(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mocker: MockerFixture
    ) -> None:
        event = InternalCleanupResources()

        mock_session_closed = mocker.MagicMock(spec=SessionStateData)
        mock_session_closed.state = SessionState.CLOSED
        mock_session_closed.active_streams = {999}

        mock_state.sessions = {100: mock_session_closed}
        mock_state.streams = {}

        effects = client_processor.handle_cleanup_resources(event=event, state=mock_state)

        assert 100 not in mock_state.sessions
        assert len(effects) == 2
        cleanup_ids = sorted([e.stream_id for e in effects if isinstance(e, CleanupH3Stream)])
        assert cleanup_ids == [100, 999]

    def test_handle_connection_close(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        event = ConnectionClose(request_id=1, error_code=1000, reason="Test close")
        mock_state.connection_state = ConnectionState.CONNECTED

        effects = client_processor.handle_connection_close(event=event, state=mock_state)

        assert mock_state.connection_state == ConnectionState.CLOSING
        assert mock_state.closed_at == 123456.0
        assert effects == [
            CloseQuicConnection(error_code=1000, reason="Test close"),
            NotifyRequestDone(request_id=1, result=None),
        ]

    def test_handle_connection_close_already_closed(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        event = ConnectionClose(request_id=1, error_code=1000, reason="Test close")
        mock_state.connection_state = ConnectionState.CLOSED

        effects = client_processor.handle_connection_close(event=event, state=mock_state)

        mock_get_timestamp.assert_not_called()
        assert mock_state.connection_state == ConnectionState.CLOSED
        assert effects == [NotifyRequestDone(request_id=1, result=None)]

    def test_handle_connection_terminated(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mocker: MockerFixture
    ) -> None:
        event = TransportConnectionTerminated(error_code=500, reason_phrase="QUIC Down")
        mock_state.connection_state = ConnectionState.CONNECTED

        mock_stream = mocker.MagicMock(spec=StreamStateData)
        mock_stream.pending_read_requests = deque([(10, 100)])
        mock_stream.write_buffer = deque([(b"", 20, False)])

        mock_state.pending_session_configs = {1: "config"}
        mock_state.pending_requests = {2: 1}
        mock_state.streams = {10: mock_stream}

        effects = client_processor.handle_connection_terminated(event=event, state=mock_state)

        assert mock_state.connection_state == ConnectionState.CLOSED
        assert not mock_state.pending_session_configs
        assert not mock_state.pending_requests

        fail_effects = [e for e in effects if isinstance(e, NotifyRequestFailed)]
        failed_ids = {e.request_id for e in fail_effects}

        assert 10 in failed_ids
        assert 20 in failed_ids
        assert len(fail_effects) == 2

        emit_effects = [e for e in effects if isinstance(e, EmitConnectionEvent)]
        assert len(emit_effects) == 1
        assert emit_effects[0].event_type == EventType.CONNECTION_CLOSED

    def test_handle_connection_terminated_already_closed(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        event = TransportConnectionTerminated(error_code=500, reason_phrase="QUIC Down")
        mock_state.connection_state = ConnectionState.CLOSED

        effects = client_processor.handle_connection_terminated(event=event, state=mock_state)

        assert effects == []

    def test_handle_create_session_client_not_connected(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        headers: Headers = {b":path": b"/test"}
        event = UserCreateSession(request_id=1, path="/test", headers=headers)
        mock_state.connection_state = ConnectionState.CONNECTING

        effects = client_processor.handle_create_session(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, ConnectionError)
        assert fail_effect.exception.args[0] == "Cannot create session, connection state is connecting"

    def test_handle_create_session_client_success(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        headers: Headers = {b":path": b"/test"}
        event = UserCreateSession(request_id=1, path="/test", headers=headers)
        mock_state.connection_state = ConnectionState.CONNECTED

        effects = client_processor.handle_create_session(event=event, state=mock_state)

        assert effects == [CreateH3Session(request_id=1, path="/test", headers=headers)]

        assert 1 in mock_state.pending_session_configs
        init_data = mock_state.pending_session_configs[1]
        assert isinstance(init_data, SessionInitData)
        assert init_data.path == "/test"
        assert init_data.created_at == 123456.0

    def test_handle_create_session_server_fails(
        self, server_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        headers: Headers = {b":path": b"/test"}
        event = UserCreateSession(request_id=1, path="/test", headers=headers)

        effects = server_processor.handle_create_session(event=event, state=mock_state)

        assert len(effects) == 1
        fail_effect = effects[0]
        assert isinstance(fail_effect, NotifyRequestFailed)
        assert fail_effect.request_id == 1
        assert isinstance(fail_effect.exception, ProtocolError)
        assert fail_effect.exception.args[0] == "Server cannot create sessions using this method"

    def test_handle_get_connection_diagnostics(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        event = UserGetConnectionDiagnostics(request_id=1)
        mock_state.sessions = {1: "a", 2: "b"}
        mock_state.streams = {3: "c"}

        effects = client_processor.handle_get_connection_diagnostics(event=event, state=mock_state)

        expected_diagnostics = {
            "connection_id": "test-conn-id",
            "state": ConnectionState.CONNECTED,
            "is_client": True,
            "connected_at": 123456.0,
            "closed_at": None,
            "max_datagram_size": 1200,
            "remote_max_datagram_frame_size": 1100,
            "session_count": 2,
            "stream_count": 1,
        }
        assert effects == [NotifyRequestDone(request_id=1, result=expected_diagnostics)]

    def test_handle_goaway_received(
        self,
        client_processor: ConnectionProcessor,
        mock_state: MagicMock,
        mock_get_timestamp: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        event = GoawayReceived()
        mock_state.connection_state = ConnectionState.CONNECTED
        session1 = mocker.MagicMock(spec=SessionStateData)
        session1.state = SessionState.CONNECTED
        session2 = mocker.MagicMock(spec=SessionStateData)
        session2.state = SessionState.CLOSED
        mock_state.sessions = {4: session1, 8: session2}

        effects = client_processor.handle_goaway_received(event=event, state=mock_state)

        assert mock_state.connection_state == ConnectionState.CLOSING
        assert mock_state.closed_at == 123456.0
        assert session1.state == SessionState.DRAINING
        assert session2.state == SessionState.CLOSED
        assert effects == [
            SendH3Capsule(
                stream_id=4, capsule_type=constants.DRAIN_WEBTRANSPORT_SESSION_TYPE, capsule_data=b"", end_stream=False
            ),
            EmitSessionEvent(session_id=4, event_type=EventType.SESSION_DRAINING, data={"session_id": 4}),
        ]

    def test_handle_goaway_received_already_closing(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        event = GoawayReceived()
        mock_state.connection_state = ConnectionState.CLOSING
        mock_state.sessions = {}

        effects = client_processor.handle_goaway_received(event=event, state=mock_state)

        mock_get_timestamp.assert_not_called()
        assert mock_state.connection_state == ConnectionState.CLOSING
        assert effects == []

    def test_handle_graceful_close(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        event = UserConnectionGracefulClose(request_id=1)
        mock_state.connection_state = ConnectionState.CONNECTED
        mock_state.local_goaway_sent = False

        effects = client_processor.handle_graceful_close(event=event, state=mock_state)

        assert mock_state.connection_state == ConnectionState.CLOSING
        assert mock_state.local_goaway_sent is True
        assert effects == [SendH3Goaway(), NotifyRequestDone(request_id=1, result=None)]

    def test_handle_graceful_close_idempotent_goaway(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        event = UserConnectionGracefulClose(request_id=1)
        mock_state.connection_state = ConnectionState.CONNECTED
        mock_state.local_goaway_sent = True

        effects = client_processor.handle_graceful_close(event=event, state=mock_state)

        assert effects == [NotifyRequestDone(request_id=1, result=None)]
        assert mock_state.connection_state == ConnectionState.CONNECTED
        mock_get_timestamp.assert_not_called()

    def test_handle_graceful_close_state_already_closing(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        event = UserConnectionGracefulClose(request_id=1)
        mock_state.connection_state = ConnectionState.CLOSING
        mock_state.local_goaway_sent = False

        effects = client_processor.handle_graceful_close(event=event, state=mock_state)

        assert effects == [SendH3Goaway(), NotifyRequestDone(request_id=1, result=None)]
        assert mock_state.local_goaway_sent is True
        mock_get_timestamp.assert_not_called()

    def test_handle_headers_received_client_early_events(
        self,
        client_processor: ConnectionProcessor,
        mock_state: MagicMock,
        mock_get_timestamp: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        headers: Headers = {b":status": b"200"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        empty_headers: Headers = {}
        init_data = SessionInitData(path="/test", headers=empty_headers, created_at=0.0)

        mock_state.pending_requests = {4: 100}
        mock_state.pending_session_configs = {100: init_data}

        early_event = mocker.Mock()
        mock_state.early_event_buffer = {4: [(123.0, early_event)]}
        mock_state.early_event_count = 1

        effects = client_processor.handle_headers_received(event=event, state=mock_state)

        assert 4 in mock_state.sessions
        assert effects[-1] == ProcessProtocolEvent(event=early_event)
        assert 4 not in mock_state.early_event_buffer
        assert mock_state.early_event_count == 0

    def test_handle_headers_received_client_fail_404(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        headers: Headers = {b":status": b"404"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        empty_headers: Headers = {}
        init_data = SessionInitData(path="/", headers=empty_headers, created_at=0.0)

        mock_state.pending_requests = {4: 100}
        mock_state.pending_session_configs = {100: init_data}

        effects = client_processor.handle_headers_received(event=event, state=mock_state)

        assert 4 not in mock_state.sessions
        assert 4 not in mock_state.pending_requests
        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert effects[0].request_id == 100
        assert isinstance(effects[0].exception, ConnectionError)
        assert "Session creation failed with status '404'" in str(effects[0].exception)
        assert effects[0].exception.error_code == constants.ErrorCodes.H3_REQUEST_REJECTED

    def test_handle_headers_received_client_missing_init_data(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        headers: Headers = {b":status": b"200"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        mock_state.pending_requests = {4: 100}
        mock_state.pending_session_configs = {}

        effects = client_processor.handle_headers_received(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert effects[0].request_id == 100
        assert isinstance(effects[0].exception, SessionError)
        assert "Session init data missing" in str(effects[0].exception)

    def test_handle_headers_received_client_success_200(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        headers: Headers = {b":status": b"200"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        req_headers: Headers = {b":path": b"/test"}
        init_data = SessionInitData(path="/test", headers=req_headers, created_at=123000.0)

        mock_state.pending_requests = {4: 100}
        mock_state.pending_session_configs = {100: init_data}

        effects = client_processor.handle_headers_received(event=event, state=mock_state)

        assert 4 in mock_state.sessions
        session = mock_state.sessions[4]
        assert session.state == SessionState.CONNECTED
        assert session.ready_at == 123456.0
        assert 4 not in mock_state.pending_requests
        assert effects == [
            EmitSessionEvent(
                session_id=4,
                event_type=EventType.SESSION_READY,
                data={"session_id": 4, "ready_at": 123456.0, "path": "/test", "headers": req_headers},
            ),
            NotifyRequestDone(request_id=100, result=4),
        ]

    def test_handle_headers_received_client_unknown_stream(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        headers: Headers = {b":status": b"200"}
        event = HeadersReceived(headers=headers, stream_id=99, stream_ended=False)
        mock_state.pending_requests = {}

        effects = client_processor.handle_headers_received(event=event, state=mock_state)

        assert effects == []

    def test_handle_headers_received_server_early_events(
        self,
        server_processor: ConnectionProcessor,
        mock_state: MagicMock,
        mock_get_timestamp: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        headers: Headers = {b":method": b"CONNECT", b":protocol": b"webtransport", b":path": b"/chat"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        mock_state.connection_state = ConnectionState.CONNECTED

        early_event = mocker.Mock()
        mock_state.early_event_buffer = {4: [(123.0, early_event)]}
        mock_state.early_event_count = 1

        effects = server_processor.handle_headers_received(event=event, state=mock_state)

        assert len(effects) == 2
        assert isinstance(effects[0], EmitSessionEvent)
        assert effects[1] == ProcessProtocolEvent(event=early_event)
        assert 4 not in mock_state.early_event_buffer
        assert mock_state.early_event_count == 0

    def test_handle_headers_received_server_existing_stream(
        self, server_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        headers: Headers = {b":method": b"CONNECT"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        mock_state.sessions = {4: "existing"}

        effects = server_processor.handle_headers_received(event=event, state=mock_state)

        assert effects == []

    def test_handle_headers_received_server_new_session(
        self, server_processor: ConnectionProcessor, mock_state: MagicMock, mock_get_timestamp: MagicMock
    ) -> None:
        headers: Headers = {b":method": b"CONNECT", b":protocol": b"webtransport", b":path": b"/chat"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        mock_state.connection_state = ConnectionState.CONNECTED

        effects = server_processor.handle_headers_received(event=event, state=mock_state)

        assert effects == [
            EmitSessionEvent(
                session_id=4,
                event_type=EventType.SESSION_REQUEST,
                data={"session_id": 4, "path": "/chat", "headers": headers},
            )
        ]
        assert 4 in mock_state.sessions
        new_session = mock_state.sessions[4]
        assert isinstance(new_session, SessionStateData)
        assert new_session.state == SessionState.CONNECTING
        assert new_session.session_id == 4

    def test_handle_headers_received_server_not_connected(
        self, server_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        headers: Headers = {b":method": b"CONNECT", b":protocol": b"webtransport", b":path": b"/chat"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        mock_state.connection_state = ConnectionState.CONNECTING

        effects = server_processor.handle_headers_received(event=event, state=mock_state)

        assert effects == [SendH3Headers(stream_id=4, status=429)]
        assert not mock_state.sessions

    def test_handle_headers_received_server_rejects_limit(
        self, server_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        headers: Headers = {b":method": b"CONNECT", b":protocol": b"webtransport", b":path": b"/chat"}
        event = HeadersReceived(headers=headers, stream_id=11, stream_ended=False)
        mock_state.connection_state = ConnectionState.CONNECTED
        mock_state.sessions = {i: i for i in range(10)}

        effects = server_processor.handle_headers_received(event=event, state=mock_state)

        assert effects == [SendH3Headers(stream_id=11, status=429)]
        assert len(mock_state.sessions) == 10

    def test_handle_headers_received_server_rejects_wrong_method(
        self, server_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        headers: Headers = {b":method": b"GET", b":protocol": b"webtransport"}
        event = HeadersReceived(headers=headers, stream_id=4, stream_ended=False)
        mock_state.connection_state = ConnectionState.CONNECTED

        effects = server_processor.handle_headers_received(event=event, state=mock_state)

        assert effects == [SendH3Headers(stream_id=4, status=400)]
        assert not mock_state.sessions

    def test_handle_internal_bind_h3_session(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        event = InternalBindH3Session(request_id=100, stream_id=4)

        effects = client_processor.handle_internal_bind_h3_session(event=event, state=mock_state)

        assert not effects
        assert mock_state.pending_requests[4] == 100

    def test_handle_internal_fail_h3_session(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        empty_headers: Headers = {}
        init_data = SessionInitData(path="/", headers=empty_headers, created_at=0.0)
        mock_state.pending_session_configs = {100: init_data}

        error = ValueError("H3 Error")
        event = InternalFailH3Session(request_id=100, exception=error)

        effects = client_processor.handle_internal_fail_h3_session(event=event, state=mock_state)

        assert len(effects) == 1
        assert isinstance(effects[0], NotifyRequestFailed)
        assert effects[0].exception is error
        assert 100 not in mock_state.pending_session_configs

    def test_handle_transport_parameters_received(
        self, client_processor: ConnectionProcessor, mock_state: MagicMock
    ) -> None:
        event = TransportQuicParametersReceived(remote_max_datagram_frame_size=1500)

        effects = client_processor.handle_transport_parameters_received(event=event, state=mock_state)

        assert effects == []
        assert mock_state.remote_max_datagram_frame_size == 1500
