"""Unit tests for the pywebtransport._protocol.webtransport_engine module."""

from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ClientConfig, ServerConfig, constants
from pywebtransport._protocol.events import (
    CapsuleReceived,
    ConnectionClose,
    ConnectStreamClosed,
    DatagramReceived,
    EmitConnectionEvent,
    GoawayReceived,
    HeadersReceived,
    InternalBindH3Session,
    InternalBindQuicStream,
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalFailH3Session,
    InternalFailQuicStream,
    InternalReturnStreamData,
    NotifyRequestFailed,
    ProtocolEvent,
    RescheduleQuicTimer,
    SendQuicData,
    SendQuicDatagram,
    SettingsReceived,
    TransportConnectionTerminated,
    TransportDatagramFrameReceived,
    TransportHandshakeCompleted,
    TransportQuicParametersReceived,
    TransportQuicTimerFired,
    TransportStreamDataReceived,
    TransportStreamReset,
    TriggerQuicTimer,
    UserAcceptSession,
    UserCloseSession,
    UserConnectionGracefulClose,
    UserCreateSession,
    UserCreateStream,
    UserEvent,
    UserGetConnectionDiagnostics,
    UserGetSessionDiagnostics,
    UserGetStreamDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserRejectSession,
    UserResetStream,
    UserSendDatagram,
    UserSendStreamData,
    UserStopStream,
    UserStreamRead,
    WebTransportStreamDataReceived,
)
from pywebtransport._protocol.webtransport_engine import WebTransportEngine
from pywebtransport.types import ConnectionState, EventType


class TestWebTransportEngine:

    @pytest.fixture
    def client_config(self) -> ClientConfig:
        return ClientConfig()

    @pytest.fixture
    def server_config(self) -> ServerConfig:
        return ServerConfig()

    @pytest.fixture
    def client_engine(self, client_config: ClientConfig) -> WebTransportEngine:
        return WebTransportEngine(connection_id="conn-c", config=client_config, is_client=True)

    @pytest.fixture
    def server_engine(self, server_config: ServerConfig) -> WebTransportEngine:
        return WebTransportEngine(connection_id="conn-s", config=server_config, is_client=False)

    def test_check_client_connection_ready_conditions(self, client_engine: WebTransportEngine) -> None:
        client_engine._state.connection_state = ConnectionState.IDLE
        client_engine._state.handshake_complete = True
        client_engine._state.peer_settings_received = True
        effects, ready = client_engine._check_client_connection_ready()
        assert not ready
        assert not effects

        client_engine._state.connection_state = ConnectionState.CONNECTING
        client_engine._state.handshake_complete = False
        client_engine._state.peer_settings_received = True
        effects, ready = client_engine._check_client_connection_ready()
        assert not ready

        client_engine._state.handshake_complete = True
        client_engine._state.peer_settings_received = False
        effects, ready = client_engine._check_client_connection_ready()
        assert not ready

    def test_cleanup_stream(self, client_engine: WebTransportEngine, mocker: MockerFixture) -> None:
        mock_h3 = mocker.patch.object(client_engine._h3_engine, "cleanup_stream")

        client_engine.cleanup_stream(stream_id=1)

        mock_h3.assert_called_once_with(stream_id=1)

    def test_encode_methods_delegation(self, client_engine: WebTransportEngine, mocker: MockerFixture) -> None:
        mock_h3 = client_engine._h3_engine
        mocker.patch.object(mock_h3, "encode_capsule", return_value=b"capsule")
        mocker.patch.object(mock_h3, "encode_datagram", return_value=[b"dgram"])
        mocker.patch.object(mock_h3, "encode_goaway_frame", return_value=b"goaway")
        mocker.patch.object(mock_h3, "encode_headers", return_value=[SendQuicData(stream_id=1, data=b"h")])
        mocker.patch.object(
            mock_h3, "encode_webtransport_stream_creation", return_value=[SendQuicData(stream_id=2, data=b"wt")]
        )

        client_engine._h3_engine._local_control_stream_id = 4

        effects = client_engine.encode_capsule(stream_id=10, capsule_type=1, capsule_data=b"val")
        assert len(effects) == 1
        assert isinstance(effects[0], SendQuicData)
        assert effects[0].data == b"capsule"

        effects = client_engine.encode_datagram(stream_id=10, data=b"payload")
        assert len(effects) == 1
        assert isinstance(effects[0], SendQuicDatagram)

        effects = client_engine.encode_goaway()
        assert len(effects) == 1
        assert isinstance(effects[0], SendQuicData)
        assert effects[0].stream_id == 4

        client_engine._h3_engine._local_control_stream_id = None
        assert client_engine.encode_goaway() == []

        effects = client_engine.encode_headers(stream_id=1, status=200)
        assert len(effects) == 1

        effects = client_engine.encode_session_request(stream_id=0, path="/", authority="host", headers={})
        assert len(effects) == 1

        effects = client_engine.encode_stream_creation(stream_id=2, control_stream_id=0, is_unidirectional=True)
        assert len(effects) == 1

    def test_handle_event_client_becomes_ready_via_settings(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        client_engine._state.connection_state = ConnectionState.CONNECTING
        client_engine._state.handshake_complete = True

        mock_h3 = client_engine._h3_engine
        mock_h3._settings_received = False

        def h3_side_effect(event: Any, state: Any) -> tuple[list[Any], list[Any]]:
            mock_h3._settings_received = True
            return ([], [])

        mocker.patch.object(mock_h3, "handle_transport_event", side_effect=h3_side_effect)
        mocker.patch("pywebtransport._protocol.webtransport_engine.get_timestamp", return_value=1.0)

        effects = client_engine.handle_event(event=TransportStreamDataReceived(stream_id=0, data=b"", end_stream=False))

        assert client_engine._state.peer_settings_received is True
        assert cast(Any, client_engine._state.connection_state) == ConnectionState.CONNECTED
        assert any(isinstance(e, EmitConnectionEvent) for e in effects)

    def test_handle_event_connection_close_user(self, client_engine: WebTransportEngine, mocker: MockerFixture) -> None:
        mock_conn = client_engine._connection_processor
        mocker.patch.object(mock_conn, "handle_connection_close", return_value=[])

        client_engine._pending_user_actions.append(UserCreateSession(path="/", headers={}, request_id=1))

        effects = client_engine.handle_event(event=ConnectionClose(error_code=0, reason="", request_id=0))

        cast(MagicMock, mock_conn.handle_connection_close).assert_called()
        assert len(client_engine._pending_user_actions) == 0
        assert any(isinstance(e, NotifyRequestFailed) for e in effects)

    def test_handle_event_internal_lifecycle_events(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        mock_conn = client_engine._connection_processor
        mock_stream = client_engine._stream_processor

        mocker.patch.object(mock_conn, "handle_internal_bind_h3_session", return_value=[])
        mocker.patch.object(mock_stream, "handle_internal_bind_quic_stream", return_value=[])
        mocker.patch.object(mock_conn, "handle_cleanup_early_events", return_value=[])
        mocker.patch.object(mock_conn, "handle_cleanup_resources", return_value=[])
        mocker.patch.object(mock_conn, "handle_internal_fail_h3_session", return_value=[])
        mocker.patch.object(mock_stream, "handle_internal_fail_quic_stream", return_value=[])
        mocker.patch.object(mock_stream, "handle_return_stream_data", return_value=[])

        client_engine.handle_event(event=InternalBindH3Session(stream_id=0, request_id=1))
        cast(MagicMock, mock_conn.handle_internal_bind_h3_session).assert_called_once()

        client_engine.handle_event(
            event=InternalBindQuicStream(stream_id=1, session_id=0, is_unidirectional=False, request_id=1)
        )
        cast(MagicMock, mock_stream.handle_internal_bind_quic_stream).assert_called_once()

        client_engine.handle_event(event=InternalCleanupEarlyEvents())
        cast(MagicMock, mock_conn.handle_cleanup_early_events).assert_called_once()

        client_engine.handle_event(event=InternalCleanupResources())
        cast(MagicMock, mock_conn.handle_cleanup_resources).assert_called_once()

        client_engine.handle_event(event=InternalFailH3Session(exception=Exception(), request_id=1))
        cast(MagicMock, mock_conn.handle_internal_fail_h3_session).assert_called_once()

        client_engine.handle_event(
            event=InternalFailQuicStream(session_id=0, is_unidirectional=True, exception=Exception(), request_id=1)
        )
        cast(MagicMock, mock_stream.handle_internal_fail_quic_stream).assert_called_once()

        client_engine.handle_event(event=InternalReturnStreamData(stream_id=1, data=b""))
        cast(MagicMock, mock_stream.handle_return_stream_data).assert_called_once()

    def test_handle_event_protocol_events_delegation(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        mock_conn = client_engine._connection_processor
        mock_sess = client_engine._session_processor
        mock_stream = client_engine._stream_processor

        mocker.patch.object(mock_conn, "handle_transport_parameters_received", return_value=[])
        mocker.patch.object(mock_stream, "handle_transport_stream_reset", return_value=[])
        mocker.patch.object(mock_sess, "handle_capsule_received", return_value=[])
        mocker.patch.object(mock_sess, "handle_connect_stream_closed", return_value=[])
        mocker.patch.object(mock_sess, "handle_datagram_received", return_value=[])
        mocker.patch.object(mock_conn, "handle_goaway_received", return_value=[])
        mocker.patch.object(mock_conn, "handle_headers_received", return_value=[])
        mocker.patch.object(mock_stream, "handle_webtransport_stream_data", return_value=[])

        client_engine.handle_event(event=TransportQuicParametersReceived(remote_max_datagram_frame_size=100))
        cast(MagicMock, mock_conn.handle_transport_parameters_received).assert_called()

        client_engine.handle_event(event=TransportStreamReset(stream_id=1, error_code=0))
        cast(MagicMock, mock_stream.handle_transport_stream_reset).assert_called()

        client_engine.handle_event(event=CapsuleReceived(stream_id=0, capsule_type=1, capsule_data=b""))
        cast(MagicMock, mock_sess.handle_capsule_received).assert_called()

        client_engine.handle_event(event=ConnectStreamClosed(stream_id=0))
        cast(MagicMock, mock_sess.handle_connect_stream_closed).assert_called()

        client_engine.handle_event(event=DatagramReceived(stream_id=0, data=b""))
        cast(MagicMock, mock_sess.handle_datagram_received).assert_called()

        client_engine.handle_event(event=GoawayReceived())
        cast(MagicMock, mock_conn.handle_goaway_received).assert_called()

        client_engine.handle_event(event=HeadersReceived(stream_id=0, headers=[], stream_ended=False))
        cast(MagicMock, mock_conn.handle_headers_received).assert_called()

        client_engine.handle_event(
            event=WebTransportStreamDataReceived(stream_id=1, session_id=0, data=b"", stream_ended=False)
        )
        cast(MagicMock, mock_stream.handle_webtransport_stream_data).assert_called()

    def test_handle_event_requeues_pending_actions(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        client_engine._state.connection_state = ConnectionState.CONNECTING
        pending_action = UserCreateSession(path="/", headers={}, request_id=1)
        client_engine._pending_user_actions.append(pending_action)

        mocker.patch.object(client_engine._connection_processor, "handle_create_session", return_value=[])

        client_engine._state.handshake_complete = True
        client_engine._state.peer_settings_received = True
        client_engine.handle_event(event=TransportHandshakeCompleted())

        assert len(client_engine._pending_user_actions) == 0
        cast(MagicMock, client_engine._connection_processor.handle_create_session).assert_called_once()

    def test_handle_event_settings_received(self, client_engine: WebTransportEngine) -> None:
        settings = {
            constants.SETTINGS_WT_INITIAL_MAX_DATA: 1000,
            constants.SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI: 10,
            constants.SETTINGS_WT_INITIAL_MAX_STREAMS_UNI: 5,
        }
        client_engine.handle_event(event=SettingsReceived(settings=settings))
        assert client_engine._state.peer_initial_max_data == 1000
        assert client_engine._state.peer_initial_max_streams_bidi == 10
        assert client_engine._state.peer_initial_max_streams_uni == 5

    def test_handle_event_transport_connection_terminated(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        mock_conn = client_engine._connection_processor
        mocker.patch.object(mock_conn, "handle_connection_terminated", return_value=[])

        mock_user_event = mocker.Mock(spec=UserEvent)
        mock_user_event.request_id = 1
        client_engine._pending_user_actions.append(mock_user_event)

        effects = client_engine.handle_event(event=TransportConnectionTerminated(error_code=0, reason_phrase=""))

        cast(MagicMock, mock_conn.handle_connection_terminated).assert_called_once()
        assert any(isinstance(e, NotifyRequestFailed) for e in effects)
        assert len(client_engine._pending_user_actions) == 0

    def test_handle_event_transport_data_client_already_ready(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        mock_h3 = client_engine._h3_engine
        mock_h3._settings_received = True

        mocker.patch.object(mock_h3, "handle_transport_event", return_value=([], []))

        effects = client_engine.handle_event(event=TransportStreamDataReceived(stream_id=0, data=b"", end_stream=False))

        cast(MagicMock, mock_h3.handle_transport_event).assert_called()
        assert not any(isinstance(e, EmitConnectionEvent) for e in effects)

    @pytest.mark.parametrize(
        "event",
        [
            TransportStreamDataReceived(stream_id=0, data=b"", end_stream=False),
            TransportDatagramFrameReceived(data=b""),
        ],
    )
    def test_handle_event_transport_data_delegation_client(
        self, client_engine: WebTransportEngine, mocker: MockerFixture, event: ProtocolEvent
    ) -> None:
        mock_h3 = client_engine._h3_engine
        mocker.patch.object(mock_h3, "handle_transport_event", return_value=([], []))

        client_engine.handle_event(event=event)

        cast(MagicMock, mock_h3.handle_transport_event).assert_called()

    def test_handle_event_transport_data_delegation_server(
        self, server_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        mock_h3 = server_engine._h3_engine
        mock_h3._settings_received = True
        mocker.patch.object(mock_h3, "handle_transport_event", return_value=([], []))

        effects = server_engine.handle_event(event=TransportStreamDataReceived(stream_id=0, data=b"", end_stream=False))

        cast(MagicMock, mock_h3.handle_transport_event).assert_called()
        assert not any(isinstance(e, EmitConnectionEvent) for e in effects)

    def test_handle_event_transport_data_settings_received_before_handshake(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        client_engine._state.handshake_complete = False
        client_engine._state.connection_state = ConnectionState.CONNECTING

        mock_h3 = client_engine._h3_engine
        mock_h3._settings_received = False

        def h3_side_effect(event: Any, state: Any) -> tuple[list[Any], list[Any]]:
            mock_h3._settings_received = True
            return ([], [])

        mocker.patch.object(mock_h3, "handle_transport_event", side_effect=h3_side_effect)

        effects = client_engine.handle_event(
            event=TransportStreamDataReceived(stream_id=0, data=b"settings", end_stream=False)
        )

        assert client_engine._state.peer_settings_received is True
        assert not any(isinstance(e, EmitConnectionEvent) for e in effects)

    def test_handle_event_transport_handshake_completed_client_flow(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        mocker.patch("pywebtransport._protocol.webtransport_engine.get_timestamp", return_value=123.0)

        client_engine.handle_event(event=TransportHandshakeCompleted())
        assert client_engine._state.connection_state == ConnectionState.CONNECTING
        assert client_engine._state.handshake_complete is True
        assert client_engine._state.peer_settings_received is False

        client_engine._state.peer_settings_received = True
        effects = client_engine.handle_event(event=TransportHandshakeCompleted())

        assert cast(Any, client_engine._state.connection_state) == ConnectionState.CONNECTED
        assert client_engine._state.connected_at == 123.0
        assert any(
            isinstance(e, EmitConnectionEvent) and e.event_type == EventType.CONNECTION_ESTABLISHED for e in effects
        )

    def test_handle_event_transport_handshake_completed_client_idempotent(
        self, client_engine: WebTransportEngine
    ) -> None:
        client_engine._state.connection_state = ConnectionState.CONNECTING
        client_engine._state.handshake_complete = False

        client_engine.handle_event(event=TransportHandshakeCompleted())

        assert client_engine._state.connection_state == ConnectionState.CONNECTING
        assert client_engine._state.handshake_complete is True

    def test_handle_event_transport_handshake_completed_server(
        self, server_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        server_engine._state.connection_state = ConnectionState.CONNECTING
        mocker.patch("pywebtransport._protocol.webtransport_engine.get_timestamp", return_value=456.0)

        effects = server_engine.handle_event(event=TransportHandshakeCompleted())

        assert server_engine._state.handshake_complete is True
        assert cast(Any, server_engine._state.connection_state) == ConnectionState.CONNECTED
        assert server_engine._state.connected_at == 456.0
        assert any(
            isinstance(e, EmitConnectionEvent) and e.event_type == EventType.CONNECTION_ESTABLISHED for e in effects
        )

    @pytest.mark.parametrize("state", [ConnectionState.CONNECTED, ConnectionState.CLOSED, ConnectionState.DRAINING])
    def test_handle_event_transport_handshake_completed_unexpected_state(
        self, client_engine: WebTransportEngine, caplog: pytest.LogCaptureFixture, state: ConnectionState
    ) -> None:
        client_engine._state.connection_state = state

        client_engine.handle_event(event=TransportHandshakeCompleted())

        assert "Received TransportHandshakeCompleted in unexpected state" in caplog.text

    def test_handle_event_unknown_event(
        self, client_engine: WebTransportEngine, caplog: pytest.LogCaptureFixture
    ) -> None:

        class UnknownEvent(ProtocolEvent):
            pass

        effects = client_engine.handle_event(event=UnknownEvent())

        assert "Unhandled event type in engine" in caplog.text
        assert len(effects) == 1
        assert isinstance(effects[0], RescheduleQuicTimer)

    def test_handle_event_user_actions_delegation(
        self, client_engine: WebTransportEngine, mocker: MockerFixture
    ) -> None:
        client_engine._state.connection_state = ConnectionState.CONNECTED
        mock_conn = client_engine._connection_processor
        mock_sess = client_engine._session_processor
        mock_stream = client_engine._stream_processor

        mocker.patch.object(mock_sess, "handle_accept_session", return_value=[])
        mocker.patch.object(mock_sess, "handle_close_session", return_value=[])
        mocker.patch.object(mock_conn, "handle_graceful_close", return_value=[])
        mocker.patch.object(mock_conn, "handle_create_session", return_value=[])
        mocker.patch.object(mock_sess, "handle_create_stream", return_value=[])
        mocker.patch.object(mock_conn, "handle_get_connection_diagnostics", return_value=[])
        mocker.patch.object(mock_sess, "handle_get_session_diagnostics", return_value=[])
        mocker.patch.object(mock_stream, "handle_get_stream_diagnostics", return_value=[])
        mocker.patch.object(mock_sess, "handle_grant_data_credit", return_value=[])
        mocker.patch.object(mock_sess, "handle_grant_streams_credit", return_value=[])
        mocker.patch.object(mock_sess, "handle_reject_session", return_value=[])
        mocker.patch.object(mock_stream, "handle_reset_stream", return_value=[])
        mocker.patch.object(mock_sess, "handle_send_datagram", return_value=[])
        mocker.patch.object(mock_stream, "handle_send_stream_data", return_value=[])
        mocker.patch.object(mock_stream, "handle_stop_stream", return_value=[])
        mocker.patch.object(mock_stream, "handle_stream_read", return_value=[])

        client_engine.handle_event(event=UserAcceptSession(session_id=0, request_id=1))
        cast(MagicMock, mock_sess.handle_accept_session).assert_called()

        client_engine.handle_event(event=UserCloseSession(session_id=0, error_code=0, reason="", request_id=1))
        cast(MagicMock, mock_sess.handle_close_session).assert_called()

        client_engine.handle_event(event=UserConnectionGracefulClose(request_id=1))
        cast(MagicMock, mock_conn.handle_graceful_close).assert_called()

        client_engine.handle_event(event=UserCreateSession(path="/", headers={}, request_id=1))
        cast(MagicMock, mock_conn.handle_create_session).assert_called()

        client_engine.handle_event(event=UserCreateStream(session_id=0, is_unidirectional=True, request_id=1))
        cast(MagicMock, mock_sess.handle_create_stream).assert_called()

        client_engine.handle_event(event=UserGetConnectionDiagnostics(request_id=1))
        cast(MagicMock, mock_conn.handle_get_connection_diagnostics).assert_called()

        client_engine.handle_event(event=UserGetSessionDiagnostics(session_id=0, request_id=1))
        cast(MagicMock, mock_sess.handle_get_session_diagnostics).assert_called()

        client_engine.handle_event(event=UserGetStreamDiagnostics(stream_id=1, request_id=1))
        cast(MagicMock, mock_stream.handle_get_stream_diagnostics).assert_called()

        client_engine.handle_event(event=UserGrantDataCredit(session_id=0, max_data=1000, request_id=1))
        cast(MagicMock, mock_sess.handle_grant_data_credit).assert_called()

        client_engine.handle_event(
            event=UserGrantStreamsCredit(session_id=0, max_streams=10, is_unidirectional=True, request_id=1)
        )
        cast(MagicMock, mock_sess.handle_grant_streams_credit).assert_called()

        client_engine.handle_event(event=UserRejectSession(session_id=0, status_code=403, request_id=1))
        cast(MagicMock, mock_sess.handle_reject_session).assert_called()

        client_engine.handle_event(event=UserResetStream(stream_id=1, error_code=0, request_id=1))
        cast(MagicMock, mock_stream.handle_reset_stream).assert_called()

        client_engine.handle_event(event=UserSendDatagram(session_id=0, data=b"", request_id=1))
        cast(MagicMock, mock_sess.handle_send_datagram).assert_called()

        client_engine.handle_event(event=UserSendStreamData(stream_id=1, data=b"", end_stream=False, request_id=1))
        cast(MagicMock, mock_stream.handle_send_stream_data).assert_called()

        client_engine.handle_event(event=UserStopStream(stream_id=1, error_code=0, request_id=1))
        cast(MagicMock, mock_stream.handle_stop_stream).assert_called()

        client_engine.handle_event(event=UserStreamRead(stream_id=1, max_bytes=10, request_id=1))
        cast(MagicMock, mock_stream.handle_stream_read).assert_called()

    def test_init(self, client_engine: WebTransportEngine) -> None:
        assert client_engine._state.connection_state == ConnectionState.IDLE
        assert client_engine._is_client is True

    def test_initialize_h3_transport(self, client_engine: WebTransportEngine, mocker: MockerFixture) -> None:
        mocker.patch.object(client_engine._h3_engine, "initialize_connection", return_value=b"settings")

        effects = client_engine.initialize_h3_transport(control_id=2, encoder_id=6, decoder_id=10)

        assert client_engine._h3_engine._local_control_stream_id == 2
        assert len(effects) == 6
        assert isinstance(effects[0], SendQuicData)
        assert effects[0].stream_id == 2

    def test_timer_fired(self, client_engine: WebTransportEngine) -> None:
        effects = client_engine.handle_event(event=TransportQuicTimerFired())

        assert any(isinstance(e, TriggerQuicTimer) for e in effects)
        assert any(isinstance(e, RescheduleQuicTimer) for e in effects)

    def test_user_action_buffering_when_not_connected(self, client_engine: WebTransportEngine) -> None:
        client_engine._state.connection_state = ConnectionState.CONNECTING

        client_engine.handle_event(event=UserCreateSession(path="/", headers={}, request_id=1))
        assert len(client_engine._pending_user_actions) == 1

        client_engine.handle_event(event=UserCreateStream(session_id=0, is_unidirectional=True, request_id=2))
        assert len(client_engine._pending_user_actions) == 2

    def test_user_action_no_buffering_server(self, server_engine: WebTransportEngine, mocker: MockerFixture) -> None:
        server_engine._state.connection_state = ConnectionState.CONNECTING

        mocker.patch.object(server_engine._connection_processor, "handle_create_session", return_value=[])
        mocker.patch.object(server_engine._session_processor, "handle_create_stream", return_value=[])

        server_engine.handle_event(event=UserCreateSession(path="/", headers={}, request_id=1))
        assert len(server_engine._pending_user_actions) == 0

        server_engine.handle_event(event=UserCreateStream(session_id=0, is_unidirectional=True, request_id=2))
        assert len(server_engine._pending_user_actions) == 0
