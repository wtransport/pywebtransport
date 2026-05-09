"""Unit tests for the pywebtransport.connection module."""

import asyncio
import dataclasses
import weakref
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ClientConfig, ConnectionError, ErrorCodes, SessionError, TimeoutError, WebTransportSession
from pywebtransport._controller.controller import EndpointController
from pywebtransport._protocol.events import (
    ProtocolEvent,
    UserCloseConnection,
    UserCloseConnectionGracefully,
    UserCreateSession,
    UserGetConnectionDiagnostics,
)
from pywebtransport.connection import ConnectionDiagnostics, WebTransportConnection
from pywebtransport.types import ConnectionState, EventType, StreamDirection


class TestConnectionDiagnostics:

    def test_init(self) -> None:
        diag = ConnectionDiagnostics(
            active_session_handles=1,
            active_stream_handles=2,
            close_code=None,
            close_reason=None,
            closed_at=None,
            connected_at=100.0,
            connection_handle=42,
            early_event_count=0,
            handshake_complete=True,
            is_client=True,
            local_goaway_sent=False,
            peer_goaway_received=False,
            peer_initial_max_data=10000,
            peer_initial_max_streams_bidi=10,
            peer_initial_max_streams_uni=10,
            peer_max_datagram_frame_size=1200,
            peer_settings_received=True,
            pending_request_count=0,
            session_count=1,
            state=ConnectionState.CONNECTED,
            stream_count=2,
        )

        assert diag.active_session_handles == 1
        assert diag.active_stream_handles == 2
        assert diag.close_code is None
        assert diag.close_reason is None
        assert diag.closed_at is None
        assert diag.connected_at == 100.0
        assert diag.connection_handle == 42
        assert diag.early_event_count == 0
        assert diag.handshake_complete is True
        assert diag.is_client is True
        assert diag.local_goaway_sent is False
        assert diag.peer_goaway_received is False
        assert diag.peer_initial_max_data == 10000
        assert diag.peer_initial_max_streams_bidi == 10
        assert diag.peer_initial_max_streams_uni == 10
        assert diag.peer_max_datagram_frame_size == 1200
        assert diag.peer_settings_received is True
        assert diag.pending_request_count == 0
        assert diag.session_count == 1
        assert diag.state == ConnectionState.CONNECTED
        assert diag.stream_count == 2

        with pytest.raises(expected_exception=dataclasses.FrozenInstanceError):
            cast(Any, diag).state = ConnectionState.CLOSED

        assert not hasattr(diag, "__dict__")


class TestWebTransportConnection:

    @pytest.fixture
    def connection(
        self, mock_config: MagicMock, mock_controller: MagicMock, mocker: MockerFixture
    ) -> WebTransportConnection:
        conn = WebTransportConnection(config=mock_config, controller=mock_controller, handle=42, is_client=True)
        conn.events = mocker.Mock()

        return conn

    @pytest.fixture
    def mock_config(self, mocker: MockerFixture) -> MagicMock:
        conf = mocker.Mock(spec=ClientConfig)
        conf.max_event_listeners = 100
        conf.event_history_capacity = 100
        conf.event_queue_capacity = 100
        conf.close_timeout = 5.0

        return cast(MagicMock, conf)

    @pytest.fixture
    def mock_controller(self, mocker: MockerFixture) -> MagicMock:
        ctrl = mocker.Mock(spec=EndpointController)
        ctrl.execute_request = mocker.AsyncMock()
        ctrl.get_remote_address.return_value = ("127.0.0.1", 443)
        ctrl.get_local_addresses.return_value = [("0.0.0.0", 12345)]

        return cast(MagicMock, ctrl)

    @pytest.fixture
    def mock_session_cls(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch(target="pywebtransport.connection.WebTransportSession")

    def test_accept_factory(self, mock_controller: MagicMock, mock_config: MagicMock) -> None:
        conn = WebTransportConnection.accept(controller=mock_controller, handle=42, config=mock_config)

        assert isinstance(conn, WebTransportConnection)
        assert conn.is_client is False
        assert conn.config == mock_config
        assert conn._handle == 42

    def test_address_properties(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        assert connection.remote_address == ("127.0.0.1", 443)
        assert connection.local_addresses == [("0.0.0.0", 12345)]
        mock_controller.get_remote_address.assert_called_once_with(handle=42)
        mock_controller.get_local_addresses.assert_called_once()

        mock_controller.get_remote_address.reset_mock()

        assert connection.remote_address == ("127.0.0.1", 443)
        mock_controller.get_remote_address.assert_called_once_with(handle=42)

    def test_address_properties_local_empty(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.get_local_addresses.return_value = []

        assert connection.local_addresses == []

    @pytest.mark.asyncio
    async def test_close_already_closed(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        connection._cached_state = ConnectionState.CLOSED

        await connection.close()

        mock_controller.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_connection_error_channel_closed(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="channel closed")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close()

        spy_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_connection_error_warning(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="Something failed")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close()

        spy_logger.warning.assert_called_with("wt_connection close failed connection_handle=%d err=%s", 42, mocker.ANY)

    @pytest.mark.asyncio
    async def test_close_generic_exception(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ValueError("Unexpected")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close()

        spy_logger.warning.assert_called_with("wt_connection close failed connection_handle=%d err=%s", 42, mocker.ANY)

    @pytest.mark.asyncio
    async def test_close_idempotent(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        connection._cached_state = ConnectionState.CLOSED

        await connection.close()

        mock_controller.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_success(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        async def fake_execute(dispatcher: Any) -> None:
            dispatcher(123)

        mock_controller.execute_request.side_effect = fake_execute

        await connection.close()

        mock_controller.send_user_event.assert_called_once()
        kwargs = mock_controller.send_user_event.call_args[1]
        event = kwargs["event"]

        assert kwargs["handle"] == 42
        assert isinstance(event, UserCloseConnection)
        assert event.request_id == 123
        assert event.error_code == ErrorCodes.APP_NO_ERROR
        assert event.reason == "wt_connection close"

    @pytest.mark.asyncio
    async def test_close_timeout_or_cancelled(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = asyncio.TimeoutError
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close()

        spy_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_context_manager(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock)

        async with connection as c:
            assert c is connection

        spy_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_session_cancelled(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await connection.create_session(path="/")

    @pytest.mark.asyncio
    async def test_create_session_connection_error_propagates(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="Fail")

        with pytest.raises(expected_exception=ConnectionError):
            await connection.create_session(path="/")

    @pytest.mark.asyncio
    async def test_create_session_generic_error(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = ValueError("Fail")

        with pytest.raises(expected_exception=SessionError, match="wt_session create failed connection_handle=42"):
            await connection.create_session(path="/")

    @pytest.mark.asyncio
    async def test_create_session_handle_missing(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.return_value = 1

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed"):
            await connection.create_session(path="/")

    @pytest.mark.asyncio
    async def test_create_session_server_error(self, connection: WebTransportConnection) -> None:
        connection._is_client = False

        with pytest.raises(
            expected_exception=ConnectionError, match="wt_connection validate failed actual=False expected=true"
        ):
            await connection.create_session(path="/")

    @pytest.mark.asyncio
    async def test_create_session_success(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        async def fake_execute(dispatcher: Any) -> int:
            dispatcher(100)
            return 1

        mock_controller.execute_request.side_effect = fake_execute
        session_mock = mocker.Mock(spec=WebTransportSession)
        connection._session_handles[1] = session_mock

        session = await connection.create_session(path="/", headers={"a": "b"}, wt_available_protocols=["h3"])

        assert session is session_mock
        mock_controller.send_user_event.assert_called_once()
        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserCreateSession)
        assert event.request_id == 100
        assert event.path == "/"
        assert event.headers == {"a": "b"}
        assert event.wt_available_protocols == ["h3"]

    @pytest.mark.asyncio
    async def test_create_session_timeout(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        mock_controller.execute_request.side_effect = asyncio.TimeoutError

        with pytest.raises(expected_exception=TimeoutError, match="wt_session create failed connection_handle=42"):
            await connection.create_session(path="/")

    @pytest.mark.asyncio
    async def test_diagnostics_cancelled(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        mock_controller.execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await connection.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_connection_error_propagates(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="Fail")

        with pytest.raises(expected_exception=ConnectionError):
            await connection.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_generic_error(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = ValueError("Fail")

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await connection.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_success(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        diag_raw = {
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
            "connected_at": 1.0,
            "connection_handle": 42,
            "early_event_count": 0,
            "handshake_complete": True,
            "is_client": True,
            "local_goaway_sent": False,
            "peer_goaway_received": False,
            "peer_initial_max_data": 10000,
            "peer_initial_max_streams_bidi": 10,
            "peer_initial_max_streams_uni": 10,
            "peer_max_datagram_frame_size": 1200,
            "peer_settings_received": True,
            "pending_request_count": 0,
            "session_count": 1,
            "state": ConnectionState.CONNECTED,
            "stream_count": 0,
        }

        async def fake_execute(dispatcher: Any) -> dict[str, Any]:
            dispatcher(1)
            return diag_raw

        mock_controller.execute_request.side_effect = fake_execute

        diag = await connection.diagnostics()

        assert isinstance(diag, ConnectionDiagnostics)
        assert diag.connection_handle == 42
        assert diag.active_session_handles == 0
        mock_controller.send_user_event.assert_called_once()

        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserGetConnectionDiagnostics)
        assert event.request_id == 1

    @pytest.mark.asyncio
    async def test_execute_request_success(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        async def fake_execute(dispatcher: Any) -> str:
            dispatcher(99)
            return "done"

        mock_controller.execute_request.side_effect = fake_execute
        event_mock = mocker.Mock(spec=ProtocolEvent)

        result = await connection.execute_request(event_factory=lambda req_id: event_mock)

        assert result == "done"
        mock_controller.execute_request.assert_called_once()
        mock_controller.send_user_event.assert_called_once_with(handle=42, event=event_mock)

    def test_get_all_sessions(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        s1 = mocker.Mock()
        connection._session_handles[1] = s1

        assert connection.get_all_sessions() == [s1]

    @pytest.mark.asyncio
    async def test_graceful_shutdown_connection_error_channel_closed(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="channel closed")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.graceful_shutdown()

        spy_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_graceful_shutdown_connection_error_warning(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="Fail")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.graceful_shutdown()

        spy_logger.warning.assert_any_call("wt_connection drain failed connection_handle=%d err=%s", 42, mocker.ANY)

    @pytest.mark.asyncio
    async def test_graceful_shutdown_generic_error(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ValueError("Error")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.graceful_shutdown()

        spy_logger.warning.assert_any_call("wt_connection drain failed connection_handle=%d err=%s", 42, mocker.ANY)

    @pytest.mark.asyncio
    async def test_graceful_shutdown_success(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        async def fake_execute(dispatcher: Any) -> None:
            dispatcher(1)

        mock_controller.execute_request.side_effect = fake_execute
        mock_close = mocker.patch.object(
            target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock
        )

        await connection.graceful_shutdown()

        mock_controller.send_user_event.assert_called_once()
        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        assert isinstance(kwargs["event"], UserCloseConnectionGracefully)

        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_graceful_shutdown_timeout_or_cancelled(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = asyncio.TimeoutError
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.graceful_shutdown()

        spy_logger.warning.assert_any_call("wt_connection drain failed connection_handle=%d", 42)

    def test_handle_session_event_client_ready(
        self, connection: WebTransportConnection, mock_session_cls: MagicMock
    ) -> None:
        data = {"session_id": 1, "path": "/", "headers": {}, "wt_available_protocols": ["h3"], "wt_protocol": "h3"}

        connection._notify_owner(event_type=EventType.SESSION_READY, data=data)

        assert 1 in connection._session_handles
        assert connection._session_handles[1] == mock_session_cls.return_value
        assert data["session"] == mock_session_cls.return_value
        mock_session_cls.assert_called_once_with(
            connection=connection, session_id=1, path="/", headers={}, wt_available_protocols=["h3"], wt_protocol="h3"
        )

    def test_handle_session_event_handle_exists(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        data = {"session_id": 1, "path": "/", "headers": {}}
        existing_session = mocker.Mock()
        connection._session_handles[1] = existing_session

        connection._notify_owner(event_type=EventType.SESSION_READY, data=data)

        assert connection._session_handles[1] is existing_session

    def test_handle_session_event_missing_metadata(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"session_id": 1}

        connection._notify_owner(event_type=EventType.SESSION_READY, data=data)

        assert 1 not in connection._session_handles
        spy_logger.warning.assert_called_with("wt_session validate failed session_id=%d", 1)

    def test_handle_session_event_no_id(self, connection: WebTransportConnection) -> None:
        connection._notify_owner(event_type=EventType.SESSION_READY, data={})

        assert not connection._session_handles

    def test_handle_session_event_server_request(
        self, connection: WebTransportConnection, mock_session_cls: MagicMock
    ) -> None:
        connection._is_client = False
        data = {"session_id": 2, "path": "/", "headers": {}}

        connection._notify_owner(event_type=EventType.SESSION_REQUEST, data=data)

        assert 2 in connection._session_handles

    def test_handle_stream_event_closed(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        mocker.patch(target="asyncio.create_task")
        stream_handle = mocker.Mock()
        stream_handle.events = mocker.Mock()
        connection._stream_handles[10] = stream_handle
        data = {"stream_id": 10}

        connection._notify_owner(event_type=EventType.STREAM_CLOSED, data=data)

        assert 10 not in connection._stream_handles
        cast(MagicMock, stream_handle.events.emit_nowait).assert_called()

    def test_handle_stream_event_closed_missing(self, connection: WebTransportConnection) -> None:
        data = {"stream_id": 999}

        connection._notify_owner(event_type=EventType.STREAM_CLOSED, data=data)

        assert 999 not in connection._stream_handles

    def test_handle_stream_event_dispatch_unknown_type(self, connection: WebTransportConnection) -> None:
        connection._handle_stream_event(event_type=EventType.DATAGRAM_RECEIVED, data={"stream_id": 1})

        assert True

    def test_handle_stream_event_no_id(self, connection: WebTransportConnection) -> None:
        connection._notify_owner(event_type=EventType.STREAM_OPENED, data={})

        assert not connection._stream_handles

    def test_handle_stream_event_opened_invalid_direction(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        session_handle = mocker.Mock()
        connection._session_handles[1] = session_handle
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"stream_id": 10, "session_id": 1, "direction": 999, "is_remote": False}

        connection._notify_owner(event_type=EventType.STREAM_OPENED, data=data)

        assert 10 not in connection._stream_handles
        spy_logger.warning.assert_called_with("wt_stream validate invalid actual=%s stream_id=%d", 999, 10)

    def test_handle_stream_event_opened_missing_metadata(self, connection: WebTransportConnection) -> None:
        data = {"stream_id": 10}

        connection._notify_owner(event_type=EventType.STREAM_OPENED, data=data)

        assert 10 not in connection._stream_handles

    def test_handle_stream_event_opened_missing_session(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"stream_id": 10, "session_id": 999, "direction": StreamDirection.BIDIRECTIONAL}

        connection._notify_owner(event_type=EventType.STREAM_OPENED, data=data)

        spy_logger.warning.assert_called_with("wt_session resolve failed session_id=%d stream_id=%d", 999, 10)
        assert 10 not in connection._stream_handles

    @pytest.mark.parametrize(
        argnames="direction",
        argvalues=[StreamDirection.BIDIRECTIONAL, StreamDirection.SEND_ONLY, StreamDirection.RECEIVE_ONLY],
    )
    def test_handle_stream_event_opened_success(
        self, connection: WebTransportConnection, mocker: MockerFixture, direction: StreamDirection
    ) -> None:
        session_handle = mocker.Mock()
        session_handle.events = mocker.Mock()
        connection._session_handles[1] = session_handle
        mock_bidi = mocker.patch(target="pywebtransport.connection.WebTransportStream", return_value=mocker.Mock())
        mock_send = mocker.patch(target="pywebtransport.connection.WebTransportSendStream", return_value=mocker.Mock())
        mock_recv = mocker.patch(
            target="pywebtransport.connection.WebTransportReceiveStream", return_value=mocker.Mock()
        )
        data = {"stream_id": 10, "session_id": 1, "direction": direction, "is_remote": True}

        connection._notify_owner(event_type=EventType.STREAM_OPENED, data=data)

        assert 10 in connection._stream_handles
        cast(MagicMock, session_handle.events.emit_nowait).assert_called()

        match direction:
            case StreamDirection.BIDIRECTIONAL:
                mock_bidi.assert_called_once_with(session=session_handle, stream_id=10, is_remote=True)
            case StreamDirection.SEND_ONLY:
                mock_send.assert_called_once_with(session=session_handle, stream_id=10, is_remote=True)
            case StreamDirection.RECEIVE_ONLY:
                mock_recv.assert_called_once_with(session=session_handle, stream_id=10, is_remote=True)

    @pytest.mark.parametrize(
        argnames="event_type", argvalues=[EventType.STOP_SENDING_RECEIVED, EventType.STREAM_RESET_RECEIVED]
    )
    def test_handle_stream_event_stop_sending_reset(
        self, connection: WebTransportConnection, mocker: MockerFixture, event_type: EventType
    ) -> None:
        stream_handle = mocker.Mock()
        stream_handle.events = mocker.Mock()
        connection._stream_handles[10] = stream_handle
        data = {"stream_id": 10, "error_code": 123}

        connection._notify_owner(event_type=event_type, data=data)

        cast(MagicMock, stream_handle.events.emit_nowait).assert_called_with(event_type=event_type, data=data)

    def test_handle_stream_event_stop_sending_reset_missing(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"stream_id": 999}

        connection._notify_owner(event_type=EventType.STOP_SENDING_RECEIVED, data=data)

        spy_logger.debug.assert_called_with(
            "wt_stream resolve failed event=%s stream_id=%d", EventType.STOP_SENDING_RECEIVED, 999
        )

    def test_init(
        self,
        connection: WebTransportConnection,
        mock_config: MagicMock,
        mock_controller: MagicMock,
    ) -> None:
        assert connection._config == mock_config
        assert connection._controller == mock_controller
        assert connection._handle == 42
        assert connection._is_client is True
        assert connection._cached_state == ConnectionState.IDLE
        assert connection.events is not None
        assert connection._session_handles == {}
        assert connection._stream_handles == {}

        mock_controller.register_connection.assert_called_once_with(handle=42, callback=connection._notify_owner)

        assert not hasattr(connection, "__dict__")

    def test_notify_owner_connection_closed_cleans_handles(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        connection._cached_state = ConnectionState.CONNECTED
        connection._session_handles[1] = mocker.Mock()
        connection._stream_handles[1] = mocker.Mock()

        connection._notify_owner(event_type=EventType.CONNECTION_CLOSED, data={})

        assert connection.state == ConnectionState.CLOSED
        mock_controller.unregister_connection.assert_called_once_with(handle=42)
        assert len(connection._session_handles) == 0
        assert len(connection._stream_handles) == 0

    def test_notify_owner_connection_events(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        connection._cached_state = ConnectionState.IDLE

        connection._notify_owner(event_type=EventType.CONNECTION_ESTABLISHED, data={})

        assert connection.state == ConnectionState.CONNECTED
        cast(MagicMock, connection.events.emit_nowait).assert_called_with(
            event_type=EventType.CONNECTION_ESTABLISHED, data=mocker.ANY
        )
        assert isinstance(
            cast(MagicMock, connection.events.emit_nowait).call_args[1]["data"]["connection"], weakref.ProxyType
        )

    def test_notify_owner_data_already_populated(self, connection: WebTransportConnection) -> None:
        existing_obj = "mock_obj"
        existing_id = 999
        data = {"connection": existing_obj, "connection_handle": existing_id}

        connection._notify_owner(event_type=EventType.DATAGRAM_RECEIVED, data=data)

        assert data["connection"] == existing_obj
        assert data["connection_handle"] == connection.handle

    def test_notify_owner_exception_handler(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        cast(MagicMock, connection.events.emit_nowait).side_effect = ValueError("Boom")

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection receive failed"):
            connection._notify_owner(event_type=EventType.CONNECTION_ESTABLISHED, data={})

    def test_properties(self, connection: WebTransportConnection) -> None:
        assert connection.handle == 42
        assert connection.state == ConnectionState.IDLE
        assert connection.is_closed is False
        assert connection.is_closing is False
        assert connection.is_connected is False

    def test_remote_address_closed_state(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        connection._cached_state = ConnectionState.CLOSED

        assert connection.remote_address is None

        mock_controller.get_remote_address.assert_not_called()

    def test_remote_address_returns_none(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        mock_controller.get_remote_address.return_value = None

        assert connection.remote_address is None

        mock_controller.get_remote_address.assert_called_once_with(handle=42)

    def test_repr(self, connection: WebTransportConnection) -> None:
        assert "WebTransportConnection" in repr(connection)
        assert "handle=" in repr(connection)

    def test_route_session_event_and_close(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        mocker.patch(target="asyncio.create_task")
        session_handle = mocker.Mock()
        session_handle.events = mocker.Mock()
        connection._session_handles[1] = session_handle

        connection._notify_owner(event_type=EventType.SESSION_DATA_BLOCKED, data={"session_id": 1})

        cast(MagicMock, session_handle.events.emit_nowait).assert_called()

        connection._notify_owner(event_type=EventType.SESSION_CLOSED, data={"session_id": 1})

        assert 1 not in connection._session_handles

    def test_route_session_event_missing_id(self, connection: WebTransportConnection) -> None:
        connection._notify_owner(event_type=EventType.SESSION_DATA_BLOCKED, data={})
        assert True
