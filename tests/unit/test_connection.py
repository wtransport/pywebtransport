"""Unit tests for the pywebtransport.connection module."""

import asyncio
import dataclasses
import weakref
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport import (
    ClientConfig,
    ConnectionError,
    ErrorCodes,
    SessionError,
    StreamError,
    TimeoutError,
    WebTransportConnection,
    WebTransportSession,
)
from pywebtransport._controller.controller import EndpointController
from pywebtransport._controller.events import (
    ProtocolEvent,
    UserCloseConnection,
    UserCloseConnectionGracefully,
    UserCreateSession,
    UserCreateSessionOptimistic,
    UserDiagnoseConnection,
)
from pywebtransport.connection import ConnectionDiagnostics
from pywebtransport.types import ConnectionState, EventType, StreamDirection


class TestConnectionDiagnostics:

    def test_init(self) -> None:
        diagnostics = ConnectionDiagnostics(
            close_code=None,
            close_reason=None,
            closed_at=None,
            connected_at=100.0,
            connection_handle=42,
            handshake_completed=True,
            is_client=True,
            local_goaway_sent=False,
            peer_goaway_received=False,
            peer_initial_max_data=4 * 1024 * 1024,
            peer_initial_max_streams_bidi=10,
            peer_initial_max_streams_uni=10,
            peer_max_datagram_frame_size=1350,
            peer_settings_received=True,
            state=ConnectionState.CONNECTED,
        )

        assert diagnostics.close_code is None
        assert diagnostics.close_reason is None
        assert diagnostics.closed_at is None
        assert diagnostics.connected_at == 100.0
        assert diagnostics.connection_handle == 42
        assert diagnostics.handshake_completed is True
        assert diagnostics.is_client is True
        assert diagnostics.local_goaway_sent is False
        assert diagnostics.peer_goaway_received is False
        assert diagnostics.peer_initial_max_data == 4 * 1024 * 1024
        assert diagnostics.peer_initial_max_streams_bidi == 10
        assert diagnostics.peer_initial_max_streams_uni == 10
        assert diagnostics.peer_max_datagram_frame_size == 1350
        assert diagnostics.peer_settings_received is True
        assert diagnostics.state == ConnectionState.CONNECTED

        with pytest.raises(expected_exception=dataclasses.FrozenInstanceError):
            cast(Any, diagnostics).state = ConnectionState.CLOSED

        assert not hasattr(diagnostics, "__dict__")


class TestWebTransportConnection:

    @pytest.fixture
    def connection(
        self, mock_config: MagicMock, mock_controller: MagicMock, mocker: MockerFixture
    ) -> WebTransportConnection:
        connection = WebTransportConnection(config=mock_config, controller=mock_controller, handle=42, is_client=True)
        connection.events = mocker.Mock()

        return connection

    @pytest.fixture
    def mock_config(self, mocker: MockerFixture) -> MagicMock:
        config = mocker.Mock(spec=ClientConfig)
        config.close_timeout = 5.0

        return cast(MagicMock, config)

    @pytest.fixture
    def mock_controller(self, mocker: MockerFixture) -> MagicMock:
        controller = mocker.Mock(spec=EndpointController)
        controller.execute_request = mocker.AsyncMock()
        controller.get_remote_address.return_value = ("127.0.0.1", 443)

        return cast(MagicMock, controller)

    @pytest.fixture
    def mock_session_class(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch(target="pywebtransport.connection.WebTransportSession")

    @pytest.mark.asyncio
    async def test_close_already_closed(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        connection._handle_closed()

        await connection.close()

        mock_controller.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_cancelled(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        mock_controller.execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await connection.close()

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
        mock_controller.execute_request.side_effect = ConnectionError(message="something failed")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close()

        spy_logger.warning.assert_called_with("wt_connection close failed connection_handle=%d err=%s", 42, mocker.ANY)

    @pytest.mark.asyncio
    async def test_close_generic_error(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ValueError("unexpected")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close()

        spy_logger.warning.assert_called_with("wt_connection close failed connection_handle=%d err=%s", 42, mocker.ANY)

    @pytest.mark.asyncio
    async def test_close_gracefully_already_closed(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_close = mocker.patch.object(
            target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock
        )
        connection._handle_closed()

        await connection.close_gracefully()

        mock_controller.execute_request.assert_not_called()
        mock_close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_gracefully_cancelled(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = asyncio.CancelledError
        mock_close = mocker.patch.object(
            target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock
        )

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await connection.close_gracefully()

        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_gracefully_connection_error_channel_closed(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="channel closed")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close_gracefully()

        spy_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_gracefully_connection_error_warning(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="fail")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close_gracefully()

        spy_logger.warning.assert_any_call("wt_connection drain failed connection_handle=%d err=%s", 42, mocker.ANY)

    @pytest.mark.asyncio
    async def test_close_gracefully_generic_error(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_controller.execute_request.side_effect = ValueError("error")
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close_gracefully()

        spy_logger.warning.assert_any_call("wt_connection drain failed connection_handle=%d err=%s", 42, mocker.ANY)

    @pytest.mark.asyncio
    async def test_close_gracefully_success(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> None:
            dispatcher(1)

        mock_controller.execute_request.side_effect = fake_execute
        mock_close = mocker.patch.object(
            target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock
        )

        await connection.close_gracefully()

        mock_controller.send_protocol_event.assert_called_once()
        kwargs = mock_controller.send_protocol_event.call_args[1]

        assert kwargs["handle"] == 42
        assert isinstance(kwargs["event"], UserCloseConnectionGracefully)

        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_gracefully_timeout(
        self,
        connection: WebTransportConnection,
        mock_config: MagicMock,
        mock_controller: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> None:
            await asyncio.Event().wait()

        mock_config.close_timeout = 0.01
        mock_controller.execute_request.side_effect = fake_execute
        mock_close = mocker.patch.object(
            target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock
        )
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close_gracefully()

        spy_logger.warning.assert_called_once_with("wt_connection drain failed connection_handle=%d", 42)
        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_gracefully_waits_for_open_sessions(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> None:
            dispatcher(1)

        mock_controller.execute_request.side_effect = fake_execute
        mocker.patch.object(target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock)
        open_session = mocker.Mock(spec=WebTransportSession)
        open_session.is_closed = False
        open_session.events = mocker.Mock()
        open_session.events.wait_for = mocker.AsyncMock()
        closed_session = mocker.Mock(spec=WebTransportSession)
        closed_session.is_closed = True
        closed_session.events = mocker.Mock()
        closed_session.events.wait_for = mocker.AsyncMock()
        connection._session_handles[1] = open_session
        connection._session_handles[2] = closed_session

        await connection.close_gracefully()

        open_session.events.wait_for.assert_awaited_once_with(event_type=EventType.SESSION_CLOSED)
        closed_session.events.wait_for.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_success(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> None:
            dispatcher(123)

        mock_controller.execute_request.side_effect = fake_execute

        await connection.close()

        mock_controller.send_protocol_event.assert_called_once()
        kwargs = mock_controller.send_protocol_event.call_args[1]
        event = kwargs["event"]

        assert kwargs["handle"] == 42
        assert kwargs["request_id"] == 123
        assert isinstance(event, UserCloseConnection)
        assert event.request_id == 123
        assert event.error_code == ErrorCodes.H3_NO_ERROR
        assert event.reason is None

    @pytest.mark.asyncio
    async def test_close_timeout(
        self,
        connection: WebTransportConnection,
        mock_config: MagicMock,
        mock_controller: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> None:
            await asyncio.Event().wait()

        mock_config.close_timeout = 0.01
        mock_controller.execute_request.side_effect = fake_execute
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")

        await connection.close()

        spy_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_context_manager(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock)

        async with connection as entered_connection:
            assert entered_connection is connection

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.H3_NO_ERROR)

    @pytest.mark.asyncio
    async def test_context_manager_cancelled(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=asyncio.CancelledError):
            async with connection:
                raise asyncio.CancelledError

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.H3_NO_ERROR)

    @pytest.mark.asyncio
    async def test_context_manager_exception_with_error_code(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=ConnectionError):
            async with connection:
                raise ConnectionError(message="fail", error_code=123)

        spy_close.assert_awaited_once_with(error_code=123)

    @pytest.mark.asyncio
    async def test_context_manager_generic_error(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(target=WebTransportConnection, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=RuntimeError):
            async with connection:
                raise RuntimeError("fail")

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.H3_INTERNAL_ERROR)

    @pytest.mark.asyncio
    async def test_create_session_cancelled(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await connection.create_session(authority="localhost:443", path="/")

    @pytest.mark.asyncio
    async def test_create_session_closed(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        connection._handle_closed()

        with pytest.raises(
            expected_exception=ConnectionError, match="wt_connection resolve failed connection_handle=42"
        ):
            await connection.create_session(authority="localhost:443", path="/")

        mock_controller.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_session_connection_error(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await connection.create_session(authority="localhost:443", path="/")

    @pytest.mark.asyncio
    async def test_create_session_generic_error(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=SessionError, match="wt_session create failed connection_handle=42"):
            await connection.create_session(authority="localhost:443", path="/")

    @pytest.mark.asyncio
    async def test_create_session_handle_missing(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> Any:
            dispatcher(100)
            return resolver(1)

        mock_controller.execute_request.side_effect = fake_execute

        with pytest.raises(expected_exception=SessionError, match="wt_session create failed") as exception_info:
            await connection.create_session(authority="localhost:443", path="/")

        assert "wt_session resolve failed session_id=1" in str(exception_info.value.__cause__)

    @pytest.mark.asyncio
    async def test_create_session_optimistic_success(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> Any:
            dispatcher(100)
            return resolver(1)

        mock_controller.execute_request.side_effect = fake_execute
        session_mock = mocker.Mock(spec=WebTransportSession)
        connection._session_handles[1] = session_mock

        session = await connection.create_session_optimistic(
            authority="localhost:443", path="/", headers={"a": "b"}, wt_available_protocols=["h3"]
        )

        assert session is session_mock
        mock_controller.send_protocol_event.assert_called_once()
        kwargs = mock_controller.send_protocol_event.call_args[1]

        assert kwargs["handle"] == 42
        assert kwargs["request_id"] == 100
        event = kwargs["event"]
        assert isinstance(event, UserCreateSessionOptimistic)
        assert event.request_id == 100
        assert event.authority == "localhost:443"
        assert event.path == "/"
        assert event.headers == {"a": "b"}
        assert event.wt_available_protocols == ["h3"]

    @pytest.mark.asyncio
    async def test_create_session_server_error(self, connection: WebTransportConnection) -> None:
        connection._is_client = False

        with pytest.raises(
            expected_exception=ConnectionError, match="wt_connection validate failed actual=False expected=true"
        ):
            await connection.create_session(authority="localhost:443", path="/")

    @pytest.mark.asyncio
    async def test_create_session_success(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> Any:
            dispatcher(100)
            return resolver(1)

        mock_controller.execute_request.side_effect = fake_execute
        session_mock = mocker.Mock(spec=WebTransportSession)
        connection._session_handles[1] = session_mock

        session = await connection.create_session(authority="localhost:443", path="/")

        assert session is session_mock
        mock_controller.send_protocol_event.assert_called_once()
        kwargs = mock_controller.send_protocol_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserCreateSession)
        assert event.request_id == 100
        assert event.authority == "localhost:443"
        assert event.path == "/"
        assert event.headers == {}
        assert event.wt_available_protocols is None

    @pytest.mark.asyncio
    async def test_create_session_timeout(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        mock_controller.execute_request.side_effect = asyncio.TimeoutError

        with pytest.raises(expected_exception=TimeoutError, match="wt_session create failed connection_handle=42"):
            await connection.create_session(authority="localhost:443", path="/")

    @pytest.mark.asyncio
    async def test_diagnose_cancelled(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        mock_controller.execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await connection.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_closed(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        connection._handle_closed()

        with pytest.raises(
            expected_exception=ConnectionError, match="wt_connection resolve failed connection_handle=42"
        ):
            await connection.diagnose()

        mock_controller.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_diagnose_connection_error(
        self, connection: WebTransportConnection, mock_controller: MagicMock
    ) -> None:
        mock_controller.execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await connection.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_generic_error(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        mock_controller.execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await connection.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_success(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        diagnostic_data_raw: dict[str, Any] = {
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
            "connected_at": 1.0,
            "connection_handle": 42,
            "handshake_completed": True,
            "is_client": True,
            "local_goaway_sent": False,
            "peer_goaway_received": False,
            "peer_initial_max_data": 4 * 1024 * 1024,
            "peer_initial_max_streams_bidi": 10,
            "peer_initial_max_streams_uni": 10,
            "peer_max_datagram_frame_size": 1350,
            "peer_settings_received": True,
            "state": ConnectionState.CONNECTED,
        }

        async def fake_execute(dispatcher: Any, resolver: Any) -> dict[str, Any]:
            dispatcher(1)
            return diagnostic_data_raw

        mock_controller.execute_request.side_effect = fake_execute

        diagnostics = await connection.diagnose()

        assert diagnostics == ConnectionDiagnostics(**diagnostic_data_raw)
        mock_controller.send_protocol_event.assert_called_once()

        kwargs = mock_controller.send_protocol_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserDiagnoseConnection)
        assert event.request_id == 1

    @pytest.mark.asyncio
    async def test_execute_request_success(
        self, connection: WebTransportConnection, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        async def fake_execute(dispatcher: Any, resolver: Any) -> str:
            dispatcher(99)
            return "done"

        mock_controller.execute_request.side_effect = fake_execute
        event_mock = mocker.Mock(spec=ProtocolEvent)

        resolver = mocker.Mock()

        result = await connection._execute_request(event_factory=lambda request_id: event_mock, resolver=resolver)

        assert result == "done"
        mock_controller.execute_request.assert_called_once()
        assert mock_controller.execute_request.call_args.kwargs["resolver"] is resolver
        mock_controller.send_protocol_event.assert_called_once_with(handle=42, request_id=99, event=event_mock)

    def test_handle_closed(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        session_handle = mocker.Mock(spec=WebTransportSession)
        stream_handle = mocker.Mock()
        connection._session_handles[1] = session_handle
        connection._stream_handles[10] = stream_handle

        connection._handle_closed()

        assert connection.is_closed is True
        session_handle._handle_closed.assert_called_once_with(error_code=None)
        stream_handle._handle_closed.assert_called_once_with()
        assert not connection._session_handles
        assert not connection._stream_handles
        assert connection._incoming_sessions.get_nowait() is None

    @pytest.mark.asyncio
    async def test_handle_event_connection_closed_cleans_handles(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        session_handle = mocker.Mock(spec=WebTransportSession)
        stream_handle = mocker.Mock()
        connection._session_handles[1] = session_handle
        connection._stream_handles[1] = stream_handle
        cast(Any, connection.events).emit = mocker.AsyncMock()
        cast(Any, connection.events).close = mocker.AsyncMock()
        data: dict[str, Any] = {"connection_handle": 42}

        connection._handle_event(event_type=EventType.CONNECTION_CLOSED, data=data)
        await asyncio.sleep(0)

        assert connection.is_closed is True
        assert connection._incoming_sessions.get_nowait() is None
        assert len(connection._session_handles) == 0
        assert len(connection._stream_handles) == 0
        session_handle._handle_closed.assert_called_once_with(error_code=None)
        stream_handle._handle_closed.assert_called_once_with()
        cast(MagicMock, connection.events.emit).assert_awaited_once_with(
            event_type=EventType.CONNECTION_CLOSED, data=data
        )
        cast(MagicMock, connection.events.close).assert_awaited_once()
        cast(MagicMock, connection.events.emit_nowait).assert_not_called()

    def test_handle_event_connection_events(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        connection._handle_event(event_type=EventType.CONNECTION_ESTABLISHED, data={})

        assert connection.is_closed is False
        cast(MagicMock, connection.events.emit_nowait).assert_called_with(
            event_type=EventType.CONNECTION_ESTABLISHED, data=mocker.ANY
        )
        assert isinstance(
            cast(MagicMock, connection.events.emit_nowait).call_args[1]["data"]["connection"], weakref.ProxyType
        )

    def test_handle_event_data_already_populated(self, connection: WebTransportConnection) -> None:
        existing_object = "mock_object"
        existing_connection_handle = 999
        data = {"connection": existing_object, "connection_handle": existing_connection_handle}

        connection._handle_event(event_type=EventType.SESSION_DRAINING, data=data)

        assert data["connection"] == existing_object
        assert data["connection_handle"] == connection.handle

    def test_handle_event_exception_handler(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        cast(MagicMock, connection.events.emit_nowait).side_effect = ValueError("boom")

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection receive failed"):
            connection._handle_event(event_type=EventType.CONNECTION_ESTABLISHED, data={})

    @pytest.mark.parametrize(argnames="event_type", argvalues=[EventType.SESSION_PENDING, EventType.SESSION_READY])
    def test_handle_session_event_client_creation(
        self, connection: WebTransportConnection, mock_session_class: MagicMock, event_type: EventType
    ) -> None:
        data = {"session_id": 1, "path": "/", "headers": {}, "wt_available_protocols": ["h3"], "wt_protocol": "h3"}

        connection._handle_event(event_type=event_type, data=data)

        session = cast(MagicMock, mock_session_class.return_value)
        mock_session_class.assert_called_once_with(
            connection=connection, session_id=1, path="/", headers={}, wt_available_protocols=["h3"], wt_protocol="h3"
        )
        session.events.emit_nowait.assert_called_once_with(event_type=event_type, data=data)
        if event_type == EventType.SESSION_READY:
            session._handle_ready.assert_called_once_with(wt_protocol="h3")
        else:
            session._handle_ready.assert_not_called()
        assert connection._session_handles[1] is session
        assert data["session"] is session
        assert connection._incoming_sessions.empty()

    def test_handle_session_event_handle_exists(
        self, connection: WebTransportConnection, mock_session_class: MagicMock, mocker: MockerFixture
    ) -> None:
        data = {"session_id": 1, "path": "/", "headers": {}}
        existing_session = mocker.Mock()
        connection._session_handles[1] = existing_session

        connection._handle_event(event_type=EventType.SESSION_READY, data=data)

        assert connection._session_handles[1] is existing_session
        mock_session_class.assert_not_called()

    def test_handle_session_event_missing_metadata(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"session_id": 1}

        connection._handle_event(event_type=EventType.SESSION_READY, data=data)

        assert 1 not in connection._session_handles
        spy_logger.warning.assert_called_with("wt_session validate failed session_id=%d", 1)

    def test_handle_session_event_no_id(self, connection: WebTransportConnection) -> None:
        connection._handle_event(event_type=EventType.SESSION_READY, data={})

        assert not connection._session_handles

    @pytest.mark.parametrize(
        argnames="is_client, event_type",
        argvalues=[(False, EventType.SESSION_READY), (True, EventType.SESSION_REQUEST_RECEIVED)],
    )
    def test_handle_session_event_role_mismatch_ignored(
        self,
        connection: WebTransportConnection,
        mock_session_class: MagicMock,
        is_client: bool,
        event_type: EventType,
    ) -> None:
        connection._is_client = is_client
        data = {"session_id": 1, "path": "/", "headers": {}}

        connection._handle_event(event_type=event_type, data=data)

        assert not connection._session_handles
        assert connection._incoming_sessions.empty()
        mock_session_class.assert_not_called()

    def test_handle_session_event_server_request(
        self, connection: WebTransportConnection, mock_session_class: MagicMock
    ) -> None:
        connection._is_client = False
        data = {"session_id": 2, "path": "/", "headers": {}}

        connection._handle_event(event_type=EventType.SESSION_REQUEST_RECEIVED, data=data)

        assert connection._session_handles[2] is mock_session_class.return_value
        assert connection._incoming_sessions.get_nowait() is mock_session_class.return_value

    def test_handle_stream_event_no_id(self, connection: WebTransportConnection) -> None:
        connection._handle_event(event_type=EventType.STREAM_OPENED, data={})

        assert not connection._stream_handles

    def test_handle_stream_event_opened_handle_exists(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        session_handle = mocker.Mock()
        existing_stream = mocker.Mock()
        connection._session_handles[1] = session_handle
        connection._stream_handles[10] = existing_stream
        data = {"stream_id": 10, "session_id": 1, "direction": StreamDirection.BIDIRECTIONAL}

        connection._handle_event(event_type=EventType.STREAM_OPENED, data=data)

        assert connection._stream_handles[10] is existing_stream
        session_handle._handle_stream_opened.assert_not_called()

    def test_handle_stream_event_opened_invalid_direction(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        session_handle = mocker.Mock()
        connection._session_handles[1] = session_handle
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"stream_id": 10, "session_id": 1, "direction": 999, "is_peer_initiated": False}

        connection._handle_event(event_type=EventType.STREAM_OPENED, data=data)

        assert 10 not in connection._stream_handles
        spy_logger.warning.assert_called_with("wt_stream validate invalid actual=%s stream_id=%d", 999, 10)

    def test_handle_stream_event_opened_missing_metadata(self, connection: WebTransportConnection) -> None:
        data = {"stream_id": 10}

        connection._handle_event(event_type=EventType.STREAM_OPENED, data=data)

        assert 10 not in connection._stream_handles

    def test_handle_stream_event_opened_missing_session(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"stream_id": 10, "session_id": 999, "direction": StreamDirection.BIDIRECTIONAL}

        connection._handle_event(event_type=EventType.STREAM_OPENED, data=data)

        spy_logger.warning.assert_called_with("wt_session resolve failed session_id=%d stream_id=%d", 999, 10)
        assert 10 not in connection._stream_handles

    @pytest.mark.parametrize(
        argnames="direction, handle_class_name",
        argvalues=[
            (StreamDirection.BIDIRECTIONAL, "WebTransportBidirectionalStream"),
            (StreamDirection.RECEIVE_ONLY, "WebTransportReceiveStream"),
            (StreamDirection.SEND_ONLY, "WebTransportSendStream"),
        ],
    )
    def test_handle_stream_event_opened_success(
        self,
        connection: WebTransportConnection,
        mocker: MockerFixture,
        direction: StreamDirection,
        handle_class_name: str,
    ) -> None:
        session_handle = mocker.Mock()
        connection._session_handles[1] = session_handle
        mock_handle_class = mocker.patch(target=f"pywebtransport.connection.{handle_class_name}")
        data = {"stream_id": 10, "session_id": 1, "direction": direction, "is_peer_initiated": True}

        connection._handle_event(event_type=EventType.STREAM_OPENED, data=data)

        stream = mock_handle_class.return_value
        mock_handle_class.assert_called_once_with(session=session_handle, stream_id=10, is_peer_initiated=True)
        assert connection._stream_handles[10] is stream
        assert data["stream"] is stream
        session_handle._handle_stream_opened.assert_called_once_with(stream=stream)
        session_handle.events.emit_nowait.assert_called_once_with(event_type=EventType.STREAM_OPENED, data=data)

    @pytest.mark.asyncio
    async def test_incoming_sessions_until_closed(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        session = mocker.Mock(spec=WebTransportSession)
        connection._incoming_sessions.put_nowait(session)
        connection._incoming_sessions.put_nowait(None)

        sessions = [item async for item in connection.incoming_sessions()]

        assert sessions == [session]
        assert connection._incoming_sessions.get_nowait() is None

    def test_init(
        self,
        connection: WebTransportConnection,
        mock_config: MagicMock,
        mock_controller: MagicMock,
    ) -> None:
        assert connection._config is mock_config
        assert connection._controller is mock_controller
        assert connection._handle == 42
        assert connection._is_client is True
        assert connection._is_closed is False
        assert connection.events is not None
        assert isinstance(connection._incoming_sessions, asyncio.Queue)
        assert connection._session_handles == {}
        assert connection._stream_handles == {}

        assert not hasattr(connection, "__dict__")

    def test_properties(self, connection: WebTransportConnection, mock_config: MagicMock) -> None:
        assert connection.config is mock_config
        assert connection.handle == 42
        assert connection.is_client is True
        assert connection.is_closed is False

    def test_remote_address(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        assert connection.remote_address == ("127.0.0.1", 443)
        mock_controller.get_remote_address.assert_called_once_with(handle=42)

        mock_controller.get_remote_address.reset_mock()

        assert connection.remote_address == ("127.0.0.1", 443)
        mock_controller.get_remote_address.assert_called_once_with(handle=42)

    def test_remote_address_returns_none(self, connection: WebTransportConnection, mock_controller: MagicMock) -> None:
        mock_controller.get_remote_address.return_value = None

        assert connection.remote_address is None

        mock_controller.get_remote_address.assert_called_once_with(handle=42)

    def test_resolve_session_handle_missing(self, connection: WebTransportConnection) -> None:
        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            connection._resolve_session_handle(1)

    def test_resolve_session_handle_success(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        session_handle = mocker.Mock()
        connection._session_handles[1] = session_handle

        result = connection._resolve_session_handle(1)

        assert result is session_handle

    def test_resolve_stream_handle_missing(self, connection: WebTransportConnection) -> None:
        with pytest.raises(expected_exception=StreamError, match="wt_stream resolve failed stream_id=10"):
            connection._resolve_stream_handle(10)

    def test_resolve_stream_handle_success(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        stream_handle = mocker.Mock()
        connection._stream_handles[10] = stream_handle

        result = connection._resolve_stream_handle(10)

        assert result is stream_handle

    @pytest.mark.asyncio
    async def test_route_session_event_and_close(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        session_handle = mocker.Mock()
        session_handle.events = mocker.Mock()
        session_handle.events.emit = mocker.AsyncMock()
        session_handle.events.close = mocker.AsyncMock()
        connection._session_handles[1] = session_handle
        blocked_data: dict[str, Any] = {"session_id": 1}

        connection._handle_event(event_type=EventType.SESSION_DATA_BLOCKED, data=blocked_data)

        assert blocked_data["session"] is session_handle
        session_handle.events.emit_nowait.assert_called_once_with(
            event_type=EventType.SESSION_DATA_BLOCKED, data=blocked_data
        )

        closed_data: dict[str, Any] = {"session_id": 1, "error_code": 7}
        connection._handle_event(event_type=EventType.SESSION_CLOSED, data=closed_data)
        await asyncio.sleep(0)

        assert 1 not in connection._session_handles
        session_handle._handle_closed.assert_called_once_with(error_code=7)
        session_handle.events.emit.assert_awaited_once_with(event_type=EventType.SESSION_CLOSED, data=closed_data)
        session_handle.events.close.assert_awaited_once()
        session_handle.events.emit_nowait.assert_called_once()

    def test_route_session_event_missing_id(self, connection: WebTransportConnection) -> None:
        connection._handle_event(event_type=EventType.SESSION_DATA_BLOCKED, data={})

        assert not connection._session_handles

    def test_route_session_event_not_found(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"session_id": 999}

        connection._handle_event(event_type=EventType.SESSION_DATA_BLOCKED, data=data)

        spy_logger.debug.assert_called_with(
            "wt_session resolve failed event=%s session_id=%d", EventType.SESSION_DATA_BLOCKED, 999
        )

    @pytest.mark.asyncio
    async def test_route_stream_event_closed(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        stream_handle = mocker.Mock()
        stream_handle.events = mocker.Mock()
        stream_handle.events.emit = mocker.AsyncMock()
        stream_handle.events.close = mocker.AsyncMock()
        connection._stream_handles[10] = stream_handle
        data: dict[str, Any] = {"stream_id": 10}

        connection._handle_event(event_type=EventType.STREAM_CLOSED, data=data)
        await asyncio.sleep(0)

        assert 10 not in connection._stream_handles
        assert data["stream"] is stream_handle
        stream_handle._handle_closed.assert_called_once_with()
        stream_handle.events.emit.assert_awaited_once_with(event_type=EventType.STREAM_CLOSED, data=data)
        stream_handle.events.close.assert_awaited_once()
        stream_handle.events.emit_nowait.assert_not_called()

    def test_route_stream_event_closed_missing(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"stream_id": 999}

        connection._handle_event(event_type=EventType.STREAM_CLOSED, data=data)

        assert 999 not in connection._stream_handles
        spy_logger.debug.assert_called_with(
            "wt_stream resolve failed event=%s stream_id=%d", EventType.STREAM_CLOSED, 999
        )

    def test_route_stream_event_no_id(self, connection: WebTransportConnection) -> None:
        connection._handle_event(event_type=EventType.STREAM_CLOSED, data={})

        assert not connection._stream_handles

    @pytest.mark.parametrize(
        argnames="event_type", argvalues=[EventType.STREAM_RESET_RECEIVED, EventType.STREAM_STOP_SENDING_RECEIVED]
    )
    def test_route_stream_event_stop_sending_reset(
        self, connection: WebTransportConnection, mocker: MockerFixture, event_type: EventType
    ) -> None:
        stream_handle = mocker.Mock()
        stream_handle.events = mocker.Mock()
        connection._stream_handles[10] = stream_handle
        data = {"stream_id": 10, "error_code": 123}

        connection._handle_event(event_type=event_type, data=data)

        cast(MagicMock, stream_handle.events.emit_nowait).assert_called_with(event_type=event_type, data=data)
        stream_handle._handle_closed.assert_not_called()

    def test_route_stream_event_stop_sending_reset_missing(
        self, connection: WebTransportConnection, mocker: MockerFixture
    ) -> None:
        spy_logger = mocker.patch(target="pywebtransport.connection._logger")
        data = {"stream_id": 999}

        connection._handle_event(event_type=EventType.STREAM_STOP_SENDING_RECEIVED, data=data)

        spy_logger.debug.assert_called_with(
            "wt_stream resolve failed event=%s stream_id=%d", EventType.STREAM_STOP_SENDING_RECEIVED, 999
        )

    def test_sessions(self, connection: WebTransportConnection, mocker: MockerFixture) -> None:
        session = mocker.Mock()
        connection._session_handles[1] = session

        assert connection.sessions == [session]
