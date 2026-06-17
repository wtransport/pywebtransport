"""Unit tests for the pywebtransport.server module."""

import asyncio
from typing import Any

import pytest
from pytest_mock import MockerFixture

from pywebtransport import Event, ServerConfig, ServerError, WebTransportServer
from pywebtransport.connection import WebTransportConnection
from pywebtransport.manager import ConnectionManager, SessionManager
from pywebtransport.server import ServerDiagnostics, ServerStats
from pywebtransport.types import ConnectionState, EventType, SessionState


class TestServerDiagnostics:

    @pytest.mark.parametrize(
        argnames="diag_kwargs, expected_issue_part",
        argvalues=[
            ({"is_serving": False}, "app_server validate failed"),
            (
                {"stats": ServerStats(connections_accepted=89, connections_rejected=11)},
                "expected=health_success_rate_threshold",
            ),
            (
                {"stats": ServerStats(connections_accepted=20, connections_rejected=1)},
                None,
            ),
            (
                {"stats": ServerStats(connections_accepted=5, connections_rejected=5)},
                None,
            ),
            (
                {"connection_states": {ConnectionState.CONNECTED: 95}, "max_connections": 100},
                "expected=health_connection_usage_threshold",
            ),
            ({"connection_states": {ConnectionState.CONNECTED: 50}, "max_connections": 100}, None),
            ({"connection_states": {ConnectionState.CONNECTED: 95}, "max_connections": 0}, None),
            ({}, None),
        ],
    )
    def test_issues_property(
        self,
        diag_kwargs: dict[str, Any],
        expected_issue_part: str | None,
    ) -> None:
        defaults = {
            "connection_states": {},
            "is_serving": True,
            "max_connections": 100,
            "session_states": {},
            "stats": ServerStats(),
        }
        for k, v in defaults.items():
            if k not in diag_kwargs:
                diag_kwargs[k] = v

        diagnostics = ServerDiagnostics(**diag_kwargs)
        issues = diagnostics.issues

        if expected_issue_part:
            assert any(expected_issue_part in issue for issue in issues)
        else:
            assert not issues


class TestWebTransportServer:

    @pytest.fixture
    def mock_connection_manager(self, mocker: MockerFixture) -> Any:
        mock_manager_class = mocker.patch(target="pywebtransport.server.ConnectionManager", autospec=True)
        return mock_manager_class.return_value

    @pytest.fixture
    def mock_controller(self, mocker: MockerFixture) -> Any:
        controller = mocker.MagicMock()
        controller.get_local_addresses.return_value = [("127.0.0.1", 4433)]
        controller.close = mocker.AsyncMock()
        return controller

    @pytest.fixture
    def mock_endpoint_controller_class(self, mocker: MockerFixture, mock_controller: Any) -> Any:
        return mocker.patch(target="pywebtransport.server.EndpointController", return_value=mock_controller)

    @pytest.fixture
    def mock_server_config(self, mocker: MockerFixture) -> ServerConfig:
        mocker.patch(target="pywebtransport.config.ServerConfig.validate")
        config = ServerConfig(
            bind_host="127.0.0.1", bind_port=4433, certfile="cert.pem", keyfile="key.pem", max_connections=10
        )
        config.connection_idle_timeout = 60.0

        return config

    @pytest.fixture
    def mock_session_manager(self, mocker: MockerFixture) -> Any:
        mock_manager_class = mocker.patch(target="pywebtransport.server.SessionManager", autospec=True)
        return mock_manager_class.return_value

    @pytest.fixture
    def mock_webtransport_connection(self, mocker: MockerFixture) -> Any:
        mock_conn = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        type(mock_conn).is_closed = mocker.PropertyMock(return_value=False)
        mock_conn.events = mocker.MagicMock()
        mock_conn.initialize = mocker.AsyncMock()
        mock_conn.handle = 42
        mocker.patch(target="pywebtransport.server.WebTransportConnection", return_value=mock_conn)

        return mock_conn

    @pytest.fixture
    def server(
        self, mock_server_config: ServerConfig, mock_connection_manager: Any, mock_session_manager: Any
    ) -> WebTransportServer:
        return WebTransportServer(config=mock_server_config)

    @pytest.fixture(autouse=True)
    def setup_common_mocks(self, mocker: MockerFixture) -> None:
        mocker.patch(target="time.perf_counter", side_effect=[1000.0, 1005.0])

    @pytest.mark.asyncio
    async def test_async_context_manager(
        self, server: WebTransportServer, mock_connection_manager: Any, mock_session_manager: Any, mocker: MockerFixture
    ) -> None:
        mock_close = mocker.patch.object(target=server, attribute="close", new_callable=mocker.AsyncMock)

        async with server as s:
            assert s is server
            mock_connection_manager.__aenter__.assert_awaited_once()
            mock_session_manager.__aenter__.assert_awaited_once()

        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_context_manager_with_exception(
        self, server: WebTransportServer, mocker: MockerFixture
    ) -> None:
        mock_close = mocker.patch.object(target=server, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=ValueError, match="Test exception"):
            async with server:
                raise ValueError("Test exception")

        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close(
        self,
        server: WebTransportServer,
        mock_endpoint_controller_class: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_session_manager: Any,
    ) -> None:
        await server.listen()

        await server.close()

        mock_connection_manager.shutdown.assert_awaited_once()
        mock_session_manager.shutdown.assert_awaited_once()
        mock_controller.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_already_in_progress(self, server: WebTransportServer) -> None:
        async def dummy_task() -> None:
            await asyncio.sleep(delay=0.1)

        real_task = asyncio.create_task(coro=dummy_task())
        server._close_task = real_task
        server._serving = True

        try:
            await server.close()
        finally:
            real_task.cancel()
            try:
                await real_task
            except asyncio.CancelledError:
                pass

        assert server._serving is True

    @pytest.mark.asyncio
    async def test_close_idempotency(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mock_controller: Any
    ) -> None:
        await server.listen()

        await server.close()

        mock_controller.close.assert_awaited_once()

        await server.close()

        mock_controller.close.assert_awaited_once()
        assert not server.is_serving

    @pytest.mark.asyncio
    async def test_close_implementation_defensive_check_no_controller(
        self, server: WebTransportServer, mock_connection_manager: Any
    ) -> None:
        server._serving = True
        server._controller = None

        await server.close()

        mock_connection_manager.shutdown.assert_awaited_once()
        assert server.is_serving is False

    @pytest.mark.asyncio
    async def test_close_with_done_task(
        self,
        server: WebTransportServer,
        mock_endpoint_controller_class: Any,
        mock_controller: Any,
        mocker: MockerFixture,
    ) -> None:
        await server.listen()
        done_task = mocker.create_autospec(spec=asyncio.Task, instance=True)
        done_task.done.return_value = True
        not_done_task = mocker.create_autospec(spec=asyncio.Task, instance=True)
        not_done_task.done.return_value = False
        server._background_tasks = {done_task, not_done_task}
        mock_gather = mocker.patch(target="asyncio.gather", new_callable=mocker.AsyncMock)

        await server.close()

        done_task.cancel.assert_not_called()
        not_done_task.cancel.assert_called_once()
        mock_gather.assert_awaited_once()

        args = mock_gather.await_args[0]
        assert set(args) == {done_task, not_done_task}

    @pytest.mark.asyncio
    async def test_close_with_finished_previous_close_task(
        self, server: WebTransportServer, mock_controller: Any, mocker: MockerFixture
    ) -> None:
        server._serving = True
        server._controller = mock_controller
        done_task = mocker.create_autospec(spec=asyncio.Task, instance=True)
        done_task.done.return_value = True
        server._close_task = done_task

        await server.close()

        assert server._close_task is not done_task
        mock_controller.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_with_manager_shutdown_error(
        self,
        server: WebTransportServer,
        mock_connection_manager: Any,
        mock_endpoint_controller_class: Any,
        mock_controller: Any,
    ) -> None:
        await server.listen()
        mock_connection_manager.shutdown.side_effect = RuntimeError("Shutdown error")

        try:
            await server.close()
        except BaseException:
            pass

        mock_connection_manager.shutdown.assert_awaited_once()

    def test_connection_manager_property(
        self, server: WebTransportServer, mock_connection_manager: ConnectionManager
    ) -> None:
        assert server.connection_manager is mock_connection_manager

    @pytest.mark.asyncio
    async def test_diagnostics(
        self,
        server: WebTransportServer,
        mock_connection_manager: Any,
        mock_session_manager: Any,
        mock_endpoint_controller_class: Any,
        mocker: MockerFixture,
    ) -> None:
        await server.listen()
        mock_conn = mocker.MagicMock()
        mock_conn.state = ConnectionState.CONNECTED
        mock_session = mocker.MagicMock()
        mock_session.state = SessionState.CONNECTED
        mock_connection_manager.get_all_resources = mocker.AsyncMock(return_value=[mock_conn])
        mock_session_manager.get_all_resources = mocker.AsyncMock(return_value=[mock_session])

        diagnostics = await server.diagnostics()

        assert isinstance(diagnostics, ServerDiagnostics)
        assert diagnostics.connection_states == {ConnectionState.CONNECTED: 1}
        assert diagnostics.is_serving is True
        assert diagnostics.session_states == {SessionState.CONNECTED: 1}
        assert diagnostics.stats.to_dict()["uptime"] == 5.0

    @pytest.mark.asyncio
    async def test_diagnostics_before_listen(self, server: WebTransportServer) -> None:
        diagnostics = await server.diagnostics()

        assert diagnostics.stats.to_dict()["uptime"] == 0.0

    def test_init_with_custom_config(self, server: WebTransportServer, mock_server_config: ServerConfig) -> None:
        assert server.config is mock_server_config

    @pytest.mark.asyncio
    async def test_initialize_and_register_connection_event_forwarding(
        self, server: WebTransportServer, mock_webtransport_connection: Any, mocker: MockerFixture
    ) -> None:
        server_emit = mocker.patch.object(target=server, attribute="emit", new_callable=mocker.AsyncMock)

        await server._initialize_and_register_connection(connection=mock_webtransport_connection)

        call_args = mock_webtransport_connection.events.on.call_args
        assert call_args is not None
        handler = call_args.kwargs["handler"]

        event_data = {"session_id": "s1"}
        test_event = Event(type=EventType.SESSION_REQUEST, data=event_data)

        await handler(test_event)

        server_emit.assert_awaited_once()
        assert server_emit.await_args is not None

        emit_kwargs = server_emit.await_args.kwargs
        assert emit_kwargs["data"]["connection"] is mock_webtransport_connection
        assert emit_kwargs["data"]["session_id"] == "s1"
        assert emit_kwargs["event_type"] == EventType.SESSION_REQUEST

    @pytest.mark.asyncio
    async def test_initialize_and_register_connection_event_forwarding_nodata(
        self, server: WebTransportServer, mock_webtransport_connection: Any, mocker: MockerFixture
    ) -> None:
        server_emit = mocker.patch.object(target=server, attribute="emit", new_callable=mocker.AsyncMock)

        await server._initialize_and_register_connection(connection=mock_webtransport_connection)

        call_args = mock_webtransport_connection.events.on.call_args
        assert call_args is not None
        handler = call_args.kwargs["handler"]

        test_event = Event(type=EventType.SESSION_REQUEST, data=None)

        await handler(test_event)

        server_emit.assert_awaited_once()
        assert server_emit.await_args is not None

        emit_kwargs = server_emit.await_args.kwargs
        assert emit_kwargs["data"]["connection"] is mock_webtransport_connection

    @pytest.mark.asyncio
    async def test_initialize_and_register_connection_failure(
        self,
        server: WebTransportServer,
        mock_connection_manager: Any,
        mock_webtransport_connection: Any,
        mocker: MockerFixture,
    ) -> None:
        mock_connection_manager.add_connection.side_effect = ValueError("Add failed")

        await server._initialize_and_register_connection(connection=mock_webtransport_connection)

        assert server._stats.connection_errors == 1
        assert server._stats.connections_rejected == 1
        mock_webtransport_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_initialize_and_register_connection_failure_closed(
        self,
        server: WebTransportServer,
        mock_connection_manager: Any,
        mock_webtransport_connection: Any,
        mocker: MockerFixture,
    ) -> None:
        mock_connection_manager.add_connection.side_effect = ValueError("Add failed")
        type(mock_webtransport_connection).is_closed = mocker.PropertyMock(return_value=True)

        await server._initialize_and_register_connection(connection=mock_webtransport_connection)

        assert server._stats.connections_rejected == 1
        mock_webtransport_connection.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_initialize_and_register_connection_success(
        self,
        server: WebTransportServer,
        mock_connection_manager: Any,
        mock_webtransport_connection: Any,
        mocker: MockerFixture,
    ) -> None:
        await server._initialize_and_register_connection(connection=mock_webtransport_connection)

        mock_webtransport_connection.events.on.assert_called_once()
        mock_webtransport_connection.initialize.assert_not_awaited()
        mock_connection_manager.add_connection.assert_awaited_once_with(connection=mock_webtransport_connection)
        assert server._stats.connections_accepted == 1

        once_call = mock_webtransport_connection.events.once.call_args
        assert once_call is not None
        assert once_call.kwargs["event_type"] == EventType.CONNECTION_CLOSED
        cleanup_handler = once_call.kwargs["handler"]

        await cleanup_handler(Event(type=EventType.CONNECTION_CLOSED, data=None))

        mock_webtransport_connection.events.off.assert_called_once()

    @pytest.mark.asyncio
    async def test_initialize_connection_session_registration_failure(
        self,
        server: WebTransportServer,
        mock_webtransport_connection: Any,
        mock_session_manager: Any,
        mocker: MockerFixture,
    ) -> None:
        mock_logger = mocker.patch(target="pywebtransport.server._logger.warning")
        mock_session_manager.add_session.side_effect = ValueError("Session limit reached")

        await server._initialize_and_register_connection(connection=mock_webtransport_connection)

        call_args = mock_webtransport_connection.events.on.call_args
        handler = call_args.kwargs["handler"]
        mock_session = mocker.Mock()
        mock_session.session_id = 999
        event = Event(type=EventType.SESSION_REQUEST, data={"session": mock_session})

        await handler(event)

        mock_logger.assert_called_with(
            "app_manager register failed component=session session_id=%d err=%s", 999, mocker.ANY, exc_info=True
        )

    @pytest.mark.asyncio
    async def test_listen_cert_file_not_found(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mocker: MockerFixture
    ) -> None:
        mock_endpoint_controller_class.side_effect = FileNotFoundError("Cert missing")

        with pytest.raises(expected_exception=ServerError, match="sys_file open failed"):
            await server.listen()

    @pytest.mark.asyncio
    async def test_listen_generic_exception(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mocker: MockerFixture
    ) -> None:
        mock_endpoint_controller_class.side_effect = Exception("Generic error")

        with pytest.raises(expected_exception=ServerError, match="app_server open failed"):
            await server.listen()

    @pytest.mark.asyncio
    async def test_listen_raises_error_if_already_serving(self, server: WebTransportServer) -> None:
        server._serving = True

        with pytest.raises(expected_exception=ServerError, match="app_server validate failed"):
            await server.listen()

    @pytest.mark.asyncio
    async def test_listen_success(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mock_controller: Any
    ) -> None:
        await server.listen()

        assert server.is_serving
        assert server.local_addresses == [("127.0.0.1", 4433)]

        mock_endpoint_controller_class.assert_called_once()
        call_kwargs = mock_endpoint_controller_class.call_args.kwargs
        assert call_kwargs["config"] is server._config
        assert call_kwargs["is_client"] is False

        mock_controller.set_spawn_callback.assert_called_once_with(callback=server._spawn_connection_callback)

    @pytest.mark.asyncio
    async def test_listen_success_no_addresses(
        self,
        server: WebTransportServer,
        mock_endpoint_controller_class: Any,
        mock_controller: Any,
        mocker: MockerFixture,
    ) -> None:
        mock_controller.get_local_addresses.return_value = []
        spy_logger = mocker.patch(target="pywebtransport.server._logger.info")

        await server.listen()

        assert server.is_serving
        assert server.local_addresses == []
        spy_logger.assert_any_call("app_server open")

    @pytest.mark.asyncio
    async def test_listen_with_explicit_host_port(
        self,
        server: WebTransportServer,
        mock_endpoint_controller_class: Any,
        mock_controller: Any,
        mocker: MockerFixture,
    ) -> None:
        mock_new_config = mocker.MagicMock(spec=ServerConfig)
        mock_update = mocker.patch.object(target=server._config, attribute="update", return_value=mock_new_config)

        await server.listen(host="1.2.3.4", port=9999)

        mock_update.assert_called_once_with(bind_host="1.2.3.4", bind_port=9999)
        mock_endpoint_controller_class.assert_called_once()
        call_kwargs = mock_endpoint_controller_class.call_args.kwargs

        assert call_kwargs["config"] is mock_new_config
        assert call_kwargs["is_client"] is False
        assert "loop" in call_kwargs
        assert server.is_serving
        mock_controller.set_spawn_callback.assert_called_once_with(callback=server._spawn_connection_callback)

    def test_local_addresses_empty(self, server: WebTransportServer, mock_controller: Any) -> None:
        server._controller = mock_controller
        mock_controller.get_local_addresses.return_value = []

        assert server.local_addresses == []

    def test_local_addresses_no_controller(self, server: WebTransportServer) -> None:
        server._controller = None

        assert server.local_addresses == []

    @pytest.mark.asyncio
    async def test_serve_forever_cancelled(
        self, server: WebTransportServer, mocker: MockerFixture, mock_endpoint_controller_class: Any
    ) -> None:
        await server.listen()

        assert server._shutdown_event is not None

        task = asyncio.create_task(coro=server.serve_forever())
        await asyncio.sleep(delay=0.01)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert task.result() is None

    @pytest.mark.asyncio
    async def test_serve_forever_graceful_exit(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mocker: MockerFixture
    ) -> None:
        await server.listen()

        async def trigger_shutdown() -> None:
            await asyncio.sleep(delay=0.01)
            await server.close()

        asyncio.create_task(coro=trigger_shutdown())
        await server.serve_forever()

        assert not server.is_serving

    @pytest.mark.asyncio
    async def test_serve_forever_keyboard_interrupt(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mocker: MockerFixture
    ) -> None:
        await server.listen()

        assert server._shutdown_event is not None

        mocker.patch.object(target=server._shutdown_event, attribute="wait", side_effect=KeyboardInterrupt)

        with pytest.raises(expected_exception=KeyboardInterrupt):
            await server.serve_forever()

    @pytest.mark.asyncio
    async def test_serve_forever_not_listening(self, server: WebTransportServer) -> None:
        with pytest.raises(expected_exception=ServerError, match="app_server validate failed"):
            await server.serve_forever()

    @pytest.mark.asyncio
    async def test_serve_forever_wait_exception(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mocker: MockerFixture
    ) -> None:
        await server.listen()

        assert server._shutdown_event is not None

        mocker.patch.object(target=server._shutdown_event, attribute="wait", side_effect=ValueError("Wait error"))

        with pytest.raises(expected_exception=ServerError, match="rt_event resolve failed"):
            await server.serve_forever()

    def test_session_manager_property(self, server: WebTransportServer, mock_session_manager: SessionManager) -> None:
        assert server.session_manager is mock_session_manager

    @pytest.mark.asyncio
    async def test_spawn_connection_callback_exception(
        self, server: WebTransportServer, mocker: MockerFixture, mock_controller: Any
    ) -> None:
        server._controller = mock_controller
        mock_logger = mocker.patch(target="pywebtransport.server._logger.warning")
        mocker.patch(
            target="pywebtransport.server.WebTransportConnection.accept",
            side_effect=ValueError("Factory failed"),
        )

        server._spawn_connection_callback(handle=1)

        mock_logger.assert_called_once_with(
            "wt_connection open failed connection_handle=%d err=%s", 1, mocker.ANY, exc_info=True
        )

    @pytest.mark.asyncio
    async def test_spawn_connection_callback_not_initialized(
        self, server: WebTransportServer, mocker: MockerFixture
    ) -> None:
        mock_logger = mocker.patch(target="pywebtransport.server._logger.warning")
        server._controller = None

        server._spawn_connection_callback(handle=1)

        mock_logger.assert_called_once_with("app_server validate failed")

    @pytest.mark.asyncio
    async def test_spawn_connection_callback_success(
        self,
        server: WebTransportServer,
        mocker: MockerFixture,
        mock_webtransport_connection: Any,
        mock_controller: Any,
    ) -> None:
        server._controller = mock_controller
        mock_accept = mocker.patch(
            target="pywebtransport.server.WebTransportConnection.accept",
            return_value=mock_webtransport_connection,
        )
        mock_init_task = mocker.patch.object(target=server, attribute="_initialize_and_register_connection")
        mock_create_task = mocker.patch(target="asyncio.create_task")

        server._spawn_connection_callback(handle=1)

        mock_accept.assert_called_once_with(controller=mock_controller, handle=1, config=server._config)
        mock_init_task.assert_called_once()
        mock_create_task.assert_called_once()

        call_args = mock_create_task.call_args
        if call_args:
            coro = call_args.kwargs.get("coro") or call_args.args[0]
            if asyncio.iscoroutine(coro):
                coro.close()

    def test_str_representation(
        self, server: WebTransportServer, mock_controller: Any, mock_connection_manager: Any, mock_session_manager: Any
    ) -> None:
        server._serving = True
        server._controller = mock_controller
        mock_connection_manager.__len__.return_value = 5
        mock_session_manager.__len__.return_value = 2

        representation = str(server)

        assert "status=serving" in representation
        assert "addresses=[127.0.0.1:4433]" in representation
        assert "connections=5" in representation
        assert "sessions=2" in representation

    def test_str_representation_not_serving(self, server: WebTransportServer) -> None:
        representation = str(server)

        assert "status=stopped" in representation
        assert "addresses=unknown" in representation


class TestServerStats:

    def test_success_rate(self) -> None:
        stats = ServerStats()

        assert stats.success_rate == 1.0

        stats.connections_accepted = 8
        stats.connections_rejected = 2

        assert stats.success_rate == 0.8

        stats.connections_accepted = 0
        stats.connections_rejected = 10

        assert stats.success_rate == 0.0

    def test_to_dict(self, mocker: MockerFixture) -> None:
        mocker.patch(target="time.perf_counter", return_value=1010.0)
        stats = ServerStats(start_time=1000.0)
        stats.connections_accepted = 5
        stats.connections_rejected = 5

        data = stats.to_dict()

        assert data["total_connections_attempted"] == 10
        assert data["success_rate"] == 0.5
        assert data["uptime"] == 10.0

    def test_to_dict_no_start_time(self) -> None:
        stats = ServerStats(start_time=None)

        data = stats.to_dict()

        assert data["uptime"] == 0.0

    def test_total_connections_attempted(self) -> None:
        stats = ServerStats()

        assert stats.total_connections_attempted == 0

        stats.connections_accepted = 5
        stats.connections_rejected = 3

        assert stats.total_connections_attempted == 8
