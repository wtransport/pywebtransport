"""Unit tests for the pywebtransport.server module."""

import asyncio
from typing import Any

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ServerConfig, ServerError, WebTransportConnection, WebTransportServer


class TestWebTransportServer:

    @pytest.fixture
    def mock_controller(self, mocker: MockerFixture) -> Any:
        controller = mocker.MagicMock()
        controller.close = mocker.AsyncMock()
        controller.get_local_addresses.return_value = [("127.0.0.1", 4433)]
        controller.start = mocker.AsyncMock()
        return controller

    @pytest.fixture
    def mock_endpoint_controller_class(self, mocker: MockerFixture, mock_controller: Any) -> Any:
        return mocker.patch(target="pywebtransport.server.EndpointController", return_value=mock_controller)

    @pytest.fixture
    def mock_server_config(self) -> ServerConfig:
        return ServerConfig(
            bind_host="127.0.0.1",
            bind_port=4433,
            certfile="cert.pem",
            keyfile="key.pem",
            max_connections=10,
            max_idle_timeout=60.0,
        )

    @pytest.fixture
    def mock_connection_class(self, mocker: MockerFixture) -> Any:
        mock_connection = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        mock_connection.handle = 42

        return mocker.patch(target="pywebtransport.server.WebTransportConnection", return_value=mock_connection)

    @pytest.fixture
    def server(self, mock_server_config: ServerConfig) -> WebTransportServer:
        return WebTransportServer(config=mock_server_config)

    @pytest.mark.asyncio
    async def test_async_context_manager(self, server: WebTransportServer, mocker: MockerFixture) -> None:
        mock_close = mocker.patch.object(target=server, attribute="close", new_callable=mocker.AsyncMock)

        async with server as entered_server:
            assert entered_server is server
            mock_close.assert_not_awaited()

        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_context_manager_with_exception(
        self, server: WebTransportServer, mocker: MockerFixture
    ) -> None:
        mock_close = mocker.patch.object(target=server, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=ValueError, match="test exception"):
            async with server:
                raise ValueError("test exception")

        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close(
        self,
        server: WebTransportServer,
        mock_endpoint_controller_class: Any,
        mock_controller: Any,
    ) -> None:
        await server.listen()

        await server.close()

        mock_controller.close.assert_awaited_once()
        assert server.is_closed
        assert server._controller is None
        assert server._incoming_connections.get_nowait() is None

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
    async def test_close_defensive_check_no_controller(self, server: WebTransportServer) -> None:
        server._serving = True
        server._controller = None

        await server.close()

        assert server.is_serving is False
        assert server.is_closed

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
    async def test_close_reaches_terminal_state_on_controller_error(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mock_controller: Any
    ) -> None:
        await server.listen()
        mock_controller.close.side_effect = RuntimeError("controller close failed")

        with pytest.raises(expected_exception=RuntimeError, match="controller close failed"):
            await server.close()

        assert server.is_closed
        assert server._closing is False
        assert server._shutdown_event.is_set()

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

    def test_create_connection_success(
        self, server: WebTransportServer, mock_connection_class: Any, mock_controller: Any
    ) -> None:
        result = server._create_connection(mock_controller, 1)

        mock_connection_class.assert_called_once_with(
            handle=1, controller=mock_controller, config=server._config, is_client=False
        )
        assert result is mock_connection_class.return_value
        assert server._incoming_connections.get_nowait() is result

    @pytest.mark.asyncio
    async def test_incoming_connections_until_closed(self, server: WebTransportServer, mocker: MockerFixture) -> None:
        connection = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        server._incoming_connections.put_nowait(connection)
        server._incoming_connections.put_nowait(None)

        connections = [item async for item in server.incoming_connections()]

        assert connections == [connection]
        assert server._incoming_connections.get_nowait() is None

    def test_init_with_custom_config(self, server: WebTransportServer, mock_server_config: ServerConfig) -> None:
        assert server.config is mock_server_config

    def test_is_closed_default(self, server: WebTransportServer) -> None:
        assert not server.is_closed

    @pytest.mark.asyncio
    async def test_listen_after_close_raises_error(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mock_controller: Any
    ) -> None:
        await server.listen()
        await server.close()

        with pytest.raises(expected_exception=ServerError, match="app_server validate failed"):
            await server.listen()

    @pytest.mark.asyncio
    async def test_listen_cert_file_not_found(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mocker: MockerFixture
    ) -> None:
        mock_endpoint_controller_class.side_effect = FileNotFoundError("cert missing")

        with pytest.raises(expected_exception=ServerError, match="sys_file open failed"):
            await server.listen()

    @pytest.mark.asyncio
    async def test_listen_generic_exception(
        self, server: WebTransportServer, mock_endpoint_controller_class: Any, mocker: MockerFixture
    ) -> None:
        mock_endpoint_controller_class.side_effect = Exception("generic error")

        with pytest.raises(expected_exception=ServerError, match="app_server open failed"):
            await server.listen()

    @pytest.mark.asyncio
    async def test_listen_raises_error_if_already_closed(self, server: WebTransportServer) -> None:
        server._closed = True

        with pytest.raises(expected_exception=ServerError, match="app_server validate failed"):
            await server.listen()

    @pytest.mark.asyncio
    async def test_listen_raises_error_if_already_closing(self, server: WebTransportServer) -> None:
        server._closing = True

        with pytest.raises(expected_exception=ServerError, match="app_server validate failed"):
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

        assert call_kwargs["connection_factory"] == server._create_connection

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
        await server.listen(host="1.2.3.4", port=9999)

        mock_endpoint_controller_class.assert_called_once()
        call_kwargs = mock_endpoint_controller_class.call_args.kwargs

        assert call_kwargs["config"].bind_host == "1.2.3.4"
        assert call_kwargs["config"].bind_port == 9999
        assert call_kwargs["config"] is server._config
        assert call_kwargs["is_client"] is False
        assert server.is_serving
        assert server.config.bind_host == "1.2.3.4"
        assert server.config.bind_port == 9999
        assert call_kwargs["connection_factory"] == server._create_connection

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

        mocker.patch.object(target=server._shutdown_event, attribute="wait", side_effect=ValueError("wait error"))

        with pytest.raises(expected_exception=ServerError, match="rt_event resolve failed"):
            await server.serve_forever()
