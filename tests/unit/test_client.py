"""Unit tests for the pywebtransport.client module."""

import asyncio
import socket
from typing import Any, Callable

import pytest
from pytest_mock import MockerFixture

from pywebtransport import (
    ClientConfig,
    ClientError,
    ConnectionError,
    Headers,
    TimeoutError,
    WebTransportClient,
    WebTransportConnection,
    WebTransportSession,
)
from pywebtransport.client import (
    _ConnectionTarget,
    _merge_headers,
    _normalize_headers,
    _parse_webtransport_url,
    _resolve_host,
)
from pywebtransport.version import __version__


class TestWebTransportClient:

    @pytest.fixture
    def client(self, mock_client_config: Any) -> WebTransportClient:
        return WebTransportClient(config=mock_client_config)

    @pytest.fixture
    def mock_client_config(self, mocker: MockerFixture) -> Any:
        mock = mocker.create_autospec(spec=ClientConfig, instance=True)
        mock.connect_timeout = 10.0
        mock.connection_attempt_delay = 0.250
        mock.headers = None
        mock.user_agent = None
        mock.wt_available_protocols = None

        return mock

    @pytest.fixture
    def mock_connection_class(self, mocker: MockerFixture, mock_webtransport_connection: Any) -> Any:
        return mocker.patch(
            target="pywebtransport.client.WebTransportConnection", return_value=mock_webtransport_connection
        )

    @pytest.fixture
    def mock_connection_factory(self, mocker: MockerFixture, mock_session: Any) -> Callable[[], Any]:
        def factory() -> Any:
            connection = mocker.create_autospec(spec=WebTransportConnection, instance=True)
            connection.create_session = mocker.AsyncMock(return_value=mock_session)
            connection.create_session_optimistic = mocker.AsyncMock(return_value=mock_session)
            connection.close = mocker.AsyncMock()
            return connection

        return factory

    @pytest.fixture
    def mock_controller(self, mocker: MockerFixture, mock_webtransport_connection: Any) -> Any:
        controller = mocker.MagicMock()
        controller.close = mocker.AsyncMock()
        controller.connect = mocker.AsyncMock(return_value=mock_webtransport_connection)
        controller.get_local_addresses.return_value = [("192.0.2.1", 0)]
        controller.start = mocker.AsyncMock()

        return controller

    @pytest.fixture
    def mock_controller_class(self, mocker: MockerFixture, mock_controller: Any) -> Any:
        return mocker.patch(target="pywebtransport.client.EndpointController", return_value=mock_controller)

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> Any:
        session = mocker.create_autospec(spec=WebTransportSession, instance=True)

        return session

    @pytest.fixture
    def mock_webtransport_connection(self, mocker: MockerFixture, mock_session: Any) -> Any:
        connection = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        connection.create_session = mocker.AsyncMock(return_value=mock_session)
        connection.create_session_optimistic = mocker.AsyncMock(return_value=mock_session)
        connection.close = mocker.AsyncMock()

        return connection

    @pytest.fixture(autouse=True)
    def setup_common_mocks(self, mocker: MockerFixture) -> None:
        mocker.patch(
            target="pywebtransport.client._parse_webtransport_url",
            return_value=_ConnectionTarget(authority="example.com", host="example.com", path="/", port=443),
        )
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["192.0.2.1"],
            new_callable=mocker.AsyncMock,
        )

    @pytest.mark.asyncio
    async def test_close_closes_controller(self, client: WebTransportClient, mock_controller: Any) -> None:
        client._controller = mock_controller

        await client.close()

        mock_controller.close.assert_awaited_once()
        assert client._controller is None

    @pytest.mark.asyncio
    async def test_close_idempotency_and_concurrency(self, client: WebTransportClient) -> None:
        await asyncio.gather(client.close(), client.close())

        assert client.is_closed

    @pytest.mark.asyncio
    async def test_close_reaches_terminal_state_on_controller_error(
        self, client: WebTransportClient, mock_controller: Any
    ) -> None:
        client._controller = mock_controller
        mock_controller.close.side_effect = RuntimeError("controller close failed")

        with pytest.raises(expected_exception=RuntimeError, match="controller close failed"):
            await client.close()

        assert client.is_closed

    @pytest.mark.asyncio
    async def test_close_sequential_calls(self, client: WebTransportClient) -> None:
        await client.close()

        assert client.is_closed

        await client.close()

    @pytest.mark.asyncio
    async def test_close_with_done_task(self, client: WebTransportClient, mocker: MockerFixture) -> None:
        done_task = mocker.create_autospec(spec=asyncio.Task, instance=True)
        done_task.done.return_value = True
        not_done_task = mocker.create_autospec(spec=asyncio.Task, instance=True)
        not_done_task.done.return_value = False
        client._background_tasks = {done_task, not_done_task}

        async def fake_gather(*tasks: Any, **kwargs: Any) -> list[Any]:
            for task in tasks:
                client._background_tasks.discard(task)
            return []

        mock_gather = mocker.patch(
            target="asyncio.gather",
            new_callable=mocker.AsyncMock,
            side_effect=fake_gather,
        )

        await client.close()

        done_task.cancel.assert_not_called()
        not_done_task.cancel.assert_called_once()
        mock_gather.assert_awaited_once()

        args = mock_gather.await_args[0]
        assert set(args) == {done_task, not_done_task}

    @pytest.mark.asyncio
    async def test_connect_already_initialized(
        self, client: WebTransportClient, mock_controller_class: Any, mock_controller: Any
    ) -> None:
        client._controller = mock_controller

        await client.connect(url="https://example.com")

        mock_controller_class.assert_not_called()
        mock_controller.connect.assert_awaited_once_with(remote_address=("192.0.2.1", 443), server_name="example.com")

    @pytest.mark.asyncio
    async def test_connect_concurrent_initialization(
        self, client: WebTransportClient, mock_controller_class: Any, mock_controller: Any
    ) -> None:
        await client._controller_lock.acquire()

        task1 = asyncio.create_task(coro=client.connect(url="https://example.com/1"))
        task2 = asyncio.create_task(coro=client.connect(url="https://example.com/2"))

        await asyncio.sleep(delay=0.01)

        client._controller_lock.release()

        await asyncio.gather(task1, task2)

        mock_controller_class.assert_called_once()
        assert mock_controller.connect.call_count == 2

    @pytest.mark.asyncio
    async def test_connect_controller_raises_exception(
        self, client: WebTransportClient, mock_controller_class: Any, mock_controller: Any
    ) -> None:
        mock_controller.connect.side_effect = RuntimeError("controller error")

        with pytest.raises(
            expected_exception=ClientError, match="wt_connection open failed actual=example.com"
        ) as exception_info:
            await client.connect(url="https://example.com")

        cause_level_1 = exception_info.value.__cause__
        assert cause_level_1 is not None
        cause_level_2 = cause_level_1.__cause__
        assert cause_level_2 is not None

        assert "controller error" in str(cause_level_2)

    @pytest.mark.asyncio
    async def test_connect_fails_during_session_creation(
        self,
        client: WebTransportClient,
        mock_webtransport_connection: Any,
        mock_controller_class: Any,
    ) -> None:
        mock_webtransport_connection.create_session.side_effect = RuntimeError("session init failed")

        with pytest.raises(
            expected_exception=ClientError,
            match="wt_connection open failed actual=example.com",
        ):
            await client.connect(url="https://example.com")

        mock_webtransport_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_failure_certificate(self, client: WebTransportClient, mock_controller_class: Any) -> None:
        mock_controller_class.side_effect = Exception("certificate verify failed")

        with pytest.raises(
            expected_exception=ConnectionError,
            match="wt_connection validate failed actual=example.com",
        ):
            await client.connect(url="https://example.com")

    @pytest.mark.asyncio
    async def test_connect_failure_connection_refused(
        self, client: WebTransportClient, mock_controller_class: Any
    ) -> None:
        mock_controller_class.side_effect = ConnectionRefusedError()

        with pytest.raises(
            expected_exception=ConnectionError,
            match="wt_connection open failed actual=example.com",
        ):
            await client.connect(url="https://example.com")

    @pytest.mark.asyncio
    async def test_connect_failure_generic(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        mock_controller_class.side_effect = RuntimeError("generic failure")

        with pytest.raises(
            expected_exception=ClientError,
            match="wt_connection open failed actual=example.com",
        ):
            await client.connect(url="https://example.com")

        mock_webtransport_connection.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connect_failure_timeout(self, client: WebTransportClient, mock_controller_class: Any) -> None:
        mock_controller_class.side_effect = asyncio.TimeoutError()

        with pytest.raises(
            expected_exception=TimeoutError,
            match="wt_connection open failed actual=example.com",
        ):
            await client.connect(url="https://example.com")

    @pytest.mark.asyncio
    async def test_connect_failure_timeout_during_session(
        self,
        client: WebTransportClient,
        mock_webtransport_connection: Any,
        mock_controller_class: Any,
    ) -> None:
        async def create_session(**kwargs: Any) -> Any:
            await asyncio.Event().wait()

        mock_webtransport_connection.create_session.side_effect = create_session

        with pytest.raises(
            expected_exception=TimeoutError,
            match="wt_session open failed actual=example.com",
        ):
            await client.connect(url="https://example.com", timeout=0.01)

        mock_webtransport_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_abandoned_attempt_fails_late(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_session: Any,
        mock_connection_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        mock_client_config.connection_attempt_delay = 0.1
        sync_event = asyncio.Event()
        connection2 = mock_connection_factory()

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            if remote_address[0] == "10.0.0.1":
                await sync_event.wait()
                raise ConnectionError(message="late failure")
            return connection2

        mock_controller.connect.side_effect = connect

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        assert client._background_tasks

        sync_event.set()
        while client._background_tasks:
            await asyncio.wait(client._background_tasks)

        assert not client._background_tasks
        connection2.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_abandoned_attempt_succeeds_late(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_session: Any,
        mock_connection_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        mock_client_config.connection_attempt_delay = 0.1
        sync_event = asyncio.Event()
        connection1 = mock_connection_factory()
        connection2 = mock_connection_factory()

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            if remote_address[0] == "10.0.0.1":
                await sync_event.wait()
                return connection1
            return connection2

        mock_controller.connect.side_effect = connect

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        connection1.close.assert_not_called()

        sync_event.set()
        while client._background_tasks:
            await asyncio.wait(client._background_tasks)

        connection1.close.assert_awaited_once()
        connection2.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_all_fail(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_controller: Any,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["2001:db8::1", "192.0.2.1"],
            new_callable=mocker.AsyncMock,
        )
        mock_controller.connect.side_effect = ConnectionError(message="attempt failed")

        with pytest.raises(
            expected_exception=ClientError,
            match="wt_connection open failed actual=example.com",
        ):
            await client.connect(url="https://example.com")

        assert mock_controller.connect.await_count == 2

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_fallback(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_session: Any,
        mock_connection_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["2001:db8::1", "192.0.2.1"],
            new_callable=mocker.AsyncMock,
        )
        connection2 = mock_connection_factory()

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            if remote_address[0] == "2001:db8::1":
                raise ConnectionError(message="attempt failed")
            return connection2

        mock_controller.connect.side_effect = connect

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        assert mock_controller.connect.await_count == 2
        connection2.create_session.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_outer_loop_simultaneous_success_and_failure(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_session: Any,
        mock_connection_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        mock_client_config.connection_attempt_delay = 0.0
        sync_event = asyncio.Event()
        connection1 = mock_connection_factory()

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            await sync_event.wait()
            if remote_address[0] == "10.0.0.1":
                return connection1
            raise ConnectionError(message="simultaneous failure")

        mock_controller.connect.side_effect = connect
        original_wait = asyncio.wait

        async def mock_wait(fs: Any, *args: Any, **kwargs: Any) -> tuple[Any, Any]:
            done, pending = await original_wait(fs, *args, **kwargs)

            def is_failure(task: asyncio.Task[Any]) -> bool:
                if task.cancelled():
                    return True
                return task.exception() is not None

            ordered_done = sorted(list(done), key=is_failure)
            return ordered_done, pending

        mocker.patch(target="asyncio.wait", side_effect=mock_wait)

        connect_task = asyncio.create_task(coro=client.connect(url="https://example.com"))

        await asyncio.sleep(delay=0.01)
        sync_event.set()

        session = await connect_task

        assert session is mock_session
        connection1.create_session.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_simultaneous_success(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_session: Any,
        mock_connection_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        mock_client_config.connection_attempt_delay = 0.0
        sync_event = asyncio.Event()
        connection1 = mock_connection_factory()
        connection2 = mock_connection_factory()

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            await sync_event.wait()
            return connection1 if remote_address[0] == "10.0.0.1" else connection2

        mock_controller.connect.side_effect = connect

        connect_task = asyncio.create_task(coro=client.connect(url="https://example.com"))

        await asyncio.sleep(delay=0.01)
        sync_event.set()

        session = await connect_task

        assert session is mock_session
        while client._background_tasks:
            await asyncio.wait(client._background_tasks)
        assert connection1.close.call_count + connection2.close.call_count == 1

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_staggered_failure(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        mock_client_config.connection_attempt_delay = 0.1

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            await asyncio.sleep(delay=0.2)
            raise ConnectionError(message="simulated failure")

        mock_controller.connect.side_effect = connect

        with pytest.raises(
            expected_exception=ClientError,
            match="wt_connection open failed actual=example.com",
        ):
            await client.connect(url="https://example.com")

        assert mock_controller.connect.await_count == 2

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_staggered_success(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_session: Any,
        mock_connection_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        mock_client_config.connection_attempt_delay = 0.1
        connection1 = mock_connection_factory()
        connection2 = mock_connection_factory()

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            if remote_address[0] == "10.0.0.1":
                await asyncio.sleep(delay=0.3)
                return connection1
            await asyncio.sleep(delay=0.1)
            return connection2

        mock_controller.connect.side_effect = connect

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        connection2.create_session.assert_awaited_once()
        connection1.close.assert_not_called()

        while client._background_tasks:
            await asyncio.wait(client._background_tasks)
        connection1.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_while_loop_double_success(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_session: Any,
        mock_connection_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["10.0.0.1", "10.0.0.2", "10.0.0.3"],
            new_callable=mocker.AsyncMock,
        )
        mock_client_config.connection_attempt_delay = 0.1
        sync_event = asyncio.Event()
        connection1 = mock_connection_factory()
        connection2 = mock_connection_factory()

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            if remote_address[0] == "10.0.0.3":
                raise ConnectionError(message="fast failure")
            await sync_event.wait()
            return connection1 if remote_address[0] == "10.0.0.1" else connection2

        mock_controller.connect.side_effect = connect

        async def trigger_event() -> None:
            await asyncio.sleep(delay=0.3)
            sync_event.set()

        asyncio.create_task(coro=trigger_event())

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        while client._background_tasks:
            await asyncio.wait(client._background_tasks)
        assert mock_controller.connect.await_count == 3
        assert connection1.close.call_count + connection2.close.call_count == 1

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_while_loop_simultaneous_success_and_failure(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_session: Any,
        mock_connection_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._resolve_host",
            return_value=["10.0.0.1", "10.0.0.2", "10.0.0.3"],
            new_callable=mocker.AsyncMock,
        )
        mock_client_config.connection_attempt_delay = 0.1
        sync_event = asyncio.Event()
        connection1 = mock_connection_factory()

        async def connect(*, remote_address: tuple[str, int], server_name: str) -> Any:
            if remote_address[0] == "10.0.0.3":
                raise ConnectionError(message="fast failure")
            await sync_event.wait()
            if remote_address[0] == "10.0.0.1":
                return connection1
            raise ConnectionError(message="simultaneous failure")

        mock_controller.connect.side_effect = connect
        original_wait = asyncio.wait

        async def mock_wait(fs: Any, *args: Any, **kwargs: Any) -> tuple[Any, Any]:
            done, pending = await original_wait(fs, *args, **kwargs)

            def is_failure(task: asyncio.Task[Any]) -> bool:
                if task.cancelled():
                    return True
                return task.exception() is not None

            ordered_done = sorted(list(done), key=is_failure)
            return ordered_done, pending

        mocker.patch(target="asyncio.wait", side_effect=mock_wait)

        async def trigger_event() -> None:
            await asyncio.sleep(delay=0.3)
            sync_event.set()

        asyncio.create_task(coro=trigger_event())

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        assert mock_controller.connect.await_count == 3
        connection1.create_session.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_optimistic_success(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_webtransport_connection: Any,
        mock_session: Any,
    ) -> None:
        session = await client.connect_optimistic(url="https://example.com")

        mock_controller_class.assert_called_once()
        mock_controller.connect.assert_awaited_once_with(remote_address=("192.0.2.1", 443), server_name="example.com")

        mock_webtransport_connection.create_session_optimistic.assert_awaited_once()
        mock_webtransport_connection.create_session.assert_not_called()

        _, session_kwargs = mock_webtransport_connection.create_session_optimistic.call_args
        assert session_kwargs["authority"] == "example.com"
        assert session_kwargs["path"] == "/"

        assert session is mock_session

    @pytest.mark.asyncio
    async def test_connect_success(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_webtransport_connection: Any,
        mock_session: Any,
    ) -> None:
        session = await client.connect(url="https://example.com")

        mock_controller_class.assert_called_once()
        assert mock_controller_class.call_args.kwargs["connection_factory"] == client._create_connection
        mock_controller.connect.assert_awaited_once_with(remote_address=("192.0.2.1", 443), server_name="example.com")
        mock_webtransport_connection.create_session.assert_awaited_once()

        _, session_kwargs = mock_webtransport_connection.create_session.call_args
        assert session_kwargs["authority"] == "example.com"
        assert session_kwargs["path"] == "/"
        assert session_kwargs["wt_available_protocols"] is None

        assert session_kwargs["headers"] == [("user-agent", f"PyWebTransport/{__version__}")]

        assert session is mock_session

    @pytest.mark.asyncio
    async def test_connect_success_with_wt_available_protocols(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_webtransport_connection: Any,
        mock_session: Any,
    ) -> None:
        session = await client.connect(url="https://example.com", wt_available_protocols=["h3", "dummy"])

        assert session is mock_session
        mock_webtransport_connection.create_session.assert_awaited_once()

        _, session_kwargs = mock_webtransport_connection.create_session.call_args
        assert session_kwargs["wt_available_protocols"] == ["h3", "dummy"]

    @pytest.mark.asyncio
    async def test_connect_success_with_wt_available_protocols_from_config(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_webtransport_connection: Any,
        mock_session: Any,
    ) -> None:
        mock_client_config.wt_available_protocols = ("config-proto",)

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        mock_webtransport_connection.create_session.assert_awaited_once()

        _, session_kwargs = mock_webtransport_connection.create_session.call_args
        assert session_kwargs["wt_available_protocols"] == ["config-proto"]
        assert isinstance(session_kwargs["wt_available_protocols"], list)

    @pytest.mark.asyncio
    async def test_connect_user_agent_from_config(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_class: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        mock_client_config.user_agent = "CustomClient/1.2.3"

        await client.connect(url="https://example.com")

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        assert passed_headers == [("user-agent", "CustomClient/1.2.3")]

    @pytest.mark.asyncio
    async def test_connect_user_agent_injection_dict_mode(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_client_config: Any,
        mock_webtransport_connection: Any,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client._normalize_headers",
            return_value={"host": "example.com"},
        )

        await client.connect(url="https://example.com")

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        assert passed_headers == {"host": "example.com", "user-agent": f"PyWebTransport/{__version__}"}

    @pytest.mark.asyncio
    async def test_connect_when_closed(self, client: WebTransportClient) -> None:
        await client.close()

        with pytest.raises(expected_exception=ClientError, match="app_client validate failed"):
            await client.connect(url="https://example.com")

    @pytest.mark.asyncio
    async def test_connect_with_explicit_user_agent_header(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_client_config: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        custom_user_agent = "ExplicitUA/1.0"

        await client.connect(url="https://example.com", headers={"user-agent": custom_user_agent})

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        assert passed_headers == [("user-agent", custom_user_agent)]

    @pytest.mark.asyncio
    async def test_connect_with_headers_merging(
        self,
        client: WebTransportClient,
        mock_controller_class: Any,
        mock_client_config: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        mock_client_config.headers = {"global": "1"}
        client.set_default_headers(headers={"default": "2"})

        await client.connect(url="https://example.com", headers={"local": "3"})

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        assert passed_headers == [
            ("global", "1"),
            ("default", "2"),
            ("local", "3"),
            ("user-agent", f"PyWebTransport/{__version__}"),
        ]

    @pytest.mark.asyncio
    async def test_connect_with_headers_merging_real_config(
        self,
        mock_controller_class: Any,
        mock_controller: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        config = ClientConfig(headers={"global": "1"})
        client = WebTransportClient(config=config)

        await client.connect(url="https://example.com", headers={"local": "2"})

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        assert passed_headers == [("global", "1"), ("local", "2"), ("user-agent", f"PyWebTransport/{__version__}")]

    @pytest.mark.asyncio
    async def test_context_manager(self, client: WebTransportClient) -> None:
        async with client as entered_client:
            assert entered_client is client
            assert not client.is_closed

        assert client.is_closed

    def test_create_connection_success(
        self, client: WebTransportClient, mock_controller: Any, mock_connection_class: Any
    ) -> None:
        result = client._create_connection(mock_controller, 7)

        assert result is mock_connection_class.return_value
        mock_connection_class.assert_called_once_with(
            handle=7, controller=mock_controller, config=client.config, is_client=True
        )

    def test_initialization_custom_config(self, mocker: MockerFixture) -> None:
        mock_config = mocker.Mock(spec=ClientConfig)

        client = WebTransportClient(config=mock_config)

        assert client.config is mock_config
        assert client._controller is None
        assert not client.is_closed

    def test_initialization_default(self) -> None:
        client = WebTransportClient()

        assert isinstance(client.config, ClientConfig)
        assert client._controller is None
        assert not client.is_closed

    def test_local_addresses_empty(self, client: WebTransportClient, mock_controller: Any) -> None:
        client._controller = mock_controller
        mock_controller.get_local_addresses.return_value = []

        assert client.local_addresses == []

    def test_local_addresses_no_controller(self, client: WebTransportClient) -> None:
        client._controller = None

        assert client.local_addresses == []

    def test_local_addresses_with_controller(self, client: WebTransportClient, mock_controller: Any) -> None:
        client._controller = mock_controller

        assert client.local_addresses == [("192.0.2.1", 0)]

    @pytest.mark.asyncio
    async def test_race_addresses_controller_not_initialized(
        self, client: WebTransportClient, mock_client_config: Any
    ) -> None:
        client._controller = None

        with pytest.raises(expected_exception=ConnectionError) as exception_info:
            await client._race_addresses(
                addresses=["127.0.0.1"], host="example.com", port=443, connection_config=mock_client_config
            )

        assert isinstance(exception_info.value.__cause__, ClientError)
        assert "app_client validate failed" in str(exception_info.value.__cause__)

    @pytest.mark.asyncio
    async def test_race_addresses_empty_list(
        self, client: WebTransportClient, mock_client_config: Any, mocker: MockerFixture
    ) -> None:
        client._controller = mocker.MagicMock()

        with pytest.raises(
            expected_exception=ConnectionError, match="wt_connection open failed actual=example.com"
        ) as exception_info:
            await client._race_addresses(
                addresses=[], host="example.com", port=443, connection_config=mock_client_config
            )

        assert exception_info.value.__cause__ is None


def test_merge_headers_dict() -> None:
    base: Headers = {"a": "1"}
    update: Headers = {"b": "2"}

    result = _merge_headers(base=base, update=update)

    assert result == {"a": "1", "b": "2"}


def test_merge_headers_list() -> None:
    base: Headers = [("a", "1")]
    update: Headers = [("b", "2")]

    result = _merge_headers(base=base, update=update)

    assert result == [("a", "1"), ("b", "2")]


def test_merge_headers_mixed() -> None:
    base: Headers = {"a": "1"}
    update: Headers = [("b", "2")]

    result = _merge_headers(base=base, update=update)

    assert result == [("a", "1"), ("b", "2")]


def test_merge_headers_none() -> None:
    base: Headers = {"a": "1"}
    base_list: Headers = [("a", "1")]

    assert _merge_headers(base=base, update=None) == {"a": "1"}
    assert _merge_headers(base=base_list, update=None) == [("a", "1")]


def test_normalize_headers_dict() -> None:
    headers: Headers = {"Content-Type": "application/json", "USER-AGENT": "test-client"}

    normalized = _normalize_headers(headers=headers)

    assert isinstance(normalized, dict)
    assert normalized == {"content-type": "application/json", "user-agent": "test-client"}


def test_normalize_headers_list() -> None:
    headers: Headers = [("Content-Type", "application/json"), ("USER-AGENT", "test-client")]

    normalized = _normalize_headers(headers=headers)

    assert isinstance(normalized, list)
    assert normalized == [("content-type", "application/json"), ("user-agent", "test-client")]


@pytest.mark.parametrize(
    argnames="url, expected_message",
    argvalues=[
        ("ftp://example.com", "wt_url validate invalid actual=ftp expected=https"),
        ("http://example.com", "wt_url validate invalid actual=http expected=https"),
        ("https://", "wt_url validate invalid"),
    ],
)
def test_parse_webtransport_url_raises_error(url: str, expected_message: str) -> None:
    with pytest.raises(expected_exception=ClientError, match=expected_message):
        _parse_webtransport_url(url=url)


def test_parse_webtransport_url_raises_error_on_malformed_port() -> None:
    with pytest.raises(expected_exception=ClientError, match="wt_url validate invalid actual="):
        _parse_webtransport_url(url="https://example.com:notaport")


@pytest.mark.parametrize(
    argnames="url, expected",
    argvalues=[
        ("https://[::1]:9090/q?a=1#f", _ConnectionTarget(authority="[::1]:9090", host="::1", path="/q?a=1", port=9090)),
        ("https://example.com", _ConnectionTarget(authority="example.com", host="example.com", path="/", port=443)),
        ("https://example.com:0", _ConnectionTarget(authority="example.com:0", host="example.com", path="/", port=0)),
        (
            "https://localhost:8080/path",
            _ConnectionTarget(authority="localhost:8080", host="localhost", path="/path", port=8080),
        ),
        (
            "https://user:pass@test.com:4433",
            _ConnectionTarget(authority="test.com:4433", host="test.com", path="/", port=4433),
        ),
    ],
)
def test_parse_webtransport_url_success(url: str, expected: _ConnectionTarget) -> None:
    parsed_url = _parse_webtransport_url(url=url)

    assert parsed_url == expected


@pytest.mark.asyncio
async def test_resolve_host_domain_name(mocker: MockerFixture) -> None:
    mock_loop = mocker.MagicMock()
    mock_loop.getaddrinfo = mocker.AsyncMock(
        return_value=[(socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.100", 0))]
    )
    mocker.patch(target="asyncio.get_running_loop", return_value=mock_loop)

    result = await _resolve_host(host="example.com")

    assert result == ["192.0.2.100"]
    mock_loop.getaddrinfo.assert_awaited_once_with(
        host="example.com", port=0, family=socket.AF_UNSPEC, type=socket.SOCK_DGRAM
    )


@pytest.mark.asyncio
async def test_resolve_host_empty_results(mocker: MockerFixture) -> None:
    mock_loop = mocker.MagicMock()
    mock_loop.getaddrinfo = mocker.AsyncMock(return_value=[])
    mocker.patch(target="asyncio.get_running_loop", return_value=mock_loop)

    with pytest.raises(expected_exception=ConnectionError, match="sys resolve failed actual=empty.local"):
        await _resolve_host(host="empty.local")


@pytest.mark.asyncio
async def test_resolve_host_gaierror_translation(mocker: MockerFixture) -> None:
    mock_loop = mocker.MagicMock()
    mock_loop.getaddrinfo = mocker.AsyncMock(side_effect=socket.gaierror("Name or service not known"))
    mocker.patch(target="asyncio.get_running_loop", return_value=mock_loop)

    with pytest.raises(expected_exception=ConnectionError, match="sys resolve failed actual=invalid.local"):
        await _resolve_host(host="invalid.local")


@pytest.mark.asyncio
async def test_resolve_host_ipv4_fast_path(mocker: MockerFixture) -> None:
    mock_loop = mocker.patch(target="asyncio.get_running_loop")

    result = await _resolve_host(host="192.0.2.1")

    assert result == ["192.0.2.1"]
    mock_loop.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_host_ipv6_fast_path(mocker: MockerFixture) -> None:
    mock_loop = mocker.patch(target="asyncio.get_running_loop")

    result = await _resolve_host(host="2001:db8::1")

    assert result == ["2001:db8::1"]
    mock_loop.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_host_multiple_ips_and_deduplication(mocker: MockerFixture) -> None:
    mock_loop = mocker.MagicMock()
    mock_loop.getaddrinfo = mocker.AsyncMock(
        return_value=[
            (socket.AF_INET6, socket.SOCK_DGRAM, 17, "", ("2001:db8::1", 0)),
            (socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.100", 0)),
            (socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.100", 0)),
            (socket.AF_INET6, socket.SOCK_DGRAM, 17, "", ("2001:db8::2", 0)),
        ]
    )
    mocker.patch(target="asyncio.get_running_loop", return_value=mock_loop)

    result = await _resolve_host(host="multihomed.local")

    assert result == ["2001:db8::1", "192.0.2.100", "2001:db8::2"]
