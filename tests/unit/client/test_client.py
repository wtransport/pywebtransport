"""Unit tests for the pywebtransport.client.client module."""

import asyncio
from typing import Any, Callable

import pytest
from pytest_mock import MockerFixture

from pywebtransport import (
    ClientConfig,
    ClientError,
    ConnectionError,
    TimeoutError,
    WebTransportClient,
    WebTransportSession,
)
from pywebtransport.client import ClientDiagnostics, ClientStats
from pywebtransport.connection import WebTransportConnection
from pywebtransport.manager import ConnectionManager
from pywebtransport.types import ConnectionState, EventType


class TestClientDiagnostics:

    @pytest.mark.parametrize(
        argnames="stats_data, expected_issue_part",
        argvalues=[
            ({}, None),
            ({"connections_attempted": 20, "success_rate": 0.5}, "Low connection success rate"),
            ({"avg_connect_time": 6.5}, "Slow average connection time"),
        ],
    )
    def test_issues_property(
        self, mocker: MockerFixture, stats_data: dict[str, Any], expected_issue_part: str | None
    ) -> None:
        mock_stats = mocker.create_autospec(spec=ClientStats, instance=True)
        mock_stats.to_dict.return_value = stats_data
        diagnostics = ClientDiagnostics(stats=mock_stats, connection_states={})

        issues = diagnostics.issues

        if expected_issue_part:
            assert any(expected_issue_part in issue for issue in issues)
        else:
            assert not issues


class TestClientStats:

    def test_avg_connect_time(self) -> None:
        stats = ClientStats(created_at=0)

        assert stats.avg_connect_time == 0.0

        stats.connections_successful = 2
        stats.total_connect_time = 5.0

        assert stats.avg_connect_time == 2.5

    def test_initialization(self) -> None:
        stats = ClientStats(created_at=1000.0)

        assert stats.created_at == 1000.0
        assert stats.connections_attempted == 0
        assert stats.connections_successful == 0
        assert stats.connections_failed == 0
        assert stats.total_connect_time == 0.0
        assert stats.min_connect_time == float("inf")
        assert stats.max_connect_time == 0.0

    def test_success_rate(self) -> None:
        stats = ClientStats(created_at=0)

        assert stats.success_rate == 1.0

        stats.connections_attempted = 10
        stats.connections_successful = 8

        assert stats.success_rate == 0.8

        stats.connections_attempted = 10
        stats.connections_successful = 0

        assert stats.success_rate == 0.0

    def test_to_dict(self, mocker: MockerFixture) -> None:
        mocker.patch(target="pywebtransport.client.client.get_timestamp", return_value=1010.0)
        stats = ClientStats(created_at=1000.0)
        stats.min_connect_time = 1.2

        stats_dict = stats.to_dict()

        assert stats_dict["uptime"] == 10.0
        assert stats_dict["min_connect_time"] == 1.2
        assert stats_dict["max_connect_time"] == 0.0

        stats.min_connect_time = float("inf")
        stats_dict = stats.to_dict()

        assert stats_dict["min_connect_time"] == 0.0


class TestWebTransportClient:

    @pytest.fixture
    def client(self, mock_client_config: Any, mock_connection_manager: Any) -> WebTransportClient:
        return WebTransportClient(config=mock_client_config)

    @pytest.fixture
    def mock_client_config(self, mocker: MockerFixture) -> Any:
        mock = mocker.create_autospec(spec=ClientConfig, instance=True)
        mock.connect_timeout = 10.0
        mock.connection_attempt_delay = 0.250
        mock.headers = None
        mock.max_connections = 100
        mock.connection_idle_timeout = 60.0
        mock.max_event_queue_size = 100
        mock.max_event_listeners = 50
        mock.max_event_history_size = 100
        mock.subprotocols = None
        mock.user_agent = None

        return mock

    @pytest.fixture
    def mock_conn_factory(self, mocker: MockerFixture, mock_session: Any) -> Callable[[], Any]:
        def _factory() -> Any:
            conn = mocker.create_autospec(spec=WebTransportConnection, instance=True)
            conn.state = ConnectionState.CONNECTING
            conn.is_closed = False
            conn.is_connected = False
            conn.events = mocker.MagicMock()
            conn.events.wait_for = mocker.AsyncMock()
            conn.create_session = mocker.AsyncMock(return_value=mock_session)
            conn.close = mocker.AsyncMock()
            return conn

        return _factory

    @pytest.fixture
    def mock_connection_cls(self, mocker: MockerFixture, mock_webtransport_connection: Any) -> Any:
        return mocker.patch(
            target="pywebtransport.client.client.WebTransportConnection", return_value=mock_webtransport_connection
        )

    @pytest.fixture
    def mock_connection_manager(self, mocker: MockerFixture) -> Any:
        manager = mocker.create_autospec(spec=ConnectionManager, instance=True)
        manager.__aenter__ = mocker.AsyncMock()
        manager.__aexit__ = mocker.AsyncMock()
        manager.__len__.return_value = 0
        mocker.patch(target="pywebtransport.client.client.ConnectionManager", return_value=manager)

        return manager

    @pytest.fixture
    def mock_controller_cls(self, mocker: MockerFixture, mock_controller: Any) -> Any:
        return mocker.patch(target="pywebtransport.client.client.EndpointController", return_value=mock_controller)

    @pytest.fixture
    def mock_controller(self, mocker: MockerFixture) -> Any:
        controller = mocker.MagicMock()
        controller.connect = mocker.AsyncMock(return_value=42)

        return controller

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> Any:
        session = mocker.create_autospec(spec=WebTransportSession, instance=True)
        session.session_id = "session-123"
        session.is_closed = False

        return session

    @pytest.fixture
    def mock_webtransport_connection(self, mocker: MockerFixture, mock_session: Any) -> Any:
        connection = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        connection.is_closed = False
        connection.state = ConnectionState.CONNECTED
        connection.is_connected = True
        connection.events = mocker.MagicMock()
        connection.events.wait_for = mocker.AsyncMock()
        connection.create_session = mocker.AsyncMock(return_value=mock_session)
        connection.close = mocker.AsyncMock()

        return connection

    @pytest.fixture(autouse=True)
    def setup_common_mocks(self, mocker: MockerFixture) -> None:
        mocker.patch(
            target="pywebtransport.client.client.parse_webtransport_url", return_value=("example.com", 443, "/")
        )
        mocker.patch(target="pywebtransport.client.client.format_duration")
        mocker.patch(target="pywebtransport.client.client.get_timestamp", return_value=1000.0)
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["192.0.2.1"],
            new_callable=mocker.AsyncMock,
        )

    @pytest.mark.asyncio
    async def test_close_closes_controller(
        self, client: WebTransportClient, mock_connection_manager: Any, mock_controller: Any
    ) -> None:
        client._controller = mock_controller

        await client.close()

        mock_connection_manager.shutdown.assert_awaited_once()
        mock_controller.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_idempotency_and_concurrency(
        self, client: WebTransportClient, mock_connection_manager: Any
    ) -> None:
        await asyncio.gather(client.close(), client.close())

        assert client.is_closed
        mock_connection_manager.shutdown.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_sequential_calls(self, client: WebTransportClient, mock_connection_manager: Any) -> None:
        await client.close()

        assert client.is_closed

        await client.close()

        mock_connection_manager.shutdown.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_already_initialized(
        self, client: WebTransportClient, mock_controller_cls: Any, mock_connection_cls: Any, mock_controller: Any
    ) -> None:
        client._controller = mock_controller

        await client.connect(url="https://example.com")

        mock_controller_cls.assert_not_called()
        mock_controller.connect.assert_awaited_once_with(
            remote_host="192.0.2.1", remote_port=443, server_name="example.com"
        )
        mock_connection_cls.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_concurrent_initialization(
        self, client: WebTransportClient, mock_controller_cls: Any, mock_connection_cls: Any, mock_controller: Any
    ) -> None:
        await client._init_lock.acquire()

        task1 = asyncio.create_task(coro=client.connect(url="https://example.com/1"))
        task2 = asyncio.create_task(coro=client.connect(url="https://example.com/2"))

        await asyncio.sleep(delay=0.01)

        client._init_lock.release()

        await asyncio.gather(task1, task2)

        mock_controller_cls.assert_called_once()
        assert mock_controller.connect.call_count == 2
        assert mock_connection_cls.call_count == 2

    @pytest.mark.asyncio
    async def test_connect_controller_raises_exception(
        self, client: WebTransportClient, mock_controller_cls: Any, mock_controller: Any
    ) -> None:
        mock_controller.connect.side_effect = RuntimeError("Controller error")

        with pytest.raises(expected_exception=ClientError, match="Failed to connect"):
            await client.connect(url="https://example.com")

    @pytest.mark.asyncio
    async def test_connect_controller_returns_none(self, client: WebTransportClient, mock_controller_cls: Any) -> None:
        mock_controller_cls.return_value = None

        with pytest.raises(
            expected_exception=ClientError, match=r"Failed to connect to .*: .*Failed to initialize endpoint controller"
        ):
            await client.connect(url="https://example.com")

    @pytest.mark.asyncio
    async def test_connect_failure_certificate(
        self, client: WebTransportClient, mock_controller_cls: Any, mock_connection_cls: Any
    ) -> None:
        mock_controller_cls.side_effect = Exception("certificate verify failed")

        with pytest.raises(expected_exception=ConnectionError, match="Certificate verification failed"):
            await client.connect(url="https://example.com")

        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_failure_connection_refused(
        self, client: WebTransportClient, mock_controller_cls: Any, mock_connection_cls: Any
    ) -> None:
        mock_controller_cls.side_effect = ConnectionRefusedError()

        with pytest.raises(expected_exception=ConnectionError, match="Connection refused"):
            await client.connect(url="https://example.com")

        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_failure_generic(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        mock_controller_cls.side_effect = RuntimeError("Generic failure")

        with pytest.raises(expected_exception=ClientError, match="Failed to connect to .*: Generic failure"):
            await client.connect(url="https://example.com")

        await asyncio.sleep(delay=0.01)
        mock_webtransport_connection.close.assert_not_awaited()
        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_failure_timeout(
        self, client: WebTransportClient, mock_controller_cls: Any, mock_connection_cls: Any
    ) -> None:
        mock_controller_cls.side_effect = asyncio.TimeoutError()

        with pytest.raises(expected_exception=TimeoutError, match="Connection timeout to .* during .*"):
            await client.connect(url="https://example.com")

        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_fails_during_session_creation(
        self,
        client: WebTransportClient,
        mock_webtransport_connection: Any,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
    ) -> None:
        mock_webtransport_connection.create_session.side_effect = RuntimeError("Session init failed")

        with pytest.raises(expected_exception=ClientError, match="Session init failed"):
            await client.connect(url="https://example.com")

        mock_webtransport_connection.close.assert_awaited_once()
        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_fails_initial_handshake(
        self,
        client: WebTransportClient,
        mock_webtransport_connection: Any,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
    ) -> None:
        mock_webtransport_connection.state = ConnectionState.FAILED

        with pytest.raises(expected_exception=ClientError, match="Failed to connect to any resolved IP"):
            await client.connect(url="https://example.com")

        await asyncio.sleep(delay=0.01)
        mock_webtransport_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_all_fail(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_conn_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["2001:db8::1", "192.0.2.1"],
            new_callable=mocker.AsyncMock,
        )

        conn1 = mock_conn_factory()
        conn1.state = ConnectionState.FAILED
        conn2 = mock_conn_factory()
        conn2.state = ConnectionState.FAILED

        mock_connection_cls.side_effect = [conn1, conn2]

        with pytest.raises(expected_exception=ClientError, match="Failed to connect to any resolved IP"):
            await client.connect(url="https://example.com")

        await asyncio.sleep(delay=0.01)
        assert mock_connection_cls.call_count == 2
        conn1.close.assert_awaited_once()
        conn2.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_fallback(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_session: Any,
        mock_conn_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["2001:db8::1", "192.0.2.1"],
            new_callable=mocker.AsyncMock,
        )

        conn1 = mock_conn_factory()
        conn1.state = ConnectionState.FAILED
        conn2 = mock_conn_factory()
        conn2.state = ConnectionState.CONNECTED
        conn2.is_connected = True

        mock_connection_cls.side_effect = [conn1, conn2]

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        await asyncio.sleep(delay=0.01)
        assert mock_connection_cls.call_count == 2
        conn1.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_outer_loop_simultaneous_success_and_failure(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_session: Any,
        mock_conn_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        client.config.connection_attempt_delay = 0.0
        sync_event = asyncio.Event()

        conn1 = mock_conn_factory()
        conn2 = mock_conn_factory()

        async def wait_for_1(*args: Any, **kwargs: Any) -> None:
            await sync_event.wait()
            conn1.state = ConnectionState.CONNECTED

        async def wait_for_2(*args: Any, **kwargs: Any) -> None:
            await sync_event.wait()
            raise ConnectionError(message="Simultaneous failure")

        conn1.events.wait_for.side_effect = wait_for_1
        conn2.events.wait_for.side_effect = wait_for_2

        mock_connection_cls.side_effect = [conn1, conn2]

        original_wait = asyncio.wait

        async def mock_wait(fs: Any, *args: Any, **kwargs: Any) -> tuple[Any, Any]:
            done, pending = await original_wait(fs, *args, **kwargs)

            def is_failure(t: asyncio.Task[Any]) -> bool:
                if t.cancelled():
                    return True
                return t.exception() is not None

            ordered_done = sorted(list(done), key=is_failure)
            return ordered_done, pending

        mocker.patch(target="pywebtransport.client.client.asyncio.wait", side_effect=mock_wait)

        connect_task = asyncio.create_task(coro=client.connect(url="https://example.com"))

        await asyncio.sleep(delay=0.01)
        sync_event.set()

        session = await connect_task

        assert session is mock_session
        await asyncio.sleep(delay=0.01)

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_simultaneous_success(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_session: Any,
        mock_conn_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        client.config.connection_attempt_delay = 0.0
        sync_event = asyncio.Event()

        conn1 = mock_conn_factory()
        conn2 = mock_conn_factory()

        async def wait_for_1(*args: Any, **kwargs: Any) -> None:
            await sync_event.wait()
            conn1.state = ConnectionState.CONNECTED

        async def wait_for_2(*args: Any, **kwargs: Any) -> None:
            await sync_event.wait()
            conn2.state = ConnectionState.CONNECTED

        conn1.events.wait_for.side_effect = wait_for_1
        conn2.events.wait_for.side_effect = wait_for_2

        mock_connection_cls.side_effect = [conn1, conn2]

        connect_task = asyncio.create_task(coro=client.connect(url="https://example.com"))

        await asyncio.sleep(delay=0.01)
        sync_event.set()

        session = await connect_task

        assert session is mock_session
        await asyncio.sleep(delay=0.01)
        assert conn1.close.call_count + conn2.close.call_count == 1

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_staggered_failure(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_conn_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        client.config.connection_attempt_delay = 0.1

        conn1 = mock_conn_factory()
        conn2 = mock_conn_factory()

        async def wait_for_fail(*args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(delay=0.2)
            raise ConnectionError(message="Simulated failure")

        conn1.events.wait_for.side_effect = wait_for_fail
        conn2.events.wait_for.side_effect = wait_for_fail

        mock_connection_cls.side_effect = [conn1, conn2]

        with pytest.raises(expected_exception=ClientError, match="Failed to connect to any resolved IP"):
            await client.connect(url="https://example.com")

        await asyncio.sleep(delay=0.01)
        conn1.close.assert_awaited_once()
        conn2.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_staggered_success(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_session: Any,
        mock_conn_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["10.0.0.1", "10.0.0.2"],
            new_callable=mocker.AsyncMock,
        )
        client.config.connection_attempt_delay = 0.1

        conn1 = mock_conn_factory()
        conn2 = mock_conn_factory()

        async def wait_for_1(*args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(delay=0.3)
            conn1.state = ConnectionState.CONNECTED

        async def wait_for_2(*args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(delay=0.1)
            conn2.state = ConnectionState.CONNECTED

        conn1.events.wait_for.side_effect = wait_for_1
        conn2.events.wait_for.side_effect = wait_for_2

        mock_connection_cls.side_effect = [conn1, conn2]

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        await asyncio.sleep(delay=0.01)
        conn1.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_while_loop_double_success(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_session: Any,
        mock_conn_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["10.0.0.1", "10.0.0.2", "10.0.0.3"],
            new_callable=mocker.AsyncMock,
        )
        client.config.connection_attempt_delay = 0.1
        sync_event = asyncio.Event()

        conn1 = mock_conn_factory()
        conn2 = mock_conn_factory()
        conn3 = mock_conn_factory()

        async def wait_for_1(*args: Any, **kwargs: Any) -> None:
            await sync_event.wait()
            conn1.state = ConnectionState.CONNECTED

        async def wait_for_2(*args: Any, **kwargs: Any) -> None:
            await sync_event.wait()
            conn2.state = ConnectionState.CONNECTED

        async def wait_for_3(*args: Any, **kwargs: Any) -> None:
            raise ConnectionError(message="Fast failure")

        conn1.events.wait_for.side_effect = wait_for_1
        conn2.events.wait_for.side_effect = wait_for_2
        conn3.events.wait_for.side_effect = wait_for_3

        mock_connection_cls.side_effect = [conn1, conn2, conn3]

        async def trigger_event() -> None:
            await asyncio.sleep(delay=0.3)
            sync_event.set()

        asyncio.create_task(coro=trigger_event())

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        await asyncio.sleep(delay=0.01)
        assert conn1.close.call_count + conn2.close.call_count == 1
        conn3.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_happy_eyeballs_while_loop_simultaneous_success_and_failure(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_session: Any,
        mock_conn_factory: Callable[[], Any],
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            target="pywebtransport.client.client.resolve_host",
            return_value=["10.0.0.1", "10.0.0.2", "10.0.0.3"],
            new_callable=mocker.AsyncMock,
        )
        client.config.connection_attempt_delay = 0.1
        sync_event = asyncio.Event()

        conn1 = mock_conn_factory()
        conn2 = mock_conn_factory()
        conn3 = mock_conn_factory()

        async def wait_for_1(*args: Any, **kwargs: Any) -> None:
            await sync_event.wait()
            conn1.state = ConnectionState.CONNECTED

        async def wait_for_2(*args: Any, **kwargs: Any) -> None:
            await sync_event.wait()
            raise ConnectionError(message="Simultaneous failure")

        async def wait_for_3(*args: Any, **kwargs: Any) -> None:
            raise ConnectionError(message="Fast failure")

        conn1.events.wait_for.side_effect = wait_for_1
        conn2.events.wait_for.side_effect = wait_for_2
        conn3.events.wait_for.side_effect = wait_for_3

        mock_connection_cls.side_effect = [conn1, conn2, conn3]

        original_wait = asyncio.wait

        async def mock_wait(fs: Any, *args: Any, **kwargs: Any) -> tuple[Any, Any]:
            done, pending = await original_wait(fs, *args, **kwargs)

            def is_failure(t: asyncio.Task[Any]) -> bool:
                if t.cancelled():
                    return True
                return t.exception() is not None

            ordered_done = sorted(list(done), key=is_failure)
            return ordered_done, pending

        mocker.patch(target="pywebtransport.client.client.asyncio.wait", side_effect=mock_wait)

        async def trigger_event() -> None:
            await asyncio.sleep(delay=0.3)
            sync_event.set()

        asyncio.create_task(coro=trigger_event())

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        await asyncio.sleep(delay=0.01)
        conn3.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_success(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_webtransport_connection: Any,
        mock_session: Any,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(target="pywebtransport.client.client.get_timestamp", side_effect=[2000.0, 2001.23])

        session = await client.connect(url="https://example.com")

        mock_controller_cls.assert_called_once()
        mock_controller.connect.assert_awaited_once_with(
            remote_host="192.0.2.1", remote_port=443, server_name="example.com"
        )
        mock_connection_cls.assert_called_once()

        args, kwargs = mock_connection_cls.call_args
        assert kwargs["controller"] is mock_controller
        assert kwargs["handle"] == 42
        assert kwargs["is_client"] is True

        mock_connection_manager.add_connection.assert_awaited_once_with(connection=mock_webtransport_connection)
        mock_webtransport_connection.create_session.assert_awaited_once()

        _, session_kwargs = mock_webtransport_connection.create_session.call_args
        assert session_kwargs["path"] == "/"
        assert session_kwargs["subprotocols"] is None

        headers = session_kwargs["headers"]
        if isinstance(headers, dict):
            assert "user-agent" in headers
        else:
            assert any(k == "user-agent" for k, v in headers)

        assert session is mock_session
        stats = client._stats
        assert stats.connections_successful == 1
        assert stats.total_connect_time == pytest.approx(expected=1.23)

    @pytest.mark.asyncio
    async def test_connect_success_with_subprotocols(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_webtransport_connection: Any,
        mock_session: Any,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(target="pywebtransport.client.client.get_timestamp", side_effect=[2000.0, 2001.23])

        session = await client.connect(url="https://example.com", subprotocols=["h3", "dummy"])

        assert session is mock_session
        mock_webtransport_connection.create_session.assert_awaited_once()

        _, session_kwargs = mock_webtransport_connection.create_session.call_args
        assert session_kwargs["subprotocols"] == ["h3", "dummy"]

    @pytest.mark.asyncio
    async def test_connect_success_with_subprotocols_from_config(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_webtransport_connection: Any,
        mock_session: Any,
        mocker: MockerFixture,
    ) -> None:
        mock_client_config.subprotocols = ["config-proto"]
        mocker.patch(target="pywebtransport.client.client.get_timestamp", side_effect=[2000.0, 2001.23])

        session = await client.connect(url="https://example.com")

        assert session is mock_session
        mock_webtransport_connection.create_session.assert_awaited_once()

        _, session_kwargs = mock_webtransport_connection.create_session.call_args
        assert session_kwargs["subprotocols"] == ["config-proto"]

    @pytest.mark.asyncio
    async def test_connect_ua_from_config(
        self,
        client: WebTransportClient,
        mock_client_config: Any,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        mock_client_config.user_agent = "CustomClient/1.2.3"

        await client.connect(url="https://example.com")

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        if isinstance(passed_headers, dict):
            assert passed_headers["user-agent"] == "CustomClient/1.2.3"
        else:
            ua_header = next((v for k, v in passed_headers if k == "user-agent"), None)
            assert ua_header == "CustomClient/1.2.3"

    @pytest.mark.asyncio
    async def test_connect_ua_injection_dict_mode(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_client_config: Any,
        mock_webtransport_connection: Any,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(target="pywebtransport.client.client.normalize_headers", return_value={"host": "example.com"})

        await client.connect(url="https://example.com")

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        assert isinstance(passed_headers, dict)
        assert "user-agent" in passed_headers
        assert "PyWebTransport" in passed_headers["user-agent"]

    @pytest.mark.asyncio
    async def test_connect_waits_for_events_if_not_connected(
        self,
        client: WebTransportClient,
        mock_webtransport_connection: Any,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
    ) -> None:
        mock_webtransport_connection.state = ConnectionState.CONNECTING

        async def simulate_connect(*args: Any, **kwargs: Any) -> None:
            mock_webtransport_connection.state = ConnectionState.CONNECTED

        mock_webtransport_connection.events.wait_for.side_effect = simulate_connect

        await client.connect(url="https://example.com")

        mock_webtransport_connection.events.wait_for.assert_awaited_once()
        call_args = mock_webtransport_connection.events.wait_for.call_args[1]

        assert EventType.CONNECTION_ESTABLISHED in call_args["event_type"]
        assert EventType.CONNECTION_FAILED in call_args["event_type"]

    @pytest.mark.asyncio
    async def test_connect_waits_for_events_raises_exception(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_controller: Any,
        mock_connection_manager: Any,
        mock_conn_factory: Callable[[], Any],
    ) -> None:
        conn = mock_conn_factory()
        conn.events.wait_for.side_effect = asyncio.TimeoutError("Wait timeout")
        mock_connection_cls.return_value = conn

        with pytest.raises(expected_exception=ClientError, match="Failed to connect"):
            await client.connect(url="https://example.com")

        await asyncio.sleep(delay=0.01)
        conn.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_when_closed(self, client: WebTransportClient) -> None:
        await client.close()

        with pytest.raises(expected_exception=ClientError, match="Client is closed"):
            await client.connect(url="https://example.com")

    @pytest.mark.asyncio
    async def test_connect_with_explicit_user_agent_header(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_client_config: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        custom_ua = "ExplicitUA/1.0"

        await client.connect(url="https://example.com", headers={"user-agent": custom_ua})

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        if isinstance(passed_headers, dict):
            assert passed_headers["user-agent"] == custom_ua
        else:
            ua_values = [v for k, v in passed_headers if k == "user-agent"]
            assert custom_ua in ua_values

    @pytest.mark.asyncio
    async def test_connect_with_headers_merging(
        self,
        client: WebTransportClient,
        mock_controller_cls: Any,
        mock_connection_cls: Any,
        mock_client_config: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        mock_client_config.headers = {"global": "1"}
        client.set_default_headers(headers={"default": "2"})

        await client.connect(url="https://example.com", headers={"local": "3"})

        mock_webtransport_connection.create_session.assert_awaited_once()
        passed_headers = mock_webtransport_connection.create_session.call_args.kwargs["headers"]

        if isinstance(passed_headers, dict):
            assert passed_headers["global"] == "1"
            assert passed_headers["default"] == "2"
            assert passed_headers["local"] == "3"
        else:
            header_dict = dict(passed_headers)
            assert header_dict["global"] == "1"
            assert header_dict["default"] == "2"
            assert header_dict["local"] == "3"

    @pytest.mark.asyncio
    async def test_context_manager(self, client: WebTransportClient, mock_connection_manager: Any) -> None:
        async with client:
            mock_connection_manager.__aenter__.assert_awaited_once()

        mock_connection_manager.shutdown.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_diagnostics(
        self, client: WebTransportClient, mock_connection_manager: Any, mocker: MockerFixture
    ) -> None:
        mock_conn = mocker.MagicMock()
        mock_conn.state = ConnectionState.CONNECTED
        mock_connection_manager.get_all_resources = mocker.AsyncMock(return_value=[mock_conn])

        diagnostics = await client.diagnostics()

        assert isinstance(diagnostics, ClientDiagnostics)
        assert diagnostics.stats is client._stats
        assert diagnostics.connection_states == {ConnectionState.CONNECTED: 1}

    def test_initialization_custom_config(self, mocker: MockerFixture) -> None:
        mock_config = mocker.Mock(spec=ClientConfig)
        mock_config.max_connections = 15
        mock_config.connection_idle_timeout = 30.0
        mock_config.max_event_queue_size = 50
        mock_config.max_event_listeners = 20
        mock_config.max_event_history_size = 50
        mock_cm = mocker.patch(target="pywebtransport.client.client.ConnectionManager", autospec=True)

        client = WebTransportClient(config=mock_config)

        assert client.config is mock_config
        assert client._controller is None
        mock_cm.assert_called_once_with(max_connections=15)

    def test_initialization_default(self, mocker: MockerFixture) -> None:
        mock_cm_constructor = mocker.patch(target="pywebtransport.client.client.ConnectionManager", autospec=True)

        WebTransportClient()

        mock_cm_constructor.assert_called_once_with(max_connections=100)

    @pytest.mark.asyncio
    async def test_race_addresses_controller_not_initialized(
        self, client: WebTransportClient, mock_client_config: Any
    ) -> None:
        client._controller = None

        with pytest.raises(expected_exception=ConnectionError) as exc_info:
            await client._race_addresses(
                addresses=["127.0.0.1"], port=443, host="example.com", conn_config=mock_client_config
            )

        assert isinstance(exc_info.value.__cause__, ClientError)
        assert "Endpoint controller is not initialized" in str(exc_info.value.__cause__)

    def test_str_representation(self, client: WebTransportClient, mock_connection_manager: Any) -> None:
        mock_connection_manager.__len__.return_value = 5

        assert str(client) == "WebTransportClient(status=open, connections=5)"

        client._closed = True

        assert str(client) == "WebTransportClient(status=closed, connections=5)"
