"""Unit tests for the pywebtransport.client.client module."""

import asyncio
from typing import Any

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
        "stats_data, expected_issue_part",
        [
            ({}, None),
            ({"connections_attempted": 20, "success_rate": 0.5}, "Low connection success rate"),
            ({"avg_connect_time": 6.5}, "Slow average connection time"),
        ],
    )
    def test_issues_property(
        self, mocker: MockerFixture, stats_data: dict[str, Any], expected_issue_part: str | None
    ) -> None:
        mock_stats = mocker.create_autospec(ClientStats, instance=True)
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
        mocker.patch("pywebtransport.client.client.get_timestamp", return_value=1010.0)
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
        mock = mocker.create_autospec(ClientConfig, instance=True)
        mock.connect_timeout = 10.0
        mock.update.return_value = mock
        mock.max_connections = 100
        mock.connection_idle_timeout = 60.0
        mock.max_event_queue_size = 100
        mock.max_event_listeners = 50
        mock.max_event_history_size = 100
        mock.user_agent = None
        return mock

    @pytest.fixture
    def mock_connect_class_method(self, mocker: MockerFixture, mock_webtransport_connection: Any) -> Any:
        return mocker.patch(
            "pywebtransport.client.client.WebTransportConnection.connect",
            return_value=mock_webtransport_connection,
            new_callable=mocker.AsyncMock,
        )

    @pytest.fixture
    def mock_connection_manager(self, mocker: MockerFixture) -> Any:
        manager = mocker.create_autospec(ConnectionManager, instance=True)
        manager.__aenter__ = mocker.AsyncMock()
        manager.__aexit__ = mocker.AsyncMock()
        manager.__len__.return_value = 0
        mocker.patch("pywebtransport.client.client.ConnectionManager", return_value=manager)
        return manager

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> Any:
        session = mocker.create_autospec(WebTransportSession, instance=True)
        session.session_id = "session-123"
        session.is_closed = False
        return session

    @pytest.fixture
    def mock_webtransport_connection(self, mocker: MockerFixture, mock_session: Any) -> Any:
        connection = mocker.create_autospec(WebTransportConnection, instance=True)
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
        mocker.patch("pywebtransport.client.client.parse_webtransport_url", return_value=("example.com", 443, "/"))
        mocker.patch("pywebtransport.client.client.format_duration")
        mocker.patch("pywebtransport.client.client.get_timestamp", return_value=1000.0)

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
    async def test_connect_failure_certificate(
        self, client: WebTransportClient, mock_connect_class_method: Any
    ) -> None:
        mock_connect_class_method.side_effect = Exception("certificate verify failed")

        with pytest.raises(ConnectionError, match="Certificate verification failed"):
            await client.connect(url="https://example.com")

        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_failure_connection_refused(
        self, client: WebTransportClient, mock_connect_class_method: Any
    ) -> None:
        mock_connect_class_method.side_effect = ConnectionRefusedError()

        with pytest.raises(ConnectionError, match="Connection refused"):
            await client.connect(url="https://example.com")

        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_failure_generic(
        self, client: WebTransportClient, mock_connect_class_method: Any, mock_webtransport_connection: Any
    ) -> None:
        mock_connect_class_method.side_effect = RuntimeError("Generic failure")

        with pytest.raises(ClientError, match="Failed to connect to .*: Generic failure"):
            await client.connect(url="https://example.com")

        mock_webtransport_connection.close.assert_not_awaited()
        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_failure_timeout(self, client: WebTransportClient, mock_connect_class_method: Any) -> None:
        mock_connect_class_method.side_effect = asyncio.TimeoutError()

        with pytest.raises(TimeoutError, match="Connection timeout to .* during .*"):
            await client.connect(url="https://example.com")

        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_fails_during_session_creation(
        self, client: WebTransportClient, mock_webtransport_connection: Any, mock_connect_class_method: Any
    ) -> None:
        mock_webtransport_connection.create_session.side_effect = RuntimeError("Session init failed")

        with pytest.raises(ClientError, match="Session init failed"):
            await client.connect(url="https://example.com")

        mock_webtransport_connection.close.assert_awaited_once()
        assert client._stats.connections_failed == 1

    @pytest.mark.asyncio
    async def test_connect_fails_initial_handshake(
        self, client: WebTransportClient, mock_webtransport_connection: Any, mock_connect_class_method: Any
    ) -> None:
        mock_webtransport_connection.state = ConnectionState.FAILED

        with pytest.raises(ClientError, match="Connection failed state"):
            await client.connect(url="https://example.com")

        mock_webtransport_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_success(
        self,
        client: WebTransportClient,
        mock_connect_class_method: Any,
        mock_connection_manager: Any,
        mock_webtransport_connection: Any,
        mock_session: Any,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch("pywebtransport.client.client.get_timestamp", side_effect=[2000.0, 2001.23])

        session = await client.connect(url="https://example.com")

        mock_connect_class_method.assert_awaited_once()
        mock_connection_manager.add_connection.assert_awaited_once_with(connection=mock_webtransport_connection)
        mock_webtransport_connection.create_session.assert_awaited_once()
        args, kwargs = mock_webtransport_connection.create_session.call_args
        assert kwargs["path"] == "/"

        headers = kwargs["headers"]
        if isinstance(headers, dict):
            assert "user-agent" in headers
        else:
            assert any(k == "user-agent" for k, v in headers)

        assert session is mock_session
        stats = client._stats
        assert stats.connections_successful == 1
        assert stats.total_connect_time == pytest.approx(1.23)

    @pytest.mark.asyncio
    async def test_connect_ua_from_config(
        self, client: WebTransportClient, mock_client_config: Any, mock_connect_class_method: Any
    ) -> None:
        mock_client_config.user_agent = "CustomClient/1.2.3"

        await client.connect(url="https://example.com")

        mock_client_config.update.assert_called_once()
        passed_headers = mock_client_config.update.call_args.kwargs["headers"]

        if isinstance(passed_headers, dict):
            assert passed_headers["user-agent"] == "CustomClient/1.2.3"
        else:
            ua_header = next((v for k, v in passed_headers if k == "user-agent"), None)
            assert ua_header == "CustomClient/1.2.3"

    @pytest.mark.asyncio
    async def test_connect_ua_injection_dict_mode(
        self, client: WebTransportClient, mock_connect_class_method: Any, mock_client_config: Any, mocker: MockerFixture
    ) -> None:
        mocker.patch("pywebtransport.client.client.normalize_headers", return_value={"host": "example.com"})

        await client.connect(url="https://example.com")

        mock_client_config.update.assert_called_once()
        passed_headers = mock_client_config.update.call_args.kwargs["headers"]

        assert isinstance(passed_headers, dict)
        assert "user-agent" in passed_headers
        assert "PyWebTransport" in passed_headers["user-agent"]

    @pytest.mark.asyncio
    async def test_connect_waits_for_events_if_not_connected(
        self, client: WebTransportClient, mock_webtransport_connection: Any, mock_connect_class_method: Any
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
    async def test_connect_when_closed(self, client: WebTransportClient) -> None:
        await client.close()

        with pytest.raises(ClientError, match="Client is closed"):
            await client.connect(url="https://example.com")

    @pytest.mark.asyncio
    async def test_connect_with_explicit_user_agent_header(
        self, client: WebTransportClient, mock_connect_class_method: Any, mock_client_config: Any
    ) -> None:
        custom_ua = "ExplicitUA/1.0"

        await client.connect(url="https://example.com", headers={"user-agent": custom_ua})

        mock_client_config.update.assert_called_once()
        passed_headers = mock_client_config.update.call_args.kwargs["headers"]

        if isinstance(passed_headers, dict):
            assert passed_headers["user-agent"] == custom_ua
        else:
            ua_values = [v for k, v in passed_headers if k == "user-agent"]
            assert custom_ua in ua_values

    @pytest.mark.asyncio
    async def test_connect_with_headers(
        self,
        client: WebTransportClient,
        mock_connect_class_method: Any,
        mock_client_config: Any,
        mock_webtransport_connection: Any,
    ) -> None:
        client.set_default_headers(headers={"default": "header"})

        await client.connect(url="https://example.com", headers={"extra": "header"})

        mock_client_config.update.assert_called_once()
        passed_headers = mock_client_config.update.call_args.kwargs["headers"]

        if isinstance(passed_headers, dict):
            assert passed_headers["default"] == "header"
            assert passed_headers["extra"] == "header"
            assert "user-agent" in passed_headers
        else:
            header_dict = dict(passed_headers)
            assert header_dict["default"] == "header"
            assert header_dict["extra"] == "header"
            assert "user-agent" in header_dict

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
        mock_cm = mocker.patch("pywebtransport.client.client.ConnectionManager", autospec=True)

        client = WebTransportClient(config=mock_config)

        assert client.config is mock_config
        mock_cm.assert_called_once_with(max_connections=15)

    def test_initialization_default(self, mocker: MockerFixture) -> None:
        mock_cm_constructor = mocker.patch("pywebtransport.client.client.ConnectionManager", autospec=True)

        WebTransportClient()

        mock_cm_constructor.assert_called_once_with(max_connections=100)

    def test_str_representation(self, client: WebTransportClient, mock_connection_manager: Any) -> None:
        mock_connection_manager.__len__.return_value = 5

        assert str(client) == "WebTransportClient(status=open, connections=5)"

        client._closed = True

        assert str(client) == "WebTransportClient(status=closed, connections=5)"
