"""Unit tests for the pywebtransport.client.fleet module."""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from _pytest.logging import LogCaptureFixture
from pytest_asyncio import fixture as asyncio_fixture
from pytest_mock import MockerFixture

from pywebtransport import ClientError, ConnectionError, WebTransportClient, WebTransportSession
from pywebtransport.client import ClientFleet


class TestClientFleet:

    @asyncio_fixture
    async def fleet(self, fleet_unactivated: ClientFleet) -> AsyncGenerator[ClientFleet, None]:
        async with fleet_unactivated as activated_fleet:
            yield activated_fleet

    @pytest.fixture
    def fleet_unactivated(self, mock_clients: list[Any]) -> ClientFleet:
        return ClientFleet(clients=mock_clients)

    @pytest.fixture
    def mock_clients(self, mocker: MockerFixture) -> list[Any]:
        clients = []
        for i in range(3):
            client = mocker.create_autospec(spec=WebTransportClient, instance=True, name=f"Client-{i}")
            client.__aenter__ = mocker.AsyncMock(return_value=client)
            client.__aexit__ = mocker.AsyncMock(return_value=None)
            client.connect = mocker.AsyncMock(return_value=mocker.create_autospec(spec=WebTransportSession))
            clients.append(client)
        return clients

    @pytest.mark.asyncio
    async def test_aenter_and_aexit_lifecycle(self, fleet_unactivated: ClientFleet, mock_clients: list[Any]) -> None:
        async with fleet_unactivated:
            assert fleet_unactivated._active
            for client in mock_clients:
                cast(AsyncMock, client.__aenter__).assert_awaited_once()

        for client in mock_clients:
            cast(AsyncMock, client.__aexit__).assert_awaited_once()
        assert not fleet_unactivated._active

    @pytest.mark.asyncio
    async def test_aenter_cleanup_error(self, mock_clients: list[Any], caplog: LogCaptureFixture) -> None:
        successful_client = mock_clients[0]
        cast(AsyncMock, successful_client.__aenter__).return_value = successful_client
        cast(AsyncMock, successful_client.__aexit__).side_effect = IOError("Cleanup fail")

        failing_client = mock_clients[1]
        cast(AsyncMock, failing_client.__aenter__).side_effect = RuntimeError("Startup fail")

        fleet = ClientFleet(clients=mock_clients)

        with pytest.raises(expected_exception=ExceptionGroup):
            async with fleet:
                pass

        assert "Error during fleet cleanup after activation failure" in caplog.text

    @pytest.mark.asyncio
    async def test_aenter_rollback_logic(self, mock_clients: list[Any], mocker: MockerFixture) -> None:
        successful_client = mock_clients[0]
        failing_client = mock_clients[1]
        started_event = asyncio.Event()

        async def success_side_effect() -> Any:
            started_event.set()
            return successful_client

        async def fail_side_effect() -> None:
            await started_event.wait()
            raise RuntimeError("Activation failed")

        cast(AsyncMock, successful_client.__aenter__).side_effect = success_side_effect
        cast(AsyncMock, failing_client.__aenter__).side_effect = fail_side_effect

        fleet = ClientFleet(clients=mock_clients)

        with pytest.raises(expected_exception=ExceptionGroup):
            async with fleet:
                pass

        cast(AsyncMock, successful_client.__aexit__).assert_awaited_once()
        cast(AsyncMock, failing_client.__aexit__).assert_not_awaited()

    @pytest.mark.asyncio
    async def test_aexit_logs_errors(
        self, fleet_unactivated: ClientFleet, mock_clients: list[Any], caplog: LogCaptureFixture
    ) -> None:
        cast(AsyncMock, mock_clients[0].__aexit__).side_effect = IOError("Close failed")

        async with fleet_unactivated:
            pass

        assert "Error closing clients in fleet" in caplog.text
        cast(AsyncMock, mock_clients[1].__aexit__).assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_all_concurrency_limit(self, fleet: ClientFleet, mocker: MockerFixture) -> None:
        mock_sem = mocker.MagicMock()
        mock_sem.__aenter__ = mocker.AsyncMock()
        mock_sem.__aexit__ = mocker.AsyncMock()
        fleet._connect_sem = mock_sem

        await fleet.connect_all(url="https://example.com")

        assert mock_sem.__aenter__.await_count == 3

    @pytest.mark.asyncio
    async def test_connect_all_with_mixed_results(
        self, fleet: ClientFleet, mock_clients: list[Any], caplog: LogCaptureFixture
    ) -> None:
        url = "https://example.com"
        subprotocols = ["proto1", "proto2"]
        error = ConnectionError(message="Failed to connect")
        cast(AsyncMock, mock_clients[1].connect).side_effect = error

        sessions = await fleet.connect_all(url=url, subprotocols=subprotocols)

        assert len(sessions) == 2
        cast(AsyncMock, mock_clients[0].connect).assert_awaited_once_with(url=url, subprotocols=subprotocols)
        cast(AsyncMock, mock_clients[2].connect).assert_awaited_once_with(url=url, subprotocols=subprotocols)
        assert f"Client failed to connect: {error}" in caplog.text

    def test_get_client_after_close(self, fleet_unactivated: ClientFleet) -> None:
        with pytest.raises(expected_exception=ClientError, match="ClientFleet has not been activated"):
            fleet_unactivated.get_client()

    def test_get_client_count(self, fleet_unactivated: ClientFleet, mock_clients: list[Any]) -> None:
        assert fleet_unactivated.get_client_count() == len(mock_clients)

    def test_get_client_round_robin(self, fleet: ClientFleet, mock_clients: list[Any]) -> None:
        client_order = [fleet.get_client() for _ in range(len(mock_clients) + 1)]

        assert client_order[0] is mock_clients[0]
        assert client_order[1] is mock_clients[1]
        assert client_order[2] is mock_clients[2]
        assert client_order[3] is mock_clients[0]

    def test_init_success(self, mock_clients: list[Any]) -> None:
        fleet = ClientFleet(clients=mock_clients, max_concurrent_handshakes=10)

        assert fleet._clients == mock_clients
        assert fleet._max_concurrent_handshakes == 10
        assert fleet._active is False
        assert fleet._connect_sem._value == 10
        assert fleet._current_index == 0

    def test_init_with_no_clients(self) -> None:
        with pytest.raises(expected_exception=ValueError, match="ClientFleet requires at least one client instance"):
            ClientFleet(clients=[])

    @pytest.mark.asyncio
    @pytest.mark.parametrize(argnames="method_name", argvalues=["connect_all"])
    async def test_methods_raise_if_not_activated_async(self, fleet_unactivated: ClientFleet, method_name: str) -> None:
        method_to_test = getattr(fleet_unactivated, method_name)
        kwargs = {"url": "https://url"} if method_name == "connect_all" else {}

        with pytest.raises(expected_exception=ClientError, match="ClientFleet has not been activated"):
            await method_to_test(**kwargs)
