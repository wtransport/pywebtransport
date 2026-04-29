"""Unit tests for the pywebtransport.manager.connection module."""

import asyncio
from typing import cast
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport.connection import WebTransportConnection
from pywebtransport.events import EventEmitter
from pywebtransport.manager import ConnectionManager
from pywebtransport.types import ConnectionState


class TestConnectionManager:

    @pytest.fixture
    def manager(self) -> ConnectionManager:
        return ConnectionManager(max_connections=10)

    @pytest.fixture
    def mock_connection(self, mocker: MockerFixture) -> MagicMock:
        conn = mocker.Mock(spec=WebTransportConnection)
        conn.handle = 1
        conn.state = ConnectionState.CONNECTED
        conn.is_closed = False
        conn.events = EventEmitter()
        conn.close = mocker.AsyncMock()
        return cast(MagicMock, conn)

    @pytest.mark.asyncio
    async def test_add_connection(self, manager: ConnectionManager, mock_connection: MagicMock) -> None:
        async with manager:
            conn_handle = await manager.add_connection(connection=mock_connection)

            assert conn_handle == 1
            assert len(manager) == 1
            assert await manager.get_resource(resource_id=1) is mock_connection

    @pytest.mark.asyncio
    async def test_close_resource_idempotency(self, manager: ConnectionManager, mock_connection: MagicMock) -> None:
        mock_connection.is_closed = True

        await manager._close_resource(resource=mock_connection)

        mock_connection.close.assert_not_called()

    def test_get_resource_id(self, manager: ConnectionManager, mock_connection: MagicMock) -> None:
        assert manager._get_resource_id(resource=mock_connection) == 1

    @pytest.mark.asyncio
    async def test_get_stats_includes_states(self, manager: ConnectionManager, mocker: MockerFixture) -> None:
        c1 = mocker.Mock(spec=WebTransportConnection)
        c1.handle = 10
        c1.events = EventEmitter()
        c1.state = ConnectionState.CONNECTED
        c1.is_closed = False

        c2 = mocker.Mock(spec=WebTransportConnection)
        c2.handle = 20
        c2.events = EventEmitter()
        c2.state = ConnectionState.CONNECTING
        c2.is_closed = False

        async with manager:
            await manager.add_connection(connection=c1)
            await manager.add_connection(connection=c2)

            stats = await manager.get_stats()

            assert stats["current_count"] == 2
            assert stats["states"]["connected"] == 1
            assert stats["states"]["connecting"] == 1

    @pytest.mark.asyncio
    async def test_get_stats_not_active(self, manager: ConnectionManager) -> None:
        stats = await manager.get_stats()

        assert stats == {}

    @pytest.mark.asyncio
    async def test_handle_resource_closed_guards_not_active(self, manager: ConnectionManager) -> None:
        await manager._handle_resource_closed(resource_id=1)

    @pytest.mark.asyncio
    async def test_handle_resource_closed_schedules_close(
        self, manager: ConnectionManager, mock_connection: MagicMock
    ) -> None:
        async with manager:
            await manager.add_connection(connection=mock_connection)

            await manager._handle_resource_closed(resource_id=1)

            assert len(manager) == 0
            await asyncio.sleep(delay=0)
            mock_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handle_resource_closed_task_lifecycle(
        self, manager: ConnectionManager, mock_connection: MagicMock
    ) -> None:
        close_finished = asyncio.Event()

        async def close_side_effect() -> None:
            await close_finished.wait()

        mock_connection.close.side_effect = close_side_effect

        async with manager:
            await manager.add_connection(connection=mock_connection)

            await manager._handle_resource_closed(resource_id=1)

            assert len(manager._closing_tasks) == 1

            close_finished.set()

            for _ in range(5):
                if len(manager._closing_tasks) == 0:
                    break
                await asyncio.sleep(delay=0)

            assert len(manager._closing_tasks) == 0

    @pytest.mark.asyncio
    async def test_handle_resource_closed_unmanaged(
        self, manager: ConnectionManager, mock_connection: MagicMock
    ) -> None:
        async with manager:
            await manager._handle_resource_closed(resource_id=1)

            mock_connection.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_remove_connection(self, manager: ConnectionManager, mock_connection: MagicMock) -> None:
        async with manager:
            await manager.add_connection(connection=mock_connection)

            removed = await manager.remove_connection(connection_handle=1)

            assert removed is mock_connection
            assert len(manager) == 0
            await asyncio.sleep(delay=0)
            mock_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_remove_connection_missing(self, manager: ConnectionManager) -> None:
        async with manager:
            removed = await manager.remove_connection(connection_handle=999)

            assert removed is None

    @pytest.mark.asyncio
    async def test_remove_connection_not_active(self, manager: ConnectionManager) -> None:
        assert await manager.remove_connection(connection_handle=1) is None

    def test_schedule_close_idempotency(self, manager: ConnectionManager, mock_connection: MagicMock) -> None:
        mock_connection.is_closed = True

        manager._schedule_close(connection=mock_connection)

        mock_connection.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_shutdown_waits_for_closing_tasks(
        self, manager: ConnectionManager, mock_connection: MagicMock
    ) -> None:
        close_event = asyncio.Event()

        async def delayed_close() -> None:
            await close_event.wait()

        mock_connection.close.side_effect = delayed_close

        async with manager:
            await manager.add_connection(connection=mock_connection)
            await manager.remove_connection(connection_handle=1)

            assert len(manager._closing_tasks) == 1

            shutdown_task = asyncio.create_task(coro=manager.shutdown())
            await asyncio.sleep(delay=0.01)

            assert not shutdown_task.done()

            close_event.set()
            await shutdown_task

        assert len(manager._closing_tasks) == 0
