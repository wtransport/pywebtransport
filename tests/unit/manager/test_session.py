"""Unit tests for the pywebtransport.manager.session module."""

from typing import cast
from unittest.mock import MagicMock, Mock

import pytest
from pytest_mock import MockerFixture

from pywebtransport.constants import ErrorCodes
from pywebtransport.events import EventEmitter
from pywebtransport.manager.session import SessionManager
from pywebtransport.session import WebTransportSession
from pywebtransport.types import SessionState


class FalsySession(Mock):

    def __bool__(self) -> bool:
        return False


class TestSessionManager:

    @pytest.fixture
    def manager(self) -> SessionManager:
        return SessionManager(max_sessions=10)

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> MagicMock:
        session = mocker.Mock(spec=WebTransportSession)
        session.session_id = 1
        session.state = SessionState.CONNECTED
        session.is_closed = False
        session.events = mocker.MagicMock(spec=EventEmitter)
        session.close = mocker.AsyncMock()
        return cast(MagicMock, session)

    @pytest.mark.asyncio
    async def test_add_session(self, manager: SessionManager, mock_session: MagicMock) -> None:
        async with manager:
            session_id = await manager.add_session(session=mock_session)

            assert session_id == 1
            assert len(manager) == 1
            assert await manager.get_resource(resource_id=1) is mock_session

    @pytest.mark.asyncio
    async def test_close_resource_already_closed(self, manager: SessionManager, mock_session: MagicMock) -> None:
        mock_session.is_closed = True

        await manager._close_resource(resource=mock_session)

        mock_session.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_resource_open(self, manager: SessionManager, mock_session: MagicMock) -> None:
        mock_session.is_closed = False

        await manager._close_resource(resource=mock_session)

        mock_session.close.assert_awaited_once_with(error_code=ErrorCodes.NO_ERROR, reason="Session manager shutdown")

    def test_get_resource_id(self, manager: SessionManager, mock_session: MagicMock) -> None:
        assert manager._get_resource_id(resource=mock_session) == 1

    @pytest.mark.asyncio
    async def test_get_sessions_by_state(self, manager: SessionManager, mocker: MockerFixture) -> None:
        s1 = mocker.Mock(spec=WebTransportSession, session_id=1, events=mocker.MagicMock(spec=EventEmitter))
        s1.state = SessionState.CONNECTED
        s1.is_closed = False
        s2 = mocker.Mock(spec=WebTransportSession, session_id=2, events=mocker.MagicMock(spec=EventEmitter))
        s2.state = SessionState.CLOSING
        s2.is_closed = False
        s3 = mocker.Mock(spec=WebTransportSession, session_id=3, events=mocker.MagicMock(spec=EventEmitter))
        s3.state = SessionState.CONNECTED
        s3.is_closed = False

        async with manager:
            await manager.add_session(session=s1)
            await manager.add_session(session=s2)
            await manager.add_session(session=s3)

            connected = await manager.get_sessions_by_state(state=SessionState.CONNECTED)
            assert len(connected) == 2
            assert s1 in connected
            assert s3 in connected

            closing = await manager.get_sessions_by_state(state=SessionState.CLOSING)
            assert len(closing) == 1
            assert s2 in closing

    @pytest.mark.asyncio
    async def test_get_sessions_by_state_not_active(self, manager: SessionManager) -> None:
        assert await manager.get_sessions_by_state(state=SessionState.CONNECTED) == []

    @pytest.mark.asyncio
    async def test_get_stats_includes_states(self, manager: SessionManager, mocker: MockerFixture) -> None:
        s1 = mocker.Mock(spec=WebTransportSession, session_id=1, events=mocker.MagicMock(spec=EventEmitter))
        s1.state = SessionState.CONNECTED
        s1.is_closed = False
        s2 = mocker.Mock(spec=WebTransportSession, session_id=2, events=mocker.MagicMock(spec=EventEmitter))
        s2.state = SessionState.DRAINING
        s2.is_closed = False

        async with manager:
            await manager.add_session(session=s1)
            await manager.add_session(session=s2)

            stats = await manager.get_stats()

            assert stats["current_count"] == 2
            assert stats["states"]["connected"] == 1
            assert stats["states"]["draining"] == 1

    @pytest.mark.asyncio
    async def test_get_stats_not_active(self, manager: SessionManager) -> None:
        stats = await manager.get_stats()

        assert stats == {}

    def test_init(self, manager: SessionManager) -> None:
        assert len(manager) == 0

    @pytest.mark.asyncio
    async def test_remove_session(
        self, manager: SessionManager, mock_session: MagicMock, mocker: MockerFixture
    ) -> None:
        spy_off = mocker.spy(mock_session.events, "off")

        async with manager:
            await manager.add_session(session=mock_session)
            assert len(manager) == 1

            removed = await manager.remove_session(session_id=1)

            assert removed is mock_session
            assert len(manager) == 0
            stats = await manager.get_stats()
            assert stats["total_closed"] == 1
            spy_off.assert_called_once()

    @pytest.mark.asyncio
    async def test_remove_session_falsy(self, manager: SessionManager, mocker: MockerFixture) -> None:
        session = FalsySession(spec=WebTransportSession)
        session.session_id = 1
        session.events = mocker.MagicMock(spec=EventEmitter)
        session.is_closed = False
        session.state = SessionState.CONNECTED

        async with manager:
            await manager.add_session(session=cast(WebTransportSession, session))

            removed = await manager.remove_session(session_id=1)

            assert removed is session
            stats = await manager.get_stats()
            assert stats["total_closed"] == 1

    @pytest.mark.asyncio
    async def test_remove_session_handler_error(
        self, manager: SessionManager, mock_session: MagicMock, mocker: MockerFixture
    ) -> None:
        cast(MagicMock, mock_session.events.off).side_effect = ValueError("Handler not found")

        async with manager:
            await manager.add_session(session=mock_session)

            removed = await manager.remove_session(session_id=1)

            assert removed is mock_session
            assert len(manager) == 0

    @pytest.mark.asyncio
    async def test_remove_session_missing(self, manager: SessionManager) -> None:
        async with manager:
            removed = await manager.remove_session(session_id=999)

            assert removed is None

    @pytest.mark.asyncio
    async def test_remove_session_not_active(self, manager: SessionManager) -> None:
        assert await manager.remove_session(session_id=1) is None
