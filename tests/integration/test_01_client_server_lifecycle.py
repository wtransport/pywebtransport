"""Integration tests for the basic client-server connection lifecycle."""

import asyncio

import pytest

from pywebtransport import ClientError, WebTransportClient, WebTransportServer, WebTransportSession
from pywebtransport.types import EventType

pytestmark = pytest.mark.asyncio


async def test_client_initiated_close(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, server_session = sessions

    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(coro=server_session.events.wait_for(event_type=EventType.SESSION_CLOSED, timeout=2.0))
        await client_session.close(reason="client-initiated close")

    assert client_session.is_closed is True
    assert server_session.is_closed is True


async def test_rejected_session_fails(client: WebTransportClient, server: WebTransportServer, url: str) -> None:
    with pytest.RaisesGroup(pytest.RaisesExc(ClientError, match="0x10b")):
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(coro=client.connect(url=url))
            connection = await anext(server.incoming_connections())
            server_session = await anext(connection.incoming_sessions())
            await server_session.reject()

    assert server_session.is_closed is True


async def test_server_initiated_close(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, server_session = sessions

    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(coro=client_session.events.wait_for(event_type=EventType.SESSION_CLOSED, timeout=2.0))
        await server_session.close(reason="server-initiated close")

    assert client_session.is_closed is True
    assert server_session.is_closed is True


async def test_successful_connection_and_session(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, server_session = sessions

    await client_session.ensure_ready()
    await server_session.ensure_ready()

    assert client_session.is_closed is False
    assert server_session.is_closed is False
    assert server_session.path == client_session.path
