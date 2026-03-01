"""Integration tests for the basic client-server connection lifecycle."""

import asyncio
from typing import Any

import pytest

from pywebtransport import ClientError, ServerApp, WebTransportClient, WebTransportSession
from pywebtransport.types import EventType, SessionState

pytestmark = pytest.mark.asyncio


async def test_client_initiated_close(
    server_app: ServerApp, server: tuple[str, int], client: WebTransportClient
) -> None:
    host, port = server
    url = f"https://{host}:{port}/"
    server_entered = asyncio.Event()

    @server_app.route(path="/")
    async def simple_handler(session: WebTransportSession, **kwargs: Any) -> None:
        server_entered.set()
        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except (asyncio.CancelledError, Exception):
            pass

    session = await client.connect(url=url)
    assert session.state == SessionState.CONNECTED

    async with asyncio.timeout(delay=2.0):
        await server_entered.wait()

    await session.close(reason="Client-initiated close")

    try:
        async with asyncio.timeout(delay=2.0):
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    except TimeoutError:
        pass

    assert session.is_closed is True


async def test_connection_to_non_existent_route_fails(
    server: tuple[str, int], client: WebTransportClient, server_app: ServerApp
) -> None:
    host, port = server
    url = f"https://{host}:{port}/nonexistent"

    with pytest.raises(expected_exception=ClientError) as exc_info:
        await client.connect(url=url)

    error_message = str(exc_info.value).lower()
    assert "404" in error_message or "rejected" in error_message or "timeout" in error_message


async def test_server_initiated_close(
    server_app: ServerApp, server: tuple[str, int], client: WebTransportClient
) -> None:
    host, port = server
    url = f"https://{host}:{port}/close-me"

    @server_app.route(path="/close-me")
    async def immediate_close_handler(session: WebTransportSession, **kwargs: Any) -> None:
        await asyncio.sleep(delay=0.2)
        await session.close(reason="Server closed immediately.")

    session = await client.connect(url=url)

    try:
        async with asyncio.timeout(delay=5.0):
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    except TimeoutError:
        if not session.is_closed:
            pytest.fail(reason="Timed out waiting for server-initiated close.")

    assert session.is_closed is True


async def test_successful_connection_and_session(
    server_app: ServerApp, server: tuple[str, int], client: WebTransportClient
) -> None:
    host, port = server
    url = f"https://{host}:{port}/"
    handler_called = asyncio.Event()

    @server_app.route(path="/")
    async def basic_handler(session: WebTransportSession, **kwargs: Any) -> None:
        handler_called.set()
        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except asyncio.CancelledError:
            pass

    session = None
    try:
        session = await client.connect(url=url)
        assert session.state == SessionState.CONNECTED

        async with asyncio.timeout(delay=2.0):
            await handler_called.wait()
    finally:
        if session and not session.is_closed:
            await session.close()
