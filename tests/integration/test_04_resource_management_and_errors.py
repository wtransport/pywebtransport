"""Integration tests for resource management and error handling."""

import asyncio
from typing import Any

import pytest

from pywebtransport import (
    ClientConfig,
    ClientError,
    ConnectionError,
    ServerApp,
    StreamError,
    TimeoutError,
    WebTransportClient,
    WebTransportSession,
)
from pywebtransport.types import EventType, SessionState

pytestmark = pytest.mark.asyncio


async def idle_handler(session: WebTransportSession, **kwargs: Any) -> None:
    try:
        await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    except asyncio.CancelledError:
        pass


@pytest.mark.parametrize(argnames="server_app", argvalues=[{"max_connections": 1}], indirect=True)
async def test_max_connections_limit_rejection(
    server_app: ServerApp, server: tuple[str, int], client_config: ClientConfig
) -> None:
    server_app.route(path="/")(idle_handler)
    host, port = server
    url = f"https://{host}:{port}/"

    async with WebTransportClient(config=client_config) as client1:
        session1 = await client1.connect(url=url)
        try:
            async with WebTransportClient(config=client_config) as client2:
                with pytest.raises(
                    expected_exception=(ClientError, ConnectionError, TimeoutError, asyncio.TimeoutError)
                ):
                    await client2.connect(url=url)
        finally:
            if not session1.is_closed:
                await session1.close()


@pytest.mark.parametrize(argnames="server_app", argvalues=[{"initial_max_streams_bidi": 2}], indirect=True)
async def test_max_streams_limit(server_app: ServerApp, server: tuple[str, int], client_config: ClientConfig) -> None:
    server_app.route(path="/")(idle_handler)
    host, port = server
    url = f"https://{host}:{port}/"

    async with WebTransportClient(config=client_config) as client:
        async with await client.connect(url=url) as session:
            await asyncio.sleep(delay=0.2)

            s1 = await session.create_bidirectional_stream()
            s2 = await session.create_bidirectional_stream()

            await s1.write(data=b"1")
            await s2.write(data=b"2")

            with pytest.raises(expected_exception=(StreamError, TimeoutError, asyncio.TimeoutError)):
                async with asyncio.timeout(delay=1.0):
                    await session.create_bidirectional_stream()


@pytest.mark.parametrize(
    argnames="server_app", argvalues=[{"connection_idle_timeout": 0.2, "resource_cleanup_interval": 0.1}], indirect=True
)
async def test_server_cleans_up_closed_connection(
    server_app: ServerApp, server: tuple[str, int], client_config: ClientConfig
) -> None:
    server_app.route(path="/")(idle_handler)
    host, port = server
    url = f"https://{host}:{port}/"
    connection_manager = server_app.server.connection_manager

    assert len(await connection_manager.get_all_resources()) == 0

    async with WebTransportClient(config=client_config) as client:
        session = await client.connect(url=url)
        try:
            assert session.state == SessionState.CONNECTED

            async with asyncio.timeout(delay=2.0):
                while len(await connection_manager.get_all_resources()) < 1:
                    await asyncio.sleep(delay=0.05)

            assert len(await connection_manager.get_all_resources()) == 1
        finally:
            await session.close()
            await client.close()

    async with asyncio.timeout(delay=5.0):
        while len(await connection_manager.get_all_resources()) > 0:
            await asyncio.sleep(delay=0.1)

    assert len(await connection_manager.get_all_resources()) == 0
