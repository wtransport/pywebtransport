"""Integration tests for high-level ServerApp features."""

import asyncio
import http
from typing import Any

import pytest

from pywebtransport import (
    ClientError,
    Event,
    Headers,
    ServerApp,
    WebTransportClient,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport.server import MiddlewareRejected
from pywebtransport.types import EventType, SessionProtocol
from pywebtransport.utils import get_header_as_str

pytestmark = pytest.mark.asyncio


async def test_middleware_accepts_session(
    server: tuple[str, int], client: WebTransportClient, server_app: ServerApp
) -> None:
    host, port = server
    handler_was_reached = asyncio.Event()

    async def auth_middleware(session: SessionProtocol) -> None:
        token = get_header_as_str(headers=session.headers, key="x-auth-token")
        if token != "valid-token":
            raise MiddlewareRejected(status_code=http.HTTPStatus.FORBIDDEN)

    server_app.add_middleware(middleware=auth_middleware)

    @server_app.route(path="/protected")
    async def protected_handler(session: WebTransportSession, **kwargs: Any) -> None:
        handler_was_reached.set()
        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass

    headers: Headers = {"x-auth-token": "valid-token"}
    async with await client.connect(url=f"https://{host}:{port}/protected", headers=headers):
        async with asyncio.timeout(2.0):
            await handler_was_reached.wait()


async def test_middleware_rejects_session(
    server: tuple[str, int], client: WebTransportClient, server_app: ServerApp
) -> None:
    host, port = server

    async def auth_middleware(session: SessionProtocol) -> None:
        token = get_header_as_str(headers=session.headers, key="x-auth-token")
        if token != "valid-token":
            raise MiddlewareRejected(status_code=http.HTTPStatus.FORBIDDEN)

    server_app.add_middleware(middleware=auth_middleware)

    @server_app.route(path="/protected")
    async def protected_handler(session: WebTransportSession, **kwargs: Any) -> None:
        pytest.fail("Rejected session reached the route handler.")

    headers: Headers = {"x-auth-token": "invalid-token"}
    with pytest.raises(ClientError) as exc_info:
        await client.connect(url=f"https://{host}:{port}/protected", headers=headers)

    error_message = str(exc_info.value).lower()
    assert "403" in error_message or "rejected" in error_message or "timeout" in error_message


async def test_pattern_routing_with_params(
    server: tuple[str, int], client: WebTransportClient, server_app: ServerApp
) -> None:
    host, port = server

    @server_app.pattern_route(pattern=r"/items/(?P<item_id>[a-zA-Z0-9-]+)")
    async def item_handler(session: WebTransportSession, item_id: str, **kwargs: Any) -> None:
        async def on_stream(event: Event) -> None:
            if isinstance(event.data, dict):
                s = event.data.get("stream")
                if isinstance(s, WebTransportStream):
                    _ = await s.read()
                    response_message = f"Accessed item: {item_id}".encode()
                    await s.write(data=response_message)
                    await s.close()

        session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass

    async with await client.connect(url=f"https://{host}:{port}/items/123-abc") as session:
        stream = await session.create_bidirectional_stream()
        await stream.write(data=b"get item data")
        response = await stream.read()
        assert response == b"Accessed item: 123-abc"


async def test_routing_to_path_one(server: tuple[str, int], client: WebTransportClient, server_app: ServerApp) -> None:
    host, port = server
    handler_one_called = asyncio.Event()
    handler_two_called = asyncio.Event()

    @server_app.route(path="/path_one")
    async def handler_one(session: WebTransportSession, **kwargs: Any) -> None:
        handler_one_called.set()
        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass

    @server_app.route(path="/path_two")
    async def handler_two(session: WebTransportSession, **kwargs: Any) -> None:
        handler_two_called.set()
        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass

    async with await client.connect(url=f"https://{host}:{port}/path_one"):
        async with asyncio.timeout(2.0):
            await handler_one_called.wait()

    assert handler_one_called.is_set()
    assert not handler_two_called.is_set()


async def test_routing_to_path_two(server: tuple[str, int], client: WebTransportClient, server_app: ServerApp) -> None:
    host, port = server
    handler_one_called = asyncio.Event()
    handler_two_called = asyncio.Event()

    @server_app.route(path="/path_one")
    async def handler_one(session: WebTransportSession, **kwargs: Any) -> None:
        handler_one_called.set()
        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass

    @server_app.route(path="/path_two")
    async def handler_two(session: WebTransportSession, **kwargs: Any) -> None:
        handler_two_called.set()
        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass

    async with await client.connect(url=f"https://{host}:{port}/path_two"):
        async with asyncio.timeout(2.0):
            await handler_two_called.wait()

    assert handler_two_called.is_set()
    assert not handler_one_called.is_set()
