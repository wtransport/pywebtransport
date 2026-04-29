"""Integration tests for data exchange over streams and datagrams."""

import asyncio
from typing import Any

import pytest

from pywebtransport import (
    Event,
    ServerApp,
    SessionClosedError,
    WebTransportClient,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport.types import EventType

pytestmark = pytest.mark.asyncio


async def test_bidirectional_stream_echo(
    server: tuple[str, int], client: WebTransportClient, server_app: ServerApp
) -> None:
    host, port = server
    server_handler_finished = asyncio.Event()

    @server_app.route(path="/echo")
    async def echo_handler(session: WebTransportSession, **kwargs: Any) -> None:
        try:
            stream = await session.accept_bidirectional_stream()
            data = await stream.read()
            await stream.write(data=data)
            await stream.close()
        except SessionClosedError:
            pass
        finally:
            server_handler_finished.set()

    session = await client.connect(url=f"https://{host}:{port}/echo")
    async with session:
        stream = await session.create_bidirectional_stream()
        test_message = b"Hello, bidirectional world!"
        await stream.write(data=test_message)

        response = await stream.read()
        assert response == test_message
        await stream.close()

    async with asyncio.timeout(delay=2.0):
        await server_handler_finished.wait()


async def test_concurrent_streams_and_datagrams(
    server: tuple[str, int], client: WebTransportClient, server_app: ServerApp
) -> None:
    host, port = server
    num_concurrent_ops = 5

    @server_app.route(path="/concurrent")
    async def concurrent_handler(session: WebTransportSession, **kwargs: Any) -> None:
        async def handle_streams() -> None:
            try:
                while True:
                    stream = await session.accept_bidirectional_stream()

                    async def process_stream(*, s: WebTransportStream) -> None:
                        try:
                            data = await s.read()
                            await s.write(data=data)
                            await s.close()
                        except Exception:
                            pass

                    asyncio.create_task(coro=process_stream(s=stream))
            except SessionClosedError:
                pass

        stream_task = asyncio.create_task(coro=handle_streams())

        async def on_datagram(event: Event) -> None:
            if isinstance(event.data, dict) and (d := event.data.get("data")):
                await session.send_datagram(data=d)

        session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_datagram)

        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass
        finally:
            stream_task.cancel()

    async def _stream_worker(*, session: WebTransportSession, i: int) -> bool:
        stream = await session.create_bidirectional_stream()
        msg = f"Stream worker {i}".encode()
        await stream.write(data=msg)
        response = await stream.read()
        await stream.close()
        return response == msg

    async def _datagram_worker(*, session: WebTransportSession, i: int) -> bool:
        msg = f"Datagram worker {i}".encode()
        fut: asyncio.Future[bool] = asyncio.Future()

        def listener(event: Event) -> None:
            if isinstance(event.data, dict) and event.data.get("data") == msg:
                if not fut.done():
                    fut.set_result(True)

        session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=listener)
        for _ in range(3):
            if fut.done():
                break
            await session.send_datagram(data=msg)
            await asyncio.sleep(delay=0.1)

        try:
            async with asyncio.timeout(delay=2.0):
                await fut
            return True
        except TimeoutError:
            return False
        finally:
            session.events.off(event_type=EventType.DATAGRAM_RECEIVED, handler=listener)

    session = await client.connect(url=f"https://{host}:{port}/concurrent")
    async with session:
        stream_tasks = [_stream_worker(session=session, i=i) for i in range(num_concurrent_ops)]
        datagram_tasks = [_datagram_worker(session=session, i=i) for i in range(num_concurrent_ops)]
        results = await asyncio.gather(*(stream_tasks + datagram_tasks), return_exceptions=True)

    for result in results:
        assert result is True


async def test_datagram_echo(server: tuple[str, int], client: WebTransportClient, server_app: ServerApp) -> None:
    host, port = server
    server_handler_finished = asyncio.Event()

    @server_app.route(path="/datagram")
    async def datagram_echo_handler(session: WebTransportSession, **kwargs: Any) -> None:
        async def on_datagram(event: Event) -> None:
            if isinstance(event.data, dict) and (data := event.data.get("data")):
                await session.send_datagram(data=data)
                server_handler_finished.set()

        session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_datagram)
        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass

    session = await client.connect(url=f"https://{host}:{port}/datagram")
    async with session:
        test_message = b"Hello, datagram world!"
        fut: asyncio.Future[bool] = asyncio.Future()

        def listener(event: Event) -> None:
            if isinstance(event.data, dict) and event.data.get("data") == test_message:
                if not fut.done():
                    fut.set_result(True)

        session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=listener)

        for _ in range(5):
            await session.send_datagram(data=test_message)
            try:
                async with asyncio.timeout(delay=0.5):
                    await asyncio.shield(arg=fut)
                break
            except TimeoutError:
                continue
        else:
            pytest.fail(reason="Datagram echo timed out")

    async with asyncio.timeout(delay=2.0):
        await server_handler_finished.wait()


async def test_unidirectional_stream_to_server(
    server: tuple[str, int], client: WebTransportClient, server_app: ServerApp
) -> None:
    host, port = server
    data_queue: asyncio.Queue[bytes] = asyncio.Queue()

    @server_app.route(path="/uni")
    async def uni_handler(session: WebTransportSession, **kwargs: Any) -> None:
        try:
            stream = await session.accept_unidirectional_stream()
            data = await stream.read()
            await data_queue.put(item=data)
        except SessionClosedError:
            pass

    session = await client.connect(url=f"https://{host}:{port}/uni")
    async with session:
        stream = await session.create_unidirectional_stream()
        test_message = b"Hello, unidirectional world!"
        await stream.write(data=test_message)
        await stream.close()

        async with asyncio.timeout(delay=2.0):
            received_data = await data_queue.get()

    assert received_data == test_message
