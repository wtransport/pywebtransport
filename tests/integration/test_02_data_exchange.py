"""Integration tests for data exchange over streams and datagrams."""

import asyncio

import pytest

from pywebtransport import WebTransportSession

pytestmark = pytest.mark.asyncio


async def test_bidirectional_stream_echo(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, server_session = sessions
    data = b"Hello, bidirectional world!"

    client_stream = await client_session.create_bidirectional_stream()
    await client_stream.write(data=data, end_stream=True)
    server_stream = await anext(server_session.incoming_bidirectional_streams())
    await server_stream.write(data=await server_stream.read_all(), end_stream=True)

    assert await client_stream.read_all() == data


async def test_concurrent_streams_and_datagrams(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, server_session = sessions
    messages = [f"Message {index}".encode() for index in range(5)]

    async def exchange_stream(*, data: bytes) -> bytes:
        stream = await client_session.create_bidirectional_stream()
        await stream.write(data=data, end_stream=True)
        return await stream.read_all()

    async def echo_stream() -> None:
        stream = await anext(server_session.incoming_bidirectional_streams())
        await stream.write(data=await stream.read_all(), end_stream=True)

    async def exchange_datagrams() -> set[bytes]:
        for data in messages:
            await client_session.send_datagram(data=data)
        return {await client_session.read_datagram() for _ in messages}

    async def echo_datagrams() -> None:
        for _ in messages:
            await server_session.send_datagram(data=await server_session.read_datagram())

    async with asyncio.TaskGroup() as task_group:
        stream_tasks = [task_group.create_task(coro=exchange_stream(data=data)) for data in messages]
        datagram_task = task_group.create_task(coro=exchange_datagrams())
        for _ in messages:
            task_group.create_task(coro=echo_stream())
        task_group.create_task(coro=echo_datagrams())

    assert [task.result() for task in stream_tasks] == messages
    assert datagram_task.result() == set(messages)


async def test_datagram_echo(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, server_session = sessions
    data = b"Hello, datagram world!"

    await client_session.send_datagram(data=data)
    await server_session.send_datagram(data=await server_session.read_datagram())

    assert await client_session.read_datagram() == data


async def test_unidirectional_stream_to_client(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, server_session = sessions
    data = b"Hello, unidirectional world!"

    server_stream = await server_session.create_unidirectional_stream()
    await server_stream.write(data=data, end_stream=True)
    client_stream = await anext(client_session.incoming_unidirectional_streams())

    assert await client_stream.read_all() == data


async def test_unidirectional_stream_to_server(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, server_session = sessions
    data = b"Hello, unidirectional world!"

    client_stream = await client_session.create_unidirectional_stream()
    await client_stream.write(data=data, end_stream=True)
    server_stream = await anext(server_session.incoming_unidirectional_streams())

    assert await server_stream.read_all() == data
