"""High-performance Benchmark Server System Under Test."""

import asyncio
import http
import logging
from pathlib import Path
from typing import Final

from pywebtransport import (
    ConnectionError,
    DatagramError,
    ServerConfig,
    SessionError,
    StreamError,
    WebTransportBidirectionalStream,
    WebTransportConnection,
    WebTransportServer,
    WebTransportSession,
)
from pywebtransport.utils import generate_self_signed_cert

CERT_HOSTNAME: Final[str] = "localhost"
CERT_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.crt")
KEY_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.key")

SERVER_HOST: Final[str] = "::"
SERVER_PORT: Final[int] = 4433

CHUNK_SIZE: Final[int] = 65536
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * (100 * 1024 * 1024))

logging.basicConfig(level=logging.CRITICAL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


async def discard_stream(*, stream: WebTransportBidirectionalStream) -> None:
    try:
        while await stream.read(max_size=CHUNK_SIZE):
            pass
        await stream.write(data=b"", end_stream=True)
    except (ConnectionError, StreamError):
        pass


async def exchange_stream(*, stream: WebTransportBidirectionalStream) -> None:
    async def receive() -> None:
        while await stream.read(max_size=CHUNK_SIZE):
            pass

    try:
        await asyncio.gather(stream.write_all(data=STATIC_VIEW[: 1024 * 1024], end_stream=True), receive())
    except (ConnectionError, StreamError):
        pass


async def echo_datagrams(*, session: WebTransportSession) -> None:
    try:
        while True:
            data = await session.read_datagram()
            try:
                await session.send_datagram(data=data)
            except DatagramError:
                pass
    except (ConnectionError, SessionError):
        pass


async def echo_stream(*, stream: WebTransportBidirectionalStream) -> None:
    try:
        while data := await stream.read(max_size=CHUNK_SIZE):
            await stream.write(data=data)
        await stream.write(data=b"", end_stream=True)
    except (ConnectionError, StreamError):
        pass


async def produce_stream(*, stream: WebTransportBidirectionalStream) -> None:
    try:
        size = int(await stream.read(max_size=128))
        await stream.write_all(data=STATIC_VIEW[:size], end_stream=True)
    except (ConnectionError, StreamError, ValueError):
        pass


async def respond_stream(*, stream: WebTransportBidirectionalStream) -> None:
    try:
        await stream.write_all(data=await stream.read_all(), end_stream=True)
    except (ConnectionError, StreamError):
        pass


async def serve_connection(*, connection: WebTransportConnection) -> None:
    async with asyncio.TaskGroup() as task_group:
        async for session in connection.incoming_sessions():
            task_group.create_task(coro=serve_session(session=session))


async def serve_session(*, session: WebTransportSession) -> None:
    try:
        async with session:
            match session.path:
                case "/discard":
                    await session.accept()
                    async with asyncio.TaskGroup() as task_group:
                        async for stream in session.incoming_bidirectional_streams():
                            task_group.create_task(coro=discard_stream(stream=stream))
                case "/duplex":
                    await session.accept()
                    async with asyncio.TaskGroup() as task_group:
                        async for stream in session.incoming_bidirectional_streams():
                            task_group.create_task(coro=exchange_stream(stream=stream))
                case "/echo":
                    await session.accept()
                    async with asyncio.TaskGroup() as task_group:
                        task_group.create_task(coro=echo_datagrams(session=session))
                        async for stream in session.incoming_bidirectional_streams():
                            task_group.create_task(coro=echo_stream(stream=stream))
                case "/latency":
                    await session.accept()
                    async with asyncio.TaskGroup() as task_group:
                        async for stream in session.incoming_bidirectional_streams():
                            task_group.create_task(coro=respond_stream(stream=stream))
                case "/produce":
                    await session.accept()
                    async with asyncio.TaskGroup() as task_group:
                        async for stream in session.incoming_bidirectional_streams():
                            task_group.create_task(coro=produce_stream(stream=stream))
                case _:
                    await session.reject(headers=[("allow", "")], status_code=http.HTTPStatus.METHOD_NOT_ALLOWED)
    except (ConnectionError, SessionError):
        pass


async def main() -> None:
    if not CERT_PATH.exists() or not KEY_PATH.exists():
        generate_self_signed_cert(hostname=CERT_HOSTNAME, output_dir=".")

    config = ServerConfig(
        bind_host=SERVER_HOST,
        bind_port=SERVER_PORT,
        certfile=str(CERT_PATH),
        keyfile=str(KEY_PATH),
        max_connections=4000,
    )

    async with WebTransportServer(config=config) as server:
        await server.listen()

        async with asyncio.TaskGroup() as task_group:
            async for connection in server.incoming_connections():
                task_group.create_task(coro=serve_connection(connection=connection))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
