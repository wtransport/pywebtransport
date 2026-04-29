"""High-performance Benchmark Server System Under Test."""

import asyncio
import logging
from pathlib import Path
from typing import Any, Final

import uvloop

from pywebtransport import (
    ConnectionError,
    Event,
    ServerApp,
    ServerConfig,
    SessionClosedError,
    StreamError,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport.types import EventType
from pywebtransport.utils import generate_self_signed_cert

CERT_HOSTNAME: Final[str] = "localhost"
CERT_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.crt")
KEY_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.key")

SERVER_HOST: Final[str] = "::"
SERVER_PORT: Final[int] = 4433

CHUNK_SIZE: Final[int] = 65536
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * (100 * 1024 * 1024))

logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger(name="bench_server")


class BenchmarkServerApp(ServerApp):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._register_routes()

    async def handle_discard(self, session: WebTransportSession, **kwargs: Any) -> None:
        async def stream_drainer(*, stream: WebTransportStream) -> None:
            try:
                while await stream.read(max_bytes=CHUNK_SIZE):
                    pass
            except Exception:
                pass
            finally:
                if hasattr(stream, "close"):
                    await stream.close()

        async def on_dgram(event: Event) -> None:
            pass

        session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_dgram)
        try:
            while True:
                stream = await session.accept_bidirectional_stream()
                asyncio.create_task(coro=stream_drainer(stream=stream))
        except SessionClosedError:
            pass

    async def handle_duplex(self, session: WebTransportSession, **kwargs: Any) -> None:
        async def stream_handler(*, stream: WebTransportStream) -> None:
            try:

                async def sender() -> None:
                    await stream.write_all(data=STATIC_VIEW[: 1024 * 1024], end_stream=False)
                    await stream.write(data=b"", end_stream=True)

                async def receiver() -> None:
                    while await stream.read(max_bytes=CHUNK_SIZE):
                        pass

                await asyncio.gather(sender(), receiver())
            except (ConnectionError, StreamError):
                pass
            except Exception:
                if not stream.is_closed:
                    await stream.close()

        try:
            while True:
                stream = await session.accept_bidirectional_stream()
                asyncio.create_task(coro=stream_handler(stream=stream))
        except SessionClosedError:
            pass

    async def handle_echo(self, session: WebTransportSession, **kwargs: Any) -> None:
        async def datagram_loop() -> None:
            async def on_dgram(event: Event) -> None:
                if isinstance(event.data, dict) and (data := event.data.get("data")):
                    try:
                        await session.send_datagram(data=data)
                    except Exception:
                        pass

            session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_dgram)

        async def stream_handler(*, stream: WebTransportStream) -> None:
            try:
                while True:
                    data = await stream.read(max_bytes=CHUNK_SIZE)
                    if not data:
                        break
                    await stream.write(data=data)

                await stream.write(data=b"", end_stream=True)
                await stream.read(max_bytes=1)

            except (ConnectionError, StreamError):
                pass
            except Exception:
                if not stream.is_closed:
                    await stream.close()

        async def stream_accept_loop() -> None:
            try:
                while True:
                    stream = await session.accept_bidirectional_stream()
                    asyncio.create_task(coro=stream_handler(stream=stream))
            except SessionClosedError:
                pass

        t1 = asyncio.create_task(coro=datagram_loop())
        t2 = asyncio.create_task(coro=stream_accept_loop())

        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
        except Exception:
            pass
        finally:
            t1.cancel()
            t2.cancel()

    async def handle_latency(self, session: WebTransportSession, **kwargs: Any) -> None:
        async def stream_responder(*, stream: WebTransportStream) -> None:
            try:
                data = await stream.read_all()
                await stream.write_all(data=data, end_stream=True)
                await stream.read(max_bytes=1)
            except (ConnectionError, StreamError):
                pass
            except Exception:
                if not stream.is_closed:
                    await stream.close()

        try:
            while True:
                stream = await session.accept_bidirectional_stream()
                asyncio.create_task(coro=stream_responder(stream=stream))
        except SessionClosedError:
            pass

    async def handle_produce(self, session: WebTransportSession, **kwargs: Any) -> None:
        async def stream_producer(*, stream: WebTransportStream) -> None:
            try:
                cmd_bytes = await stream.read(max_bytes=128)
                try:
                    size_to_send = int(cmd_bytes)
                except ValueError:
                    return

                await stream.write_all(data=STATIC_VIEW[:size_to_send], end_stream=True)
                await stream.read(max_bytes=1)
            except (ConnectionError, StreamError):
                pass
            except Exception:
                if not stream.is_closed:
                    await stream.close()

        try:
            while True:
                stream = await session.accept_bidirectional_stream()
                asyncio.create_task(coro=stream_producer(stream=stream))
        except SessionClosedError:
            pass

    def _register_routes(self) -> None:
        self.route(path="/discard")(self.handle_discard)
        self.route(path="/duplex")(self.handle_duplex)
        self.route(path="/echo")(self.handle_echo)
        self.route(path="/latency")(self.handle_latency)
        self.route(path="/produce")(self.handle_produce)


async def main() -> None:
    if not CERT_PATH.exists() or not KEY_PATH.exists():
        generate_self_signed_cert(hostname=CERT_HOSTNAME, output_dir=".")

    config = ServerConfig(
        bind_host=SERVER_HOST,
        bind_port=SERVER_PORT,
        certfile=str(CERT_PATH),
        keyfile=str(KEY_PATH),
        max_connections=10000,
        max_sessions=10000,
        initial_max_data=100 * 1024 * 1024,
        initial_max_streams_bidi=10000,
        initial_max_streams_uni=10000,
        flow_control_window=100 * 1024 * 1024,
        max_stream_read_buffer_size=200 * 1024 * 1024,
        max_stream_write_buffer_size=200 * 1024 * 1024,
        event_queue_capacity=100000,
    )

    app = BenchmarkServerApp(config=config)

    async with app:
        await app.serve()


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        pass
