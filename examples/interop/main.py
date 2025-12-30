"""WebTransport interoperability test server."""

import asyncio
import logging
from collections import deque
from typing import Any

import uvloop

from pywebtransport import ServerApp, ServerConfig, WebTransportSession, WebTransportStream
from pywebtransport import __version__ as LIB_VERSION
from pywebtransport.serializer import JSONSerializer
from pywebtransport.types import EventType
from pywebtransport.utils import generate_self_signed_cert

HOST = "::"
PORT = 4433

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("interop")


def deque_converter(o: Any) -> Any:
    """Convert deque to list for JSON serialization."""
    if isinstance(o, deque):
        return list(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


class InteropServer(ServerApp):
    """High-performance WebTransport interoperability server."""

    def __init__(self, config: ServerConfig) -> None:
        """Initialize server with JSON serializer and route registration."""
        super().__init__(config=config)
        self._serializer = JSONSerializer(dump_kwargs={"default": deque_converter})
        self._register_routes()
        logger.info("InteropServer initialized (v%s)", LIB_VERSION)

    def _register_routes(self) -> None:
        """Register request handlers."""
        self.route(path="/echo")(self.handle_echo)
        self.route(path="/stats")(self.handle_stats)
        self.route(path="/status")(self.handle_status)

    async def handle_echo(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Handle bidirectional stream and datagram echo."""
        sid = session.session_id
        logger.info("Session %s: echo started", sid)

        async def on_datagram(event: Any) -> None:
            if isinstance(event.data, dict) and (data := event.data.get("data")):
                try:
                    await session.send_datagram(data=data)
                except Exception:
                    pass

        async def on_stream(event: Any) -> None:
            if isinstance(event.data, dict) and (stream := event.data.get("stream")):
                if isinstance(stream, WebTransportStream):
                    asyncio.create_task(self._echo_stream(stream))

        session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_datagram)
        session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
            logger.info("Session %s: closed", sid)
        except Exception:
            pass

    async def handle_stats(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Respond with current session diagnostics."""
        sid = session.session_id
        logger.info("Session %s: stats started", sid)

        async def on_stream(event: Any) -> None:
            if isinstance(event.data, dict) and (stream := event.data.get("stream")):
                if isinstance(stream, WebTransportStream):
                    try:
                        await stream.read_all()
                        payload = self._serializer.serialize(obj=await session.diagnostics())
                        await stream.write(data=payload)
                        await stream.write(data=b"", end_stream=True)
                    except Exception as e:
                        logger.error("Session %s: stats stream error: %s", sid, e)

        session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
            logger.info("Session %s: closed", sid)
        except Exception:
            pass

    async def handle_status(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Respond with global server diagnostics."""
        sid = session.session_id
        logger.info("Session %s: status started", sid)

        async def on_stream(event: Any) -> None:
            if isinstance(event.data, dict) and (stream := event.data.get("stream")):
                if isinstance(stream, WebTransportStream):
                    try:
                        await stream.read_all()
                        payload = self._serializer.serialize(obj=await self.server.diagnostics())
                        await stream.write(data=payload)
                        await stream.write(data=b"", end_stream=True)
                    except Exception as e:
                        logger.error("Session %s: status stream error: %s", sid, e)

        session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
            logger.info("Session %s: closed", sid)
        except Exception:
            pass

    async def _echo_stream(self, stream: WebTransportStream) -> None:
        """Echo data back to the client."""
        try:
            while True:
                data = await stream.read(max_bytes=65536)
                if not data:
                    break
                await stream.write(data=data)
            await stream.write(data=b"", end_stream=True)
        except Exception:
            pass


async def main() -> None:
    """Configure and start the server."""
    generate_self_signed_cert(hostname="localhost")

    config = ServerConfig(
        bind_host=HOST,
        bind_port=PORT,
        certfile="localhost.crt",
        keyfile="localhost.key",
    )

    app = InteropServer(config=config)
    logger.info("Server starting on https://[%s]:%s", HOST, PORT)

    async with app:
        await app.serve()


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        pass
