"""E2E test server for WebTransport streams and datagrams."""

import asyncio
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Final

from pywebtransport import (
    ConnectionError,
    Event,
    ServerApp,
    ServerConfig,
    SessionError,
    StreamError,
    StructuredDatagramTransport,
    StructuredStream,
    TimeoutError,
    WebTransportReceiveStream,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport.constants import DEFAULT_MAX_MESSAGE_SIZE
from pywebtransport.serializer import JSONSerializer, MsgPackSerializer
from pywebtransport.types import ConnectionState, EventType, SessionState
from pywebtransport.utils import generate_self_signed_cert, get_timestamp

CERT_HOSTNAME: Final[str] = "localhost"
CERT_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.crt")
KEY_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.key")

DEBUG_MODE: Final[bool] = "--debug" in sys.argv
SERVER_HOST: Final[str] = "::"
SERVER_PORT: Final[int] = 4433

JSON_SERIALIZER = JSONSerializer()
MSGPACK_SERIALIZER = MsgPackSerializer()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)

logger = logging.getLogger(name="e2e_server")


@dataclass(kw_only=True)
class StatusUpdate:
    """Represents a status update message."""

    status: str
    timestamp: float


@dataclass(kw_only=True)
class UserData:
    """Represents user data structure."""

    id: int
    name: str
    email: str


class E2EServerApp(ServerApp):
    """E2E test server application with full test support."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the E2E server application."""
        super().__init__(**kwargs)
        self.server.on(event_type=EventType.CONNECTION_ESTABLISHED, handler=self._on_connection_established)
        self.server.on(event_type=EventType.SESSION_REQUEST, handler=self._on_session_request)
        self._register_handlers()
        logger.info("E2E Server initialized with full test support")

    async def _diagnostics_handler(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Handle requests for server statistics on the /diagnostics path."""
        logger.info("Diagnostics request from session %s", session.session_id)
        stream: WebTransportStream | None = None
        try:
            stream_event = await session.events.wait_for(event_type=EventType.STREAM_OPENED, timeout=5.0)
            if not isinstance(stream_event.data, dict):
                logger.warning("Diagnostics handler: Received invalid stream event data.")
                return

            stream = stream_event.data.get("stream")
            if not isinstance(stream, WebTransportStream):
                logger.warning("Diagnostics handler: Client opened a non-bidirectional stream.")
                return

            diagnostics = await self.server.diagnostics()
            stats_json = json.dumps(obj=asdict(obj=diagnostics), indent=2).encode(encoding="utf-8")
            await stream.write(data=stats_json, end_stream=True)
            logger.info("Sent diagnostics: %s bytes", len(stats_json))
        except asyncio.TimeoutError:
            logger.error("Diagnostics handler: Client connected but never opened a stream.")
        except Exception as e:
            logger.error("Diagnostics handler error: %s", e)
        finally:
            if not session.is_closed:
                await session.close()

    async def _health_handler(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Handle health check requests on the /health path."""
        logger.info("Health check from session %s", session.session_id)
        try:
            diagnostics = await self.server.diagnostics()
            stats = diagnostics.stats
            active_sessions = diagnostics.session_states.get(SessionState.CONNECTED, 0)
            active_connections = sum(v for k, v in diagnostics.connection_states.items() if k != ConnectionState.CLOSED)

            health_data = {
                "status": "healthy",
                "timestamp": time.time(),
                "uptime": (get_timestamp() - stats.start_time) if stats.start_time else 0.0,
                "active_sessions": active_sessions,
                "active_connections": active_connections,
            }
            await session.send_datagram(data=json.dumps(obj=health_data).encode(encoding="utf-8"))
            logger.info("Sent health status: %s", health_data["status"])
        except Exception as e:
            logger.error("Health handler error: %s", e)
        finally:
            if not session.is_closed:
                await session.close()

    async def _on_connection_established(self, event: Any) -> None:
        """Handle connection established events."""
        logger.info("New connection established")

    async def _on_session_request(self, event: Any) -> None:
        """Handle session request events."""
        if isinstance(event.data, dict):
            session_id = event.data.get("session_id")
            path = event.data.get("path", "/")
            logger.info("Session request: %s for path '%s'", session_id, path)

    def _register_handlers(self) -> None:
        """Centralize registration for all server routes."""
        self.route(path="/")(echo_handler)
        self.route(path="/echo")(echo_handler)
        self.route(path="/health")(self._health_handler)
        self.route(path="/diagnostics")(self._diagnostics_handler)
        self.route(path="/structured-echo/json")(structured_echo_json_handler)
        self.route(path="/structured-echo/msgpack")(structured_echo_msgpack_handler)


MESSAGE_REGISTRY: dict[int, type[Any]] = {1: UserData, 2: StatusUpdate}


async def _structured_echo_base_handler(*, session: WebTransportSession, serializer: Any, serializer_name: str) -> None:
    """Provide the base handler logic for structured echo."""
    session_id = session.session_id
    logger.info("Structured handler started for session %s (%s)", session_id, serializer_name)

    try:
        s_stream_manager_task = asyncio.create_task(
            coro=handle_all_structured_streams(session=session, serializer=serializer)
        )
        s_datagram_task = asyncio.create_task(coro=handle_structured_datagram(session=session, serializer=serializer))
        await asyncio.gather(s_stream_manager_task, s_datagram_task, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Structured handler error for session %s: %s", session_id, e, exc_info=True)
    finally:
        logger.info("Structured handler finished for session %s", session_id)


async def echo_handler(session: WebTransportSession, **kwargs: Any) -> None:
    """Handle echoing streams and datagrams."""
    session_id = session.session_id
    logger.info("Handler started for session %s on path %s", session_id, session.path)

    try:
        datagram_task = asyncio.create_task(coro=handle_datagrams(session=session))
        stream_task = asyncio.create_task(coro=handle_incoming_streams(session=session))
        await asyncio.gather(datagram_task, stream_task, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Handler error for session %s: %s", session_id, e, exc_info=True)
    finally:
        logger.info("Handler finished for session %s", session_id)


async def handle_all_structured_streams(*, session: WebTransportSession, serializer: Any) -> None:
    """Listen for and handle all incoming streams for a structured session."""
    session_id = session.session_id

    async def stream_opened_handler(event: Event) -> None:
        if not isinstance(event.data, dict):
            return
        stream = event.data.get("stream")
        if isinstance(stream, WebTransportStream):
            asyncio.create_task(coro=handle_structured_stream(stream=stream, serializer=serializer))

    session.events.on(event_type=EventType.STREAM_OPENED, handler=stream_opened_handler)
    try:
        await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    except (asyncio.CancelledError, ConnectionError):
        pass
    except Exception as e:
        logger.error("Structured stream manager for session %s error: %s", session_id, e, exc_info=True)
    finally:
        session.events.off(event_type=EventType.STREAM_OPENED, handler=stream_opened_handler)


async def handle_bidirectional_stream(*, stream: WebTransportStream) -> None:
    """Handle echo logic for a bidirectional stream."""
    try:
        request_data = await stream.read_all()
        echo_data = b"ECHO: " + request_data
        await stream.write_all(data=echo_data, end_stream=True)
    except (asyncio.CancelledError, ConnectionError, StreamError):
        pass
    except Exception as e:
        logger.error("Bidirectional stream %s error: %s", stream.stream_id, e, exc_info=True)
        await stream.close(error_code=1)


async def handle_datagrams(*, session: WebTransportSession) -> None:
    """Receive and echo datagrams for a session."""
    session_id = session.session_id
    logger.debug("Starting datagram handler for session %s", session_id)

    async def datagram_handler(event: Event) -> None:
        if not isinstance(event.data, dict):
            return
        data = event.data.get("data")
        if not isinstance(data, bytes):
            return

        try:
            echo_data = b"ECHO: " + data
            await session.send_datagram(data=echo_data)
        except (asyncio.CancelledError, ConnectionError, SessionError) as e:
            logger.warning("Datagram handler error for session %s: %s", session_id, e)

    session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=datagram_handler)
    try:
        await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    except (asyncio.CancelledError, ConnectionError):
        pass
    except Exception as e:
        logger.error("Datagram handler error for session %s: %s", session_id, e, exc_info=True)
    finally:
        session.events.off(event_type=EventType.DATAGRAM_RECEIVED, handler=datagram_handler)


async def handle_incoming_streams(*, session: WebTransportSession) -> None:
    """Listen for and handle all incoming streams for a session."""
    session_id = session.session_id
    logger.debug("Starting stream handler for session %s", session_id)

    async def stream_opened_handler(event: Event) -> None:
        if not isinstance(event.data, dict):
            return
        stream = event.data.get("stream")
        if stream:
            asyncio.create_task(coro=handle_single_stream(stream=stream, session_id=session_id))

    session.events.on(event_type=EventType.STREAM_OPENED, handler=stream_opened_handler)
    try:
        await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    except (asyncio.CancelledError, ConnectionError):
        pass
    except Exception as e:
        logger.error("Stream handler error for session %s: %s", session_id, e, exc_info=True)
    finally:
        session.events.off(event_type=EventType.STREAM_OPENED, handler=stream_opened_handler)


async def handle_receive_stream(*, stream: WebTransportReceiveStream) -> None:
    """Handle data from a receive-only stream."""
    try:
        await stream.read_all()
    except (asyncio.CancelledError, ConnectionError, StreamError):
        pass
    except Exception as e:
        logger.error("Receive stream %s error: %s", stream.stream_id, e, exc_info=True)


async def handle_single_stream(*, stream: Any, session_id: int) -> None:
    """Process a single stream based on its type."""
    stream_id = stream.stream_id

    try:
        if isinstance(stream, WebTransportStream):
            await handle_bidirectional_stream(stream=stream)
        elif isinstance(stream, WebTransportReceiveStream):
            await handle_receive_stream(stream=stream)
        else:
            logger.warning("Unknown stream type for %s", stream_id)
    except Exception as e:
        logger.error("Error processing stream %s: %s", stream_id, e, exc_info=True)
    finally:
        pass


async def handle_structured_datagram(*, session: WebTransportSession, serializer: Any) -> None:
    """Receive and echo structured datagrams for a session."""
    session_id = session.session_id
    logger.debug("Starting structured datagram handler for session %s", session_id)

    try:
        structured_datagram_transport = StructuredDatagramTransport(
            session=session, registry=MESSAGE_REGISTRY, serializer=serializer
        )
        structured_datagram_transport.initialize()

        while not session.is_closed:
            obj = await structured_datagram_transport.receive_obj()
            await structured_datagram_transport.send_obj(obj=obj)

    except (asyncio.CancelledError, ConnectionError, SessionError, TimeoutError):
        logger.debug("Structured datagram handler for session %s closing.", session_id)
    except Exception as e:
        logger.error("Structured datagram handler error for session %s: %s", session_id, e, exc_info=True)
    finally:
        logger.debug("Structured datagram handler for session %s finished.", session_id)


async def handle_structured_stream(*, stream: WebTransportStream, serializer: Any) -> None:
    """Handle echoing structured objects on a single, existing bidirectional stream."""
    raw_stream = stream
    stream_id = raw_stream.stream_id
    logger.debug("Handling structured stream %s", stream_id)

    try:
        structured_stream = StructuredStream(
            stream=raw_stream,
            registry=MESSAGE_REGISTRY,
            serializer=serializer,
            max_message_size=DEFAULT_MAX_MESSAGE_SIZE,
        )
        async for obj in structured_stream:
            logger.debug("Echoing object on stream %s: %s", stream_id, obj)
            await structured_stream.send_obj(obj=obj)
    except (asyncio.CancelledError, ConnectionError, StreamError):
        pass
    except Exception as e:
        logger.error("Structured stream %s error: %s", stream_id, e, exc_info=True)
    finally:
        if not raw_stream.is_closed:
            await raw_stream.close()


async def structured_echo_json_handler(session: WebTransportSession, **kwargs: Any) -> None:
    """Handle echoing structured objects using JSON."""
    await _structured_echo_base_handler(session=session, serializer=JSON_SERIALIZER, serializer_name="JSON")


async def structured_echo_msgpack_handler(session: WebTransportSession, **kwargs: Any) -> None:
    """Handle echoing structured objects using MsgPack."""
    await _structured_echo_base_handler(session=session, serializer=MSGPACK_SERIALIZER, serializer_name="MsgPack")


async def main() -> None:
    """Configure and start the WebTransport E2E test server."""
    logger.info("Starting WebTransport E2E Test Server...")

    if not CERT_PATH.exists() or not KEY_PATH.exists():
        logger.info("Generating self-signed certificate for %s...", CERT_HOSTNAME)
        generate_self_signed_cert(hostname=CERT_HOSTNAME, output_dir=".")

    config = ServerConfig(
        bind_host=SERVER_HOST,
        bind_port=SERVER_PORT,
        certfile=str(CERT_PATH),
        keyfile=str(KEY_PATH),
        log_level="DEBUG" if DEBUG_MODE else "INFO",
    )
    app = E2EServerApp(config=config)

    logger.info("Server binding to %s:%s", config.bind_host, config.bind_port)
    if DEBUG_MODE:
        logger.info("Debug mode enabled - verbose logging active")
    logger.info("Ready for E2E tests!")

    try:
        async with app:
            await app.serve()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped gracefully by user.")
    except Exception as e:
        logger.critical("Server crashed unexpectedly: %s", e, exc_info=True)
        sys.exit(1)
