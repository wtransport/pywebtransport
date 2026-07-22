"""E2E test server for WebTransport streams and datagrams."""

import asyncio
import json
import logging
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Final

from pywebtransport import (
    ConnectionError,
    Event,
    ServerApp,
    ServerConfig,
    SessionClosedError,
    SessionError,
    StreamError,
    WebTransportReceiveStream,
    WebTransportServer,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport.framework import MiddlewareRejected
from pywebtransport.types import ConnectionState, EventType, SessionProtocol, SessionState
from pywebtransport.utils import generate_self_signed_cert, init_tracing

CERT_HOSTNAME: Final[str] = "localhost"
CERT_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.crt")
KEY_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.key")

SERVER_HOST: Final[str] = "::"
SERVER_PORT: Final[int] = 4433
OPTIMISTIC_SERVER_PORT: Final[int] = 4434
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="e2e_server")


class E2EServerApp(ServerApp):
    """E2E test server application with full test support."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the E2E server application."""
        super().__init__(**kwargs)
        self.server.on(event_type=EventType.CONNECTION_ESTABLISHED, handler=self._on_connection_established)
        self.server.on(event_type=EventType.SESSION_REQUEST, handler=self._on_session_request)
        self._register_middleware()
        self._register_handlers()
        logger.info("E2E Server initialized")

    def _register_middleware(self) -> None:
        """Register custom middleware for testing specific features."""

        @self.middleware
        async def negotiate_protocol(*, session: SessionProtocol) -> None:
            if session.path == "/protocol-negotiation":
                offered = session.wt_available_protocols
                if not offered:
                    raise MiddlewareRejected(status_code=400)

                if "trigger-missing" in offered:
                    session.wt_protocol = None
                    return

                if "trigger-mismatch" in offered:
                    session.wt_protocol = "alien-proto"
                    return

                if "chat-v2" in offered:
                    session.wt_protocol = "chat-v2"
                elif "chat-v1" in offered:
                    session.wt_protocol = "chat-v1"
                else:
                    raise MiddlewareRejected(status_code=403)

    async def _diagnostics_handler(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Handle requests for server statistics on the /diagnostics path."""
        logger.info("Diagnostics request from session %d", session.session_id)
        try:
            async with asyncio.timeout(delay=5.0):
                stream = await session.accept_bidirectional_stream()

            diagnostics = await self.server.diagnostics()
            stats_json = json.dumps(obj=asdict(obj=diagnostics), indent=2).encode(encoding="utf-8")
            await stream.write(data=stats_json, end_stream=True)
            logger.info("Sent diagnostics: %d bytes", len(stats_json))
        except asyncio.TimeoutError:
            logger.error("Client connected but never opened a stream")
        except SessionClosedError:
            pass
        except Exception as e:
            logger.error("Diagnostics handler error: %s", e)
        finally:
            if not session.is_closed:
                await session.close()

    async def _export_keying_handler(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Handle requests for TLS keying material export on the /export-keying-material path."""
        logger.info("TLS Export request from session %d", session.session_id)
        try:
            async with asyncio.timeout(delay=5.0):
                stream = await session.accept_bidirectional_stream()
                req_data = await stream.read_all()

            req = json.loads(s=req_data.decode(encoding="utf-8"))
            label = req["label"]
            context = bytes.fromhex(req["context_hex"])
            length = req["length"]

            server_key = await session.export_keying_material(label=label, context=context, length=length)

            await stream.write_all(data=server_key, end_stream=True)
            logger.info("Successfully exported and sent %d bytes of keying material", len(server_key))
        except Exception as e:
            logger.error("TLS Export handler error: %s", e, exc_info=True)
        finally:
            if not session.is_closed:
                await session.close()

    async def _health_handler(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Handle health check requests on the /health path."""
        logger.info("Health check from session %d", session.session_id)
        try:
            diagnostics = await self.server.diagnostics()
            stats = diagnostics.stats
            active_sessions = diagnostics.session_states.get(SessionState.CONNECTED, 0)
            active_connections = sum(v for k, v in diagnostics.connection_states.items() if k != ConnectionState.CLOSED)

            health_data = {
                "status": "healthy",
                "timestamp": time.time(),
                "uptime": (time.perf_counter() - stats.start_time) if stats.start_time else 0.0,
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
            logger.info("Session request: %d for path '%s'", session_id, path)

    def _register_handlers(self) -> None:
        """Centralize registration for all server routes."""
        self.route(path="/")(echo_handler)
        self.route(path="/diagnostics")(self._diagnostics_handler)
        self.route(path="/echo")(echo_handler)
        self.route(path="/export-keying-material")(self._export_keying_handler)
        self.route(path="/health")(self._health_handler)
        self.route(path="/protocol-negotiation")(echo_handler)


async def echo_handler(session: WebTransportSession, **kwargs: Any) -> None:
    """Handle echoing streams and datagrams."""
    session_id = session.session_id
    logger.info("Handler started for session %d on path %s", session_id, session.path)

    try:
        datagram_task = asyncio.create_task(coro=handle_datagrams(session=session))
        stream_task = asyncio.create_task(coro=handle_incoming_streams(session=session))
        await asyncio.gather(datagram_task, stream_task, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Handler error for session %d: %s", session_id, e, exc_info=True)
    finally:
        logger.info("Handler finished for session %d", session_id)


async def handle_bidirectional_stream(*, stream: WebTransportStream) -> None:
    """Handle echo logic for a bidirectional stream."""
    try:
        request_data = await stream.read_all()
        echo_data = b"ECHO: " + request_data
        await stream.write_all(data=echo_data, end_stream=True)
    except (ConnectionError, StreamError, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.error("Bidirectional stream %d error: %s", stream.stream_id, e, exc_info=True)
        await stream.close(error_code=1)


async def handle_datagrams(*, session: WebTransportSession) -> None:
    """Receive and echo datagrams for a session."""
    session_id = session.session_id
    logger.debug("Starting datagram handler for session %d", session_id)

    async def datagram_handler(event: Event) -> None:
        if isinstance(event.data, dict) and (data := event.data.get("data")):
            if isinstance(data, bytes):
                try:
                    echo_data = b"ECHO: " + data
                    await session.send_datagram(data=echo_data)
                except asyncio.CancelledError:
                    pass
                except (ConnectionError, SessionError) as e:
                    logger.debug("Datagram echo failed due to client disconnect: %s", e)

    session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=datagram_handler)
    try:
        await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    except (ConnectionError, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.error("Datagram handler error for session %d: %s", session_id, e, exc_info=True)
    finally:
        session.events.off(event_type=EventType.DATAGRAM_RECEIVED, handler=datagram_handler)


async def handle_incoming_streams(*, session: WebTransportSession) -> None:
    """Listen for and handle all incoming streams for a session."""
    session_id = session.session_id
    logger.debug("Starting stream handler for session %d", session_id)

    async def accept_bidi() -> None:
        try:
            while True:
                stream = await session.accept_bidirectional_stream()
                asyncio.create_task(coro=handle_single_stream(stream=stream, session_id=session_id))
        except SessionClosedError:
            pass

    async def accept_uni() -> None:
        try:
            while True:
                stream = await session.accept_unidirectional_stream()
                asyncio.create_task(coro=handle_single_stream(stream=stream, session_id=session_id))
        except SessionClosedError:
            pass

    t1 = asyncio.create_task(coro=accept_bidi())
    t2 = asyncio.create_task(coro=accept_uni())

    try:
        await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    except Exception:
        pass
    finally:
        t1.cancel()
        t2.cancel()


async def handle_receive_stream(*, stream: WebTransportReceiveStream) -> None:
    """Handle data from a receive-only stream."""
    try:
        await stream.read_all()
    except (ConnectionError, StreamError, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.error("Receive stream %d error: %s", stream.stream_id, e, exc_info=True)


async def handle_single_stream(*, stream: Any, session_id: int) -> None:
    """Process a single stream based on its type."""
    stream_id = stream.stream_id

    try:
        if isinstance(stream, WebTransportStream):
            await handle_bidirectional_stream(stream=stream)
        elif isinstance(stream, WebTransportReceiveStream):
            await handle_receive_stream(stream=stream)
        else:
            logger.warning("Unknown stream type for %d", stream_id)
    except Exception as e:
        logger.error("Error processing stream %d: %s", stream_id, e, exc_info=True)
    finally:
        pass


async def run_optimistic_server(*, config: ServerConfig, port: int) -> None:
    """Run a bare WebTransportServer that delays session acceptance."""
    server = WebTransportServer(config=config)

    async def _on_session_request(event: Event) -> None:
        if not isinstance(event.data, dict):
            return

        session = event.data.get("session")
        if not isinstance(session, WebTransportSession):
            return

        session_id = session.session_id
        logger.info("Optimistic session %d awaiting acceptance", session_id)

        datagram_task = asyncio.create_task(coro=handle_datagrams(session=session))
        stream_task = asyncio.create_task(coro=handle_incoming_streams(session=session))

        await asyncio.sleep(delay=1.0)
        await session.accept()
        logger.info("Optimistic session %d accepted", session_id)

        try:
            await asyncio.gather(datagram_task, stream_task, return_exceptions=True)
        finally:
            if not session.is_closed:
                await session.close()

    server.on(event_type=EventType.SESSION_REQUEST, handler=_on_session_request)

    async with server:
        await server.listen(host=config.bind_host, port=port)
        await server.serve_forever()


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

    logger.info("Server binding to %s:%d", config.bind_host, config.bind_port)
    logger.info("Optimistic server binding to %s:%d", SERVER_HOST, OPTIMISTIC_SERVER_PORT)
    if DEBUG_MODE:
        logger.info("Debug mode enabled")
    logger.info("Ready for E2E tests")

    try:
        async with app, asyncio.TaskGroup() as tg:
            tg.create_task(coro=app.serve())
            tg.create_task(coro=run_optimistic_server(config=config, port=OPTIMISTIC_SERVER_PORT))
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped gracefully by user")
    except Exception as e:
        logger.critical("Server crashed unexpectedly: %s", e, exc_info=True)
        sys.exit(1)
