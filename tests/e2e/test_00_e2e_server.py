"""E2E test server for WebTransport streams and datagrams."""

import asyncio
import http
import json
import logging
import sys
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
    WebTransportReceiveStream,
    WebTransportServer,
    WebTransportSession,
)
from pywebtransport.utils import generate_self_signed_cert, init_tracing

CERT_HOSTNAME: Final[str] = "localhost"
CERT_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.crt")
KEY_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.key")

SERVER_HOST: Final[str] = "::"
SERVER_PORT: Final[int] = 4433
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="e2e_server")


async def discard_stream(*, stream: WebTransportReceiveStream) -> None:
    """Read a unidirectional stream to its end and drop each chunk."""
    try:
        async for _ in stream:
            pass
    except (ConnectionError, StreamError) as e:
        logger.debug("Stream %d discard ended: %s", stream.stream_id, e)


async def discard_unidirectional_streams(*, session: WebTransportSession) -> None:
    """Discard each incoming unidirectional stream until the session is closed."""
    async with asyncio.TaskGroup() as task_group:
        async for stream in session.incoming_unidirectional_streams():
            task_group.create_task(coro=discard_stream(stream=stream))


async def echo_bidirectional_streams(*, session: WebTransportSession) -> None:
    """Echo each incoming bidirectional stream until the session is closed."""
    async with asyncio.TaskGroup() as task_group:
        async for stream in session.incoming_bidirectional_streams():
            task_group.create_task(coro=echo_stream(stream=stream))


async def echo_datagrams(*, session: WebTransportSession) -> None:
    """Echo datagrams until the session is closed."""
    try:
        while True:
            data = await session.read_datagram()
            try:
                await session.send_datagram(data=b"ECHO: " + data)
            except DatagramError as e:
                logger.debug("Datagram echo dropped for session %d: %s", session.session_id, e)
    except (ConnectionError, SessionError) as e:
        logger.debug("Datagram echo ended for session %d: %s", session.session_id, e)


async def echo_session(*, session: WebTransportSession) -> None:
    """Echo datagrams and bidirectional streams and discard unidirectional streams until the session is closed."""
    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(coro=discard_unidirectional_streams(session=session))
        task_group.create_task(coro=echo_bidirectional_streams(session=session))
        task_group.create_task(coro=echo_datagrams(session=session))


async def echo_stream(*, stream: WebTransportBidirectionalStream) -> None:
    """Echo the data of a bidirectional stream."""
    try:
        data = await stream.read_all()
        await stream.write_all(data=b"ECHO: " + data, end_stream=True)
    except (ConnectionError, StreamError) as e:
        logger.debug("Stream %d echo ended: %s", stream.stream_id, e)


async def export_keying_material(*, stream: WebTransportBidirectionalStream) -> None:
    """Respond on a bidirectional stream with the keying material it requests."""
    try:
        request = json.loads(s=await stream.read_all())
        keying_material = await stream.session.export_keying_material(
            label=request["label"], context=bytes.fromhex(request["context_hex"]), length=request["length"]
        )
        await stream.write_all(data=keying_material, end_stream=True)
    except (ConnectionError, SessionError, StreamError) as e:
        logger.debug("Stream %d keying material export ended: %s", stream.stream_id, e)


async def serve_connection(*, connection: WebTransportConnection) -> None:
    """Serve each session delivered by the connection."""
    async with asyncio.TaskGroup() as task_group:
        async for session in connection.incoming_sessions():
            task_group.create_task(coro=serve_session(session=session))


async def serve_session(*, session: WebTransportSession) -> None:
    """Answer the session request by its path and serve the accepted session."""
    logger.info("Session %d requested for path %s", session.session_id, session.path)

    try:
        async with session:
            match session.path:
                case "/":
                    await session.accept()
                    await echo_session(session=session)
                case "/acceptance-delay":
                    await asyncio.sleep(delay=1.0)
                    await session.accept()
                    await echo_session(session=session)
                case "/keying-material-export":
                    await session.accept()
                    async with asyncio.TaskGroup() as task_group:
                        async for stream in session.incoming_bidirectional_streams():
                            task_group.create_task(coro=export_keying_material(stream=stream))
                case "/protocol-negotiation":
                    wt_available_protocols = session.wt_available_protocols or []
                    wt_protocol = next(
                        (protocol for protocol in ("chat-v2", "chat-v1") if protocol in wt_available_protocols), None
                    )

                    if not wt_available_protocols:
                        await session.reject(status_code=http.HTTPStatus.BAD_REQUEST)
                    elif "no-selection" in wt_available_protocols:
                        await session.accept()
                        await echo_session(session=session)
                    elif wt_protocol is not None:
                        await session.accept(wt_protocol=wt_protocol)
                        await echo_session(session=session)
                    else:
                        await session.reject(status_code=http.HTTPStatus.FORBIDDEN)
                case _:
                    await session.reject(headers=[("allow", "")], status_code=http.HTTPStatus.METHOD_NOT_ALLOWED)
    except Exception as e:
        logger.error("Session %d failed: %s", session.session_id, e, exc_info=True)


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
    )

    async with WebTransportServer(config=config) as server:
        await server.listen()

        logger.info("Server binding to %s:%d", config.bind_host, config.bind_port)
        if DEBUG_MODE:
            logger.info("Debug mode enabled")
        logger.info("Ready for E2E tests")

        async with asyncio.TaskGroup() as task_group:
            async for connection in server.incoming_connections():
                task_group.create_task(coro=serve_connection(connection=connection))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped gracefully by user")
    except Exception as e:
        logger.critical("Server crashed unexpectedly: %s", e, exc_info=True)
        sys.exit(1)
