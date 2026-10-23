"""WebTransport interoperability test server."""

import asyncio
import base64
import http
import json
import logging
import random
import uuid
from collections import deque
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Final, Literal
from urllib.parse import parse_qs, urlparse

from pywebtransport import (
    ConnectionError,
    DatagramError,
    Event,
    ServerConfig,
    SessionError,
    StreamError,
    TimeoutError,
    WebTransportBidirectionalStream,
    WebTransportConnection,
    WebTransportReceiveStream,
    WebTransportServer,
    WebTransportSession,
)
from pywebtransport import __version__ as LIB_VERSION
from pywebtransport.types import EventType
from pywebtransport.utils import generate_self_signed_cert

CERT_HOSTNAME: Final[str] = "localhost"
CERT_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.crt")
KEY_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.key")

SERVER_HOST: Final[str] = "::"
SERVER_PORT: Final[int] = 4433

BATON_TIMEOUT: Final[float] = 10.0
MAX_PADDING: Final[int] = 65536

ERR_BORED: Final[int] = 0x04
ERR_BRUH: Final[int] = 0x02
ERR_DA_YAMN: Final[int] = 0x01
ERR_IDC: Final[int] = 0x01
ERR_I_LIED: Final[int] = 0x03
ERR_SUS: Final[int] = 0x03
ERR_WHATEVER: Final[int] = 0x02

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(name="interop")


@dataclass(kw_only=True)
class BatonState:
    """Track the batons still in play within a Devious Baton session."""

    active_batons: int


def attach_baton_handler(*, stream: WebTransportBidirectionalStream) -> None:
    """Reset the sending part when the peer resets the stream, per Devious Baton Section 4.6."""

    async def on_reset(event: Event) -> None:
        try:
            await stream.reset(error_code=ERR_WHATEVER)
        except (ConnectionError, StreamError) as e:
            logger.debug("Stream %d reset failed: %s", stream.stream_id, e)

    stream.events.on(event_type=EventType.STREAM_RESET_RECEIVED, handler=on_reset)


def create_baton_message(*, baton: int) -> bytes:
    """Create a Baton message, padded when the baton is a multiple of 5."""
    padding = b""
    if baton % 5 == 0:
        padding = random.randbytes(random.randint(1, 256))
    return encode_varint(value=len(padding)) + padding + bytes([baton])


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
                await session.send_datagram(data=data)
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
    """Echo the data of a bidirectional stream as it arrives."""
    try:
        async for chunk in stream:
            await stream.write(data=chunk)
        await stream.write(data=b"", end_stream=True)
    except (ConnectionError, StreamError) as e:
        logger.debug("Stream %d echo ended: %s", stream.stream_id, e)


def encode_json_value(value: Any) -> Any:
    """Convert a value json cannot serialize into a serializable one."""
    match value:
        case bytes() | bytearray() | memoryview():
            return base64.b64encode(value).decode("ascii")
        case deque() | set() | frozenset():
            return list(value)
        case uuid.UUID():
            return str(value)
        case Enum():
            return value.value
        case datetime():
            return value.isoformat()
        case _ if is_dataclass(value) and not isinstance(value, type):
            return asdict(obj=value)
        case _:
            raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def encode_varint(*, value: int) -> bytes:
    """Encode an integer as a QUIC variable-length integer."""
    if value < 0x40:
        return value.to_bytes(length=1)
    if value < 0x4000:
        return (value | 0x4000).to_bytes(length=2)
    if value < 0x40000000:
        return (value | 0x80000000).to_bytes(length=4)
    return (value | 0xC000000000000000).to_bytes(length=8)


def is_valid_baton_datagram(*, data: bytes) -> bool:
    """Check that a datagram is a padding length, the padding and a one-byte baton."""
    if not data:
        return False

    length_size = 1 << (data[0] >> 6)
    if len(data) < length_size:
        return False

    padding_length = int.from_bytes(bytes([data[0] & 0x3F]) + data[1:length_size])
    return len(data) == length_size + padding_length + 1


async def pass_batons(*, session: WebTransportSession, count: int, baton: int) -> None:
    """Start the requested batons and pass them on until the session is closed, per Devious Baton Section 4."""
    logger.info("Session %d: devious baton started (count=%d)", session.session_id, count)
    state = BatonState(active_batons=count)

    try:
        for _ in range(count):
            try:
                stream = await session.create_unidirectional_stream()
            except (StreamError, TimeoutError):
                await session.close(error_code=ERR_DA_YAMN, reason="insufficient stream credit")
                return
            async with asyncio.timeout(delay=BATON_TIMEOUT):
                await stream.write_all(data=create_baton_message(baton=baton), end_stream=True)
    except asyncio.TimeoutError:
        await session.close(error_code=ERR_BORED, reason="baton timeout")
        return
    except (ConnectionError, SessionError, StreamError) as e:
        logger.debug("Baton start ended for session %d: %s", session.session_id, e)
        return

    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(coro=process_baton_datagrams(session=session))
        task_group.create_task(
            coro=process_bidirectional_baton_messages(
                session=session, state=state, expected_baton=(baton + 1) % 256 if count == 1 else None
            )
        )
        task_group.create_task(coro=process_unidirectional_baton_messages(session=session, state=state))


async def process_baton_datagrams(*, session: WebTransportSession) -> None:
    """Close the session with ERR_BRUH on the first malformed baton datagram."""
    try:
        while True:
            data = await session.read_datagram()
            if not is_valid_baton_datagram(data=data):
                await session.close(error_code=ERR_BRUH, reason="malformed baton datagram")
                return
    except (ConnectionError, SessionError) as e:
        logger.debug("Baton datagram processing ended for session %d: %s", session.session_id, e)


async def process_baton_message(
    *,
    session: WebTransportSession,
    stream: WebTransportBidirectionalStream | WebTransportReceiveStream,
    state: BatonState,
    source: Literal["bidi_peer", "bidi_self", "uni"],
    expected_baton: int | None,
) -> None:
    """Process an incoming Baton message, per Devious Baton Section 4.2."""
    try:
        padding_length, length_encoded = await read_varint(stream=stream)
        if padding_length > MAX_PADDING:
            await session.close(error_code=ERR_SUS, reason="padding too large")
            return

        padding = await stream.readexactly(size=padding_length)
        baton_encoded = await stream.readexactly(size=1)
        baton = baton_encoded[0]

        if expected_baton is not None and baton != expected_baton:
            await session.close(error_code=ERR_SUS, reason="unexpected baton value")
            return

        if baton == 0:
            state.active_batons -= 1
            if state.active_batons <= 0:
                await session.close()
            return

        if baton % 7 == 0:
            try:
                await session.send_datagram(data=length_encoded + padding + baton_encoded)
            except DatagramError as e:
                logger.debug("Baton datagram dropped for session %d: %s", session.session_id, e)

        next_baton = (baton + 1) % 256
        message = create_baton_message(baton=next_baton)

        match source:
            case "bidi_peer":
                if isinstance(stream, WebTransportBidirectionalStream):
                    async with asyncio.timeout(delay=BATON_TIMEOUT):
                        await stream.write_all(data=message, end_stream=True)
            case "bidi_self":
                try:
                    unidirectional_stream = await session.create_unidirectional_stream()
                except (StreamError, TimeoutError):
                    await session.close(error_code=ERR_DA_YAMN, reason="insufficient stream credit")
                    return
                async with asyncio.timeout(delay=BATON_TIMEOUT):
                    await unidirectional_stream.write_all(data=message, end_stream=True)
            case "uni":
                try:
                    bidirectional_stream = await session.create_bidirectional_stream()
                except (StreamError, TimeoutError):
                    await session.close(error_code=ERR_DA_YAMN, reason="insufficient stream credit")
                    return
                attach_baton_handler(stream=bidirectional_stream)
                async with asyncio.timeout(delay=BATON_TIMEOUT):
                    await bidirectional_stream.write_all(data=message, end_stream=True)
                await process_baton_message(
                    session=session,
                    stream=bidirectional_stream,
                    state=state,
                    source="bidi_self",
                    expected_baton=(next_baton + 1) % 256,
                )
    except asyncio.IncompleteReadError:
        await session.close(error_code=ERR_BRUH, reason="malformed baton")
    except (asyncio.TimeoutError, TimeoutError):
        await session.close(error_code=ERR_BORED, reason="baton timeout")
    except StreamError as e:
        if e.error_code != ERR_I_LIED:
            await session.close(error_code=ERR_SUS, reason="stream processing error")
    except (ConnectionError, SessionError) as e:
        logger.debug("Baton message processing ended for session %d: %s", session.session_id, e)


async def process_bidirectional_baton_messages(
    *, session: WebTransportSession, state: BatonState, expected_baton: int | None
) -> None:
    """Process the Baton message of each incoming bidirectional stream until the session is closed."""
    async with asyncio.TaskGroup() as task_group:
        async for stream in session.incoming_bidirectional_streams():
            attach_baton_handler(stream=stream)
            task_group.create_task(
                coro=process_baton_message(
                    session=session, stream=stream, state=state, source="bidi_peer", expected_baton=expected_baton
                )
            )


async def process_unidirectional_baton_messages(*, session: WebTransportSession, state: BatonState) -> None:
    """Process the Baton message of each incoming unidirectional stream until the session is closed."""
    async with asyncio.TaskGroup() as task_group:
        async for stream in session.incoming_unidirectional_streams():
            task_group.create_task(
                coro=process_baton_message(
                    session=session, stream=stream, state=state, source="uni", expected_baton=None
                )
            )


async def read_varint(*, stream: WebTransportBidirectionalStream | WebTransportReceiveStream) -> tuple[int, bytes]:
    """Read a QUIC variable-length integer, returning its value and encoded bytes."""
    first = await stream.readexactly(size=1)
    length = 1 << (first[0] >> 6)
    encoded = first + await stream.readexactly(size=length - 1)
    return int.from_bytes(bytes([first[0] & 0x3F]) + encoded[1:]), encoded


async def respond_diagnostics(*, stream: WebTransportBidirectionalStream) -> None:
    """Respond on a bidirectional stream with the diagnostics of its session."""
    try:
        await stream.read_all()
        diagnostics = await stream.session.diagnose()
        payload = json.dumps(obj=asdict(obj=diagnostics), default=encode_json_value).encode(encoding="utf-8")
        await stream.write_all(data=payload, end_stream=True)
    except (ConnectionError, SessionError, StreamError) as e:
        logger.debug("Stream %d diagnostics response ended: %s", stream.stream_id, e)


async def serve_connection(*, connection: WebTransportConnection) -> None:
    """Serve each session delivered by the connection."""
    async with asyncio.TaskGroup() as task_group:
        async for session in connection.incoming_sessions():
            task_group.create_task(coro=serve_session(session=session))


async def serve_session(*, session: WebTransportSession) -> None:
    """Answer the session request by its path and serve the accepted session."""
    request_target = urlparse(url=session.path)
    logger.info("Session %d requested for path %s", session.session_id, request_target.path)

    try:
        async with session:
            match request_target.path:
                case "/echo":
                    await session.accept()
                    await echo_session(session=session)
                case "/stats":
                    await session.accept()
                    async with asyncio.TaskGroup() as task_group:
                        async for stream in session.incoming_bidirectional_streams():
                            task_group.create_task(coro=respond_diagnostics(stream=stream))
                case "/webtransport/devious-baton":
                    query = parse_qs(qs=request_target.query)
                    try:
                        version = int(query.get("version", ["0"])[0])
                        count = int(query.get("count", ["1"])[0])
                        baton = int(query["baton"][0]) if "baton" in query else random.randint(1, 255)
                    except ValueError:
                        await session.reject(status_code=http.HTTPStatus.BAD_REQUEST)
                        return

                    if version != 0 or not 1 <= count <= 255 or not 1 <= baton <= 255:
                        await session.reject(status_code=http.HTTPStatus.BAD_REQUEST)
                        return

                    await session.accept()
                    await pass_batons(session=session, count=count, baton=baton)
                case _:
                    await session.reject(headers=[("allow", "")], status_code=http.HTTPStatus.METHOD_NOT_ALLOWED)
    except Exception as e:
        logger.error("Session %d failed: %s", session.session_id, e, exc_info=True)


async def main() -> None:
    """Configure and start the server."""
    if not CERT_PATH.exists() or not KEY_PATH.exists():
        generate_self_signed_cert(hostname=CERT_HOSTNAME, output_dir=".")

    config = ServerConfig(
        bind_host=SERVER_HOST,
        bind_port=SERVER_PORT,
        certfile=str(CERT_PATH),
        keyfile=str(KEY_PATH),
    )

    async with WebTransportServer(config=config) as server:
        await server.listen()
        logger.info("Interop server (v%s) listening on https://[%s]:%d", LIB_VERSION, SERVER_HOST, SERVER_PORT)

        async with asyncio.TaskGroup() as task_group:
            async for connection in server.incoming_connections():
                task_group.create_task(coro=serve_connection(connection=connection))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
