"""WebTransport interoperability test server."""

import asyncio
import http
import logging
import random
from collections import deque
from pathlib import Path
from typing import Any, Final, Optional, Union
from urllib.parse import parse_qs, urlparse

import uvloop

from pywebtransport import (
    ServerApp,
    ServerConfig,
    StreamError,
    TimeoutError,
    WebTransportReceiveStream,
    WebTransportSendStream,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport import __version__ as LIB_VERSION
from pywebtransport.serializer import JSONSerializer
from pywebtransport.server.middleware import MiddlewareRejected
from pywebtransport.types import EventType, SessionProtocol, StreamDirection
from pywebtransport.utils import generate_self_signed_cert

CERT_HOSTNAME: Final[str] = "localhost"
CERT_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.crt")
KEY_PATH: Final[Path] = Path(f"{CERT_HOSTNAME}.key")

HOST: Final[str] = "::"
PORT: Final[int] = 4433

BATON_TIMEOUT: Final[float] = 10.0
MAX_PADDING: Final[int] = 65536

ERR_BORED: Final[int] = 0x04
ERR_BRUH: Final[int] = 0x02
ERR_DA_YAMN: Final[int] = 0x01
ERR_IDC: Final[int] = 0x01
ERR_I_LIED: Final[int] = 0x03
ERR_SUS: Final[int] = 0x03
ERR_WHATEVER: Final[int] = 0x02

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(name)s] %(message)s", datefmt="%H:%M:%S"
)
logger = logging.getLogger(name="interop")


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
        self.add_middleware(middleware=self._validate_baton_params)
        self._register_routes()
        logger.info("InteropServer initialized (v%s)", LIB_VERSION)

    async def handle_devious_baton(self, session: WebTransportSession, **kwargs: Any) -> None:
        """Handle the Devious Baton protocol."""
        query = parse_qs(qs=urlparse(url=session.path).query)
        count = int(query.get("count", ["1"])[0])
        baton_arg = query.get("baton", [None])[0]
        initial_baton = int(baton_arg) if baton_arg is not None else random.randint(1, 255)

        logger.info("Session %s: devious baton started (count=%d)", session.session_id, count)

        state = {"active_batons": count}

        try:
            for i in range(count):
                try:
                    stream: WebTransportSendStream = await session.create_unidirectional_stream()
                except (TimeoutError, StreamError):
                    logger.warning("Session %s: failed to create initial stream", session.session_id)
                    await session.close(error_code=ERR_DA_YAMN, reason="Insufficient stream credit")
                    return

                self._attach_baton_handlers(session, stream)
                payload = self._create_baton_payload(initial_baton)

                try:
                    async with asyncio.timeout(delay=BATON_TIMEOUT):
                        await stream.write(data=payload)
                        await stream.close()
                except asyncio.TimeoutError:
                    await session.close(error_code=ERR_BORED, reason="Timeout sending initial baton")
                    return

        except Exception as e:
            logger.error("Server init loop failed: %s", e)
            await session.close(error_code=ERR_DA_YAMN, reason="Insufficient credit or error")
            return

        async def on_datagram(event: Any) -> None:
            if isinstance(event.data, dict) and (data := event.data.get("data")):
                if not self._validate_baton_datagram(data):
                    await session.close(error_code=ERR_BRUH, reason="Malformed baton datagram")

        async def on_stream(event: Any) -> None:
            if isinstance(event.data, dict) and (stream := event.data.get("stream")):
                if not stream.is_remote:
                    return

                self._attach_baton_handlers(session, stream)

                source_type = "unknown"
                match stream.direction:
                    case StreamDirection.RECEIVE_ONLY:
                        source_type = "uni"
                    case StreamDirection.BIDIRECTIONAL:
                        source_type = "bidi_peer"

                if source_type != "unknown":
                    expected = None
                    if source_type == "bidi_peer":
                        expected = (initial_baton + 1) % 256 if count == 1 else None

                    asyncio.create_task(
                        coro=self._handle_baton_stream(session, stream, source_type, state, expected_val=expected)
                    )

        session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_datagram)
        session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

        try:
            await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
            logger.info("Session %s: closed", session.session_id)
        except Exception:
            pass

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
                    asyncio.create_task(coro=self._echo_stream(stream))

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

    def _attach_baton_handlers(
        self,
        session: WebTransportSession,
        stream: Union[WebTransportReceiveStream, WebTransportSendStream, WebTransportStream],
    ) -> None:
        """Attach error handling listeners for Devious Baton Section 4.6."""

        async def _on_stop_sending(event: Any) -> None:
            if isinstance(stream, (WebTransportSendStream, WebTransportStream)):
                try:
                    await stream.reset(error_code=ERR_WHATEVER)
                except Exception:
                    pass

        async def _on_reset(event: Any) -> None:
            if isinstance(stream, WebTransportStream):
                try:
                    await stream.reset(error_code=ERR_WHATEVER)
                except Exception:
                    pass

        stream.events.on(event_type=EventType.STOP_SENDING_RECEIVED, handler=_on_stop_sending)
        stream.events.on(event_type=EventType.STREAM_RESET_RECEIVED, handler=_on_reset)

    async def _baton_read_varint(
        self, stream: Union[WebTransportReceiveStream, WebTransportStream]
    ) -> tuple[int, bytes]:
        """Read a QUIC variable-length integer from the stream, returning value and raw bytes."""
        b1 = await stream.readexactly(n=1)

        first = b1[0]
        prefix = first >> 6
        length = 1 << prefix

        if length == 1:
            return first & 0x3F, b1

        rest = await stream.readexactly(n=length - 1)

        raw = b1 + rest
        data = bytes([first]) + rest
        value = int.from_bytes(data, "big")

        match length:
            case 2:
                return value & 0x3FFF, raw
            case 4:
                return value & 0x3FFFFFFF, raw
            case _:
                return value & 0x3FFFFFFFFFFFFFFF, raw

    def _baton_write_varint(self, value: int) -> bytes:
        """Encode an integer into QUIC variable-length bytes."""
        if value < 64:
            return bytes([value])
        elif value < 16384:
            return bytes([(0x1 << 6) | (value >> 8), value & 0xFF])
        elif value < 1073741824:
            return bytes([(0x2 << 6) | (value >> 24), (value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF])
        else:
            return bytes(
                [
                    (0x3 << 6) | (value >> 56),
                    (value >> 48) & 0xFF,
                    (value >> 40) & 0xFF,
                    (value >> 32) & 0xFF,
                    (value >> 24) & 0xFF,
                    (value >> 16) & 0xFF,
                    (value >> 8) & 0xFF,
                    value & 0xFF,
                ]
            )

    def _create_baton_payload(self, baton: int) -> bytes:
        """Create a baton message payload with optional padding."""
        padding = b""
        if baton % 5 == 0:
            p_len = random.randint(1, 256)
            padding = bytes([random.randint(0, 255) for _ in range(p_len)])
        return self._baton_write_varint(value=len(padding)) + padding + bytes([baton])

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

    async def _handle_baton_stream(
        self,
        session: WebTransportSession,
        stream: Union[WebTransportReceiveStream, WebTransportStream],
        source_type: str,
        state: dict[str, int],
        expected_val: Optional[int] = None,
    ) -> None:
        """Process incoming baton messages and perform stream switching."""
        try:
            padding_len, len_bytes = await self._baton_read_varint(stream)

            if padding_len > MAX_PADDING:
                await session.close(error_code=ERR_SUS, reason="Padding too large")
                return

            padding_data = b""
            if padding_len > 0:
                padding_data = await stream.readexactly(n=padding_len)

            baton_bytes = await stream.readexactly(n=1)

            baton_val = baton_bytes[0]
            if expected_val is not None and baton_val != expected_val:
                await session.close(error_code=ERR_SUS, reason="Unexpected baton value")
                return

            if baton_val == 0:
                state["active_batons"] -= 1
                if state["active_batons"] <= 0:
                    await session.close()
                return

            if baton_val % 7 == 0:
                dgram_msg = len_bytes + padding_data + baton_bytes
                try:
                    await session.send_datagram(data=dgram_msg)
                except Exception:
                    pass

            next_baton = (baton_val + 1) % 256
            out_msg = self._create_baton_payload(next_baton)

            match source_type:
                case "uni":
                    try:
                        new_bidi_stream = await session.create_bidirectional_stream()
                    except TimeoutError:
                        await session.close(error_code=ERR_BORED, reason="Timeout waiting for credit")
                        return
                    except Exception:
                        await session.close(error_code=ERR_DA_YAMN, reason="Insufficient credit")
                        return

                    self._attach_baton_handlers(session, new_bidi_stream)

                    try:
                        async with asyncio.timeout(delay=BATON_TIMEOUT):
                            await new_bidi_stream.write(data=out_msg)
                            await new_bidi_stream.write(data=b"", end_stream=True)
                    except asyncio.TimeoutError:
                        await session.close(error_code=ERR_BORED, reason="Timeout writing")
                        return

                    asyncio.create_task(
                        coro=self._handle_baton_stream(
                            session, new_bidi_stream, "bidi_self", state, (next_baton + 1) % 256
                        )
                    )

                case "bidi_peer":
                    if isinstance(stream, WebTransportStream):
                        try:
                            async with asyncio.timeout(delay=BATON_TIMEOUT):
                                await stream.write(data=out_msg)
                                await stream.write(data=b"", end_stream=True)
                        except asyncio.TimeoutError:
                            await session.close(error_code=ERR_BORED, reason="Timeout writing")
                            return

                case "bidi_self":
                    try:
                        new_uni_stream = await session.create_unidirectional_stream()
                    except TimeoutError:
                        await session.close(error_code=ERR_BORED, reason="Timeout waiting for credit")
                        return
                    except Exception:
                        await session.close(error_code=ERR_DA_YAMN, reason="Insufficient credit")
                        return

                    self._attach_baton_handlers(session, new_uni_stream)

                    try:
                        async with asyncio.timeout(delay=BATON_TIMEOUT):
                            await new_uni_stream.write(data=out_msg)
                            await new_uni_stream.close()
                    except asyncio.TimeoutError:
                        await session.close(error_code=ERR_BORED, reason="Timeout writing")
                        return

        except asyncio.IncompleteReadError:
            await session.close(error_code=ERR_BRUH, reason="Malformed baton")
        except StreamError as e:
            if e.error_code == ERR_I_LIED:
                return
            await session.close(error_code=ERR_SUS, reason="Stream processing error")
        except Exception:
            await session.close(error_code=ERR_SUS, reason="Processing error")

    def _register_routes(self) -> None:
        """Register request handlers."""
        self.route(path="/echo")(self.handle_echo)
        self.route(path="/stats")(self.handle_stats)
        self.route(path="/status")(self.handle_status)
        self.pattern_route(pattern=r"/webtransport/devious-baton(\?.*)?")(self.handle_devious_baton)

    def _validate_baton_datagram(self, data: bytes) -> bool:
        """Validate datagram format: Varint(Padding Length) + Padding + Baton(1 byte)."""
        if not data:
            return False

        try:
            first = data[0]
            prefix = first >> 6
            len_len = 1 << prefix

            if len(data) < len_len:
                return False

            padding_len = 0
            if len_len == 1:
                padding_len = first & 0x3F
            else:
                raw = bytearray(data[:len_len])
                raw[0] &= 0x3F
                padding_len = int.from_bytes(raw, "big")

            if len(data) != len_len + padding_len + 1:
                return False

            return True
        except Exception:
            return False

    async def _validate_baton_params(self, *, session: SessionProtocol) -> None:
        """Middleware to validate Devious Baton parameters during handshake."""
        if "/webtransport/devious-baton" not in session.path:
            return

        query = parse_qs(qs=urlparse(url=session.path).query)
        try:
            if "version" in query:
                version = int(query["version"][0])
                if version != 0:
                    raise ValueError

            if "count" in query:
                count = int(query["count"][0])
                if count < 1 or count > 255:
                    raise ValueError

            if "baton" in query:
                baton = int(query["baton"][0])
                if not (1 <= baton <= 255):
                    raise ValueError

        except ValueError:
            raise MiddlewareRejected(status_code=http.HTTPStatus.BAD_REQUEST)


async def main() -> None:
    """Configure and start the server."""
    if not CERT_PATH.exists() or not KEY_PATH.exists():
        generate_self_signed_cert(hostname=CERT_HOSTNAME, output_dir=".")

    config = ServerConfig(bind_host=HOST, bind_port=PORT, certfile=str(CERT_PATH), keyfile=str(KEY_PATH))

    app = InteropServer(config=config)
    logger.info("Server starting on https://[%s]:%s", HOST, PORT)

    async with app:
        await app.serve()


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        pass
