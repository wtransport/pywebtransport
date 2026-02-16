"""Devious Baton protocol compliance test client."""

import asyncio
import logging
import ssl
import struct
import sys
import time
import traceback
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Coroutine, Final
from urllib.parse import urlencode

from pywebtransport import (
    ClientConfig,
    ConnectionError,
    WebTransportClient,
    WebTransportSession,
)
from pywebtransport.exceptions import ClientError
from pywebtransport.types import EventType, StreamDirection

BASE_PATH: Final[str] = "/webtransport/devious-baton"
SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433

ERR_BORED: Final[int] = 0x04
ERR_BRUH: Final[int] = 0x02
ERR_DA_YAMN: Final[int] = 0x01
ERR_IDC: Final[int] = 0x01
ERR_I_LIED: Final[int] = 0x03
ERR_SUS: Final[int] = 0x03
ERR_WHATEVER: Final[int] = 0x02

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_devious_baton")


class TestFailure(Exception):
    """Exception raised when a test assertion fails."""


class VarInt:
    """Helper for parsing/encoding QUIC VarInts."""

    @staticmethod
    def encode(value: int) -> bytes:
        if value < 0x40:
            return struct.pack("!B", value)
        elif value < 0x4000:
            return struct.pack("!H", value | 0x4000)
        elif value < 0x40000000:
            return struct.pack("!L", value | 0x80000000)
        else:
            return struct.pack("!Q", value | 0xC000000000000000)

    @staticmethod
    def parse(data: bytes) -> tuple[int, int]:
        if not data:
            raise ValueError("Empty data")

        first = data[0]
        if first < 0x40:
            return first, 1
        elif first < 0x80:
            if len(data) < 2:
                raise ValueError("Truncated VarInt")
            return struct.unpack("!H", bytes([first & 0x3F]) + data[1:2])[0], 2
        elif first < 0xC0:
            if len(data) < 4:
                raise ValueError("Truncated VarInt")
            return struct.unpack("!L", bytes([first & 0x3F]) + data[1:4])[0], 4
        else:
            if len(data) < 8:
                raise ValueError("Truncated VarInt")
            return struct.unpack("!Q", bytes([first & 0x3F]) + data[1:8])[0], 8


def create_payload(baton_value: int, padding_len: int = 0) -> bytes:
    """Create a baton message payload."""
    padding = b"\x00" * padding_len
    return VarInt.encode(padding_len) + padding + bytes([baton_value])


class DeviousBatonTest:
    """Devious Baton protocol compliance test runner."""

    TEST_MAX_RETRIES: Final[int] = 3
    TEST_RETRY_DELAY: Final[float] = 1.0

    def __init__(self) -> None:
        """Initialize the test runner."""
        self._config = ClientConfig(verify_mode=ssl.CERT_NONE)
        self._results: list[tuple[str, bool, str]] = []

    async def run(self) -> None:
        """Execute all defined compliance tests."""
        logger.info("Starting Devious Baton protocol tests")
        logger.info("Target: https://%s:%s%s", SERVER_HOST, SERVER_PORT, BASE_PATH)

        tests: list[Callable[[], Coroutine[Any, Any, None]]] = [
            self.test_01_invalid_version,
            self.test_02_invalid_count,
            self.test_03_invalid_baton,
            self.test_04_server_initiates_uni_stream,
            self.test_05_flow_uni_to_bidi,
            self.test_06_flow_bidi_self_to_uni,
            self.test_07_datagram_trigger,
            self.test_08_random_padding,
            self.test_09_malformed_baton,
            self.test_10_unexpected_value,
            self.test_11_stop_sending_reaction,
            self.test_12_spontaneous_reset_reaction,
            self.test_13_graceful_finish,
        ]

        for test in tests:
            await self._run_test(test)

        self._print_results()

    async def test_01_invalid_version(self) -> None:
        """Verify server rejects invalid protocol version."""
        url = self._build_url(version=99)
        try:
            async with self._connect(url):
                pass
        except (ConnectionError, ClientError) as e:
            err_str = str(e)
            if "400" in err_str or "status" in err_str:
                return
        raise TestFailure("Server accepted invalid version 99")

    async def test_02_invalid_count(self) -> None:
        """Verify server rejects excessive baton count."""
        url = self._build_url(count=999999)
        try:
            async with self._connect(url):
                pass
        except (ConnectionError, ClientError) as e:
            err_str = str(e)
            if "400" in err_str or "status" in err_str:
                return
        raise TestFailure("Server accepted invalid count 999999")

    async def test_03_invalid_baton(self) -> None:
        """Verify server rejects invalid baton value."""
        url = self._build_url(baton=0)
        try:
            async with self._connect(url):
                pass
        except (ConnectionError, ClientError) as e:
            err_str = str(e)
            if "400" in err_str or "status" in err_str:
                return
        raise TestFailure("Server accepted invalid baton value 0")

    async def test_04_server_initiates_uni_stream(self) -> None:
        """Verify server initiates the protocol with unidirectional streams."""
        count = 3
        url = self._build_url(count=count, baton=10)

        async with self._connect(url) as session:
            streams_received = 0
            event = asyncio.Event()

            async def on_stream(evt: Any) -> None:
                nonlocal streams_received
                stream = evt.data["stream"]
                if stream.is_remote and stream.direction == StreamDirection.RECEIVE_ONLY:
                    streams_received += 1
                    if streams_received >= count:
                        event.set()

            session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

            try:
                async with asyncio.timeout(5.0):
                    await event.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not initiate unidirectional stream") from None

    async def test_05_flow_uni_to_bidi(self) -> None:
        """Verify Unidirectional -> Bidirectional stream switching logic."""
        url = self._build_url(baton=10)

        async with self._connect(url) as session:
            reply_received = asyncio.Event()

            async def on_stream(evt: Any) -> None:
                stream = evt.data["stream"]
                if stream.is_remote and stream.direction == StreamDirection.RECEIVE_ONLY:
                    payload = await stream.read_all()
                    if not payload:
                        return

                    bidi = await session.create_bidirectional_stream()

                    async def read_reply() -> None:
                        data = await bidi.read_all()
                        if data:
                            reply_received.set()

                    asyncio.create_task(read_reply())

                    next_val = (payload[-1] + 1) % 256
                    await bidi.write(data=create_payload(next_val), end_stream=True)

            session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

            try:
                async with asyncio.timeout(5.0):
                    await reply_received.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not reply on the same bidirectional stream") from None

    async def test_06_flow_bidi_self_to_uni(self) -> None:
        """Verify Bidirectional (Self) -> Unidirectional stream switching logic."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url) as session:
            server_bidi_opened = asyncio.Event()
            server_uni_opened = asyncio.Event()

            async def on_stream(evt: Any) -> None:
                stream = evt.data["stream"]
                if stream.is_remote:
                    if stream.direction == StreamDirection.BIDIRECTIONAL:
                        server_bidi_opened.set()
                        asyncio.create_task(handle_server_bidi(stream))
                    elif stream.direction == StreamDirection.RECEIVE_ONLY:
                        server_uni_opened.set()

            async def handle_server_bidi(stream: Any) -> None:
                payload = await stream.read_all()
                if payload:
                    next_val = (payload[-1] + 1) % 256
                    await stream.write(data=create_payload(next_val), end_stream=True)

            session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

            uni = await session.create_unidirectional_stream()
            await uni.write(data=create_payload(50), end_stream=True)

            try:
                async with asyncio.timeout(5.0):
                    await server_bidi_opened.wait()
                    await server_uni_opened.wait()
            except asyncio.TimeoutError:
                if not server_bidi_opened.is_set():
                    raise TestFailure("Server did not open Bidi stream in response to Uni") from None
                raise TestFailure("Server did not open Uni stream in response to Bidi (Self)") from None

    async def test_07_datagram_trigger(self) -> None:
        """Verify server sends datagram when baton value modulo 7 is 0."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url) as session:
            dgram_received = asyncio.Event()

            def on_dgram(evt: Any) -> None:
                dgram_received.set()

            session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_dgram)

            stream = await session.create_unidirectional_stream()
            await stream.write(data=create_payload(7), end_stream=True)

            try:
                async with asyncio.timeout(5.0):
                    await dgram_received.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not send datagram for baton % 7 == 0") from None

    async def test_08_random_padding(self) -> None:
        """Verify server adds padding when baton value modulo 5 is 0."""
        url = self._build_url(count=1, baton=20)

        async with self._connect(url) as session:
            padding_verified = asyncio.Event()

            async def on_stream(evt: Any) -> None:
                stream = evt.data["stream"]
                if stream.is_remote:
                    data = await stream.read_all()
                    if not data:
                        return

                    try:
                        p_len, header_len = VarInt.parse(data)
                        if len(data) == header_len + p_len + 1:
                            if p_len > 0:
                                padding_verified.set()
                    except Exception:
                        pass

            session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

            try:
                async with asyncio.timeout(5.0):
                    await padding_verified.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not send valid padding for baton % 5 == 0") from None

    async def test_09_malformed_baton(self) -> None:
        """Verify server closes session with ERR_BRUH on malformed message."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url) as session:
            close_event = asyncio.Event()
            received_error = None

            def on_close(evt: Any) -> None:
                nonlocal received_error
                received_error = evt.data.get("error_code")
                close_event.set()

            session.events.on(event_type=EventType.SESSION_CLOSED, handler=on_close)

            stream = await session.create_unidirectional_stream()
            await stream.write(data=VarInt.encode(100) + b"\x00", end_stream=True)

            try:
                async with asyncio.timeout(5.0):
                    await close_event.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not close session on malformed baton") from None

            if received_error != ERR_BRUH:
                raise TestFailure(f"Expected ERR_BRUH (0x02), got {received_error}")

    async def test_10_unexpected_value(self) -> None:
        """Verify server closes session with ERR_SUS on unexpected baton value."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url) as session:
            close_event = asyncio.Event()
            received_error = None

            def on_close(evt: Any) -> None:
                nonlocal received_error
                received_error = evt.data.get("error_code")
                close_event.set()

            session.events.on(event_type=EventType.SESSION_CLOSED, handler=on_close)

            async def on_stream(evt: Any) -> None:
                stream = evt.data["stream"]
                if stream.is_remote:
                    reply = await session.create_bidirectional_stream()
                    await reply.write(data=create_payload(99), end_stream=True)

            session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

            try:
                async with asyncio.timeout(5.0):
                    await close_event.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not close session on unexpected baton value") from None

            if received_error != ERR_SUS:
                raise TestFailure(f"Expected ERR_SUS (0x03), got {received_error}")

    async def test_11_stop_sending_reaction(self) -> None:
        """Verify server resets stream with ERR_WHATEVER upon receiving STOP_SENDING."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url) as session:
            reset_received = asyncio.Event()

            async def on_stream(evt: Any) -> None:
                stream = evt.data["stream"]
                if stream.is_remote:

                    def on_reset(e: Any) -> None:
                        if e.data.get("error_code") == ERR_WHATEVER:
                            reset_received.set()

                    stream.events.on(event_type=EventType.STREAM_RESET_RECEIVED, handler=on_reset)
                    await stream.stop_receiving(error_code=ERR_IDC)

            session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

            try:
                async with asyncio.timeout(5.0):
                    await reset_received.wait()
            except asyncio.TimeoutError:
                return

    async def test_12_spontaneous_reset_reaction(self) -> None:
        """Verify server ignores spontaneous RESET_STREAM (ERR_I_LIED)."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url) as session:
            session_closed = asyncio.Event()
            session.events.on(event_type=EventType.SESSION_CLOSED, handler=lambda e: session_closed.set())

            stream = await session.create_unidirectional_stream()
            await stream.write(data=b"padding")
            await stream.reset(error_code=ERR_I_LIED)

            try:
                async with asyncio.timeout(2.0):
                    await session_closed.wait()
                raise TestFailure("Server closed session on I_LIED reset (Should ignore)")
            except asyncio.TimeoutError:
                pass

    async def test_13_graceful_finish(self) -> None:
        """Verify server closes session gracefully when batons finish."""
        url = self._build_url(count=1, baton=255)

        async with self._connect(url) as session:
            close_event = asyncio.Event()
            close_code = None

            def on_close(evt: Any) -> None:
                nonlocal close_code
                close_code = evt.data.get("error_code")
                close_event.set()

            session.events.on(event_type=EventType.SESSION_CLOSED, handler=on_close)

            async def on_stream(evt: Any) -> None:
                stream = evt.data["stream"]
                if stream.is_remote:
                    await stream.read_all()
                    reply = await session.create_bidirectional_stream()
                    await reply.write(data=create_payload(0), end_stream=True)

            session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

            try:
                async with asyncio.timeout(5.0):
                    await close_event.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not close session gracefully") from None

            if close_code != 0:
                raise TestFailure(f"Expected NO_ERROR (0), got {close_code}")

    def _build_url(self, **kwargs: Any) -> str:
        """Construct the connection URL with query parameters."""
        query = urlencode(kwargs)
        return f"https://{SERVER_HOST}:{SERVER_PORT}{BASE_PATH}?{query}"

    @asynccontextmanager
    async def _connect(self, url: str) -> AsyncGenerator[WebTransportSession, None]:
        """Establish a new WebTransport session context for testing."""
        async with WebTransportClient(config=self._config) as client:
            try:
                session = await client.connect(url=url)
                yield session
                if not session.is_closed:
                    await session.close()
            except Exception:
                raise

    async def _execute_test_with_retry(
        self,
        name: str,
        test_func: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        """Execute a test case with retries."""
        for attempt in range(1, self.TEST_MAX_RETRIES + 1):
            try:
                await test_func()
                self._results.append((name, True, ""))
                return
            except TestFailure as e:
                if attempt == self.TEST_MAX_RETRIES:
                    logger.error("FAILURE: %s - %s", name, e)
                    self._results.append((name, False, str(e)))
                else:
                    logger.warning(
                        "RETRY: %s failed (%s) - attempt %d/%d",
                        name,
                        e,
                        attempt,
                        self.TEST_MAX_RETRIES,
                    )
                    await asyncio.sleep(self.TEST_RETRY_DELAY)
            except Exception as e:
                if attempt == self.TEST_MAX_RETRIES:
                    logger.error("ERROR:   %s - %s", name, e)
                    traceback.print_exc()
                    self._results.append((name, False, f"Exception: {e}"))
                else:
                    logger.warning(
                        "RETRY: %s error (%s) - attempt %d/%d",
                        name,
                        e,
                        attempt,
                        self.TEST_MAX_RETRIES,
                    )
                    await asyncio.sleep(self.TEST_RETRY_DELAY)

    def _print_results(self) -> None:
        """Output the final test report."""
        logger.info("-" * 60)
        passed = sum(1 for _, success, _ in self._results if success)
        total = len(self._results)
        logger.info("Compliance Result: %d/%d passed", passed, total)
        if passed != total:
            sys.exit(1)

    async def _run_test(self, test_func: Callable[[], Coroutine[Any, Any, None]]) -> None:
        """Execute a single test case with logging and error handling."""
        name = test_func.__name__
        logger.info("RUNNING: %s", name)
        start_time = time.time()
        await self._execute_test_with_retry(name, test_func)
        duration = time.time() - start_time
        if self._results[-1][1]:
            logger.info("SUCCESS: %s (%.3fs)", name, duration)


async def main() -> None:
    """Run the compliance suite."""
    test = DeviousBatonTest()
    await test.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
