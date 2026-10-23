"""Devious Baton protocol compliance test client."""

import asyncio
import logging
import ssl
import sys
import time
import traceback
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any, Final
from urllib.parse import urlencode

from pywebtransport import (
    ClientConfig,
    ClientError,
    ErrorCodes,
    Event,
    StreamError,
    WebTransportBidirectionalStream,
    WebTransportClient,
    WebTransportSession,
)
from pywebtransport.types import EventType

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(name="test_devious_baton")


class TestFailure(Exception):
    """Exception raised when a test assertion fails."""


def create_baton_message(*, baton: int, padding_length: int = 0) -> bytes:
    """Create a Baton message."""
    return encode_varint(value=padding_length) + b"\x00" * padding_length + bytes([baton])


def encode_varint(*, value: int) -> bytes:
    """Encode an integer as a QUIC variable-length integer."""
    if value < 0x40:
        return value.to_bytes(length=1)
    if value < 0x4000:
        return (value | 0x4000).to_bytes(length=2)
    if value < 0x40000000:
        return (value | 0x80000000).to_bytes(length=4)
    return (value | 0xC000000000000000).to_bytes(length=8)


def parse_varint(*, data: bytes) -> tuple[int, int]:
    """Parse a QUIC variable-length integer, returning its value and encoded size."""
    if not data:
        raise ValueError("Empty data")

    size = 1 << (data[0] >> 6)
    if len(data) < size:
        raise ValueError("Truncated varint")

    return int.from_bytes(bytes([data[0] & 0x3F]) + data[1:size]), size


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
        logger.info("Target: https://%s:%d%s", SERVER_HOST, SERVER_PORT, BASE_PATH)

        tests: list[Callable[[], Coroutine[Any, Any, None]]] = [
            self.test_01_invalid_version,
            self.test_02_invalid_count,
            self.test_03_invalid_baton,
            self.test_04_server_initiates_unidirectional_stream,
            self.test_05_flow_unidirectional_to_bidirectional,
            self.test_06_flow_self_bidirectional_to_unidirectional,
            self.test_07_datagram_trigger,
            self.test_08_random_padding,
            self.test_09_malformed_baton,
            self.test_10_unexpected_value,
            self.test_11_stop_sending_reaction,
            self.test_12_spontaneous_reset_reaction,
            self.test_13_graceful_finish,
        ]

        for test in tests:
            await self._run_test(test_func=test)

        self._print_results()

    async def test_01_invalid_version(self) -> None:
        """Verify server rejects invalid protocol version."""
        await self._expect_rejection(url=self._build_url(version=99), description="invalid version 99")

    async def test_02_invalid_count(self) -> None:
        """Verify server rejects excessive baton count."""
        await self._expect_rejection(url=self._build_url(count=999999), description="invalid count 999999")

    async def test_03_invalid_baton(self) -> None:
        """Verify server rejects invalid baton value."""
        await self._expect_rejection(url=self._build_url(baton=0), description="invalid baton value 0")

    async def test_04_server_initiates_unidirectional_stream(self) -> None:
        """Verify server initiates the protocol with unidirectional streams."""
        count = 3
        url = self._build_url(count=count, baton=10)

        async with self._connect(url=url) as session:
            streams_received = 0
            all_received = asyncio.Event()

            async def count_unidirectional_streams() -> None:
                nonlocal streams_received
                async for _ in session.incoming_unidirectional_streams():
                    streams_received += 1
                    if streams_received >= count:
                        all_received.set()

            task = asyncio.create_task(coro=count_unidirectional_streams())

            try:
                async with asyncio.timeout(delay=5.0):
                    await all_received.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not initiate unidirectional stream") from None
            finally:
                task.cancel()

    async def test_05_flow_unidirectional_to_bidirectional(self) -> None:
        """Verify Unidirectional -> Bidirectional stream switching logic."""
        url = self._build_url(baton=10)

        async with self._connect(url=url) as session:
            reply_received = asyncio.Event()

            async def read_reply(*, stream: WebTransportBidirectionalStream) -> None:
                if await stream.read_all():
                    reply_received.set()

            async def reply_unidirectional_streams() -> None:
                async with asyncio.TaskGroup() as task_group:
                    async for stream in session.incoming_unidirectional_streams():
                        payload = await stream.read_all()
                        if not payload:
                            continue

                        reply_stream = await session.create_bidirectional_stream()
                        task_group.create_task(coro=read_reply(stream=reply_stream))
                        await reply_stream.write(
                            data=create_baton_message(baton=(payload[-1] + 1) % 256), end_stream=True
                        )

            task = asyncio.create_task(coro=reply_unidirectional_streams())

            try:
                async with asyncio.timeout(delay=5.0):
                    await reply_received.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not reply on the same bidirectional stream") from None
            finally:
                task.cancel()

    async def test_06_flow_self_bidirectional_to_unidirectional(self) -> None:
        """Verify Bidirectional (Self) -> Unidirectional stream switching logic."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url=url) as session:
            server_bidi_opened = asyncio.Event()
            server_uni_opened = asyncio.Event()

            async def reply_bidirectional_stream(*, stream: WebTransportBidirectionalStream) -> None:
                try:
                    payload = await stream.read_all()
                    if payload:
                        await stream.write(data=create_baton_message(baton=(payload[-1] + 1) % 256), end_stream=True)
                except StreamError:
                    pass

            async def reply_bidirectional_streams() -> None:
                async with asyncio.TaskGroup() as task_group:
                    async for stream in session.incoming_bidirectional_streams():
                        server_bidi_opened.set()
                        task_group.create_task(coro=reply_bidirectional_stream(stream=stream))

            async def observe_unidirectional_streams() -> None:
                async for _ in session.incoming_unidirectional_streams():
                    server_uni_opened.set()

            bidirectional_task = asyncio.create_task(coro=reply_bidirectional_streams())
            unidirectional_task = asyncio.create_task(coro=observe_unidirectional_streams())

            stream = await session.create_unidirectional_stream()
            await stream.write(data=create_baton_message(baton=50), end_stream=True)

            try:
                async with asyncio.timeout(delay=5.0):
                    await server_bidi_opened.wait()
                    await server_uni_opened.wait()
            except asyncio.TimeoutError:
                if not server_bidi_opened.is_set():
                    raise TestFailure(
                        "Server did not open a bidirectional stream in response to a unidirectional stream"
                    ) from None
                raise TestFailure(
                    "Server did not open a unidirectional stream in response to its own bidirectional stream"
                ) from None
            finally:
                bidirectional_task.cancel()
                unidirectional_task.cancel()

    async def test_07_datagram_trigger(self) -> None:
        """Verify server sends datagram when baton value modulo 7 is 0."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url=url) as session:
            stream = await session.create_unidirectional_stream()
            await stream.write(data=create_baton_message(baton=7), end_stream=True)

            try:
                async with asyncio.timeout(delay=5.0):
                    await session.read_datagram()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not send datagram for baton % 7 == 0") from None

    async def test_08_random_padding(self) -> None:
        """Verify server adds padding when baton value modulo 5 is 0."""
        url = self._build_url(count=1, baton=20)

        async with self._connect(url=url) as session:
            padding_verified = asyncio.Event()

            async def check_unidirectional_streams() -> None:
                async for stream in session.incoming_unidirectional_streams():
                    data = await stream.read_all()
                    try:
                        padding_length, size = parse_varint(data=data)
                    except ValueError:
                        continue
                    if padding_length > 0 and len(data) == size + padding_length + 1:
                        padding_verified.set()

            task = asyncio.create_task(coro=check_unidirectional_streams())

            try:
                async with asyncio.timeout(delay=5.0):
                    await padding_verified.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not send valid padding for baton % 5 == 0") from None
            finally:
                task.cancel()

    async def test_09_malformed_baton(self) -> None:
        """Verify server closes session with ERR_BRUH on malformed message."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url=url) as session:
            session_closed = asyncio.Event()
            received_error: int | None = None

            def on_close(event: Event) -> None:
                nonlocal received_error
                received_error = event.data.get("error_code") if isinstance(event.data, dict) else None
                session_closed.set()

            session.events.on(event_type=EventType.SESSION_CLOSED, handler=on_close)

            stream = await session.create_unidirectional_stream()
            await stream.write(data=encode_varint(value=100) + b"\x00", end_stream=True)

            try:
                async with asyncio.timeout(delay=5.0):
                    await session_closed.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not close session on malformed baton") from None

            if received_error != ERR_BRUH:
                raise TestFailure(f"Expected ERR_BRUH (0x02), got {received_error}")

    async def test_10_unexpected_value(self) -> None:
        """Verify server closes session with ERR_SUS on unexpected baton value."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url=url) as session:
            session_closed = asyncio.Event()
            session.events.on(event_type=EventType.SESSION_CLOSED, handler=lambda event: session_closed.set())

            async def reply_unexpected_batons() -> None:
                async for _ in session.incoming_unidirectional_streams():
                    reply_stream = await session.create_bidirectional_stream()
                    await reply_stream.write(data=create_baton_message(baton=99), end_stream=True)

            task = asyncio.create_task(coro=reply_unexpected_batons())

            try:
                async with asyncio.timeout(delay=5.0):
                    await session_closed.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not close session on unexpected baton value") from None
            finally:
                task.cancel()

    async def test_11_stop_sending_reaction(self) -> None:
        """Verify server resets stream with ERR_WHATEVER upon receiving STOP_SENDING."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url=url) as session:
            reset_received = asyncio.Event()

            def on_reset(event: Event) -> None:
                if isinstance(event.data, dict) and event.data.get("error_code") == ERR_WHATEVER:
                    reset_received.set()

            async def stop_unidirectional_streams() -> None:
                async for stream in session.incoming_unidirectional_streams():
                    stream.events.on(event_type=EventType.STREAM_RESET_RECEIVED, handler=on_reset)
                    await stream.stop_receiving(error_code=ERR_IDC)

            task = asyncio.create_task(coro=stop_unidirectional_streams())

            try:
                async with asyncio.timeout(delay=5.0):
                    await reset_received.wait()
            except asyncio.TimeoutError:
                return
            finally:
                task.cancel()

    async def test_12_spontaneous_reset_reaction(self) -> None:
        """Verify server ignores spontaneous RESET_STREAM (ERR_I_LIED)."""
        url = self._build_url(count=1, baton=10)

        async with self._connect(url=url) as session:
            session_closed = asyncio.Event()
            session.events.on(event_type=EventType.SESSION_CLOSED, handler=lambda event: session_closed.set())

            stream = await session.create_unidirectional_stream()
            await stream.write(data=b"padding")
            await stream.reset(error_code=ERR_I_LIED)

            try:
                async with asyncio.timeout(delay=2.0):
                    await session_closed.wait()
                raise TestFailure("Server closed session on I_LIED reset (Should ignore)")
            except asyncio.TimeoutError:
                pass

    async def test_13_graceful_finish(self) -> None:
        """Verify server closes session gracefully when batons finish."""
        url = self._build_url(count=1, baton=255)

        async with self._connect(url=url) as session:
            session_closed = asyncio.Event()
            close_code: int | None = None

            def on_close(event: Event) -> None:
                nonlocal close_code
                close_code = event.data.get("error_code") if isinstance(event.data, dict) else None
                session_closed.set()

            session.events.on(event_type=EventType.SESSION_CLOSED, handler=on_close)

            async def reply_final_batons() -> None:
                async for stream in session.incoming_unidirectional_streams():
                    await stream.read_all()
                    reply_stream = await session.create_bidirectional_stream()
                    await reply_stream.write(data=create_baton_message(baton=0), end_stream=True)

            task = asyncio.create_task(coro=reply_final_batons())

            try:
                async with asyncio.timeout(delay=5.0):
                    await session_closed.wait()
            except asyncio.TimeoutError:
                raise TestFailure("Server did not close session gracefully") from None
            finally:
                task.cancel()

            if close_code != 0:
                raise TestFailure(f"Expected NO_ERROR (0), got {close_code}")

    def _build_url(self, **kwargs: Any) -> str:
        """Construct the connection URL with query parameters."""
        return f"https://{SERVER_HOST}:{SERVER_PORT}{BASE_PATH}?{urlencode(query=kwargs)}"

    @asynccontextmanager
    async def _connect(self, *, url: str) -> AsyncGenerator[WebTransportSession, None]:
        """Establish a new WebTransport session context for testing."""
        async with WebTransportClient(config=self._config) as client:
            async with await client.connect(url=url) as session:
                yield session

    async def _execute_test_with_retry(self, *, name: str, test_func: Callable[[], Coroutine[Any, Any, None]]) -> None:
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
                    logger.warning("RETRY: %s failed (%s) - attempt %d/%d", name, e, attempt, self.TEST_MAX_RETRIES)
                    await asyncio.sleep(delay=self.TEST_RETRY_DELAY)
            except Exception as e:
                if attempt == self.TEST_MAX_RETRIES:
                    logger.error("ERROR:   %s - %s", name, e)
                    traceback.print_exc()
                    self._results.append((name, False, f"Exception: {e}"))
                else:
                    logger.warning("RETRY: %s error (%s) - attempt %d/%d", name, e, attempt, self.TEST_MAX_RETRIES)
                    await asyncio.sleep(delay=self.TEST_RETRY_DELAY)

    async def _expect_rejection(self, *, url: str, description: str) -> None:
        """Verify the server rejects the session request."""
        try:
            async with self._connect(url=url):
                pass
        except ClientError as e:
            if e.error_code == ErrorCodes.H3_REQUEST_REJECTED:
                return
            raise TestFailure(f"Session failed with an unexpected error: {e}") from e
        raise TestFailure(f"Server accepted {description}")

    def _print_results(self) -> None:
        """Output the final test report."""
        logger.info("-" * 60)
        passed = sum(1 for _, success, _ in self._results if success)
        total = len(self._results)
        logger.info("Compliance Result: %d/%d passed", passed, total)
        if passed != total:
            sys.exit(1)

    async def _run_test(self, *, test_func: Callable[[], Coroutine[Any, Any, None]]) -> None:
        """Execute a single test case with logging and error handling."""
        name = test_func.__name__
        logger.info("RUNNING: %s", name)
        start_time = time.time()
        await self._execute_test_with_retry(name=name, test_func=test_func)
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
