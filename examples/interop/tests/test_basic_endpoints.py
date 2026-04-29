"""Core WebTransport endpoints compliance test client."""

import asyncio
import json
import logging
import ssl
import sys
import time
import traceback
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Any, AsyncGenerator, Callable, Coroutine, Final

from pywebtransport import ClientConfig, WebTransportClient, WebTransportSession
from pywebtransport.server import ServerDiagnostics
from pywebtransport.session import SessionDiagnostics
from pywebtransport.types import EventType

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
TIMEOUT: Final[float] = 15.0

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
)
logger = logging.getLogger(name="test_basic_endpoints")


class TestFailure(Exception):
    """Exception raised when a test assertion fails."""


class BasicEndpointsTest:
    """Basic endpoints compliance test runner."""

    TEST_MAX_RETRIES: Final[int] = 3
    TEST_RETRY_DELAY: Final[float] = 1.0

    def __init__(self) -> None:
        """Initialize the test runner."""
        self._config = ClientConfig(verify_mode=ssl.CERT_NONE)
        self._results: list[tuple[str, bool, str]] = []

    async def run(self) -> None:
        """Execute all defined compliance tests."""
        logger.info("Starting Basic Endpoints tests")
        logger.info("Target: https://%s:%d", SERVER_HOST, SERVER_PORT)

        tests: list[Callable[[], Coroutine[Any, Any, None]]] = [
            self.test_01_echo_endpoint,
            self.test_02_status_endpoint,
            self.test_03_stats_endpoint,
        ]

        for test in tests:
            await self._run_test(test_func=test)

        self._print_results()

    async def test_01_echo_endpoint(self) -> None:
        """Test /echo: Bidirectional stream and datagram echo."""
        url = self._build_url(path="/echo")
        async with self._connect(url=url) as session:
            payload = b"Hello, WebTransport!"
            stream = await session.create_bidirectional_stream()
            await stream.write(data=payload, end_stream=True)
            response = await stream.read_all()
            await stream.close()

            if response != payload:
                raise TestFailure(f"Stream Echo mismatch. Sent: {payload!r}, Recv: {response!r}")
            logger.info("[PASS] Stream Echo verified")

            dgram_payload = b"Datagram Test"
            loop = asyncio.get_running_loop()
            recv_future = loop.create_future()

            async def on_dgram(evt: Any) -> None:
                if isinstance(evt.data, dict) and (data := evt.data.get("data")):
                    if not recv_future.done():
                        recv_future.set_result(data)

            session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_dgram)
            await session.send_datagram(data=dgram_payload)

            try:
                async with asyncio.timeout(delay=TIMEOUT):
                    received_dgram = await recv_future
            except asyncio.TimeoutError:
                raise TestFailure("Server did not echo datagram") from None
            finally:
                session.events.off(event_type=EventType.DATAGRAM_RECEIVED, handler=on_dgram)

            if received_dgram != dgram_payload:
                raise TestFailure(f"Datagram Echo mismatch. Sent: {dgram_payload!r}, Recv: {received_dgram!r}")
            logger.info("[PASS] Datagram Echo verified")

    async def test_02_status_endpoint(self) -> None:
        """Test /status: Request server diagnostics."""
        url = self._build_url(path="/status")
        async with self._connect(url=url) as session:
            stream = await session.create_bidirectional_stream()
            await stream.write(data=b"", end_stream=True)
            data = await stream.read_all()

            try:
                diag_dict = json.loads(data.decode("utf-8"))
                diagnostics = ServerDiagnostics(**diag_dict)
            except Exception as e:
                raise TestFailure(f"Failed to parse server diagnostics: {e}") from e

            if not isinstance(diagnostics, ServerDiagnostics):
                raise TestFailure(f"Invalid type: {type(diagnostics)}")

            pretty_json = json.dumps(obj=asdict(obj=diagnostics), indent=2, default=str)
            logger.info("[PASS] Server Status Received:\n%s", pretty_json)

    async def test_03_stats_endpoint(self) -> None:
        """Test /stats: Request session diagnostics."""
        url = self._build_url(path="/stats")
        async with self._connect(url=url) as session:
            stream = await session.create_bidirectional_stream()
            await stream.write(data=b"", end_stream=True)
            data = await stream.read_all()

            try:
                stats_dict = json.loads(data.decode("utf-8"))
                stats = SessionDiagnostics(**stats_dict)
            except Exception as e:
                raise TestFailure(f"Failed to parse session diagnostics: {e}") from e

            if not isinstance(stats, SessionDiagnostics):
                raise TestFailure(f"Invalid type: {type(stats)}")

            pretty_json = json.dumps(obj=asdict(obj=stats), indent=2, default=str)
            logger.info("[PASS] Session Stats Received:\n%s", pretty_json)

    def _build_url(self, path: str) -> str:
        """Construct the connection URL."""
        return f"https://{SERVER_HOST}:{SERVER_PORT}{path}"

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

    async def _execute_test_with_retry(self, name: str, test_func: Callable[[], Coroutine[Any, Any, None]]) -> None:
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
        await self._execute_test_with_retry(name=name, test_func=test_func)
        duration = time.time() - start_time
        if self._results[-1][1]:
            logger.info("SUCCESS: %s (%.3fs)", name, duration)


async def main() -> None:
    """Run the compliance suite."""
    test = BasicEndpointsTest()
    await test.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
