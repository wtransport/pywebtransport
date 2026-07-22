"""E2E test for WebTransport datagrams."""

import asyncio
import logging
import ssl
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, ConnectionError, DatagramError, Event, TimeoutError, WebTransportClient
from pywebtransport.types import EventType
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(level=logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_datagrams")


async def test_basic_datagram() -> bool:
    """Test sending a single datagram and receiving its echo."""
    logger.info("Test 05A: Basic Datagram Echo")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Session ready for datagrams")

            test_message = b"Hello, Datagram!"
            expected_response = b"ECHO: " + test_message

            logger.info("Sending datagram: %r", test_message)

            wait_task = asyncio.create_task(
                coro=session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED, timeout=5.0)
            )

            await session.send_datagram(data=test_message)

            logger.info("Waiting for echo...")
            event: Event = await wait_task

            response = None
            if isinstance(event.data, dict):
                response = event.data.get("data")

            if response == expected_response:
                logger.info("SUCCESS: Received correct datagram echo")
                return True
            else:
                logger.error("FAILURE: Datagram echo mismatch. Got: %r", response)
                return False
    except (ConnectionError, TimeoutError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_multiple_datagrams() -> bool:
    """Test sending multiple datagrams and receiving all echoes."""
    logger.info("Test 05B: Multiple Datagrams")
    logger.info("-" * 50)

    num_datagrams = 10
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Sending %d datagrams and awaiting echoes...", num_datagrams)

            received_events: list[bytes] = []
            receiver_done = asyncio.Event()

            async def datagram_handler(event: Event) -> None:
                data = None
                if isinstance(event.data, dict):
                    data = event.data.get("data")

                if isinstance(data, bytes):
                    received_events.append(data)
                    if len(received_events) >= num_datagrams:
                        receiver_done.set()

            session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=datagram_handler)

            for i in range(num_datagrams):
                await session.send_datagram(data=f"Datagram message {i + 1}".encode())

            try:
                async with asyncio.timeout(delay=5.0):
                    await receiver_done.wait()
            except asyncio.TimeoutError:
                logger.warning("Receiver timed out")
            finally:
                session.events.off(event_type=EventType.DATAGRAM_RECEIVED, handler=datagram_handler)

            if len(received_events) != num_datagrams:
                logger.error(
                    "FAILURE: Expected %d datagrams, but received %d",
                    num_datagrams,
                    len(received_events),
                )
                return False

            for i, data in enumerate(received_events):
                expected = f"ECHO: Datagram message {i + 1}".encode()
                if data != expected:
                    logger.error("FAILURE: Datagram %d mismatch. Got: %r", i + 1, data)
                    return False

            logger.info("SUCCESS: Received %d correct datagram echoes", num_datagrams)
            return True

    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_datagram_sizes() -> bool:
    """Test that sending an oversized datagram raises DatagramError."""
    logger.info("Test 05C: Datagram Size Limits")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)

            connection = session._connection()
            if not connection:
                logger.error("FAILURE: Connection lost unexpectedly")
                return False

            diags = await connection.diagnostics()
            remote_max = diags.peer_max_datagram_frame_size
            logger.info("Max datagram size from diagnostics: remote=%d", remote_max)

            if remote_max is None:
                logger.error(
                    "FAILURE: Remote max datagram size is None (not negotiated), cannot test oversized datagram"
                )
                return False

            logger.info("Testing oversized datagram...")
            try:
                oversized_data = b"X" * (remote_max + 1)
                await session.send_datagram(data=oversized_data)
                logger.error("FAILURE: Sending oversized datagram should have raised an exception")
                return False
            except DatagramError as e:
                logger.info("SUCCESS: Oversized datagram correctly raised DatagramError: %s", e)
                return True
            except Exception as e:
                logger.error("FAILURE: Unexpected exception type for oversized datagram: %s (%s)", type(e).__name__, e)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_datagram_burst() -> bool:
    """Test sending a burst of datagrams in rapid succession."""
    logger.info("Test 05D: Datagram Burst")
    logger.info("-" * 50)

    burst_size = 50
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Starting burst of %d datagrams...", burst_size)
            start_time = time.time()

            tasks = [session.send_datagram(data=f"Burst {i}".encode()) for i in range(burst_size)]
            await asyncio.gather(*tasks)
            duration = time.time() - start_time
            rate = burst_size / duration if duration > 0 else float("inf")

            logger.info("SUCCESS: Sent %d datagrams in %.3fs (%.1f dgrams/s)", burst_size, duration, rate)
            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the datagram tests."""
    logger.info("Starting Test 05: Datagrams")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Basic Datagram Echo", test_basic_datagram),
        ("Multiple Datagrams", test_multiple_datagrams),
        ("Datagram Size Limits", test_datagram_sizes),
        ("Datagram Burst", test_datagram_burst),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        logger.info("")
        try:
            if await test_func():
                logger.info("%s: PASSED", test_name)
                passed += 1
            else:
                logger.error("%s: FAILED", test_name)
        except Exception as e:
            logger.error("%s: CRASHED - %s", test_name, e, exc_info=True)
        await asyncio.sleep(delay=1)

    logger.info("")
    logger.info("=" * 50)
    logger.info("Test 05 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 05 PASSED: All tests successful")
        return 0
    else:
        logger.error("TEST 05 FAILED: Some tests failed")
        return 1


if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("\nTest interrupted by user")
        exit_code = 130
    except Exception as e:
        logger.critical("Test suite crashed with an unhandled exception: %s", e, exc_info=True)
    finally:
        sys.exit(exit_code)
