"""E2E test for concurrent WebTransport stream handling."""

import asyncio
import logging
import ssl
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import (
    ClientConfig,
    ConnectionError,
    StreamError,
    TimeoutError,
    WebTransportClient,
    WebTransportSession,
)
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(level=logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_concurrent_streams")


async def test_sequential_streams() -> bool:
    """Test creating and using multiple streams sequentially in one session."""
    logger.info("Test 03A: Sequential Multiple Streams")
    logger.info("-" * 50)

    stream_count = 3
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect(url=SERVER_URL) as session:
                logger.info("Connected, session ID: %d", session.session_id)

                for index in range(stream_count):
                    stream_number = index + 1
                    logger.info("Creating and testing stream %d/%d...", stream_number, stream_count)
                    stream = await session.create_bidirectional_stream()
                    test_message = f"Sequential stream {stream_number}".encode()
                    await stream.write_all(data=test_message, end_stream=True)
                    response = await stream.read_all()

                    expected = b"ECHO: " + test_message
                    if response != expected:
                        logger.error("FAILURE: Stream %d echo failed", stream_number)
                        return False
                    logger.info("Stream %d echo successful", stream_number)

                logger.info("SUCCESS: All sequential streams worked correctly")
                return True
    except (ConnectionError, TimeoutError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_concurrent_streams() -> bool:
    """Test handling multiple streams concurrently using asyncio tasks."""
    logger.info("Test 03B: Concurrent Streams")
    logger.info("-" * 50)

    stream_count = 10
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    async def stream_task(*, session: WebTransportSession, task_id: int) -> bool:
        """Define the work for a single concurrent stream test."""
        try:
            stream = await session.create_bidirectional_stream()
            logger.debug("Task %d: Stream created (ID=%d)", task_id, stream.stream_id)
            test_message = f"Concurrent stream {task_id}".encode()
            await stream.write_all(data=test_message, end_stream=True)
            response = await stream.read_all()

            expected = b"ECHO: " + test_message
            if response == expected:
                logger.debug("Task %d: Echo successful", task_id)
                return True
            else:
                logger.error("Task %d: Echo failed", task_id)
                return False
        except Exception as e:
            logger.error("Task %d: Failed with an exception: %s", task_id, e)
            return False

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect(url=SERVER_URL) as session:
                logger.info("Starting %d concurrent stream tasks...", stream_count)
                start_time = time.time()

                tasks = [
                    asyncio.create_task(coro=stream_task(session=session, task_id=index + 1))
                    for index in range(stream_count)
                ]
                results = await asyncio.gather(*tasks)
                duration = time.time() - start_time
                logger.info("All tasks completed in %.2fs", duration)

                success_count = sum(1 for result in results if result is True)
                if success_count == stream_count:
                    logger.info("SUCCESS: All concurrent streams completed successfully")
                    return True
                else:
                    logger.error("FAILURE: %d/%d concurrent streams failed", stream_count - success_count, stream_count)
                    return False
    except (ConnectionError, TimeoutError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_stream_lifecycle() -> bool:
    """Test the full lifecycle management of a single stream."""
    logger.info("Test 03C: Stream Lifecycle Management")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect(url=SERVER_URL) as session:
                stream = await session.create_bidirectional_stream()
                logger.info("Stream created: %d", stream.stream_id)

                await stream.write_all(data=b"Lifecycle test", end_stream=True)
                await stream.read_all()
                logger.info("Data exchanged")

                try:
                    await stream.write(data=b"This should fail")
                    logger.error("FAILURE: Write on a half-closed stream should not succeed")
                    return False
                except StreamError:
                    logger.info("SUCCESS: Write on a half-closed stream correctly failed")
                    return True
                except Exception as e:
                    logger.error("FAILURE: Unexpected error on second write: %s", e)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_stream_stress() -> bool:
    """Perform a stress test by rapidly creating and using streams."""
    logger.info("Test 03D: Stream Stress Test")
    logger.info("-" * 50)

    iteration_count = 20
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect(url=SERVER_URL) as session:
                logger.info("Starting stress test with %d iterations...", iteration_count)

                start_time = time.time()
                for index in range(iteration_count):
                    stream = await session.create_bidirectional_stream()
                    test_message = f"Stress test {index + 1}".encode()
                    await stream.write_all(data=test_message, end_stream=True)
                    response = await stream.read_all()
                    expected = b"ECHO: " + test_message
                    if response != expected:
                        logger.error("FAILURE: Iteration %d echo mismatch", index + 1)
                        return False

                duration = time.time() - start_time
                rate = iteration_count / duration if duration > 0 else float("inf")
                logger.info(
                    "SUCCESS: %d stream operations in %.2fs (%.1f operations/s)", iteration_count, duration, rate
                )
                return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the concurrent streams test."""
    logger.info("Starting Test 03: Concurrent Streams")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Sequential Multiple Streams", test_sequential_streams),
        ("Concurrent Streams", test_concurrent_streams),
        ("Stream Lifecycle Management", test_stream_lifecycle),
        ("Stream Stress Test", test_stream_stress),
    ]
    passed = 0
    total = len(tests)

    for test_name, test_function in tests:
        logger.info("")
        try:
            if await test_function():
                logger.info("%s: PASSED", test_name)
                passed += 1
            else:
                logger.error("%s: FAILED", test_name)
        except Exception as e:
            logger.error("%s: CRASHED - %s", test_name, e, exc_info=True)
        await asyncio.sleep(delay=1)

    logger.info("")
    logger.info("=" * 50)
    logger.info("Test 03 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 03 PASSED: All tests successful")
        return 0
    else:
        logger.error("TEST 03 FAILED: Some tests failed")
        return 1


if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = asyncio.run(main=main())
    except KeyboardInterrupt:
        logger.warning("\nTest interrupted by user")
        exit_code = 130
    except Exception as e:
        logger.critical("Test suite crashed with an unhandled exception: %s", e, exc_info=True)
    finally:
        sys.exit(exit_code)
