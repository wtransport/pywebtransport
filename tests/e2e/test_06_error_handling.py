"""E2E test for WebTransport error handling and edge cases."""

import asyncio
import logging
import ssl
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import (
    ClientConfig,
    ClientError,
    ConnectionError,
    SessionError,
    StreamError,
    TimeoutError,
    WebTransportClient,
)
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_error_handling")


async def test_connection_timeout() -> bool:
    """Test the handling of a connection timeout to an unreachable port."""
    logger.info("--- Test 06A: Connection Timeout ---")
    unreachable_url = f"https://{SERVER_HOST}:9999/"
    config = ClientConfig(verify_mode=ssl.CERT_NONE, connect_timeout=2.0)

    logger.info("Attempting connection to unreachable server: %s", unreachable_url)
    start_time = time.time()
    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=unreachable_url)
        logger.error("FAILURE: Connection should have failed but it succeeded.")
        return False
    except (TimeoutError, ConnectionError, ClientError):
        duration = time.time() - start_time
        logger.info("SUCCESS: Connection correctly failed after %.1fs.", duration)
        return True
    except Exception as e:
        logger.error("FAILURE: An unexpected exception was caught: %s", type(e).__name__, exc_info=True)
        return False


async def test_invalid_server_address() -> bool:
    """Test handling of various invalid server addresses."""
    logger.info("--- Test 06B: Invalid Server Address ---")
    invalid_urls = ["https://invalid-hostname-for-testing.local/", "http://127.0.0.1:4433/"]
    config = ClientConfig(verify_mode=ssl.CERT_NONE, connect_timeout=2.0)

    try:
        async with WebTransportClient(config=config) as client:
            for i, invalid_url in enumerate(invalid_urls):
                logger.info("Testing invalid URL %d: %s", i + 1, invalid_url)
                try:
                    await client.connect(url=invalid_url)
                    logger.error("FAILURE: Connection to %s should have failed.", invalid_url)
                    return False
                except Exception:
                    logger.info("   - SUCCESS: Connection to %s correctly failed.", invalid_url)
            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred in the test setup: %s", e, exc_info=True)
        return False


async def test_stream_errors() -> bool:
    """Test error handling for various stream operations."""
    logger.info("--- Test 06C: Stream Error Handling ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            stream = await session.create_bidirectional_stream()
            await stream.close()
            logger.info("Stream closed for testing subsequent operations.")

            logger.info("Testing write operation on a closed stream...")
            try:
                await stream.write(data=b"This should fail")
                logger.error("FAILURE: Write on a closed stream should have failed.")
                return False
            except StreamError:
                logger.info("   - SUCCESS: Write on a closed stream correctly failed.")
            except Exception as e:
                logger.error("   - FAILURE: Unexpected error on write: %s", e)
                return False

            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_read_timeout() -> bool:
    """Test the handling of a stream read timeout."""
    logger.info("--- Test 06D: Stream Read Timeout ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            stream = await session.create_bidirectional_stream()
            logger.info("Attempting to read from a stream with no data (should time out)...")
            start_time = time.time()
            try:
                async with asyncio.timeout(delay=1.0):
                    await stream.read(max_bytes=1024)
                logger.error("FAILURE: Read operation should have timed out.")
                return False
            except asyncio.TimeoutError:
                duration = time.time() - start_time
                logger.info("SUCCESS: Read correctly timed out after %.1fs.", duration)
                await stream.close()
                return True
            except Exception as e:
                logger.error("FAILURE: Unexpected exception during read: %s", e)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_session_closure_handling() -> bool:
    """Test that operations on a closed session correctly raise errors."""
    logger.info("--- Test 06E: Operations on Closed Session ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Connected, session ID: %d", session.session_id)
            await session.close()
            logger.info("Session closed.")

            logger.info("Testing stream creation on closed session...")
            try:
                await session.create_bidirectional_stream()
                logger.error("FAILURE: Stream creation on closed session should have failed.")
                return False
            except SessionError:
                logger.info("   - SUCCESS: Stream creation correctly failed.")
            except Exception as e:
                logger.error("   - FAILURE: Unexpected error: %s", e)
                return False

            logger.info("Testing datagram send on closed session...")
            try:
                await session.send_datagram(data=b"This should fail")
                logger.error("FAILURE: Datagram send on closed session should have failed.")
                return False
            except SessionError:
                logger.info("   - SUCCESS: Datagram send correctly failed.")
            except Exception as e:
                logger.error("   - FAILURE: Unexpected error: %s", e)
                return False

            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_malformed_operations() -> bool:
    """Test handling of malformed API operations."""
    logger.info("--- Test 06F: Malformed Operations ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            stream = await session.create_bidirectional_stream()

            logger.info("Testing invalid write data (None)...")
            try:
                await stream.write(data=None)  # type: ignore
                logger.error("FAILURE: Writing None should have failed.")
                return False
            except StreamError:
                logger.info("   - SUCCESS: Writing None correctly failed with StreamError.")
            except Exception as e:
                logger.error("   - FAILURE: Unexpected error for None write: %s", e)
                return False

            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the error handling test suite."""
    logger.info("--- Starting Test 06: Error Handling ---")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Connection Timeout", test_connection_timeout),
        ("Invalid Server Address", test_invalid_server_address),
        ("Stream Error Handling", test_stream_errors),
        ("Read Timeout", test_read_timeout),
        ("Operations on Closed Session", test_session_closure_handling),
        ("Malformed Operations", test_malformed_operations),
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
    logger.info("=" * 60)
    logger.info("Test 06 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 06 PASSED: All error handling tests successful!")
        return 0
    else:
        logger.error("TEST 06 FAILED: Some error handling tests failed!")
        return 1


if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("\nTest interrupted by user.")
        exit_code = 130
    except Exception as e:
        logger.critical("Test suite crashed with an unhandled exception: %s", e, exc_info=True)
    finally:
        sys.exit(exit_code)
