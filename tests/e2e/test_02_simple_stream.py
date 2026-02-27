"""E2E test for simple bidirectional stream operations."""

import asyncio
import logging
import ssl
import sys
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, ConnectionError, StreamError, TimeoutError, WebTransportClient

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)

logger = logging.getLogger("test_simple_stream")


async def test_stream_creation() -> bool:
    """Test the ability to create and inspect a bidirectional stream."""
    logger.info("Test 02A: Stream Creation")
    logger.info("-" * 30)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            logger.info("Connecting to %s...", SERVER_URL)
            session = await client.connect(url=SERVER_URL)
            logger.info("Connected, session ID: %s", session.session_id)

            logger.info("Creating bidirectional stream...")
            stream = await session.create_bidirectional_stream()

            logger.info("Stream created successfully!")
            logger.info("   - Stream ID: %s", stream.stream_id)

            await stream.close()
            logger.info("Stream closed.")
            return True
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Connection failed: %s", e)
        return False
    except StreamError as e:
        logger.error("FAILURE: Stream creation failed: %s", e, exc_info=True)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_simple_echo() -> bool:
    """Test sending data and receiving an echo on a single stream."""
    logger.info("Test 02B: Simple Echo")
    logger.info("-" * 30)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            stream = await session.create_bidirectional_stream()
            logger.info("Stream %s created for echo test.", stream.stream_id)

            test_message = b"Hello, WebTransport!"
            logger.info("Sending: %r", test_message)
            await stream.write(data=test_message, end_stream=True)

            logger.info("Waiting for echo response...")
            response_data = await stream.read_all()
            logger.info("Received response: %r", response_data)

            expected_response = b"ECHO: " + test_message
            if response_data == expected_response:
                logger.info("SUCCESS: Echo response matches expected content.")
                return True
            else:
                logger.error("FAILURE: Echo response mismatch!")
                logger.error("   - Expected: %r", expected_response)
                logger.error("   - Received: %r", response_data)
                return False
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_multiple_messages() -> bool:
    """Test sending multiple messages, each on a separate stream, within one session."""
    logger.info("Test 02C: Multiple Messages")
    logger.info("-" * 30)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            messages = [b"Message 1", b"Message 2", b"Message 3"]
            success_count = 0

            for i, message in enumerate(messages):
                logger.info("Processing message %d/%d: %r", i + 1, len(messages), message)
                stream = await session.create_bidirectional_stream()
                try:
                    await stream.write(data=message, end_stream=True)
                    response_data = await stream.read_all()
                    expected = b"ECHO: " + message
                    if response_data == expected:
                        logger.info("   - Echo for message %d successful.", i + 1)
                        success_count += 1
                    else:
                        logger.error("   - FAILURE: Echo for message %d mismatch!", i + 1)
                finally:
                    await stream.close()

            if success_count == len(messages):
                logger.info("SUCCESS: All %d messages echoed correctly!", len(messages))
                return True
            else:
                logger.error("FAILURE: Only %d/%d messages were successful.", success_count, len(messages))
                return False
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the simple stream operations test."""
    logger.info("Starting Test 02: Simple Stream Operations")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Stream Creation", test_stream_creation),
        ("Simple Echo", test_simple_echo),
        ("Multiple Messages", test_multiple_messages),
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
        await asyncio.sleep(1)

    logger.info("")
    logger.info("=" * 50)
    logger.info("Test 02 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 02 PASSED: All stream operations successful!")
        return 0
    else:
        logger.error("TEST 02 FAILED: Some stream operations failed!")
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
