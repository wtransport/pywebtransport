"""E2E test for WebTransport diagnostics."""

import asyncio
import logging
import ssl
import sys
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import ConnectionState, SessionState, StreamDirection, StreamReceiveState, StreamSendState
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_diagnostics")


async def test_connection_diagnostics() -> bool:
    """Test the retrieval and correctness of connection-level diagnostics."""
    logger.info("Test 07A: Connection Diagnostics")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect(url=SERVER_URL) as session:
                connection = session.connection
                diagnostics = await connection.diagnose()
                logger.info("Connection diagnostics retrieved")
                logger.info("   - Connection Handle: %d", diagnostics.connection_handle)
                logger.info("   - State: %s", diagnostics.state)

                if (
                    diagnostics.connection_handle == connection.handle
                    and diagnostics.state == ConnectionState.CONNECTED
                    and diagnostics.is_client
                    and diagnostics.handshake_completed
                    and diagnostics.peer_settings_received
                    and diagnostics.connected_at is not None
                    and diagnostics.closed_at is None
                ):
                    logger.info("SUCCESS: Connection diagnostics appear correct")
                    return True
                else:
                    logger.error("FAILURE: Connection diagnostics mismatch: %s", diagnostics)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_session_diagnostics() -> bool:
    """Test the retrieval and correctness of session-level diagnostics."""
    logger.info("Test 07B: Session Diagnostics")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect(url=SERVER_URL) as session:
                logger.info("Performing operations to generate diagnostics...")

                total_bytes_written = 0
                for i in range(3):
                    data = f"Diagnostics test {i + 1}".encode()
                    stream = await session.create_bidirectional_stream()
                    await stream.write_all(data=data, end_stream=True)
                    await stream.read_all()
                    total_bytes_written += len(data)

                diagnostics = await session.diagnose()
                logger.info("Session diagnostics retrieved")
                logger.info("   - Streams Opened: %d", diagnostics.local_streams_bidi_opened)
                logger.info("   - Data Sent: %d", diagnostics.data_sent)

                if (
                    diagnostics.session_id == session.session_id
                    and diagnostics.state == SessionState.CONNECTED
                    and diagnostics.ready_at is not None
                    and diagnostics.local_streams_bidi_opened >= 3
                    and diagnostics.data_sent >= total_bytes_written
                ):
                    logger.info("SUCCESS: Session diagnostics appear correct")
                    return True
                else:
                    logger.error("FAILURE: Session diagnostics mismatch: %s", diagnostics)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_stream_diagnostics() -> bool:
    """Test the retrieval and correctness of stream-level diagnostics."""
    logger.info("Test 07C: Stream Diagnostics")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect(url=SERVER_URL) as session:
                stream = await session.create_bidirectional_stream()

                diagnostics = await stream.diagnose()
                logger.info("Stream diagnostics retrieved")
                logger.info("   - Stream ID: %d", diagnostics.stream_id)
                logger.info("   - Receive State: %s", diagnostics.receive_state)
                logger.info("   - Send State: %s", diagnostics.send_state)

                await stream.close()

                if (
                    diagnostics.stream_id == stream.stream_id
                    and diagnostics.session_id == session.session_id
                    and diagnostics.direction == StreamDirection.BIDIRECTIONAL
                    and not diagnostics.is_peer_initiated
                    and diagnostics.receive_state == StreamReceiveState.RECEIVE
                    and diagnostics.send_state == StreamSendState.READY
                ):
                    logger.info("SUCCESS: Stream diagnostics appear correct")
                    return True
                else:
                    logger.error("FAILURE: Stream diagnostics mismatch: %s", diagnostics)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the diagnostics test suite."""
    logger.info("Starting Test 07: Diagnostics")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Connection Diagnostics", test_connection_diagnostics),
        ("Session Diagnostics", test_session_diagnostics),
        ("Stream Diagnostics", test_stream_diagnostics),
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
    logger.info("Test 07 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 07 PASSED: All tests successful")
        return 0
    else:
        logger.error("TEST 07 FAILED: Some tests failed")
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
