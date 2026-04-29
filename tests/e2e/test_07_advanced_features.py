"""E2E test for advanced WebTransport features."""

import asyncio
import json
import logging
import ssl
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import EventType
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_advanced_features")


async def test_session_statistics() -> bool:
    """Test the retrieval and correctness of session-level statistics."""
    logger.info("--- Test 07A: Session Statistics ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Performing operations to generate statistics...")

            for i in range(3):
                stream = await session.create_bidirectional_stream()
                await stream.write_all(data=f"Stats test {i + 1}".encode(), end_stream=True)
                await stream.read_all()

            for i in range(5):
                await session.send_datagram(data=f"Datagram {i + 1}".encode())

            final_stats = await session.diagnostics()
            logger.info("Final session statistics retrieved.")

            streams_ok = final_stats.local_streams_bidi_opened >= 3
            datagrams_ok = final_stats.datagrams_sent >= 5

            if streams_ok and datagrams_ok:
                logger.info("SUCCESS: Session statistics appear correct.")
                return True
            else:
                logger.error("FAILURE: Session statistics mismatch.")
                logger.error("   - Streams Created: %d (expected >= 3)", final_stats.local_streams_bidi_opened)
                logger.error("   - Datagrams Sent: %d (expected >= 5)", final_stats.datagrams_sent)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_connection_info() -> bool:
    """Test the retrieval of underlying connection information."""
    logger.info("--- Test 07B: Connection Information ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            connection = session._connection()
            if not connection:
                logger.error("FAILURE: No connection object available on session.")
                return False

            logger.info("Retrieving connection information...")
            logger.info("   - Connection Handle: %d", connection.handle)
            logger.info("   - State: %s", connection.state.value)
            logger.info("   - Remote Address: %s", connection.remote_address)

            if connection.is_connected and connection.remote_address:
                logger.info("SUCCESS: Connection information retrieved successfully.")
                return True
            else:
                logger.error("FAILURE: Connection information is incomplete or state is incorrect.")
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_client_statistics() -> bool:
    """Test the retrieval of client-wide statistics across multiple connections."""
    logger.info("--- Test 07C: Client-Wide Statistics ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            logger.info("Performing 3 connections to generate client stats...")
            for _ in range(3):
                session = await client.connect(url=SERVER_URL)
                await session.close()
                await asyncio.sleep(delay=0.2)

            final_stats = (await client.diagnostics()).stats
            logger.info("Final client statistics:")
            logger.info("   - Connections Attempted: %d", final_stats.connections_attempted)
            logger.info("   - Connections Successful: %d", final_stats.connections_successful)
            logger.info("   - Avg Connect Time: %.3fs", final_stats.avg_connect_time)

            if final_stats.connections_attempted >= 3 and final_stats.connections_successful >= 3:
                logger.info("SUCCESS: Client statistics appear correct.")
                return True
            else:
                logger.error("FAILURE: Client statistics are incorrect.")
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_stream_management_diagnostics() -> bool:
    """Test advanced stream management features via diagnostics."""
    logger.info("--- Test 07D: Stream Management (Diagnostics) ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Connected, session: %d", session.session_id)
            connection = session._connection()
            if not connection:
                logger.error("FAILURE: Connection lost.")
                return False

            streams = [await session.create_bidirectional_stream() for _ in range(5)]
            logger.info("Created %d streams.", len(streams))

            conn_diag = await connection.diagnostics()
            logger.info("Connection diagnostics:")
            logger.info("   - Stream count: %d", conn_diag.stream_count)

            if conn_diag.stream_count == 5:
                logger.info("SUCCESS: Connection diagnostics correctly report stream count.")
            else:
                logger.error("FAILURE: Stream count is incorrect in connection diagnostics.")
                return False

            for stream in streams:
                if not stream.is_closed:
                    await stream.close()

            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_datagram_statistics() -> bool:
    """Test retrieval of detailed statistics for the datagram transport."""
    logger.info("--- Test 07E: Datagram Statistics ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Sending datagrams to generate statistics...")
            total_bytes_sent = 0
            for i in range(5):
                data = f"Datagram stats test {i}".encode()
                await session.send_datagram(data=data)
                total_bytes_sent += len(data)

            final_stats = await session.diagnostics()

            logger.info("Final session datagram statistics:")
            logger.info("   - Datagrams Sent: %d", final_stats.datagrams_sent)
            logger.info("   - Bytes Sent: %d", final_stats.datagram_bytes_sent)

            if final_stats.datagrams_sent >= 5 and final_stats.datagram_bytes_sent >= total_bytes_sent:
                logger.info("SUCCESS: Datagram statistics appear correct.")
                return True
            else:
                logger.error("FAILURE: Datagram statistics are incorrect.")
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_performance_monitoring() -> bool:
    """Test a simple performance monitoring loop over multiple transfers."""
    logger.info("--- Test 07F: Performance Monitoring ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Starting simple performance monitoring loop...")

            for size in [1024, 8192]:
                latencies = []
                for _ in range(3):
                    stream = await session.create_bidirectional_stream()
                    start_time = time.time()
                    await stream.write_all(data=b"x" * size, end_stream=True)
                    await stream.read(max_bytes=size + 10)
                    latencies.append(time.time() - start_time)
                    await stream.close()

                avg_rtt_ms = (sum(latencies) / len(latencies)) * 1000
                logger.info("   - Avg RTT for %d bytes: %.1fms", size, avg_rtt_ms)

            logger.info("SUCCESS: Performance monitoring loop completed.")
            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_session_lifecycle_events() -> bool:
    """Test the real event emission lifecycle using deterministic history verification."""
    logger.info("--- Test 07G: Session Lifecycle Events ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE, event_history_capacity=50)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            await session.close()

            history = session.events.get_event_history()
            event_types = [event.type for event in history]

            expected_events = [EventType.SESSION_READY, EventType.SESSION_CLOSED]
            filtered_events = [et for et in event_types if et in expected_events]

            if filtered_events == expected_events:
                logger.info("SUCCESS: Session lifecycle events occurred in the correct order.")
                return True
            else:
                logger.error("FAILURE: Incorrect event order: %s", filtered_events)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_server_diagnostics() -> bool:
    """Test retrieving the server's diagnostics API."""
    logger.info("--- Test 07H: Server-Side Diagnostics ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)
    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL + "diagnostics")
            logger.info("Connected to /diagnostics endpoint...")
            stream = await session.create_bidirectional_stream()
            response_data = await stream.read_all()
            if not response_data:
                logger.error("FAILURE: Received no data from /diagnostics endpoint.")
                return False

            stats = json.loads(s=response_data)
            logger.info("Received server diagnostics successfully.")

            if (
                "stats" in stats
                and "connection_states" in stats
                and "session_states" in stats
                and stats["is_serving"] is True
            ):
                logger.info("SUCCESS: Server diagnostics structure is valid.")
                return True
            else:
                logger.error("FAILURE: Server diagnostics data is incomplete or invalid.")
                logger.error("Received: %s", stats)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the advanced features test suite."""
    logger.info("--- Starting Test 07: Advanced Features ---")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Session Statistics", test_session_statistics),
        ("Connection Information", test_connection_info),
        ("Client-Wide Statistics", test_client_statistics),
        ("Stream Management (Diagnostics)", test_stream_management_diagnostics),
        ("Datagram Statistics", test_datagram_statistics),
        ("Performance Monitoring", test_performance_monitoring),
        ("Session Lifecycle Events", test_session_lifecycle_events),
        ("Server-Side Diagnostics", test_server_diagnostics),
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
    logger.info("Test 07 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 07 PASSED: All advanced features tests successful!")
        return 0
    else:
        logger.error("TEST 07 FAILED: Some advanced features tests failed!")
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
