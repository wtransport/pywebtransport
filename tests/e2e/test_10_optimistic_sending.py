"""E2E test for WebTransport optimistic sending."""

import asyncio
import logging
import ssl
import sys
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import (
    ClientConfig,
    Event,
    SessionError,
    WebTransportClient,
    WebTransportSession,
)
from pywebtransport.types import EventType, SessionState
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(level=logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_optimistic_sending")


async def test_optimistic_basic_connection() -> bool:
    """Test that an optimistically created session starts connecting and resolves to connected."""
    logger.info("Test 10A: Optimistic Basic Connection")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect_optimistic(url=SERVER_URL + "acceptance-delay") as session:
                state_before = (await session.diagnose()).state
                logger.info("Session returned with state: %s", state_before)

                if state_before != SessionState.CONNECTING:
                    logger.error("FAILURE: Session should start in CONNECTING state, got %s", state_before)
                    return False

                await session.ensure_ready()
                state_after = (await session.diagnose()).state
                logger.info("Session ready with state: %s", state_after)

                if state_after != SessionState.CONNECTED:
                    logger.error("FAILURE: Session should be CONNECTED after ensure_ready, got %s", state_after)
                    return False

                logger.info("SUCCESS: Optimistic session transitioned CONNECTING -> CONNECTED correctly")
                return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_lifecycle_events() -> bool:
    """Test that optimistic sessions start unconfirmed and emit SESSION_READY before SESSION_CLOSED."""
    logger.info("Test 10B: Optimistic Lifecycle Events")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            optimistic_observed: list[EventType] = []

            async def record_optimistic(event: Event) -> None:
                optimistic_observed.append(event.type)

            async with await client.connect_optimistic(url=SERVER_URL + "acceptance-delay") as optimistic_session:
                optimistic_session.events.on(event_type=EventType.SESSION_READY, handler=record_optimistic)
                optimistic_session.events.on(event_type=EventType.SESSION_CLOSED, handler=record_optimistic)
                state_before_ready = (await optimistic_session.diagnose()).state

                await optimistic_session.ensure_ready()
                await optimistic_session.close()

            expected_optimistic = [EventType.SESSION_READY, EventType.SESSION_CLOSED]

            standard_closed = asyncio.Event()

            async def record_standard(event: Event) -> None:
                standard_closed.set()

            async with await client.connect(url=SERVER_URL) as standard_session:
                standard_session.events.on(event_type=EventType.SESSION_CLOSED, handler=record_standard)
                await standard_session.close()
                await asyncio.wait_for(standard_closed.wait(), timeout=5.0)

            if state_before_ready != SessionState.CONNECTING:
                logger.error("FAILURE: Optimistic session should start unconfirmed, got %s", state_before_ready)
                return False

            if optimistic_observed != expected_optimistic:
                logger.error("FAILURE: Optimistic event order incorrect: %s", optimistic_observed)
                return False

            logger.info("SUCCESS: Optimistic and standard sessions emitted correct, distinct event sequences")
            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_early_stream_write() -> bool:
    """Test that stream data written before session confirmation is correctly delivered."""
    logger.info("Test 10C: Optimistic Early Stream Write")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect_optimistic(url=SERVER_URL + "acceptance-delay") as session:
                logger.info("Session state before write: %s", (await session.diagnose()).state)

                test_message = b"Hello from an unconfirmed session!"
                stream = await session.create_bidirectional_stream()
                await stream.write(data=test_message, end_stream=True)
                logger.info("Sent data before calling ensure_ready()")

                await session.ensure_ready()
                logger.info("Session confirmed with state: %s", (await session.diagnose()).state)

                response_data = await stream.read_all()
                expected_response = b"ECHO: " + test_message

                if response_data == expected_response:
                    logger.info("SUCCESS: Early-written stream data was correctly delivered and echoed")
                    return True
                else:
                    logger.error("FAILURE: Echo mismatch. Expected: %r, Got: %r", expected_response, response_data)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_early_datagram_send() -> bool:
    """Test that a datagram sent before session confirmation is correctly delivered."""
    logger.info("Test 10D: Optimistic Early Datagram Send")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect_optimistic(url=SERVER_URL + "acceptance-delay") as session:
                logger.info("Session state before send: %s", (await session.diagnose()).state)

                test_message = b"Hello, unconfirmed datagram!"
                expected_response = b"ECHO: " + test_message

                await session.send_datagram(data=test_message)
                logger.info("Sent datagram before calling ensure_ready()")

                await session.ensure_ready()
                logger.info("Session confirmed with state: %s", (await session.diagnose()).state)

                async with asyncio.timeout(delay=5.0):
                    response = await session.read_datagram()

                if response == expected_response:
                    logger.info("SUCCESS: Early-sent datagram was correctly delivered and echoed")
                    return True
                else:
                    logger.error("FAILURE: Datagram echo mismatch. Got: %r", response)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_rejection_via_ensure_ready() -> bool:
    """Test that connect_optimistic returns immediately while rejection surfaces from ensure_ready."""
    logger.info("Test 10E: Optimistic Rejection via ensure_ready")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect_optimistic(
                url=SERVER_URL + "protocol-negotiation", wt_available_protocols=["unknown-proto"]
            ) as session:
                logger.info("connect_optimistic returned without raising")

                try:
                    await session.ensure_ready()
                    logger.error("FAILURE: ensure_ready should have raised for a rejected session")
                    return False
                except SessionError as e:
                    logger.info("SUCCESS: ensure_ready correctly raised SessionError for rejection: %s", e)
                    return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_early_send_before_rejection() -> bool:
    """Test that sending data before a subsequent rejection does not crash or hang the client."""
    logger.info("Test 10F: Optimistic Early Send Before Rejection")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect_optimistic(
                url=SERVER_URL + "protocol-negotiation", wt_available_protocols=["unknown-proto"]
            ) as session:
                try:
                    await session.send_datagram(data=b"This will never be seen")
                    logger.info("Datagram send before rejection did not raise immediately")
                except Exception as e:
                    logger.info("Datagram send before rejection raised early (acceptable): %s", e)

                try:
                    async with asyncio.timeout(delay=5.0):
                        await session.ensure_ready()
                    logger.error("FAILURE: ensure_ready should have raised for a rejected session")
                    return False
                except SessionError as e:
                    logger.info("SUCCESS: ensure_ready correctly raised SessionError after early send: %s", e)
                    return True
                except asyncio.TimeoutError:
                    logger.error("FAILURE: ensure_ready hung instead of raising promptly")
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_server_buffers_early_stream_data() -> bool:
    """Test that stream data sent before the server accepts the session is buffered and processed."""
    logger.info("Test 10G: Server Buffers Early Stream Data")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect_optimistic(url=SERVER_URL + "acceptance-delay") as session:
                logger.info("Session state before write: %s", (await session.diagnose()).state)

                test_message = b"Data sent before the server accepted."
                stream = await session.create_bidirectional_stream()
                await stream.write(data=test_message, end_stream=True)
                logger.info("Sent data while the server is still delaying acceptance")

                async with asyncio.timeout(delay=10.0):
                    response_data = await stream.read_all()

                expected_response = b"ECHO: " + test_message
                if response_data == expected_response:
                    logger.info("SUCCESS: Server correctly buffered and processed pre-acceptance stream data")
                    await session.ensure_ready()
                    return True
                else:
                    logger.error("FAILURE: Echo mismatch. Expected: %r, Got: %r", expected_response, response_data)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_server_stream_overflow_drop() -> bool:
    """Test that exceeding the pending stream buffer before acceptance rejects streams without aborting."""
    logger.info("Test 10H: Server Stream Overflow Drop")
    logger.info("-" * 50)

    filler_count = 8
    probe_count = 8
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    async def open_filler_stream(*, session: WebTransportSession) -> None:
        try:
            stream = await session.create_unidirectional_stream()
            await stream.write(data=b"Filler", end_stream=True)
        except Exception as e:
            logger.debug("Unidirectional filler stream raised (acceptable): %s", e)

    async def open_probe_stream(*, session: WebTransportSession) -> bool:
        try:
            stream = await session.create_bidirectional_stream()
            await stream.write(data=b"Overflow", end_stream=True)
            async with asyncio.timeout(delay=5.0):
                response_data = await stream.read_all()
        except Exception as e:
            logger.debug("Bidirectional probe stream rejected: %s", e)
            return False
        return response_data == b"ECHO: Overflow"

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect_optimistic(url=SERVER_URL + "acceptance-delay") as session:
                logger.info("Filling the pending stream buffer with %d unidirectional streams...", filler_count)
                await asyncio.gather(*[open_filler_stream(session=session) for _ in range(filler_count)])

                logger.info("Opening %d bidirectional streams to push past the buffer limit...", probe_count)
                probe_results = await asyncio.gather(*[open_probe_stream(session=session) for _ in range(probe_count)])

                succeeded = sum(probe_results)
                rejected = probe_count - succeeded

                await session.ensure_ready()

                if 0 < succeeded < probe_count and rejected > 0:
                    logger.info("SUCCESS: Session correctly survived the stream buffer overflow")
                    logger.info(
                        "   - Bidirectional streams: %d/%d succeeded, %d rejected", succeeded, probe_count, rejected
                    )
                    return True
                else:
                    logger.error("FAILURE: Overflow did not reject some but not all bidirectional streams")
                    logger.error("   - Succeeded: %d, Rejected: %d/%d", succeeded, rejected, probe_count)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_server_datagram_overflow_drop() -> bool:
    """Test that exceeding the pending datagram buffer before acceptance drops datagrams without aborting."""
    logger.info("Test 10I: Server Datagram Overflow Drop")
    logger.info("-" * 50)

    burst_size = 150
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            async with await client.connect_optimistic(url=SERVER_URL + "acceptance-delay") as session:
                logger.info("Sending a burst of %d datagrams before the server accepts...", burst_size)

                tasks = [session.send_datagram(data=f"Overflow {i}".encode()) for i in range(burst_size)]
                await asyncio.gather(*tasks)

                try:
                    async with asyncio.timeout(delay=10.0):
                        await session.ensure_ready()
                except Exception as e:
                    logger.error("FAILURE: Session did not become ready after the datagram burst: %s", e)
                    return False

                received_datagrams: list[bytes] = []
                try:
                    async with asyncio.timeout(delay=1.0):
                        while True:
                            received_datagrams.append(await session.read_datagram())
                except asyncio.TimeoutError:
                    pass

                if session.is_closed:
                    logger.error("FAILURE: Session was unexpectedly closed after the overflow burst")
                    return False

                if 0 < len(received_datagrams) < burst_size:
                    logger.info("SUCCESS: Session correctly survived the datagram buffer overflow")
                    logger.info("   - Datagrams echoed: %d/%d", len(received_datagrams), burst_size)
                    return True
                else:
                    logger.error("FAILURE: Overflow did not drop some but not all datagrams")
                    logger.error("   - Echoed: %d/%d", len(received_datagrams), burst_size)
                    return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the optimistic sending test suite."""
    logger.info("Starting Test 10: Optimistic Sending")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Optimistic Basic Connection", test_optimistic_basic_connection),
        ("Optimistic Lifecycle Events", test_optimistic_lifecycle_events),
        ("Optimistic Early Stream Write", test_optimistic_early_stream_write),
        ("Optimistic Early Datagram Send", test_optimistic_early_datagram_send),
        ("Optimistic Rejection via ensure_ready", test_optimistic_rejection_via_ensure_ready),
        ("Optimistic Early Send Before Rejection", test_optimistic_early_send_before_rejection),
        ("Server Buffers Early Stream Data", test_server_buffers_early_stream_data),
        ("Server Stream Overflow Drop", test_server_stream_overflow_drop),
        ("Server Datagram Overflow Drop", test_server_datagram_overflow_drop),
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
    logger.info("Test 10 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 10 PASSED: All tests successful")
        return 0
    else:
        logger.error("TEST 10 FAILED: Some tests failed")
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
