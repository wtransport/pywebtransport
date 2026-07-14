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
    WebTransportStream,
)
from pywebtransport.types import EventType, SessionState
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
NEGOTIATION_URL: Final[str] = SERVER_URL + "protocol-negotiation"
OPTIMISTIC_SERVER_PORT: Final[int] = 4434
OPTIMISTIC_SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{OPTIMISTIC_SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(level=logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_optimistic_sending")


async def test_optimistic_basic_connection() -> bool:
    """Test that an optimistically created session starts connecting and resolves to connected."""
    logger.info("--- Test 10A: Optimistic Basic Connection ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect_optimistic(url=SERVER_URL)
            state_before = session.state
            logger.info("Session returned with state: %s", state_before.value)

            if state_before != SessionState.CONNECTING:
                logger.error("FAILURE: Session should start in CONNECTING state, got %s", state_before.value)
                return False

            await session.ensure_ready()
            state_after = session.state
            logger.info("Session ready with state: %s", state_after.value)

            if state_after != SessionState.CONNECTED:
                logger.error("FAILURE: Session should be CONNECTED after ensure_ready, got %s", state_after.value)
                return False

            logger.info("SUCCESS: Optimistic session transitioned CONNECTING -> CONNECTED correctly.")
            await session.close()
            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_lifecycle_events() -> bool:
    """Test that optimistic sessions emit SESSION_PENDING before SESSION_READY, unlike standard sessions."""
    logger.info("--- Test 10B: Optimistic Lifecycle Events ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE, event_history_capacity=50)

    try:
        async with WebTransportClient(config=config) as client:
            optimistic_session = await client.connect_optimistic(url=SERVER_URL)
            await optimistic_session.ensure_ready()
            await optimistic_session.close()

            optimistic_history = optimistic_session.events.get_event_history()
            optimistic_types = [event.type for event in optimistic_history]
            expected_optimistic = [EventType.SESSION_PENDING, EventType.SESSION_READY, EventType.SESSION_CLOSED]
            filtered_optimistic = [et for et in optimistic_types if et in expected_optimistic]

            standard_session = await client.connect(url=SERVER_URL)
            await standard_session.close()

            standard_history = standard_session.events.get_event_history()
            standard_types = [event.type for event in standard_history]
            expected_standard = [EventType.SESSION_READY, EventType.SESSION_CLOSED]
            filtered_standard = [et for et in standard_types if et in expected_optimistic]

            if filtered_optimistic != expected_optimistic:
                logger.error("FAILURE: Optimistic event order incorrect: %s", filtered_optimistic)
                return False

            if filtered_standard != expected_standard:
                logger.error("FAILURE: Standard event order incorrect: %s", filtered_standard)
                return False

            logger.info("SUCCESS: Optimistic and standard sessions emitted correct, distinct event sequences.")
            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_early_stream_write() -> bool:
    """Test that stream data written before session confirmation is correctly delivered."""
    logger.info("--- Test 10C: Optimistic Early Stream Write ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect_optimistic(url=OPTIMISTIC_SERVER_URL)
            logger.info("Session state before write: %s", session.state.value)

            test_message = b"Hello from an unconfirmed session!"
            stream = await session.create_bidirectional_stream()
            await stream.write(data=test_message, end_stream=True)
            logger.info("Sent data before calling ensure_ready().")

            await session.ensure_ready()
            logger.info("Session confirmed with state: %s", session.state.value)

            response_data = await stream.read_all()
            expected_response = b"ECHO: " + test_message

            if response_data == expected_response:
                logger.info("SUCCESS: Early-written stream data was correctly delivered and echoed.")
                await session.close()
                return True
            else:
                logger.error("FAILURE: Echo mismatch. Expected: %r, Got: %r", expected_response, response_data)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_early_datagram_send() -> bool:
    """Test that a datagram sent before session confirmation is correctly delivered."""
    logger.info("--- Test 10D: Optimistic Early Datagram Send ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect_optimistic(url=OPTIMISTIC_SERVER_URL)
            logger.info("Session state before send: %s", session.state.value)

            test_message = b"Hello, unconfirmed datagram!"
            expected_response = b"ECHO: " + test_message

            wait_task = asyncio.create_task(
                coro=session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED, timeout=5.0)
            )

            await session.send_datagram(data=test_message)
            logger.info("Sent datagram before calling ensure_ready().")

            await session.ensure_ready()
            logger.info("Session confirmed with state: %s", session.state.value)

            event: Event = await wait_task
            response = None
            if isinstance(event.data, dict):
                response = event.data.get("data")

            if response == expected_response:
                logger.info("SUCCESS: Early-sent datagram was correctly delivered and echoed.")
                await session.close()
                return True
            else:
                logger.error("FAILURE: Datagram echo mismatch. Got: %r", response)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_rejection_via_ensure_ready() -> bool:
    """Test that connect_optimistic returns immediately while rejection surfaces from ensure_ready."""
    logger.info("--- Test 10E: Optimistic Rejection via ensure_ready ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect_optimistic(url=NEGOTIATION_URL, wt_available_protocols=["unknown-proto"])
            logger.info("connect_optimistic returned without raising, state: %s", session.state.value)

            if session.state != SessionState.CONNECTING:
                logger.error("FAILURE: Session should still be CONNECTING immediately after connect_optimistic.")
                return False

            try:
                await session.ensure_ready()
                logger.error("FAILURE: ensure_ready should have raised for a rejected session.")
                return False
            except SessionError as e:
                logger.info("SUCCESS: ensure_ready correctly raised SessionError for rejection: %s", e)
                return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_optimistic_early_send_before_rejection() -> bool:
    """Test that sending data before a subsequent rejection does not crash or hang the client."""
    logger.info("--- Test 10F: Optimistic Early Send Before Rejection ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect_optimistic(url=NEGOTIATION_URL, wt_available_protocols=["unknown-proto"])

            try:
                await session.send_datagram(data=b"This will never be seen")
                logger.info("Datagram send before rejection did not raise immediately.")
            except Exception as e:
                logger.info("Datagram send before rejection raised early (acceptable): %s", e)

            try:
                async with asyncio.timeout(delay=5.0):
                    await session.ensure_ready()
                logger.error("FAILURE: ensure_ready should have raised for a rejected session.")
                return False
            except SessionError as e:
                logger.info("SUCCESS: ensure_ready correctly raised SessionError after early send: %s", e)
                return True
            except asyncio.TimeoutError:
                logger.error("FAILURE: ensure_ready hung instead of raising promptly.")
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_server_buffers_early_stream_data() -> bool:
    """Test that stream data sent before the server accepts the session is buffered and processed."""
    logger.info("--- Test 10G: Server Buffers Early Stream Data ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect_optimistic(url=OPTIMISTIC_SERVER_URL)
            logger.info("Session state before write: %s", session.state.value)

            test_message = b"Data sent before the server accepted."
            stream = await session.create_bidirectional_stream()
            await stream.write(data=test_message, end_stream=True)
            logger.info("Sent data while the server is still delaying acceptance.")

            async with asyncio.timeout(delay=10.0):
                response_data = await stream.read_all()

            expected_response = b"ECHO: " + test_message
            if response_data == expected_response:
                logger.info("SUCCESS: Server correctly buffered and processed pre-acceptance stream data.")
                await session.ensure_ready()
                await session.close()
                return True
            else:
                logger.error("FAILURE: Echo mismatch. Expected: %r, Got: %r", expected_response, response_data)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_server_stream_overflow_drop() -> bool:
    """Test that exceeding the pending stream buffer before acceptance rejects streams without aborting."""
    logger.info("--- Test 10H: Server Stream Overflow Drop ---")
    filler_count = 8
    probe_count = 8
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    async def _open_filler_stream(*, session: WebTransportSession) -> None:
        try:
            stream = await session.create_unidirectional_stream()
            await stream.write(data=b"Filler", end_stream=True)
        except Exception as e:
            logger.debug("Unidirectional filler stream raised (acceptable): %s", e)

    async def _open_probe_stream(*, session: WebTransportSession) -> WebTransportStream:
        stream = await session.create_bidirectional_stream()
        await stream.write(data=b"Overflow", end_stream=True)
        return stream

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect_optimistic(url=OPTIMISTIC_SERVER_URL)

            logger.info("Filling the pending stream buffer with %d unidirectional streams...", filler_count)
            await asyncio.gather(*[_open_filler_stream(session=session) for _ in range(filler_count)])

            logger.info("Opening %d bidirectional streams to push past the buffer limit...", probe_count)
            probe_streams = await asyncio.gather(*[_open_probe_stream(session=session) for _ in range(probe_count)])

            succeeded = 0
            rejected = 0
            for stream in probe_streams:
                try:
                    async with asyncio.timeout(delay=5.0):
                        response_data = await stream.read_all()
                    if response_data == b"ECHO: Overflow":
                        succeeded += 1
                    else:
                        rejected += 1
                except Exception:
                    rejected += 1

            await session.ensure_ready()

            if 0 < succeeded < probe_count and rejected > 0:
                logger.info(
                    "SUCCESS: Session survived overflow; %d/%d bidirectional streams succeeded, %d were rejected.",
                    succeeded,
                    probe_count,
                    rejected,
                )
                await session.close()
                return True
            else:
                logger.error(
                    "FAILURE: Unexpected result succeeded=%d rejected=%d/%d; overflow should reject some but not "
                    "all bidirectional streams.",
                    succeeded,
                    rejected,
                    probe_count,
                )
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_server_datagram_overflow_drop() -> bool:
    """Test that exceeding the pending datagram buffer before acceptance drops datagrams without aborting."""
    logger.info("--- Test 10I: Server Datagram Overflow Drop ---")
    burst_size = 150
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect_optimistic(url=OPTIMISTIC_SERVER_URL)
            logger.info("Sending a burst of %d datagrams before the server accepts...", burst_size)

            received_events: list[bytes] = []

            async def datagram_handler(event: Event) -> None:
                data = None
                if isinstance(event.data, dict):
                    data = event.data.get("data")
                if isinstance(data, bytes):
                    received_events.append(data)

            session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=datagram_handler)

            tasks = [session.send_datagram(data=f"Overflow {i}".encode()) for i in range(burst_size)]
            await asyncio.gather(*tasks)

            try:
                async with asyncio.timeout(delay=10.0):
                    await session.ensure_ready()
            except Exception as e:
                logger.error("FAILURE: Session did not become ready after the datagram burst: %s", e)
                return False

            await asyncio.sleep(delay=1.0)
            session.events.off(event_type=EventType.DATAGRAM_RECEIVED, handler=datagram_handler)

            if session.is_closed:
                logger.error("FAILURE: Session was unexpectedly closed after the overflow burst.")
                return False

            if 0 < len(received_events) < burst_size:
                logger.info(
                    "SUCCESS: Session survived overflow; %d/%d datagrams were buffered and echoed.",
                    len(received_events),
                    burst_size,
                )
                await session.close()
                return True
            else:
                logger.error(
                    "FAILURE: Unexpected echo count %d/%d; overflow should drop some but not all datagrams.",
                    len(received_events),
                    burst_size,
                )
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the optimistic sending test suite."""
    logger.info("--- Starting Test 10: Optimistic Sending ---")

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
    logger.info("=" * 60)
    logger.info("Test 10 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 10 PASSED: All optimistic sending tests successful!")
        return 0
    else:
        logger.error("TEST 10 FAILED: Some optimistic sending tests failed!")
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
