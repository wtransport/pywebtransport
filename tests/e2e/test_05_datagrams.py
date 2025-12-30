import asyncio
import logging
import ssl
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, ConnectionError, Event, TimeoutError, WebTransportClient
from pywebtransport.types import EventType

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("aioquic").setLevel(logging.DEBUG)
    logging.getLogger("pywebtransport").setLevel(logging.DEBUG)

logger = logging.getLogger("test_datagrams")


async def test_basic_datagram() -> bool:
    logger.info("--- Test 05A: Basic Datagram Echo ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Session ready for datagrams.")

            test_message = b"Hello, Datagram!"
            expected_response = b"ECHO: " + test_message

            logger.info("Sending datagram: %r", test_message)
            await session.send_datagram(data=test_message)

            logger.info("Waiting for echo...")
            event: Event = await session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED, timeout=5.0)

            response = None
            if isinstance(event.data, dict):
                response = event.data.get("data")

            if response == expected_response:
                logger.info("SUCCESS: Received correct datagram echo.")
                return True
            else:
                logger.error("FAILURE: Datagram echo mismatch. Got: %r", response)
                return False
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_multiple_datagrams() -> bool:
    logger.info("--- Test 05B: Multiple Datagrams ---")
    num_datagrams = 10
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            logger.info("Sending %d datagrams and awaiting echoes...", num_datagrams)

            received_events: list[bytes] = []

            async def receiver() -> None:
                try:
                    for _ in range(num_datagrams):
                        event = await session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED, timeout=5.0)

                        data = None
                        if isinstance(event.data, dict):
                            data = event.data.get("data")

                        if isinstance(data, bytes):
                            received_events.append(data)
                except asyncio.TimeoutError:
                    logger.warning("Receiver timed out.")
                except Exception:
                    pass

            receiver_task = asyncio.create_task(receiver())
            await asyncio.sleep(0.1)

            for i in range(num_datagrams):
                await session.send_datagram(data=f"Datagram message {i + 1}".encode())

            await receiver_task

            if len(received_events) != num_datagrams:
                logger.error(
                    "FAILURE: Expected %d datagrams, but received %d.",
                    num_datagrams,
                    len(received_events),
                )
                return False

            for i, data in enumerate(received_events):
                expected = f"ECHO: Datagram message {i + 1}".encode()
                if data != expected:
                    logger.error("FAILURE: Datagram %d mismatch. Got: %r", i + 1, data)
                    return False

            logger.info("SUCCESS: Received %d correct datagram echoes.", num_datagrams)
            return True

    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_datagram_sizes() -> bool:
    logger.info("--- Test 05C: Datagram Size Limits ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE, max_datagram_size=1200)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)

            connection = session._connection()
            if not connection:
                logger.error("FAILURE: Connection lost unexpectedly.")
                return False

            diags = await connection.diagnostics()
            local_max = diags.max_datagram_size
            remote_max = diags.remote_max_datagram_frame_size
            logger.info("Max datagram size from diagnostics: local=%s, remote=%s", local_max, remote_max)

            if local_max != 1200:
                logger.warning("Engine state max_datagram_size (%s) mismatch config (%s)", local_max, 1200)

            logger.info("Testing oversized datagram...")
            try:
                oversized_data = b"X" * (remote_max + 1)
                await session.send_datagram(data=oversized_data)
                logger.error("FAILURE: Sending oversized datagram should have raised an exception.")
                return False
            except ValueError as e:
                if "Datagram size" in str(e):
                    logger.info("SUCCESS: Oversized datagram correctly raised ValueError: %s", e)
                    return True
                else:
                    logger.error("FAILURE: Caught ValueError, but not the expected one: %s", e)
                    return False
            except Exception as e:
                logger.error("FAILURE: Unexpected exception for oversized datagram: %s", e)
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_datagram_burst() -> bool:
    logger.info("--- Test 05G: Datagram Burst ---")
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

            logger.info("SUCCESS: Sent %d datagrams in %.3fs (%.1f dgrams/s).", burst_size, duration, rate)
            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    logger.info("--- Starting Test 05: Datagrams ---")

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
        await asyncio.sleep(1)

    logger.info("")
    logger.info("=" * 60)
    logger.info("Test 05 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 05 PASSED: All datagram tests successful!")
        return 0
    else:
        logger.error("TEST 05 FAILED: Some datagram tests failed!")
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
