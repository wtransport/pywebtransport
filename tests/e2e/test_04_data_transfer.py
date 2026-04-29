"""E2E test for WebTransport data transfer."""

import asyncio
import logging
import ssl
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, ConnectionError, TimeoutError, WebTransportClient
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(level=logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_data_transfer")


def generate_test_data(*, size: int) -> bytes:
    """Generate a block of test data of a specified size."""
    pattern = b"WebTransport Test Data 1234567890 " * (size // 34 + 1)
    return pattern[:size]


async def test_small_data() -> bool:
    """Test small data transfers (< 1KB)."""
    logger.info("--- Test 04A: Small Data Transfer ---")
    test_sizes = [10, 100, 500, 1000]
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            for size in test_sizes:
                logger.info("Testing %d bytes transfer...", size)
                test_data = generate_test_data(size=size)
                stream = await session.create_bidirectional_stream()

                await stream.write_all(data=test_data, end_stream=True)
                response = await stream.read_all()

                expected = b"ECHO: " + test_data
                if response != expected:
                    logger.error("FAILURE: Data mismatch for %d bytes.", size)
                    return False
                logger.info("   - %d bytes: OK", size)

            logger.info("SUCCESS: All small data transfers completed successfully.")
            return True
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_medium_data() -> bool:
    """Test medium data transfers (1KB - 64KB)."""
    logger.info("--- Test 04B: Medium Data Transfer ---")
    test_sizes = [1024, 4096, 16384, 65536]
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            for size in test_sizes:
                size_kb = size / 1024
                logger.info("Testing %.1f KB transfer...", size_kb)
                test_data = generate_test_data(size=size)
                stream = await session.create_bidirectional_stream()

                await stream.write_all(data=test_data, end_stream=True)
                response_data = await stream.read_all()

                expected = b"ECHO: " + test_data
                if response_data != expected:
                    logger.error("FAILURE: Data mismatch for %.1f KB.", size_kb)
                    return False
                logger.info("   - %.1f KB: OK", size_kb)

            logger.info("SUCCESS: All medium data transfers completed successfully.")
            return True
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_chunked_transfer() -> bool:
    """Test transferring data in multiple chunks using stream.write()."""
    logger.info("--- Test 04C: Chunked Transfer ---")
    total_size = 32768
    chunk_size = 4096
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            test_data = generate_test_data(size=total_size)
            stream = await session.create_bidirectional_stream()
            logger.info("Sending %d bytes in %d-byte chunks...", total_size, chunk_size)

            bytes_sent = 0
            for i in range(0, total_size, chunk_size):
                chunk = test_data[i : i + chunk_size]
                is_last_chunk = (i + chunk_size) >= total_size
                await stream.write(data=chunk, end_stream=is_last_chunk)
                bytes_sent += len(chunk)
            logger.info("All %d bytes sent.", bytes_sent)

            response_data = await stream.read_all()
            expected = b"ECHO: " + test_data
            if response_data != expected:
                logger.error("FAILURE: Chunked data transfer response mismatch.")
                return False

            logger.info("SUCCESS: Chunked transfer completed successfully.")
            return True
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_binary_data() -> bool:
    """Test the transfer of raw binary data to ensure no corruption."""
    logger.info("--- Test 04D: Binary Data Transfer ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            binary_data = bytes(range(256)) * 100
            logger.info("Testing transfer of %d raw binary bytes.", len(binary_data))
            stream = await session.create_bidirectional_stream()

            await stream.write_all(data=binary_data, end_stream=True)
            response_data = await stream.read_all()

            expected = b"ECHO: " + binary_data
            if response_data != expected:
                logger.error("FAILURE: Binary data was corrupted during transfer.")
                return False

            logger.info("SUCCESS: Binary data transfer completed without corruption.")
            return True
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_performance_benchmark() -> bool:
    """Perform a simple performance benchmark with a 1MB payload."""
    logger.info("--- Test 04E: Performance Benchmark (1MB) ---")
    test_size = 1024 * 1024
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)
            test_data = generate_test_data(size=test_size)
            stream = await session.create_bidirectional_stream()

            logger.info("Starting 1MB round-trip transfer...")
            start_time = time.time()
            await stream.write_all(data=test_data, end_stream=True)
            response_data = await stream.read_all()
            duration = time.time() - start_time

            expected = b"ECHO: " + test_data
            if response_data != expected:
                logger.error("FAILURE: Performance test data corrupted.")
                return False

            total_mb = (len(test_data) + len(response_data)) / (1024 * 1024)
            throughput = total_mb / duration if duration > 0 else float("inf")
            logger.info("SUCCESS: Benchmark completed.")
            logger.info("   - Total round-trip time: %.3fs", duration)
            logger.info("   - Aggregate throughput: %.2f MB/s", throughput)
            return True
    except (TimeoutError, ConnectionError) as e:
        logger.error("FAILURE: Test failed due to connection or timeout issue: %s", e)
        return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the data transfer tests."""
    logger.info("--- Starting Test 04: Data Transfer ---")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Small Data Transfer", test_small_data),
        ("Medium Data Transfer", test_medium_data),
        ("Chunked Transfer", test_chunked_transfer),
        ("Binary Data Transfer", test_binary_data),
        ("Performance Benchmark", test_performance_benchmark),
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
    logger.info("Test 04 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 04 PASSED: All data transfer tests successful!")
        return 0
    else:
        logger.error("TEST 04 FAILED: Some data transfer tests failed!")
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
