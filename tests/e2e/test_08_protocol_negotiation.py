"""E2E test for Application-Layer Protocol Negotiation (ALPN)."""

import asyncio
import logging
import ssl
import sys
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.utils import init_tracing

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
TEST_URL: Final[str] = SERVER_URL + "protocol-negotiation"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    init_tracing()

logger = logging.getLogger(name="test_protocol_negotiation")


async def test_exact_match() -> bool:
    """Test successful negotiation when exact matching protocols are provided."""
    logger.info("Test 08A: Exact Match")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=TEST_URL, wt_available_protocols=["chat-v2", "chat-v1"])
            agreed_protocol = session.wt_protocol

            if agreed_protocol == "chat-v2":
                logger.info("SUCCESS: Negotiated correct protocol: %s", agreed_protocol)
                return True
            else:
                logger.error("FAILURE: Expected 'chat-v2', got '%s'", agreed_protocol)
                return False
    except Exception as e:
        logger.error("FAILURE: Connection failed unexpectedly: %s", e, exc_info=True)
        return False


async def test_fallback_downgrade() -> bool:
    """Test successful negotiation utilizing fallback/downgrade to a lower protocol version."""
    logger.info("Test 08B: Fallback Downgrade")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=TEST_URL, wt_available_protocols=["chat-v3", "chat-v1"])
            agreed_protocol = session.wt_protocol

            if agreed_protocol == "chat-v1":
                logger.info("SUCCESS: Successfully downgraded and negotiated: %s", agreed_protocol)
                return True
            else:
                logger.error("FAILURE: Expected 'chat-v1', got '%s'", agreed_protocol)
                return False
    except Exception as e:
        logger.error("FAILURE: Connection failed unexpectedly: %s", e, exc_info=True)
        return False


async def test_mismatch_rejection() -> bool:
    """Test that connection is correctly rejected when no protocols match."""
    logger.info("Test 08C: Mismatch Rejection")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=TEST_URL, wt_available_protocols=["unknown-proto"])
            logger.error("FAILURE: Connection succeeded but should have been rejected")
            return False
    except Exception as e:
        logger.info("SUCCESS: Connection rejected successfully due to mismatch: %s", e)
        return True


async def test_missing_required() -> bool:
    """Test that connection is correctly rejected when no protocols are offered but are required."""
    logger.info("Test 08D: Missing Required Protocol")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=TEST_URL, wt_available_protocols=None)
            logger.error("FAILURE: Connection succeeded but should have been rejected")
            return False
    except Exception as e:
        logger.info("SUCCESS: Connection rejected successfully due to missing protocols: %s", e)
        return True


async def test_rogue_missing() -> bool:
    """Test that client aborts with WT_ALPN_ERROR when server fails to send a required protocol."""
    logger.info("Test 08E: Rogue Server Missing Protocol")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=TEST_URL, wt_available_protocols=["trigger-missing"])
            logger.error("FAILURE: Connection succeeded but should have been aborted by client")
            return False
    except Exception as e:
        error_msg = str(e).lower()
        if "0x817b3dd" in error_msg or "protocol" in error_msg:
            logger.info("SUCCESS: Client aborted connection correctly due to missing protocol: %s", e)
            return True
        else:
            logger.error("FAILURE: Connection rejected, but unexpected error: %s", e)
            return False


async def test_rogue_mismatch() -> bool:
    """Test that client aborts with WT_ALPN_ERROR when server negotiates an unrequested protocol."""
    logger.info("Test 08F: Rogue Server Mismatched Protocol")
    logger.info("-" * 50)

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=TEST_URL, wt_available_protocols=["trigger-mismatch"])
            logger.error("FAILURE: Connection succeeded but should have been aborted by client")
            return False
    except Exception as e:
        error_msg = str(e).lower()
        if "0x817b3dd" in error_msg or "alien-proto" in error_msg or "protocol" in error_msg:
            logger.info("SUCCESS: Client aborted connection correctly due to mismatched protocol: %s", e)
            return True
        else:
            logger.error("FAILURE: Connection rejected, but unexpected error: %s", e)
            return False


async def main() -> int:
    """Run the main entry point for the protocol negotiation test suite."""
    logger.info("Starting Test 08: Application-Layer Protocol Negotiation")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Exact Match", test_exact_match),
        ("Fallback Downgrade", test_fallback_downgrade),
        ("Mismatch Rejection", test_mismatch_rejection),
        ("Missing Required Protocol", test_missing_required),
        ("Rogue Missing Protocol", test_rogue_missing),
        ("Rogue Mismatched Protocol", test_rogue_mismatch),
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
    logger.info("Test 08 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 08 PASSED: All tests successful")
        return 0
    else:
        logger.error("TEST 08 FAILED: Some tests failed")
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
