"""E2E test for Application-Layer Protocol Negotiation (ALPN) / Subprotocols."""

import asyncio
import logging
import ssl
import sys
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, WebTransportClient

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
TEST_URL: Final[str] = SERVER_URL + "subprotocol"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)

logger = logging.getLogger(name="test_subprotocols")


async def test_exact_match() -> bool:
    """Test successful negotiation when exact matching protocols are provided."""
    logger.info("--- Test 09A: Exact Match ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=TEST_URL, subprotocols=["chat-v2", "chat-v1"])
            agreed_protocol = session.subprotocol

            if agreed_protocol == "chat-v2":
                logger.info("SUCCESS: Negotiated correct subprotocol: %s", agreed_protocol)
                return True
            else:
                logger.error("FAILURE: Expected 'chat-v2', got '%s'", agreed_protocol)
                return False
    except Exception as e:
        logger.error("FAILURE: Connection failed unexpectedly: %s", e, exc_info=True)
        return False


async def test_fallback_downgrade() -> bool:
    """Test successful negotiation utilizing fallback/downgrade to a lower protocol version."""
    logger.info("--- Test 09B: Fallback Downgrade ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=TEST_URL, subprotocols=["chat-v3", "chat-v1"])
            agreed_protocol = session.subprotocol

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
    """Test that connection is correctly rejected when no subprotocols match."""
    logger.info("--- Test 09C: Mismatch Rejection ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=TEST_URL, subprotocols=["unknown-proto"])
            logger.error("FAILURE: Connection succeeded but should have been rejected.")
            return False
    except Exception as e:
        logger.info("SUCCESS: Connection rejected successfully due to mismatch. (%s)", e)
        return True


async def test_missing_required() -> bool:
    """Test that connection is correctly rejected when no subprotocols are offered but are required."""
    logger.info("--- Test 09D: Missing Required Subprotocol ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=TEST_URL, subprotocols=None)
            logger.error("FAILURE: Connection succeeded but should have been rejected.")
            return False
    except Exception as e:
        logger.info("SUCCESS: Connection rejected successfully due to missing protocols. (%s)", e)
        return True


async def test_rogue_missing() -> bool:
    """Test that client aborts with WT_ALPN_ERROR when server fails to send a required subprotocol."""
    logger.info("--- Test 09E: Rogue Server Missing Subprotocol ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=TEST_URL, subprotocols=["trigger-missing"])
            logger.error("FAILURE: Connection succeeded but should have been aborted by client.")
            return False
    except Exception as e:
        error_msg = str(e).lower()
        if "0x817b3dd" in error_msg or "subprotocol" in error_msg:
            logger.info("SUCCESS: Client aborted connection correctly due to missing subprotocol. (%s)", e)
            return True
        else:
            logger.error("FAILURE: Connection rejected, but unexpected error: %s", e)
            return False


async def test_rogue_mismatch() -> bool:
    """Test that client aborts with WT_ALPN_ERROR when server negotiates an unrequested subprotocol."""
    logger.info("--- Test 09F: Rogue Server Mismatched Subprotocol ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            await client.connect(url=TEST_URL, subprotocols=["trigger-mismatch"])
            logger.error("FAILURE: Connection succeeded but should have been aborted by client.")
            return False
    except Exception as e:
        error_msg = str(e).lower()
        if "0x817b3dd" in error_msg or "alien-proto" in error_msg or "subprotocol" in error_msg:
            logger.info("SUCCESS: Client aborted connection correctly due to mismatched subprotocol. (%s)", e)
            return True
        else:
            logger.error("FAILURE: Connection rejected, but unexpected error: %s", e)
            return False


async def main() -> int:
    """Run the main entry point for the subprotocols test suite."""
    logger.info("--- Starting Test 09: Application-Layer Protocol Negotiation ---")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Exact Match", test_exact_match),
        ("Fallback Downgrade", test_fallback_downgrade),
        ("Mismatch Rejection", test_mismatch_rejection),
        ("Missing Required Subprotocol", test_missing_required),
        ("Rogue Missing Subprotocol", test_rogue_missing),
        ("Rogue Mismatched Subprotocol", test_rogue_mismatch),
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
    logger.info("Test 09 Results: %d/%d passed", passed, total)

    if passed == total:
        logger.info("TEST 09 PASSED: All subprotocol negotiation tests successful!")
        return 0
    else:
        logger.error("TEST 09 FAILED: Some subprotocol negotiation tests failed!")
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
