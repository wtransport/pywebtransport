"""E2E test for TLS keying material export feature."""

import asyncio
import json
import logging
import ssl
import sys
from collections.abc import Awaitable, Callable
from typing import Final

from pywebtransport import ClientConfig, ConnectionError, SessionError, WebTransportClient

SERVER_HOST: Final[str] = "127.0.0.1"
SERVER_PORT: Final[int] = 4433
SERVER_URL: Final[str] = f"https://{SERVER_HOST}:{SERVER_PORT}/"
DEBUG_MODE: Final[bool] = "--debug" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)

logger = logging.getLogger(name="test_tls_export")


async def test_e2e_symmetry() -> bool:
    """Test that client and server derive the exact same keying material."""
    logger.info("--- Test 10A: E2E Symmetry ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL + "export-keying-material")

            label = "EXPORTER-WebTransport-Test"
            context = b"\xde\xad\xbe\xef"
            length = 32

            stream = await session.create_bidirectional_stream()
            req_payload = json.dumps(
                obj={
                    "label": label,
                    "context_hex": context.hex(),
                    "length": length,
                }
            ).encode(encoding="utf-8")

            await stream.write_all(data=req_payload, end_stream=True)

            client_key = await session.export_keying_material(label=label, context=context, length=length)
            logger.info("Client locally derived key: %s", client_key.hex())

            server_key = await stream.read_all()
            logger.info("Server remotely derived key: %s", server_key.hex())

            if len(client_key) != length:
                logger.error("FAILURE: Derived key length mismatch. Expected %d, got %d", length, len(client_key))
                return False

            if client_key == server_key:
                logger.info("SUCCESS: Client and server derived identical keying material.")
                return True
            else:
                logger.error("FAILURE: Keying material mismatch between client and server!")
                return False
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_context_and_label_isolation() -> bool:
    """Test that different labels or contexts result in different keying material."""
    logger.info("--- Test 10B: Context & Label Isolation ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)

            base_label = "test-label-A"
            base_context = b"context-alpha"

            key_base = await session.export_keying_material(label=base_label, context=base_context, length=16)

            key_diff_label = await session.export_keying_material(label="test-label-B", context=base_context, length=16)

            key_diff_context = await session.export_keying_material(
                label=base_label, context=b"context-beta", length=16
            )

            logger.info("Base Key:           %s", key_base.hex())
            logger.info("Diff Label Key:     %s", key_diff_label.hex())
            logger.info("Diff Context Key:   %s", key_diff_context.hex())

            if key_base == key_diff_label:
                logger.error("FAILURE: Different labels produced the same key material.")
                return False

            if key_base == key_diff_context:
                logger.error("FAILURE: Different contexts produced the same key material.")
                return False

            logger.info("SUCCESS: Key derivation isolation verified correctly.")
            return True
    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def test_error_handling() -> bool:
    """Test error handling for invalid states during key derivation."""
    logger.info("--- Test 10C: Error Handling & Boundaries ---")
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    try:
        async with WebTransportClient(config=config) as client:
            session = await client.connect(url=SERVER_URL)

            await session.close()

            try:
                await session.export_keying_material(label="invalid-state", context=b"test", length=16)
                logger.error("FAILURE: Exporting keying material on a closed session did not raise an exception.")
                return False
            except (ConnectionError, SessionError) as e:
                logger.info("SUCCESS: Properly caught exception for closed session: %s", e)
                return True

    except Exception as e:
        logger.error("FAILURE: An unexpected error occurred: %s", e, exc_info=True)
        return False


async def main() -> int:
    """Run the main entry point for the TLS export test suite."""
    logger.info("--- Starting Test 10: TLS Keying Material Export ---")

    tests: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("E2E Symmetry", test_e2e_symmetry),
        ("Context & Label Isolation", test_context_and_label_isolation),
        ("Error Handling & Boundaries", test_error_handling),
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
        logger.info("TEST 10 PASSED: All TLS export tests successful!")
        return 0
    else:
        logger.error("TEST 10 FAILED: Some TLS export tests failed!")
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
