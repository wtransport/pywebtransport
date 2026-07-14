"""End-to-end tests for the pywebtransport client."""

import asyncio
import ssl
import subprocess
import sys
from collections.abc import AsyncGenerator

import pytest
from pytest_asyncio import fixture as asyncio_fixture

from pywebtransport import ClientConfig, ClientError, WebTransportClient

from .test_01_basic_connection import test_basic_connection as run_01_basic_connection
from .test_02_simple_stream import test_multiple_messages as run_02_multiple_messages
from .test_02_simple_stream import test_simple_echo as run_02_simple_echo
from .test_02_simple_stream import test_stream_creation as run_02_stream_creation
from .test_03_concurrent_streams import test_concurrent_streams as run_03_concurrent_streams
from .test_03_concurrent_streams import test_sequential_streams as run_03_sequential_streams
from .test_03_concurrent_streams import test_stream_lifecycle as run_03_stream_lifecycle
from .test_03_concurrent_streams import test_stream_stress as run_03_stream_stress
from .test_04_data_transfer import test_binary_data as run_04_binary_data
from .test_04_data_transfer import test_chunked_transfer as run_04_chunked_transfer
from .test_04_data_transfer import test_medium_data as run_04_medium_data
from .test_04_data_transfer import test_performance_benchmark as run_04_performance_benchmark
from .test_04_data_transfer import test_small_data as run_04_small_data
from .test_05_datagrams import test_basic_datagram as run_05_basic_datagram
from .test_05_datagrams import test_datagram_burst as run_05_datagram_burst
from .test_05_datagrams import test_datagram_sizes as run_05_datagram_sizes
from .test_05_datagrams import test_multiple_datagrams as run_05_multiple_datagrams
from .test_06_error_handling import test_connection_timeout as run_06_connection_timeout
from .test_06_error_handling import test_invalid_server_address as run_06_invalid_address
from .test_06_error_handling import test_malformed_operations as run_06_malformed_operations
from .test_06_error_handling import test_read_timeout as run_06_read_timeout
from .test_06_error_handling import test_session_closure_handling as run_06_session_closure
from .test_06_error_handling import test_stream_errors as run_06_stream_errors
from .test_07_advanced_features import test_client_statistics as run_07_client_statistics
from .test_07_advanced_features import test_connection_info as run_07_connection_info
from .test_07_advanced_features import test_datagram_statistics as run_07_datagram_statistics
from .test_07_advanced_features import test_performance_monitoring as run_07_performance_monitoring
from .test_07_advanced_features import test_server_diagnostics as run_07_server_diagnostics
from .test_07_advanced_features import test_session_lifecycle_events as run_07_session_lifecycle_events
from .test_07_advanced_features import test_session_statistics as run_07_session_statistics
from .test_07_advanced_features import test_stream_management_diagnostics as run_07_stream_management_diagnostics
from .test_08_protocol_negotiation import test_exact_match as run_08_exact_match
from .test_08_protocol_negotiation import test_fallback_downgrade as run_08_fallback_downgrade
from .test_08_protocol_negotiation import test_mismatch_rejection as run_08_mismatch_rejection
from .test_08_protocol_negotiation import test_missing_required as run_08_missing_required
from .test_08_protocol_negotiation import test_rogue_mismatch as run_08_rogue_mismatch
from .test_08_protocol_negotiation import test_rogue_missing as run_08_rogue_missing
from .test_09_tls_export import test_context_and_label_isolation as run_09_context_and_label_isolation
from .test_09_tls_export import test_e2e_symmetry as run_09_e2e_symmetry
from .test_09_tls_export import test_error_handling as run_09_error_handling
from .test_10_optimistic_sending import test_optimistic_basic_connection as run_10_optimistic_basic_connection
from .test_10_optimistic_sending import test_optimistic_early_datagram_send as run_10_optimistic_early_datagram_send
from .test_10_optimistic_sending import (
    test_optimistic_early_send_before_rejection as run_10_optimistic_early_send_before_rejection,
)
from .test_10_optimistic_sending import test_optimistic_early_stream_write as run_10_optimistic_early_stream_write
from .test_10_optimistic_sending import test_optimistic_lifecycle_events as run_10_optimistic_lifecycle_events
from .test_10_optimistic_sending import (
    test_optimistic_rejection_via_ensure_ready as run_10_optimistic_rejection_via_ensure_ready,
)
from .test_10_optimistic_sending import test_server_buffers_early_stream_data as run_10_server_buffers_early_stream_data
from .test_10_optimistic_sending import test_server_datagram_overflow_drop as run_10_server_datagram_overflow_drop
from .test_10_optimistic_sending import test_server_stream_overflow_drop as run_10_server_stream_overflow_drop


async def _is_server_ready() -> bool:
    config = ClientConfig(verify_mode=ssl.CERT_NONE)
    for _ in range(60):
        try:
            async with WebTransportClient(config=config) as client:
                session = await client.connect(url="https://127.0.0.1:4433/health")
                await session.close()
                return True
        except (ClientError, asyncio.TimeoutError):
            await asyncio.sleep(delay=0.5)
    return False


@asyncio_fixture(scope="module", autouse=True)
async def e2e_server() -> AsyncGenerator[None, None]:
    server_command = [
        sys.executable,
        "-m",
        "coverage",
        "run",
        "--source=src/pywebtransport",
        "--parallel-mode",
        "-m",
        "tests.e2e.test_00_e2e_server",
    ]

    server_proc = subprocess.Popen(args=server_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    is_ready = await _is_server_ready()

    if not is_ready or server_proc.poll() is not None:
        stdout, stderr = server_proc.communicate()
        pytest.fail(
            reason=f"E2E server failed to start or become ready. Exit code: {server_proc.returncode}\n"
            f"STDOUT:\n{stdout}\n"
            f"STDERR:\n{stderr}",
            pytrace=False,
        )

    yield

    server_proc.terminate()
    try:
        server_proc.communicate(timeout=5.0)
    except subprocess.TimeoutExpired:
        server_proc.kill()
        server_proc.communicate()


@pytest.mark.asyncio
class TestE2eSuite:
    async def test_01_basic_connection(self) -> None:
        assert await run_01_basic_connection() is True, "Basic connection failed"

    async def test_02_stream_creation(self) -> None:
        assert await run_02_stream_creation() is True, "Stream creation failed"

    async def test_02_simple_echo(self) -> None:
        assert await run_02_simple_echo() is True, "Simple echo failed"

    async def test_02_multiple_messages(self) -> None:
        assert await run_02_multiple_messages() is True, "Multiple messages on separate streams failed"

    async def test_03_sequential_streams(self) -> None:
        assert await run_03_sequential_streams() is True, "Sequential streams failed"

    async def test_03_concurrent_streams(self) -> None:
        assert await run_03_concurrent_streams() is True, "Concurrent streams failed"

    async def test_03_stream_lifecycle(self) -> None:
        assert await run_03_stream_lifecycle() is True, "Stream lifecycle management failed"

    async def test_03_stream_stress(self) -> None:
        assert await run_03_stream_stress() is True, "Stream stress test failed"

    async def test_04_small_data(self) -> None:
        assert await run_04_small_data() is True, "Small data transfer failed"

    async def test_04_medium_data(self) -> None:
        assert await run_04_medium_data() is True, "Medium data transfer failed"

    async def test_04_chunked_transfer(self) -> None:
        assert await run_04_chunked_transfer() is True, "Chunked data transfer failed"

    async def test_04_binary_data(self) -> None:
        assert await run_04_binary_data() is True, "Binary data transfer failed"

    async def test_04_performance_benchmark(self) -> None:
        assert await run_04_performance_benchmark() is True, "Performance benchmark failed"

    async def test_05_basic_datagram(self) -> None:
        assert await run_05_basic_datagram() is True, "Basic datagram send failed"

    async def test_05_multiple_datagrams(self) -> None:
        assert await run_05_multiple_datagrams() is True, "Multiple datagrams send failed"

    async def test_05_datagram_sizes(self) -> None:
        assert await run_05_datagram_sizes() is True, "Datagram size handling failed"

    async def test_05_datagram_burst(self) -> None:
        assert await run_05_datagram_burst() is True, "Datagram burst test failed"

    async def test_06_connection_timeout(self) -> None:
        assert await run_06_connection_timeout() is True, "Connection timeout handling failed"

    async def test_06_invalid_address(self) -> None:
        assert await run_06_invalid_address() is True, "Invalid server address handling failed"

    async def test_06_stream_errors(self) -> None:
        assert await run_06_stream_errors() is True, "Stream error handling failed"

    async def test_06_read_timeout(self) -> None:
        assert await run_06_read_timeout() is True, "Read timeout handling failed"

    async def test_06_session_closure(self) -> None:
        assert await run_06_session_closure() is True, "Session closure handling failed"

    async def test_06_malformed_operations(self) -> None:
        assert await run_06_malformed_operations() is True, "Malformed API operations handling failed"

    async def test_07_session_statistics(self) -> None:
        assert await run_07_session_statistics() is True, "Session statistics retrieval failed"

    async def test_07_connection_info(self) -> None:
        assert await run_07_connection_info() is True, "Connection info retrieval failed"

    async def test_07_client_statistics(self) -> None:
        assert await run_07_client_statistics() is True, "Client statistics retrieval failed"

    async def test_07_stream_management_diagnostics(self) -> None:
        assert await run_07_stream_management_diagnostics() is True, "Stream management diagnostics test failed"

    async def test_07_datagram_statistics(self) -> None:
        assert await run_07_datagram_statistics() is True, "Datagram statistics retrieval failed"

    async def test_07_performance_monitoring(self) -> None:
        assert await run_07_performance_monitoring() is True, "Performance monitoring test failed"

    async def test_07_session_lifecycle_events(self) -> None:
        assert await run_07_session_lifecycle_events() is True, "Session lifecycle events tracking failed"

    async def test_07_server_diagnostics(self) -> None:
        assert await run_07_server_diagnostics() is True, "Server diagnostics retrieval failed"

    async def test_08_exact_match(self) -> None:
        assert await run_08_exact_match() is True, "Exact match protocol negotiation failed"

    async def test_08_fallback_downgrade(self) -> None:
        assert await run_08_fallback_downgrade() is True, "Fallback downgrade protocol negotiation failed"

    async def test_08_mismatch_rejection(self) -> None:
        assert await run_08_mismatch_rejection() is True, "Mismatch rejection protocol test failed"

    async def test_08_missing_required(self) -> None:
        assert await run_08_missing_required() is True, "Missing required protocol test failed"

    async def test_08_rogue_missing(self) -> None:
        assert await run_08_rogue_missing() is True, "Rogue server missing protocol test failed"

    async def test_08_rogue_mismatch(self) -> None:
        assert await run_08_rogue_mismatch() is True, "Rogue server mismatched protocol test failed"

    async def test_09_e2e_symmetry(self) -> None:
        assert await run_09_e2e_symmetry() is True, "TLS export E2E symmetry test failed"

    async def test_09_context_and_label_isolation(self) -> None:
        assert await run_09_context_and_label_isolation() is True, "TLS export context and label isolation test failed"

    async def test_09_error_handling(self) -> None:
        assert await run_09_error_handling() is True, "TLS export error handling test failed"

    async def test_10_optimistic_basic_connection(self) -> None:
        assert await run_10_optimistic_basic_connection() is True, "Optimistic basic connection failed"

    async def test_10_optimistic_lifecycle_events(self) -> None:
        assert await run_10_optimistic_lifecycle_events() is True, "Optimistic lifecycle events failed"

    async def test_10_optimistic_early_stream_write(self) -> None:
        assert await run_10_optimistic_early_stream_write() is True, "Optimistic early stream write failed"

    async def test_10_optimistic_early_datagram_send(self) -> None:
        assert await run_10_optimistic_early_datagram_send() is True, "Optimistic early datagram send failed"

    async def test_10_optimistic_rejection_via_ensure_ready(self) -> None:
        assert (
            await run_10_optimistic_rejection_via_ensure_ready() is True
        ), "Optimistic rejection via ensure_ready failed"

    async def test_10_optimistic_early_send_before_rejection(self) -> None:
        assert (
            await run_10_optimistic_early_send_before_rejection() is True
        ), "Optimistic early send before rejection failed"

    async def test_10_server_buffers_early_stream_data(self) -> None:
        assert await run_10_server_buffers_early_stream_data() is True, "Server buffering of early stream data failed"

    async def test_10_server_stream_overflow_drop(self) -> None:
        assert await run_10_server_stream_overflow_drop() is True, "Server stream overflow drop failed"

    async def test_10_server_datagram_overflow_drop(self) -> None:
        assert await run_10_server_datagram_overflow_drop() is True, "Server datagram overflow drop failed"
