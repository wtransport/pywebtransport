"""Benchmark for Multiplexing Efficiency."""

import asyncio
import gc
import logging
import ssl
from typing import Any, Final, cast

import pytest
import uvloop
from pytest_benchmark.fixture import BenchmarkFixture

from pywebtransport import ClientConfig, WebTransportClient

SERVER_URL_BASE: Final[str] = "https://127.0.0.1:4433"
WARMUP_ROUNDS: Final[int] = 3
CONCURRENT_STREAMS: Final[int] = 1000
CONNECTION_COUNT: Final[int] = 100
PAYLOAD_SIZE: Final[int] = 64 * 1024
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * PAYLOAD_SIZE)

logging.basicConfig(level=logging.CRITICAL)


@pytest.fixture(scope="module")
def client_config() -> ClientConfig:
    return ClientConfig(
        verify_mode=ssl.CERT_NONE,
        write_timeout=60.0,
        stream_creation_timeout=30.0,
        initial_max_data=1024 * 1024 * 1024,
        initial_max_streams_bidi=2000,
        initial_max_streams_uni=2000,
        flow_control_window_size=1024 * 1024 * 1024,
        max_event_queue_size=20000,
    )


class TestMultiplexingEfficiency:

    def test_aggregate_throughput(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/discard"

        async def stream_worker(*, session: Any) -> None:
            stream = await session.create_unidirectional_stream()
            await stream.write_all(data=STATIC_VIEW, end_stream=True)

        async def run_multiplexing() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                tasks = [asyncio.create_task(coro=stream_worker(session=session)) for _ in range(CONCURRENT_STREAMS)]
                try:
                    await asyncio.gather(*tasks)
                finally:
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_multiplexing())
        gc.collect()

        benchmark(lambda: uvloop.run(run_multiplexing()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        total_mb = (PAYLOAD_SIZE * CONCURRENT_STREAMS) / (1024 * 1024)
        throughput = total_mb / mean_time if mean_time > 0 else 0
        benchmark.extra_info["aggregate_throughput_mb_s"] = throughput

    def test_connection_rate(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/latency"

        async def run_sequential_connections() -> None:
            for _ in range(CONNECTION_COUNT):
                async with WebTransportClient(config=client_config) as client:
                    session = await client.connect(url=url)
                    await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_sequential_connections())
        gc.collect()

        benchmark(lambda: uvloop.run(run_sequential_connections()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        rate = CONNECTION_COUNT / mean_time if mean_time > 0 else 0
        benchmark.extra_info["connections_per_second"] = rate
