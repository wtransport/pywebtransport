"""Benchmark for Concurrency and Multiplexing."""

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
CONCURRENT_STREAMS: Final[int] = 100
CONNECTION_COUNT: Final[int] = 50
PAYLOAD_SIZE: Final[int] = 64 * 1024
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * PAYLOAD_SIZE)

logging.basicConfig(level=logging.CRITICAL)


@pytest.fixture(scope="module")
def client_config() -> ClientConfig:
    return ClientConfig(
        verify_mode=ssl.CERT_NONE,
        initial_max_data=1024 * 1024 * 1024,
        initial_max_streams_bidi=2000,
        initial_max_streams_uni=2000,
        flow_control_window=1024 * 1024 * 1024,
        event_queue_capacity=20000,
    )


class TestConcurrency:

    def test_multiplexing_rps(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/discard"

        async def run_multiplexing() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)

                async def stream_worker() -> None:
                    stream = await session.create_bidirectional_stream()
                    await stream.write_all(data=STATIC_VIEW, end_stream=True)
                    await stream.read_all()
                    await stream.close()

                tasks = [asyncio.create_task(coro=stream_worker()) for _ in range(CONCURRENT_STREAMS)]
                await asyncio.gather(*tasks)

                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_multiplexing())
        gc.collect()

        benchmark(lambda: uvloop.run(run_multiplexing()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]

        rps = CONCURRENT_STREAMS / mean_time if mean_time > 0 else 0
        benchmark.extra_info["streams_per_second"] = rps

    def test_connection_rate(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/latency"

        async def run_concurrent_connections() -> None:
            async with WebTransportClient(config=client_config) as client:

                async def connect_worker() -> None:
                    session = await client.connect(url=url)
                    await session.close()

                tasks = [asyncio.create_task(coro=connect_worker()) for _ in range(CONNECTION_COUNT)]
                await asyncio.gather(*tasks)

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_concurrent_connections())
        gc.collect()

        benchmark(lambda: uvloop.run(run_concurrent_connections()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        rate = CONNECTION_COUNT / mean_time if mean_time > 0 else 0
        benchmark.extra_info["connections_per_second"] = rate
