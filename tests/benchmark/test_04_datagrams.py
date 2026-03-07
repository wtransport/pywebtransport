"""Benchmark for Datagram Performance."""

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
WARMUP_ROUNDS: Final[int] = 5
BURST_COUNT: Final[int] = 10000
PAYLOAD_SIZE: Final[int] = 64
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * PAYLOAD_SIZE)

logging.basicConfig(level=logging.CRITICAL)


@pytest.fixture(scope="module")
def client_config() -> ClientConfig:
    return ClientConfig(verify_mode=ssl.CERT_NONE)


class TestDatagramPerformance:

    def test_datagram_send_rate(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/discard"

        async def run_burst() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                tasks = [session.send_datagram(data=STATIC_VIEW) for _ in range(BURST_COUNT)]
                await asyncio.gather(*tasks)
                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_burst())
        gc.collect()

        benchmark(lambda: uvloop.run(run_burst()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        pps = BURST_COUNT / mean_time if mean_time > 0 else 0
        benchmark.extra_info["send_rate_pps"] = pps
