"""Benchmark for Resource Utilization."""

import asyncio
import gc
import logging
import os
import ssl
from typing import Final

import psutil
import pytest
import uvloop
from pytest_benchmark.fixture import BenchmarkFixture

from pywebtransport import ClientConfig, WebTransportClient, WebTransportSession

SERVER_URL_BASE: Final[str] = "https://127.0.0.1:4433"
CONNECTION_COUNT: Final[int] = 3000
STABILIZATION_SECONDS: Final[float] = 5.0

logging.basicConfig(level=logging.CRITICAL)


@pytest.fixture(scope="module")
def client_config() -> ClientConfig:
    return ClientConfig(verify_mode=ssl.CERT_NONE, connect_timeout=60.0, max_connections=4000)


class TestResources:

    def test_idle_memory_footprint(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/latency"
        process = psutil.Process(pid=os.getpid())

        def run_measurement() -> float:
            async def run_full_cycle() -> float:
                gc.collect()
                baseline_rss = float(process.memory_info().rss)

                sessions: list[WebTransportSession] = []

                async with WebTransportClient(config=client_config) as client:
                    try:
                        semaphore = asyncio.Semaphore(value=100)

                        async def connect_one() -> None:
                            async with semaphore:
                                session = await client.connect(url=url)
                                sessions.append(session)

                        tasks = [asyncio.create_task(coro=connect_one()) for _ in range(CONNECTION_COUNT)]
                        await asyncio.gather(*tasks)

                        await asyncio.sleep(delay=STABILIZATION_SECONDS)

                        gc.collect()

                        current_rss = float(process.memory_info().rss)
                        return max(0.0, current_rss - baseline_rss)

                    finally:
                        if sessions:
                            close_sem = asyncio.Semaphore(value=100)

                            async def close_one(s: WebTransportSession) -> None:
                                async with close_sem:
                                    if not s.is_closed:
                                        await s.close()

                            tasks = [asyncio.create_task(coro=close_one(s)) for s in sessions]
                            await asyncio.gather(*tasks)

            return uvloop.run(run_full_cycle())

        memory_increase = benchmark.pedantic(
            target=run_measurement, iterations=1, rounds=1
        )  # type: ignore[no-untyped-call]

        kb_per_connection = (memory_increase / 1024) / CONNECTION_COUNT
        benchmark.extra_info["memory_per_idle_connection_kb"] = kb_per_connection
