"""Benchmark for Datagram Performance."""

import asyncio
import contextlib
import gc
import logging
import ssl
from collections.abc import Generator
from typing import Any, Final, cast

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from pywebtransport import ClientConfig, WebTransportClient, WebTransportSession

SERVER_URL_BASE: Final[str] = "https://127.0.0.1:4433"
WARMUP_ROUNDS: Final[int] = 5
BURST_COUNT: Final[int] = 30000
PAYLOAD_SIZE: Final[int] = 64
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * PAYLOAD_SIZE)

logging.basicConfig(level=logging.CRITICAL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


@pytest.fixture
def client_config() -> ClientConfig:
    return ClientConfig(verify_mode=ssl.CERT_NONE)


class TestDatagramPerformance:

    def test_datagram_send_rate(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/discard"

        with self._sync_session(config=client_config, url=url) as (loop, session):

            async def run_burst() -> None:
                tasks = [session.send_datagram(data=STATIC_VIEW) for _ in range(BURST_COUNT)]
                await asyncio.gather(*tasks)

            for _ in range(WARMUP_ROUNDS):
                loop.run_until_complete(future=run_burst())
            gc.collect()

            benchmark(function_to_benchmark=lambda: loop.run_until_complete(future=run_burst()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        datagrams_per_second = BURST_COUNT / mean_time if mean_time > 0 else 0
        benchmark.extra_info["datagrams_per_second"] = datagrams_per_second

    @contextlib.contextmanager
    def _sync_session(
        self, *, config: ClientConfig, url: str
    ) -> Generator[tuple[asyncio.AbstractEventLoop, WebTransportSession], None, None]:
        with asyncio.Runner() as runner:
            loop = runner.get_loop()
            stack = contextlib.AsyncExitStack()

            try:
                client = loop.run_until_complete(future=stack.enter_async_context(cm=WebTransportClient(config=config)))
                session = loop.run_until_complete(future=client.connect(url=url))
                loop.run_until_complete(future=stack.enter_async_context(cm=session))
                yield loop, session
            finally:
                loop.run_until_complete(future=stack.aclose())
