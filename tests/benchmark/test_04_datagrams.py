"""Benchmark for Datagram Performance."""

import asyncio
import contextlib
import gc
import logging
import ssl
from collections.abc import Generator
from typing import Any, Final, cast

import pytest
import uvloop
from pytest_benchmark.fixture import BenchmarkFixture

from pywebtransport import ClientConfig, WebTransportClient, WebTransportSession

SERVER_URL_BASE: Final[str] = "https://127.0.0.1:4433"
WARMUP_ROUNDS: Final[int] = 5
BURST_COUNT: Final[int] = 30000
PAYLOAD_SIZE: Final[int] = 64
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * PAYLOAD_SIZE)

logging.basicConfig(level=logging.CRITICAL)


@pytest.fixture(scope="module")
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
                loop.run_until_complete(run_burst())
            gc.collect()

            benchmark(lambda: loop.run_until_complete(run_burst()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        pps = BURST_COUNT / mean_time if mean_time > 0 else 0
        benchmark.extra_info["send_rate_pps"] = pps

    @contextlib.contextmanager
    def _sync_session(
        self, config: ClientConfig, url: str
    ) -> Generator[tuple[asyncio.AbstractEventLoop, WebTransportSession], None, None]:
        loop = uvloop.new_event_loop()
        asyncio.set_event_loop(loop)
        stack = contextlib.AsyncExitStack()
        session: WebTransportSession | None = None

        try:
            client = loop.run_until_complete(stack.enter_async_context(WebTransportClient(config=config)))
            session = loop.run_until_complete(client.connect(url=url))
            yield loop, session
        finally:
            if session is not None:
                loop.run_until_complete(session.close())
            loop.run_until_complete(stack.aclose())
            loop.close()
            asyncio.set_event_loop(None)
