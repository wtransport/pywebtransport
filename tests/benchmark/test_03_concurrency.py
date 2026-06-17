"""Benchmark for Concurrency and Multiplexing."""

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
CONCURRENT_STREAMS: Final[int] = 20
CONCURRENT_CONNECTIONS: Final[int] = 50
PAYLOAD_SIZE: Final[int] = 65536
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * PAYLOAD_SIZE)

logging.basicConfig(level=logging.CRITICAL)


@pytest.fixture(scope="module")
def client_config() -> ClientConfig:
    return ClientConfig(verify_mode=ssl.CERT_NONE)


class TestConcurrency:

    def test_multiplexing_rps(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/discard"

        with self._sync_session(config=client_config, url=url) as (loop, session):

            async def stream_worker() -> None:
                stream = await session.create_bidirectional_stream()
                await stream.write_all(data=STATIC_VIEW, end_stream=True)
                await stream.read_all()
                await stream.close()

            async def run_multiplexing() -> None:
                tasks = [asyncio.create_task(coro=stream_worker()) for _ in range(CONCURRENT_STREAMS)]
                await asyncio.gather(*tasks)

            for _ in range(WARMUP_ROUNDS):
                loop.run_until_complete(run_multiplexing())
            gc.collect()

            benchmark(lambda: loop.run_until_complete(run_multiplexing()))

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

                tasks = [asyncio.create_task(coro=connect_worker()) for _ in range(CONCURRENT_CONNECTIONS)]
                await asyncio.gather(*tasks)

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_concurrent_connections())
        gc.collect()

        benchmark(lambda: uvloop.run(run_concurrent_connections()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        rate = CONCURRENT_CONNECTIONS / mean_time if mean_time > 0 else 0
        benchmark.extra_info["connections_per_second"] = rate

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
