"""Benchmark for Latency and RTT metrics."""

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
PAYLOAD_64B: Final[bytes] = b"x" * 64
PAYLOAD_1KB: Final[bytes] = b"x" * 1024

logging.basicConfig(level=logging.CRITICAL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


@pytest.fixture
def client_config() -> ClientConfig:
    return ClientConfig(verify_mode=ssl.CERT_NONE)


class TestLatency:

    def test_handshake_latency(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/latency"

        async def run_handshake() -> None:
            async with WebTransportClient(config=client_config) as client:
                async with await client.connect(url=url):
                    pass

        for _ in range(WARMUP_ROUNDS):
            asyncio.run(main=run_handshake())
        gc.collect()

        benchmark(function_to_benchmark=lambda: asyncio.run(main=run_handshake()))

        stats = cast(dict[str, Any], benchmark.stats)
        benchmark.extra_info["median_ms"] = stats["median"] * 1000
        benchmark.extra_info["max_ms"] = stats["max"] * 1000
        benchmark.extra_info["min_ms"] = stats["min"] * 1000

    @pytest.mark.parametrize(
        argnames="payload,label", argvalues=[(PAYLOAD_64B, "64b"), (PAYLOAD_1KB, "1kb")], ids=["64b", "1kb"]
    )
    def test_request_response_latency(
        self, *, benchmark: BenchmarkFixture, client_config: ClientConfig, payload: bytes, label: str
    ) -> None:
        url = f"{SERVER_URL_BASE}/latency"

        with self._sync_session(config=client_config, url=url) as (loop, session):

            async def run_request_response() -> None:
                stream = await session.create_bidirectional_stream()
                await stream.write_all(data=payload, end_stream=True)
                await stream.read_all()

            for _ in range(WARMUP_ROUNDS):
                loop.run_until_complete(future=run_request_response())
            gc.collect()

            benchmark(function_to_benchmark=lambda: loop.run_until_complete(future=run_request_response()))

        stats = cast(dict[str, Any], benchmark.stats)
        benchmark.extra_info[f"request_response_{label}_median_ms"] = stats["median"] * 1000
        benchmark.extra_info[f"request_response_{label}_max_ms"] = stats["max"] * 1000
        benchmark.extra_info[f"request_response_{label}_min_ms"] = stats["min"] * 1000

    def test_datagram_rtt(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/echo"
        payload = PAYLOAD_64B

        with self._sync_session(config=client_config, url=url) as (loop, session):

            async def run_datagram_rtt() -> None:
                await session.send_datagram(data=payload)
                await asyncio.wait_for(fut=session.read_datagram(), timeout=1.0)

            for _ in range(WARMUP_ROUNDS):
                loop.run_until_complete(future=run_datagram_rtt())
            gc.collect()

            benchmark(function_to_benchmark=lambda: loop.run_until_complete(future=run_datagram_rtt()))

        stats = cast(dict[str, Any], benchmark.stats)
        benchmark.extra_info["datagram_rtt_median_ms"] = stats["median"] * 1000
        benchmark.extra_info["datagram_rtt_max_ms"] = stats["max"] * 1000
        benchmark.extra_info["datagram_rtt_min_ms"] = stats["min"] * 1000

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
