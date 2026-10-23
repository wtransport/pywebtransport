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


@pytest.fixture(scope="module")
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
            asyncio.run(run_handshake())
        gc.collect()

        benchmark(lambda: asyncio.run(run_handshake()))

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

            async def run_req_res() -> None:
                stream = await session.create_bidirectional_stream()
                await stream.write_all(data=payload, end_stream=True)
                await stream.read_all()

            for _ in range(WARMUP_ROUNDS):
                loop.run_until_complete(run_req_res())
            gc.collect()

            benchmark(lambda: loop.run_until_complete(run_req_res()))

        stats = cast(dict[str, Any], benchmark.stats)
        benchmark.extra_info[f"req_res_{label}_median_ms"] = stats["median"] * 1000
        benchmark.extra_info[f"req_res_{label}_max_ms"] = stats["max"] * 1000
        benchmark.extra_info[f"req_res_{label}_min_ms"] = stats["min"] * 1000

    def test_datagram_rtt(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/echo"
        payload = PAYLOAD_64B

        with self._sync_session(config=client_config, url=url) as (loop, session):

            async def run_dgram_rtt() -> None:
                await session.send_datagram(data=payload)
                await asyncio.wait_for(session.read_datagram(), timeout=1.0)

            for _ in range(WARMUP_ROUNDS):
                loop.run_until_complete(run_dgram_rtt())
            gc.collect()

            benchmark(lambda: loop.run_until_complete(run_dgram_rtt()))

        stats = cast(dict[str, Any], benchmark.stats)
        benchmark.extra_info["dgram_rtt_median_ms"] = stats["median"] * 1000
        benchmark.extra_info["dgram_rtt_max_ms"] = stats["max"] * 1000
        benchmark.extra_info["dgram_rtt_min_ms"] = stats["min"] * 1000

    @contextlib.contextmanager
    def _sync_session(
        self, *, config: ClientConfig, url: str
    ) -> Generator[tuple[asyncio.AbstractEventLoop, WebTransportSession], None, None]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stack = contextlib.AsyncExitStack()

        try:
            client = loop.run_until_complete(stack.enter_async_context(WebTransportClient(config=config)))
            session = loop.run_until_complete(client.connect(url=url))
            loop.run_until_complete(stack.enter_async_context(session))
            yield loop, session
        finally:
            loop.run_until_complete(stack.aclose())
            loop.close()
            asyncio.set_event_loop(None)
