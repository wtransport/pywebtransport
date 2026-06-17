"""Benchmark for Latency and RTT metrics."""

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

from pywebtransport import ClientConfig, Event, WebTransportClient, WebTransportSession
from pywebtransport.types import EventType

SERVER_URL_BASE: Final[str] = "https://127.0.0.1:4433"
WARMUP_ROUNDS: Final[int] = 5
PAYLOAD_64B: Final[bytes] = b"x" * 64
PAYLOAD_1KB: Final[bytes] = b"x" * 1024

logging.basicConfig(level=logging.CRITICAL)


@pytest.fixture(scope="module")
def client_config() -> ClientConfig:
    return ClientConfig(verify_mode=ssl.CERT_NONE)


class TestLatency:

    def test_handshake_latency(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/latency"

        async def run_handshake() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_handshake())
        gc.collect()

        benchmark(lambda: uvloop.run(run_handshake()))

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
            echo_future: asyncio.Future[bool] = loop.create_future()

            async def on_dgram(event: Event) -> None:
                if isinstance(event.data, dict) and (data := event.data.get("data")):
                    if data == payload and not echo_future.done():
                        echo_future.set_result(True)

            session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_dgram)

            async def run_dgram_rtt() -> None:
                nonlocal echo_future
                echo_future = loop.create_future()
                await session.send_datagram(data=payload)
                await asyncio.wait_for(echo_future, timeout=1.0)

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
