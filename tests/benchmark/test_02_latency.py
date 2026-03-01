"""Benchmark for Latency and RTT metrics."""

import asyncio
import gc
import logging
import ssl
from typing import Any, Final, cast

import pytest
import uvloop
from pytest_benchmark.fixture import BenchmarkFixture

from pywebtransport import ClientConfig, Event, WebTransportClient
from pywebtransport.types import EventType

SERVER_URL_BASE: Final[str] = "https://127.0.0.1:4433"
WARMUP_ROUNDS: Final[int] = 10
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

        async def run_req_res() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                stream = await session.create_bidirectional_stream()
                await stream.write_all(data=payload, end_stream=True)
                await stream.read_all()
                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_req_res())
        gc.collect()

        benchmark(lambda: uvloop.run(run_req_res()))

        stats = cast(dict[str, Any], benchmark.stats)
        benchmark.extra_info[f"req_res_{label}_median_ms"] = stats["median"] * 1000
        benchmark.extra_info[f"req_res_{label}_max_ms"] = stats["max"] * 1000
        benchmark.extra_info[f"req_res_{label}_min_ms"] = stats["min"] * 1000

    def test_datagram_rtt(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/echo"
        payload = PAYLOAD_64B

        async def run_dgram_rtt() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                loop = asyncio.get_running_loop()
                echo_received = loop.create_future()

                async def on_dgram(event: Event) -> None:
                    if isinstance(event.data, dict):
                        data = event.data.get("data")
                        if data == payload:
                            if not echo_received.done():
                                echo_received.set_result(True)

                session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_dgram)
                await session.send_datagram(data=payload)
                await echo_received
                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_dgram_rtt())
        gc.collect()

        benchmark(lambda: uvloop.run(run_dgram_rtt()))

        stats = cast(dict[str, Any], benchmark.stats)
        benchmark.extra_info["dgram_rtt_median_ms"] = stats["median"] * 1000
        benchmark.extra_info["dgram_rtt_max_ms"] = stats["max"] * 1000
        benchmark.extra_info["dgram_rtt_min_ms"] = stats["min"] * 1000
