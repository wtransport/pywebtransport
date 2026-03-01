"""Benchmark for Stream Throughput."""

import asyncio
import gc
import logging
import ssl
from collections.abc import Callable, Coroutine
from typing import Any, Final, cast

import pytest
import uvloop
from pytest_benchmark.fixture import BenchmarkFixture

from pywebtransport import ClientConfig, WebTransportClient, WebTransportSession

SERVER_URL_BASE: Final[str] = "https://127.0.0.1:4433"
WARMUP_ROUNDS: Final[int] = 5
PAYLOAD_SIZE: Final[int] = 1024 * 1024
STREAMS_PER_ROUND: Final[int] = 10
STATIC_VIEW: Final[memoryview] = memoryview(b"x" * PAYLOAD_SIZE)

logging.basicConfig(level=logging.CRITICAL)


@pytest.fixture(scope="module")
def client_config() -> ClientConfig:
    return ClientConfig(
        verify_mode=ssl.CERT_NONE,
        initial_max_data=100 * 1024 * 1024,
        initial_max_streams_bidi=1000,
        initial_max_streams_uni=1000,
        flow_control_window_size=100 * 1024 * 1024,
        max_stream_read_buffer=200 * 1024 * 1024,
        max_stream_write_buffer=200 * 1024 * 1024,
        max_event_queue_size=10000,
    )


class TestStreamThroughput:

    def test_upload_throughput(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        async def upload_worker(session: WebTransportSession) -> int:
            stream = await session.create_bidirectional_stream()
            await stream.write_all(data=STATIC_VIEW, end_stream=True)
            await stream.read_all()
            return PAYLOAD_SIZE

        self._run_benchmark_scenario(
            benchmark=benchmark, client_config=client_config, endpoint="/discard", stream_handler=upload_worker
        )

    def test_download_throughput(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        cmd = str(PAYLOAD_SIZE).encode()

        async def download_worker(session: WebTransportSession) -> int:
            stream = await session.create_bidirectional_stream()
            await stream.write(data=cmd)
            received = 0
            while True:
                chunk = await stream.read(max_bytes=PAYLOAD_SIZE)
                if not chunk:
                    break
                received += len(chunk)
            await stream.close()
            return received

        self._run_benchmark_scenario(
            benchmark=benchmark, client_config=client_config, endpoint="/produce", stream_handler=download_worker
        )

    def test_duplex_throughput(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        async def duplex_worker(session: WebTransportSession) -> int:
            stream = await session.create_bidirectional_stream()

            async def sender() -> int:
                await stream.write_all(data=STATIC_VIEW, end_stream=True)
                return PAYLOAD_SIZE

            async def receiver() -> int:
                received = 0
                while True:
                    chunk = await stream.read(max_bytes=PAYLOAD_SIZE)
                    if not chunk:
                        break
                    received += len(chunk)
                return received

            results = await asyncio.gather(sender(), receiver())
            await stream.close()
            return sum(results)

        self._run_benchmark_scenario(
            benchmark=benchmark, client_config=client_config, endpoint="/duplex", stream_handler=duplex_worker
        )

    def _run_benchmark_scenario(
        self,
        *,
        benchmark: BenchmarkFixture,
        client_config: ClientConfig,
        endpoint: str,
        stream_handler: Callable[[WebTransportSession], Coroutine[Any, Any, int]],
    ) -> None:
        url = f"{SERVER_URL_BASE}{endpoint}"

        async def run_scenario() -> int:
            total_bytes = 0
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                tasks = [stream_handler(session) for _ in range(STREAMS_PER_ROUND)]
                results = await asyncio.gather(*tasks)
                total_bytes = sum(results)
                await session.close()
            return total_bytes

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_scenario())
        gc.collect()

        result_bytes = benchmark(lambda: uvloop.run(run_scenario()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]

        total_mb = result_bytes / (1024 * 1024)
        throughput = total_mb / mean_time if mean_time > 0 else 0
        benchmark.extra_info["throughput_mb_s"] = throughput
