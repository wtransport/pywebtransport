"""Benchmark for Stream Throughput."""

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
        stream_flow_control_increment_bidi=100,
        stream_flow_control_increment_uni=100,
        max_stream_read_buffer=200 * 1024 * 1024,
        max_stream_write_buffer=200 * 1024 * 1024,
        max_event_queue_size=10000,
    )


class TestStreamThroughput:

    def test_upload_throughput(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/discard"

        async def run_upload() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                for _ in range(STREAMS_PER_ROUND):
                    stream = await session.create_unidirectional_stream()
                    await stream.write_all(data=STATIC_VIEW, end_stream=True)
                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_upload())
        gc.collect()

        benchmark(lambda: uvloop.run(run_upload()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        total_mb = (PAYLOAD_SIZE * STREAMS_PER_ROUND) / (1024 * 1024)
        throughput = total_mb / mean_time if mean_time > 0 else 0
        benchmark.extra_info["throughput_mb_s"] = throughput

    def test_download_throughput(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/produce"
        cmd = str(PAYLOAD_SIZE).encode()

        async def run_download() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                for _ in range(STREAMS_PER_ROUND):
                    stream = await session.create_bidirectional_stream()
                    await stream.write(data=cmd)
                    while await stream.read(max_bytes=PAYLOAD_SIZE):
                        pass
                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_download())
        gc.collect()

        benchmark(lambda: uvloop.run(run_download()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        total_mb = (PAYLOAD_SIZE * STREAMS_PER_ROUND) / (1024 * 1024)
        throughput = total_mb / mean_time if mean_time > 0 else 0
        benchmark.extra_info["throughput_mb_s"] = throughput

    def test_duplex_throughput(self, *, benchmark: BenchmarkFixture, client_config: ClientConfig) -> None:
        url = f"{SERVER_URL_BASE}/duplex"

        async def run_duplex() -> None:
            async with WebTransportClient(config=client_config) as client:
                session = await client.connect(url=url)
                for _ in range(STREAMS_PER_ROUND):
                    stream = await session.create_bidirectional_stream()

                    async def sender() -> None:
                        await stream.write(data=STATIC_VIEW, end_stream=False)

                    async def receiver() -> None:
                        while await stream.read(max_bytes=PAYLOAD_SIZE):
                            pass

                    sender_task = asyncio.create_task(coro=sender())
                    receiver_task = asyncio.create_task(coro=receiver())

                    await asyncio.gather(sender_task, receiver_task)

                    await stream.write(data=b"", end_stream=True)

                await session.close()

        for _ in range(WARMUP_ROUNDS):
            uvloop.run(run_duplex())
        gc.collect()

        benchmark(lambda: uvloop.run(run_duplex()))

        stats = cast(dict[str, Any], benchmark.stats)
        mean_time = stats["mean"]
        total_mb = (PAYLOAD_SIZE * STREAMS_PER_ROUND * 2) / (1024 * 1024)
        throughput = total_mb / mean_time if mean_time > 0 else 0
        benchmark.extra_info["throughput_mb_s"] = throughput
