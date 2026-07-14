"""Configuration and fixtures for pywebtransport integration tests."""

import asyncio
import socket
import ssl
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Final, cast

import pytest
from pytest_asyncio import fixture as asyncio_fixture

from pywebtransport import ClientConfig, ServerApp, ServerConfig, WebTransportClient
from pywebtransport.utils import generate_self_signed_cert

CERT_HOSTNAME: Final[str] = "localhost"


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return cast(int, s.getsockname()[1])


@pytest.fixture(scope="session")
def certificates_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    cert_dir = tmp_path_factory.mktemp(basename="certs")
    generate_self_signed_cert(hostname=CERT_HOSTNAME, output_dir=str(cert_dir))
    return cert_dir


@pytest.fixture(scope="module")
def client_config(certificates_dir: Path) -> ClientConfig:
    return ClientConfig(verify_mode=ssl.CERT_NONE)


@pytest.fixture(scope="module")
def server_config(certificates_dir: Path) -> ServerConfig:
    return ServerConfig(
        certfile=str(certificates_dir / f"{CERT_HOSTNAME}.crt"), keyfile=str(certificates_dir / f"{CERT_HOSTNAME}.key")
    )


@asyncio_fixture
async def client(client_config: ClientConfig) -> AsyncGenerator[WebTransportClient, None]:
    async with WebTransportClient(config=client_config) as wt_client:
        yield wt_client


@asyncio_fixture
async def server(server_app: ServerApp) -> AsyncGenerator[tuple[str, int], None]:
    host = "127.0.0.1"
    port = find_free_port()

    async with server_app:
        server_task = asyncio.create_task(coro=server_app.serve(host=host, port=port))
        await asyncio.sleep(delay=0.1)

        try:
            yield host, port
        finally:
            if not server_task.done():
                server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


@pytest.fixture
def server_app(request: pytest.FixtureRequest, server_config: ServerConfig) -> ServerApp:
    config_overrides = getattr(request, "param", {})
    if config_overrides and isinstance(config_overrides, dict):
        custom_config = server_config.update(**config_overrides)
        app = ServerApp(config=custom_config)
    else:
        app = ServerApp(config=server_config)

    return app
