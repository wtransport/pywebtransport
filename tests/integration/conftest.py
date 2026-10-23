"""Configuration and fixtures for pywebtransport integration tests."""

import asyncio
import socket
import ssl
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Final, cast

import pytest
from pytest_asyncio import fixture as asyncio_fixture

from pywebtransport import ClientConfig, ServerConfig, WebTransportClient, WebTransportServer, WebTransportSession
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


@pytest.fixture
def client_config(request: pytest.FixtureRequest) -> ClientConfig:
    return ClientConfig(verify_mode=ssl.CERT_NONE, **getattr(request, "param", {}))


@pytest.fixture
def server_config(request: pytest.FixtureRequest, certificates_dir: Path) -> ServerConfig:
    return ServerConfig(
        certfile=str(certificates_dir / f"{CERT_HOSTNAME}.crt"),
        keyfile=str(certificates_dir / f"{CERT_HOSTNAME}.key"),
        **getattr(request, "param", {}),
    )


@asyncio_fixture
async def client(client_config: ClientConfig) -> AsyncGenerator[WebTransportClient, None]:
    async with WebTransportClient(config=client_config) as client:
        yield client


@asyncio_fixture
async def server(server_config: ServerConfig) -> AsyncGenerator[WebTransportServer, None]:
    async with WebTransportServer(config=server_config) as server:
        await server.listen(host="127.0.0.1", port=find_free_port())
        yield server


@asyncio_fixture
async def sessions(
    client: WebTransportClient, server: WebTransportServer, url: str
) -> AsyncGenerator[tuple[WebTransportSession, WebTransportSession], None]:
    async with asyncio.TaskGroup() as task_group:
        connect_task = task_group.create_task(coro=client.connect(url=url))
        connection = await anext(server.incoming_connections())
        server_session = await anext(connection.incoming_sessions())
        await server_session.accept()

    async with connect_task.result() as client_session, server_session:
        yield client_session, server_session


@pytest.fixture
def url(server: WebTransportServer) -> str:
    host, port = server.local_addresses[0]
    return f"https://{host}:{port}/"
