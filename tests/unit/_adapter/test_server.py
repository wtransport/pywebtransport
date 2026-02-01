"""Unit tests for the pywebtransport._adapter.server module."""

import asyncio
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
from aioquic.quic.connection import QuicConnection
from pytest_mock import MockerFixture

from pywebtransport import ServerConfig
from pywebtransport._adapter.server import WebTransportServerProtocol, create_server


@pytest.mark.asyncio
class TestCreateServer:

    @pytest.fixture
    def connection_creator(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def mock_create_quic_config(self, mocker: MockerFixture) -> MagicMock:
        return cast(MagicMock, mocker.patch(target="pywebtransport._adapter.server.create_quic_configuration"))

    @pytest.fixture
    def mock_quic_serve(self, mocker: MockerFixture) -> MagicMock:
        return cast(
            MagicMock, mocker.patch(target="pywebtransport._adapter.server.quic_serve", new_callable=mocker.AsyncMock)
        )

    @pytest.fixture
    def server_config(self, valid_cert_paths: tuple[Path, Path]) -> ServerConfig:
        cert, key = valid_cert_paths
        return ServerConfig(certfile=str(cert), keyfile=str(key))

    @pytest.fixture
    def valid_cert_paths(self, tmp_path: Path) -> tuple[Path, Path]:
        cert = tmp_path / "ca.pem"
        key = tmp_path / "key.pem"
        cert.touch()
        key.touch()
        return cert, key

    async def test_create_server_protocol_factory(
        self,
        server_config: ServerConfig,
        connection_creator: MagicMock,
        mock_quic_serve: MagicMock,
        mock_create_quic_config: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(target="pywebtransport._adapter.base.WebTransportCommonProtocol.__init__", return_value=None)

        await create_server(host="127.0.0.1", port=4433, config=server_config, connection_creator=connection_creator)

        factory = mock_quic_serve.call_args.kwargs["create_protocol"]
        protocol = factory(quic=MagicMock(), stream_handler=MagicMock())

        assert isinstance(protocol, WebTransportServerProtocol)
        assert protocol._server_config == server_config
        assert protocol._connection_creator == connection_creator

    async def test_create_server_success(
        self,
        server_config: ServerConfig,
        connection_creator: MagicMock,
        mock_quic_serve: MagicMock,
        mock_create_quic_config: MagicMock,
    ) -> None:
        result = await create_server(
            host="127.0.0.1", port=4433, config=server_config, connection_creator=connection_creator
        )

        assert result == mock_quic_serve.return_value
        mock_create_quic_config.assert_called_once_with(
            alpn_protocols=server_config.alpn_protocols,
            ca_certs=None,
            certfile=server_config.certfile,
            congestion_control_algorithm=server_config.congestion_control_algorithm,
            idle_timeout=server_config.connection_idle_timeout,
            is_client=False,
            keyfile=server_config.keyfile,
            max_datagram_size=server_config.max_datagram_size,
            verify_mode=server_config.verify_mode,
        )
        mock_quic_serve.assert_awaited_once()

    async def test_create_server_with_ca_certs_success(
        self,
        server_config: ServerConfig,
        connection_creator: MagicMock,
        mock_create_quic_config: MagicMock,
        mock_quic_serve: MagicMock,
        tmp_path: Path,
    ) -> None:
        ca_cert = tmp_path / "ca.pem"
        ca_cert.touch()
        server_config.ca_certs = str(ca_cert)

        await create_server(host="127.0.0.1", port=4433, config=server_config, connection_creator=connection_creator)

        assert mock_create_quic_config.call_args.kwargs["ca_certs"] == str(ca_cert)


class TestWebTransportServerProtocol:

    @pytest.fixture
    def mock_connection_creator(self, mocker: MockerFixture) -> MagicMock:
        return cast(MagicMock, mocker.Mock())

    @pytest.fixture
    def mock_loop(self, mocker: MockerFixture) -> MagicMock:
        loop = mocker.Mock(spec=asyncio.AbstractEventLoop)
        mocker.patch(target="asyncio.get_running_loop", return_value=loop)
        return cast(MagicMock, loop)

    @pytest.fixture
    def mock_quic(self, mocker: MockerFixture) -> MagicMock:
        quic = mocker.Mock(spec=QuicConnection)
        quic.host_cid = b"test_cid"
        quic.configuration = mocker.Mock()
        quic.configuration.is_client = False
        return cast(MagicMock, quic)

    @pytest.fixture
    def valid_cert_paths(self, tmp_path: Path) -> tuple[Path, Path]:
        cert = tmp_path / "ca.pem"
        key = tmp_path / "key.pem"
        cert.touch()
        key.touch()
        return cert, key

    @pytest.fixture
    def server_config(self, valid_cert_paths: tuple[Path, Path]) -> ServerConfig:
        cert, key = valid_cert_paths
        return ServerConfig(certfile=str(cert), keyfile=str(key))

    @pytest.fixture
    def protocol(
        self,
        mock_quic: MagicMock,
        server_config: ServerConfig,
        mock_connection_creator: MagicMock,
        mock_loop: MagicMock,
    ) -> WebTransportServerProtocol:
        return WebTransportServerProtocol(
            quic=mock_quic, server_config=server_config, connection_creator=mock_connection_creator, loop=mock_loop
        )

    def test_connection_made(
        self, protocol: WebTransportServerProtocol, mock_connection_creator: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_transport = mocker.Mock(spec=asyncio.DatagramTransport)
        mock_transport.is_closing.return_value = False

        protocol.connection_made(transport=mock_transport)

        assert protocol._transport == mock_transport
        mock_connection_creator.assert_called_once_with(protocol, mock_transport)
