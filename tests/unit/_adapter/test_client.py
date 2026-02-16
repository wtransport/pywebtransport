"""Unit tests for the pywebtransport._adapter.client module."""

import asyncio
import ssl
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from aioquic.quic.connection import QuicConnection
from pytest_mock import MockerFixture

from pywebtransport import ClientConfig
from pywebtransport._adapter.client import WebTransportClientProtocol, create_quic_endpoint


@pytest.mark.asyncio
class TestCreateQuicEndpoint:

    @pytest.fixture
    def client_config(self) -> ClientConfig:
        return ClientConfig()

    @pytest.fixture
    def mock_create_quic_config(self, mocker: MockerFixture) -> MagicMock:
        return cast(MagicMock, mocker.patch(target="pywebtransport._adapter.client.create_quic_configuration"))

    @pytest.fixture
    def mock_loop(self, mocker: MockerFixture) -> MagicMock:
        loop = mocker.Mock(spec=asyncio.AbstractEventLoop)
        loop.time.return_value = 1000.0

        async def side_effect(*args: Any, **kwargs: Any) -> tuple[MagicMock, WebTransportClientProtocol]:
            factory = kwargs.get("protocol_factory")
            if factory is None:
                raise ValueError("protocol_factory is required")

            protocol = factory()
            transport = mocker.Mock(spec=asyncio.DatagramTransport)
            transport.is_closing.return_value = False
            return transport, protocol

        loop.create_datagram_endpoint = mocker.AsyncMock(side_effect=side_effect)
        return cast(MagicMock, loop)

    @pytest.fixture
    def mock_quic_connection_class(self, mocker: MockerFixture) -> MagicMock:
        mock_class = mocker.patch(target="pywebtransport._adapter.client.QuicConnection", autospec=True)
        mock_instance = mock_class.return_value
        mock_instance.host_cid = b"test_cid"
        return mock_class

    async def test_create_quic_endpoint_failure_cleanup(
        self,
        client_config: ClientConfig,
        mock_loop: MagicMock,
        mock_create_quic_config: MagicMock,
        mock_quic_connection_class: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        mock_protocol_cls = mocker.patch("pywebtransport._adapter.client.WebTransportClientProtocol", autospec=True)
        mock_protocol_instance = mock_protocol_cls.return_value

        async def side_effect(*args: Any, **kwargs: Any) -> None:
            factory = kwargs.get("protocol_factory")
            if factory is not None:
                factory()
            raise OSError("Simulated connection failure")

        mock_loop.create_datagram_endpoint = mocker.AsyncMock(side_effect=side_effect)

        with pytest.raises(OSError, match="Simulated connection failure"):
            await create_quic_endpoint(host="example.com", port=4433, config=client_config, loop=mock_loop)

        mock_protocol_instance.close_connection.assert_called_once_with(error_code=0, reason_phrase="Handshake failed")

    async def test_create_quic_endpoint_no_certs(
        self,
        client_config: ClientConfig,
        mock_loop: MagicMock,
        mock_create_quic_config: MagicMock,
        mock_quic_connection_class: MagicMock,
    ) -> None:
        client_config.certfile = None
        client_config.keyfile = None

        await create_quic_endpoint(host="example.com", port=4433, config=client_config, loop=mock_loop)

        assert mock_create_quic_config.call_args.kwargs.get("certfile") is None
        assert mock_create_quic_config.call_args.kwargs.get("keyfile") is None

    async def test_create_quic_endpoint_partial_certs(
        self,
        client_config: ClientConfig,
        mock_loop: MagicMock,
        mock_create_quic_config: MagicMock,
        mock_quic_connection_class: MagicMock,
    ) -> None:
        client_config.certfile = "/path/to/cert.pem"
        client_config.keyfile = None

        await create_quic_endpoint(host="example.com", port=4433, config=client_config, loop=mock_loop)

        assert mock_create_quic_config.call_args.kwargs.get("certfile") == "/path/to/cert.pem"
        assert mock_create_quic_config.call_args.kwargs.get("keyfile") is None

    async def test_create_quic_endpoint_success(
        self,
        client_config: ClientConfig,
        mock_loop: MagicMock,
        mock_create_quic_config: MagicMock,
        mock_quic_connection_class: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        quic_config_instance = mock_create_quic_config.return_value
        quic_config_instance.server_name = "example.com"
        mock_quic_instance = mock_quic_connection_class.return_value

        def mock_init(self: Any, *args: Any, **kwargs: Any) -> None:
            self._quic = kwargs.get("quic")

        mocker.patch(
            target="pywebtransport._adapter.base.WebTransportCommonProtocol.__init__",
            side_effect=mock_init,
            autospec=True,
        )
        mocker.patch(target="pywebtransport._adapter.client.WebTransportClientProtocol.transmit")

        transport, protocol = await create_quic_endpoint(
            host="example.com", port=4433, config=client_config, loop=mock_loop
        )

        mock_create_quic_config.assert_called_once_with(
            is_client=True,
            alpn_protocols=client_config.alpn_protocols,
            congestion_control_algorithm=client_config.congestion_control_algorithm,
            max_datagram_size=client_config.max_datagram_size,
            idle_timeout=client_config.connection_idle_timeout,
            server_name="example.com",
            ca_certs=None,
            certfile=None,
            keyfile=None,
            verify_mode=client_config.verify_mode,
        )
        mock_loop.create_datagram_endpoint.assert_awaited_once()
        mock_quic_instance.connect.assert_called_once_with(addr=("example.com", 4433), now=1000.0)
        cast(MagicMock, protocol.transmit).assert_called_once()
        assert isinstance(transport, asyncio.DatagramTransport)
        assert isinstance(protocol, WebTransportClientProtocol)

    async def test_create_quic_endpoint_verify_mode(
        self,
        client_config: ClientConfig,
        mock_loop: MagicMock,
        mock_create_quic_config: MagicMock,
        mock_quic_connection_class: MagicMock,
    ) -> None:
        client_config.verify_mode = ssl.CERT_NONE

        await create_quic_endpoint(host="example.com", port=4433, config=client_config, loop=mock_loop)

        assert mock_create_quic_config.call_args.kwargs.get("verify_mode") == ssl.CERT_NONE

    async def test_create_quic_endpoint_with_ca_certs(
        self,
        client_config: ClientConfig,
        mock_loop: MagicMock,
        mock_create_quic_config: MagicMock,
        mock_quic_connection_class: MagicMock,
    ) -> None:
        client_config.ca_certs = "/path/to/ca.pem"

        await create_quic_endpoint(host="example.com", port=4433, config=client_config, loop=mock_loop)

        assert mock_create_quic_config.call_args.kwargs.get("ca_certs") == "/path/to/ca.pem"

    async def test_create_quic_endpoint_with_client_cert(
        self,
        client_config: ClientConfig,
        mock_loop: MagicMock,
        mock_create_quic_config: MagicMock,
        mock_quic_connection_class: MagicMock,
    ) -> None:
        client_config.certfile = "/path/to/cert.pem"
        client_config.keyfile = "/path/to/key.pem"

        await create_quic_endpoint(host="example.com", port=4433, config=client_config, loop=mock_loop)

        assert mock_create_quic_config.call_args.kwargs.get("certfile") == "/path/to/cert.pem"
        assert mock_create_quic_config.call_args.kwargs.get("keyfile") == "/path/to/key.pem"


class TestWebTransportClientProtocol:

    @pytest.fixture
    def client_config(self) -> ClientConfig:
        return ClientConfig()

    @pytest.fixture
    def mock_loop(self, mocker: MockerFixture) -> MagicMock:
        loop = mocker.Mock(spec=asyncio.AbstractEventLoop)
        mocker.patch(target="asyncio.get_running_loop", return_value=loop)
        return cast(MagicMock, loop)

    @pytest.fixture
    def mock_quic(self, mocker: MockerFixture) -> MagicMock:
        quic = mocker.Mock(spec=QuicConnection)
        quic.host_cid = b"test_cid"
        return cast(MagicMock, quic)

    @pytest.fixture
    def protocol(
        self, mock_quic: MagicMock, client_config: ClientConfig, mock_loop: MagicMock
    ) -> WebTransportClientProtocol:
        return WebTransportClientProtocol(
            quic=mock_quic, config=client_config, max_event_queue_size=100, loop=mock_loop
        )

    def test_protocol_initialization(self, protocol: WebTransportClientProtocol) -> None:
        assert isinstance(protocol, WebTransportClientProtocol)
