"""Unit tests for the pywebtransport._adapter.utils module."""

import pytest
from pytest_mock import MockerFixture

from pywebtransport._adapter.utils import create_quic_configuration


class TestCreateQuicConfiguration:

    def test_basic_initialization(self, mocker: MockerFixture) -> None:
        mock_config_cls = mocker.patch("pywebtransport._adapter.utils.QuicConfiguration", autospec=True)

        config = create_quic_configuration(
            is_client=True,
            alpn_protocols=["h3"],
            congestion_control_algorithm="reno",
            max_datagram_size=1350,
            idle_timeout=60.0,
        )

        assert config == mock_config_cls.return_value
        mock_config_cls.assert_called_once_with(
            alpn_protocols=["h3"],
            cafile=None,
            congestion_control_algorithm="reno",
            idle_timeout=60.0,
            is_client=True,
            max_datagram_frame_size=1350,
            server_name=None,
            verify_mode=None,
        )
        mock_config_cls.return_value.load_cert_chain.assert_not_called()

    @pytest.mark.parametrize("certfile, keyfile", [("cert.pem", None), (None, "key.pem"), (None, None)])
    def test_load_cert_chain_skipped(self, mocker: MockerFixture, certfile: str | None, keyfile: str | None) -> None:
        mock_config_cls = mocker.patch("pywebtransport._adapter.utils.QuicConfiguration", autospec=True)

        create_quic_configuration(
            is_client=True,
            alpn_protocols=["h3"],
            congestion_control_algorithm="cubic",
            max_datagram_size=1200,
            idle_timeout=30.0,
            certfile=certfile,
            keyfile=keyfile,
        )

        mock_config_cls.return_value.load_cert_chain.assert_not_called()

    def test_optional_parameters_mapping(self, mocker: MockerFixture) -> None:
        mock_config_cls = mocker.patch("pywebtransport._adapter.utils.QuicConfiguration", autospec=True)
        mock_verify = mocker.Mock()

        create_quic_configuration(
            is_client=True,
            alpn_protocols=["h3"],
            congestion_control_algorithm="reno",
            max_datagram_size=1200,
            idle_timeout=60.0,
            server_name="example.com",
            ca_certs="root.pem",
            verify_mode=mock_verify,
        )

        call_kwargs = mock_config_cls.call_args.kwargs
        assert call_kwargs["cafile"] == "root.pem"
        assert call_kwargs["server_name"] == "example.com"
        assert call_kwargs["verify_mode"] is mock_verify

    def test_with_certificates(self, mocker: MockerFixture) -> None:
        mock_config_cls = mocker.patch("pywebtransport._adapter.utils.QuicConfiguration", autospec=True)

        create_quic_configuration(
            is_client=False,
            alpn_protocols=["h3"],
            congestion_control_algorithm="cubic",
            max_datagram_size=1200,
            idle_timeout=30.0,
            certfile="cert.pem",
            keyfile="key.pem",
        )

        mock_config_cls.return_value.load_cert_chain.assert_called_once_with(certfile="cert.pem", keyfile="key.pem")
