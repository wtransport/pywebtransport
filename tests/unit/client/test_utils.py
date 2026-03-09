"""Unit tests for the pywebtransport.client.utils module."""

import socket

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ConnectionError, Headers
from pywebtransport.client.utils import normalize_headers, parse_webtransport_url, resolve_host


class TestNormalizeHeaders:

    def test_normalize_headers_dict(self) -> None:
        headers: Headers = {"Content-Type": "application/json", "USER-AGENT": "test-client"}

        normalized = normalize_headers(headers=headers)

        assert isinstance(normalized, dict)
        assert normalized == {"content-type": "application/json", "user-agent": "test-client"}

    def test_normalize_headers_list(self) -> None:
        headers: Headers = [("Content-Type", "application/json"), ("USER-AGENT", "test-client")]

        normalized = normalize_headers(headers=headers)

        assert isinstance(normalized, list)
        assert normalized == [("content-type", "application/json"), ("user-agent", "test-client")]


class TestUrlUtils:

    @pytest.mark.parametrize(
        argnames="url, error_msg",
        argvalues=[
            ("ftp://example.com", "Unsupported scheme 'ftp'. Must be 'https'"),
            ("http://example.com", "Unsupported scheme 'http'. Must be 'https'"),
            ("https://", "Missing hostname in URL"),
        ],
    )
    def test_parse_webtransport_url_raises_error(self, url: str, error_msg: str) -> None:
        with pytest.raises(expected_exception=ValueError, match=error_msg):
            parse_webtransport_url(url=url)

    @pytest.mark.parametrize(
        argnames="url, expected",
        argvalues=[
            ("https://example.com", ("example.com", 443, "/")),
            ("https://example.com:0", ("example.com", 0, "/")),
            ("https://localhost:8080/path", ("localhost", 8080, "/path")),
            ("https://[::1]:9090/q?a=1#f", ("::1", 9090, "/q?a=1")),
        ],
    )
    def test_parse_webtransport_url_success(self, url: str, expected: tuple[str, int, str]) -> None:
        parsed_url = parse_webtransport_url(url=url)

        assert parsed_url == expected


class TestResolveHost:

    @pytest.mark.asyncio
    async def test_resolve_host_domain_name(self, mocker: MockerFixture) -> None:
        mock_loop = mocker.MagicMock()
        mock_loop.getaddrinfo = mocker.AsyncMock(
            return_value=[(socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.100", 0))]
        )
        mocker.patch(target="asyncio.get_running_loop", return_value=mock_loop)

        result = await resolve_host(host="example.com")

        assert result == ["192.0.2.100"]
        mock_loop.getaddrinfo.assert_awaited_once_with(
            host="example.com", port=0, family=socket.AF_UNSPEC, type=socket.SOCK_DGRAM
        )

    @pytest.mark.asyncio
    async def test_resolve_host_empty_results(self, mocker: MockerFixture) -> None:
        mock_loop = mocker.MagicMock()
        mock_loop.getaddrinfo = mocker.AsyncMock(return_value=[])
        mocker.patch(target="asyncio.get_running_loop", return_value=mock_loop)

        with pytest.raises(expected_exception=ConnectionError, match="No DNS results for host: empty.local"):
            await resolve_host(host="empty.local")

    @pytest.mark.asyncio
    async def test_resolve_host_gaierror_translation(self, mocker: MockerFixture) -> None:
        mock_loop = mocker.MagicMock()
        mock_loop.getaddrinfo = mocker.AsyncMock(side_effect=socket.gaierror("Name or service not known"))
        mocker.patch(target="asyncio.get_running_loop", return_value=mock_loop)

        with pytest.raises(expected_exception=ConnectionError, match="DNS resolution failed for host: invalid.local"):
            await resolve_host(host="invalid.local")

    @pytest.mark.asyncio
    async def test_resolve_host_ipv4_fast_path(self, mocker: MockerFixture) -> None:
        mock_loop = mocker.patch(target="asyncio.get_running_loop")

        result = await resolve_host(host="192.0.2.1")

        assert result == ["192.0.2.1"]
        mock_loop.assert_not_called()

    @pytest.mark.asyncio
    async def test_resolve_host_ipv6_fast_path(self, mocker: MockerFixture) -> None:
        mock_loop = mocker.patch(target="asyncio.get_running_loop")

        result = await resolve_host(host="2001:db8::1")

        assert result == ["2001:db8::1"]
        mock_loop.assert_not_called()

    @pytest.mark.asyncio
    async def test_resolve_host_multiple_ips_and_deduplication(self, mocker: MockerFixture) -> None:
        mock_loop = mocker.MagicMock()
        mock_loop.getaddrinfo = mocker.AsyncMock(
            return_value=[
                (socket.AF_INET6, socket.SOCK_DGRAM, 17, "", ("2001:db8::1", 0)),
                (socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.100", 0)),
                (socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.100", 0)),
                (socket.AF_INET6, socket.SOCK_DGRAM, 17, "", ("2001:db8::2", 0)),
            ]
        )
        mocker.patch(target="asyncio.get_running_loop", return_value=mock_loop)

        result = await resolve_host(host="multihomed.local")

        assert result == ["2001:db8::1", "192.0.2.100", "2001:db8::2"]
