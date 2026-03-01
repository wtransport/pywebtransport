"""Unit tests for the pywebtransport.utils module."""

import logging
import socket
from typing import Any, cast

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ConnectionError, Headers
from pywebtransport.utils import (
    ensure_buffer,
    find_header,
    find_header_str,
    format_duration,
    get_logger,
    get_timestamp,
    merge_headers,
    resolve_host,
)


class TestDataConversionAndFormatting:

    @pytest.mark.parametrize(
        argnames="data, expected_type, expected_content",
        argvalues=[
            ("hello", bytes, b"hello"),
            (b"world", bytes, b"world"),
            (bytearray(b"array"), bytearray, bytearray(b"array")),
            (memoryview(b"view"), memoryview, b"view"),
        ],
    )
    def test_ensure_buffer(self, data: Any, expected_type: type, expected_content: Any) -> None:
        result = ensure_buffer(data=data)

        assert isinstance(result, expected_type)
        if isinstance(result, memoryview):
            assert result.tobytes() == expected_content
        else:
            assert result == expected_content

    def test_ensure_buffer_invalid_type(self) -> None:
        with pytest.raises(expected_exception=TypeError):
            ensure_buffer(data=cast(Any, 123))

    @pytest.mark.parametrize(
        argnames="seconds, expected",
        argvalues=[
            (1e-7, "100ns"),
            (5e-5, "50.0µs"),
            (0.1234, "123.4ms"),
            (5.67, "5.7s"),
            (90.5, "1m30.5s"),
            (3723.1, "1h2m3.1s"),
        ],
    )
    def test_format_duration(self, seconds: float, expected: str) -> None:
        result = format_duration(seconds=seconds)

        assert result == expected


class TestHeaderUtils:

    def test_find_header_dual_mode_dict(self) -> None:
        headers: Headers = {b"content-length": b"123", "server": "test"}

        val_bytes = find_header(headers=headers, key="content-length")
        val_str = find_header(headers=headers, key="server")
        assert val_bytes == b"123"
        assert val_str == "test"

    def test_find_header_dual_mode_list(self) -> None:
        headers: Headers = [(b"content-length", b"123"), ("server", "test")]

        val_bytes = find_header(headers=headers, key="content-length")
        val_str = find_header(headers=headers, key="server")
        assert val_bytes == b"123"
        assert val_str == "test"

    def test_find_header_from_dict(self) -> None:
        headers: Headers = {"content-type": "application/json"}

        assert find_header(headers=headers, key="content-type") == "application/json"
        assert find_header(headers=headers, key="Unknown") is None
        assert find_header(headers=headers, key="Unknown", default="default") == "default"

    def test_find_header_from_list(self) -> None:
        headers: Headers = [("Content-Type", "application/json")]

        assert find_header(headers=headers, key="content-type") == "application/json"
        assert find_header(headers=headers, key="Unknown") is None

    def test_find_header_str_decoding(self) -> None:
        headers: Headers = {b"content-type": b"application/json"}

        result = find_header_str(headers=headers, key="content-type")

        assert result == "application/json"

    def test_find_header_str_default(self) -> None:
        headers: Headers = {"host": "example.com"}

        result = find_header_str(headers=headers, key="missing", default="default")

        assert result == "default"

    def test_find_header_str_existing_string(self) -> None:
        headers: Headers = {"user-agent": "test-client"}

        result = find_header_str(headers=headers, key="user-agent")

        assert result == "test-client"

    def test_find_header_str_invalid_utf8(self) -> None:
        headers: Headers = {b"key": b"\xff\xfe"}

        result = find_header_str(headers=headers, key="key", default="fallback")

        assert result == "fallback"

    def test_merge_headers_dict(self) -> None:
        base: Headers = {"a": "1"}
        update: Headers = {"b": "2"}

        result = merge_headers(base=base, update=update)

        assert result == {"a": "1", "b": "2"}

    def test_merge_headers_list(self) -> None:
        base: Headers = [("a", "1")]
        update: Headers = [("b", "2")]

        result = merge_headers(base=base, update=update)

        assert result == [("a", "1"), ("b", "2")]

    def test_merge_headers_mixed(self) -> None:
        base: Headers = {"a": "1"}
        update: Headers = [("b", "2")]

        result = merge_headers(base=base, update=update)

        assert result == [("a", "1"), ("b", "2")]

    def test_merge_headers_none(self) -> None:
        base: Headers = {"a": "1"}
        base_list: Headers = [("a", "1")]

        assert merge_headers(base=base, update=None) == {"a": "1"}
        assert merge_headers(base=base_list, update=None) == [("a", "1")]


class TestLoggingUtils:

    def test_get_logger(self) -> None:
        logger = get_logger(name="test")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test"


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


class TestTimestamp:

    def test_get_timestamp(self, mocker: MockerFixture) -> None:
        mocker.patch(target="time.perf_counter", return_value=12345.678)

        timestamp = get_timestamp()

        assert timestamp == 12345.678
