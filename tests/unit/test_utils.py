"""Unit tests for the pywebtransport.utils module."""

import logging
from typing import Any, cast

import pytest
from pytest_mock import MockerFixture

from pywebtransport import Headers
from pywebtransport.utils import (
    ensure_buffer,
    format_duration,
    find_header,
    find_header_str,
    get_logger,
    get_timestamp,
    merge_headers,
)


class TestDataConversionAndFormatting:

    @pytest.mark.parametrize(
        "data, expected_type, expected_content",
        [
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
        with pytest.raises(TypeError):
            ensure_buffer(data=cast(Any, 123))

    @pytest.mark.parametrize(
        "seconds, expected",
        [
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


class TestTimestamp:

    def test_get_timestamp(self, mocker: MockerFixture) -> None:
        mocker.patch("time.perf_counter", return_value=12345.678)

        timestamp = get_timestamp()

        assert timestamp == 12345.678
