"""Unit tests for the pywebtransport.client.utils module."""

import pytest

from pywebtransport import Headers
from pywebtransport.client.utils import normalize_headers, parse_webtransport_url


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
            ("ftp://example.com", "Unsupported scheme 'ftp'"),
            ("http://example.com", "Unsupported scheme 'http'"),
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
