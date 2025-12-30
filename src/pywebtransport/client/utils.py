"""Shared utility functions for client-side components."""

from __future__ import annotations

import urllib.parse

from pywebtransport.constants import WEBTRANSPORT_DEFAULT_PORT, WEBTRANSPORT_SCHEME
from pywebtransport.types import URL, Headers, URLParts

__all__: list[str] = ["normalize_headers", "parse_webtransport_url", "validate_url"]


def normalize_headers(*, headers: Headers) -> Headers:
    """Normalize header keys to lowercase."""
    if isinstance(headers, dict):
        return {key.lower(): value for key, value in headers.items()}
    return [(key.lower(), value) for key, value in headers]


def parse_webtransport_url(*, url: URL) -> URLParts:
    """Parse a WebTransport URL into its host, port, and path components."""
    parsed = urllib.parse.urlparse(url=url)
    if parsed.scheme != WEBTRANSPORT_SCHEME:
        raise ValueError(f"Unsupported scheme '{parsed.scheme}'. Must be '{WEBTRANSPORT_SCHEME}'")

    if not parsed.hostname:
        raise ValueError("Missing hostname in URL")

    port = parsed.port if parsed.port is not None else WEBTRANSPORT_DEFAULT_PORT

    path = parsed.path if parsed.path else "/"
    if parsed.query:
        path += f"?{parsed.query}"

    return (parsed.hostname, port, path)


def validate_url(*, url: URL) -> bool:
    """Validate the format of a WebTransport URL."""
    try:
        parse_webtransport_url(url=url)
        return True
    except ValueError:
        return False
