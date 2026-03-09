"""Shared utility functions for client-side components."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
import urllib.parse

from pywebtransport.exceptions import ConnectionError
from pywebtransport.types import URL, Headers, URLParts

__all__: list[str] = ["normalize_headers", "parse_webtransport_url", "resolve_host"]


def normalize_headers(*, headers: Headers) -> Headers:
    """Normalize the header keys to lowercase."""
    if isinstance(headers, dict):
        return {key.lower(): value for key, value in headers.items()}
    return [(key.lower(), value) for key, value in headers]


def parse_webtransport_url(*, url: URL) -> URLParts:
    """Parse the WebTransport URL into host, port, and path components."""
    parsed = urllib.parse.urlparse(url=url)
    if parsed.scheme != "https":
        raise ValueError(f"Unsupported scheme '{parsed.scheme}'. Must be 'https'")

    if not parsed.hostname:
        raise ValueError("Missing hostname in URL")

    port = parsed.port if parsed.port is not None else 443

    path = parsed.path if parsed.path else "/"
    if parsed.query:
        path += f"?{parsed.query}"

    return (parsed.hostname, port, path)


async def resolve_host(*, host: str, port: int = 0) -> list[str]:
    """Resolve a hostname to a list of IP addresses asynchronously."""
    try:
        ipaddress.ip_address(address=host)
        return [host]
    except ValueError:
        pass

    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host=host, port=port, family=socket.AF_UNSPEC, type=socket.SOCK_DGRAM)
        if not infos:
            raise ConnectionError(message=f"No DNS results for host: {host}")

        resolved_ips: list[str] = []
        for info in infos:
            ip = info[4][0]
            if ip not in resolved_ips:
                resolved_ips.append(ip)
        return resolved_ips
    except socket.gaierror as e:
        raise ConnectionError(message=f"DNS resolution failed for host: {host}") from e
