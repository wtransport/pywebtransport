"""Shared, general-purpose utilities."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import time

from pywebtransport._wtransport import generate_self_signed_cert
from pywebtransport.exceptions import ConnectionError
from pywebtransport.types import Buffer, Headers

__all__: list[str] = [
    "ensure_buffer",
    "find_header",
    "find_header_str",
    "format_duration",
    "generate_self_signed_cert",
    "get_logger",
    "get_timestamp",
    "merge_headers",
    "resolve_host",
]


def ensure_buffer(*, data: Buffer | str, encoding: str = "utf-8") -> Buffer:
    """Validate and convert input data to a buffer format."""
    match data:
        case str():
            return data.encode(encoding=encoding)
        case bytes() | bytearray() | memoryview():
            return data
        case _:
            raise TypeError(f"Expected str or Buffer, got {type(data).__name__}")


def find_header(*, headers: Headers, key: str, default: str | bytes | None = None) -> str | bytes | None:
    """Search for a header value case-insensitively."""
    target_key = key.lower()
    target_key_bytes = target_key.encode("utf-8")

    if isinstance(headers, dict):
        if target_key in headers:
            return headers[target_key]
        return headers.get(target_key_bytes, default)

    for k, v in headers:
        if isinstance(k, bytes):
            if k.lower() == target_key_bytes:
                return v
        elif k.lower() == target_key:
            return v
    return default


def find_header_str(*, headers: Headers, key: str, default: str | None = None) -> str | None:
    """Retrieve a header value as a decoded string."""
    value = find_header(headers=headers, key=key)
    if value is None:
        return default

    if isinstance(value, str):
        return value

    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return default


def format_duration(*, seconds: float) -> str:
    """Format a duration in seconds into a human-readable string."""
    if seconds < 1e-6:
        return f"{seconds * 1e9:.0f}ns"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.1f}µs"
    if seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m{secs:.1f}s"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours}h{minutes}m{secs:.1f}s"


def get_logger(*, name: str) -> logging.Logger:
    """Retrieve a named logger instance."""
    return logging.getLogger(name=name)


def get_timestamp() -> float:
    """Return the current monotonic timestamp."""
    return time.perf_counter()


def merge_headers(*, base: Headers, update: Headers | None) -> Headers:
    """Combine two header collections."""
    if update is None:
        if isinstance(base, dict):
            return base.copy()
        return list(base)

    if isinstance(base, dict) and isinstance(update, dict):
        new_headers = base.copy()
        new_headers.update(update)
        return new_headers

    base_list = list(base.items()) if isinstance(base, dict) else list(base)
    update_list = list(update.items()) if isinstance(update, dict) else list(update)
    return base_list + update_list


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
