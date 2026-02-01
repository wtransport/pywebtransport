"""Shared, general-purpose utilities."""

from __future__ import annotations

import logging
import time

from pywebtransport._wtransport import generate_self_signed_cert
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
]


def ensure_buffer(*, data: Buffer | str, encoding: str = "utf-8") -> Buffer:
    """Ensure that the given data is in a buffer-compatible format."""
    match data:
        case str():
            return data.encode(encoding=encoding)
        case bytes() | bytearray() | memoryview():
            return data
        case _:
            raise TypeError(f"Expected str or Buffer, got {type(data).__name__}")


def find_header(*, headers: Headers, key: str, default: str | bytes | None = None) -> str | bytes | None:
    """Find a header value case-insensitively from a dict or list."""
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
    """Find a header value and decode it to a string if necessary."""
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
    """Get a logger instance with a specific name."""
    return logging.getLogger(name=name)


def get_timestamp() -> float:
    """Get the current monotonic timestamp."""
    return time.perf_counter()


def merge_headers(*, base: Headers, update: Headers | None) -> Headers:
    """Merge two sets of headers, preserving list format if present."""
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
