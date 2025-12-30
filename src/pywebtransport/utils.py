"""Shared, general-purpose utilities."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import TracebackType
from typing import Self

from pywebtransport.types import Buffer, Headers

__all__: list[str] = [
    "Timer",
    "ensure_buffer",
    "format_duration",
    "generate_self_signed_cert",
    "get_header",
    "get_header_as_str",
    "get_logger",
    "get_timestamp",
    "merge_headers",
]

_timer_logger = logging.getLogger(name="timer")


class Timer:
    """A simple context manager for performance measurement using a monotonic clock."""

    def __init__(self, *, name: str = "timer") -> None:
        """Initialize the timer."""
        self.name = name
        self.start_time: float | None = None
        self.end_time: float | None = None

    @property
    def elapsed(self) -> float:
        """Get the elapsed time in seconds."""
        if self.start_time is None:
            return 0.0

        end = self.end_time if self.end_time is not None else time.perf_counter()
        return end - self.start_time

    def __enter__(self) -> Self:
        """Start the timer upon entering the context."""
        self.start()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Stop the timer and log the duration upon exiting the context."""
        elapsed = self.stop()
        _timer_logger.debug("%s took %s", self.name, format_duration(seconds=elapsed))

    def start(self) -> None:
        """Start the timer."""
        self.start_time = time.perf_counter()
        self.end_time = None

    def stop(self) -> float:
        """Stop the timer and return the elapsed time."""
        if self.start_time is None:
            raise RuntimeError("Timer not started")

        self.end_time = time.perf_counter()
        return self.elapsed


def ensure_buffer(*, data: Buffer | str, encoding: str = "utf-8") -> Buffer:
    """Ensure that the given data is in a buffer-compatible format."""
    match data:
        case str():
            return data.encode(encoding=encoding)
        case bytes() | bytearray() | memoryview():
            return data
        case _:
            raise TypeError(f"Expected str or Buffer, got {type(data).__name__}")


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


def generate_self_signed_cert(
    *, hostname: str, output_dir: str = ".", days_valid: int = 365, key_size: int = 2048
) -> tuple[str, str]:
    """Generate a self-signed certificate and key for testing purposes."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)
    subject = issuer = x509.Name(
        attributes=[
            x509.NameAttribute(oid=NameOID.COUNTRY_NAME, value="US"),
            x509.NameAttribute(oid=NameOID.STATE_OR_PROVINCE_NAME, value="CA"),
            x509.NameAttribute(oid=NameOID.LOCALITY_NAME, value="San Francisco"),
            x509.NameAttribute(oid=NameOID.ORGANIZATION_NAME, value="PyWebTransport"),
            x509.NameAttribute(oid=NameOID.COMMON_NAME, value=hostname),
        ]
    )

    cert = (
        x509.CertificateBuilder()
        .subject_name(name=subject)
        .issuer_name(name=issuer)
        .public_key(key=private_key.public_key())
        .serial_number(number=x509.random_serial_number())
        .not_valid_before(time=datetime.now(tz=timezone.utc))
        .not_valid_after(time=datetime.now(tz=timezone.utc) + timedelta(days=days_valid))
        .add_extension(extval=x509.SubjectAlternativeName([x509.DNSName(value=hostname)]), critical=False)
        .sign(private_key=private_key, algorithm=hashes.SHA256())
    )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    cert_file = output_path / f"{hostname}.crt"
    key_file = output_path / f"{hostname}.key"

    with open(file=key_file, mode="wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    os.chmod(path=key_file, mode=0o600)

    with open(file=cert_file, mode="wb") as f:
        f.write(cert.public_bytes(encoding=serialization.Encoding.PEM))

    return (str(cert_file), str(key_file))


def get_header(*, headers: Headers, key: str, default: str | bytes | None = None) -> str | bytes | None:
    """
    Get a header value case-insensitively from a dict or list.

    .. note::
        Performance Optimization: When ``headers`` is a dict, this function assumes
        keys are already lowercased (per HTTP/3 spec) to ensure O(1) lookup.
    """
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


def get_header_as_str(*, headers: Headers, key: str, default: str | None = None) -> str | None:
    """Get a header value and decode it to a string if necessary."""
    value = get_header(headers=headers, key=key)
    if value is None:
        return default

    if isinstance(value, str):
        return value

    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return default


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
