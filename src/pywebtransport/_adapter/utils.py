"""Internal utilities for configuring aioquic endpoints."""

from __future__ import annotations

from typing import Any

from aioquic.quic.configuration import QuicConfiguration

__all__: list[str] = []


def create_quic_configuration(
    *,
    is_client: bool,
    alpn_protocols: list[str],
    congestion_control_algorithm: str,
    max_datagram_size: int,
    idle_timeout: float,
    server_name: str | None = None,
    ca_certs: str | None = None,
    certfile: str | None = None,
    keyfile: str | None = None,
    verify_mode: Any = None,
) -> QuicConfiguration:
    """Create a QUIC configuration object from specific parameters."""
    config = QuicConfiguration(
        alpn_protocols=alpn_protocols,
        cafile=ca_certs,
        congestion_control_algorithm=congestion_control_algorithm,
        idle_timeout=idle_timeout,
        is_client=is_client,
        max_datagram_frame_size=max_datagram_size,
        server_name=server_name,
        verify_mode=verify_mode,
    )

    if certfile is not None and keyfile is not None:
        config.load_cert_chain(certfile=certfile, keyfile=keyfile)

    return config
