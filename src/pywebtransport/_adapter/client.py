"""Internal aioquic protocol adapter and connection factory for the client-side."""

from __future__ import annotations

import asyncio
from typing import Any

from aioquic.quic.connection import QuicConnection

from pywebtransport._adapter.base import WebTransportCommonProtocol
from pywebtransport._adapter.utils import create_quic_configuration
from pywebtransport.config import ClientConfig
from pywebtransport.utils import get_logger

__all__: list[str] = []

_logger = get_logger(name=__name__)


class WebTransportClientProtocol(WebTransportCommonProtocol):
    """Adapt aioquic client events for the WebTransport internal engine."""

    def __init__(
        self,
        *,
        quic: QuicConnection,
        config: ClientConfig,
        max_event_queue_size: int,
        stream_handler: Any = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            quic=quic,
            config=config,
            is_client=True,
            max_event_queue_size=max_event_queue_size,
            stream_handler=stream_handler,
            loop=loop,
        )


async def create_quic_endpoint(
    *, host: str, port: int, config: ClientConfig, loop: asyncio.AbstractEventLoop
) -> tuple[asyncio.DatagramTransport, WebTransportClientProtocol]:
    """Establish the underlying QUIC transport and protocol for a client."""
    quic_config = create_quic_configuration(
        is_client=True,
        alpn_protocols=config.alpn_protocols,
        congestion_control_algorithm=config.congestion_control_algorithm,
        max_datagram_size=config.max_datagram_size,
        idle_timeout=config.connection_idle_timeout,
        server_name=host,
        ca_certs=config.ca_certs,
        certfile=config.certfile,
        keyfile=config.keyfile,
        verify_mode=config.verify_mode,
    )

    quic_connection = QuicConnection(configuration=quic_config)
    protocols: list[WebTransportClientProtocol] = []

    def protocol_factory() -> WebTransportClientProtocol:
        protocol = WebTransportClientProtocol(
            quic=quic_connection, config=config, max_event_queue_size=config.max_event_queue_size, loop=loop
        )
        protocols.append(protocol)
        return protocol

    _logger.debug("Creating datagram endpoint to %s:%d", host, port)

    try:
        transport, protocol = await loop.create_datagram_endpoint(
            protocol_factory=protocol_factory, remote_addr=(host, port)
        )
    except Exception:
        for p in protocols:
            p.close_connection(error_code=0, reason_phrase="Handshake failed")
        raise

    _logger.debug("Datagram endpoint created.")

    client_protocol = protocol
    client_protocol._quic.connect(addr=(host, port), now=loop.time())
    client_protocol.transmit()

    return transport, client_protocol
