"""Internal aioquic protocol adapter and connection factory for the client-side."""

from __future__ import annotations

import asyncio

from aioquic.quic.connection import QuicConnection

from pywebtransport._adapter.base import WebTransportCommonProtocol
from pywebtransport._adapter.utils import create_quic_configuration
from pywebtransport.config import ClientConfig
from pywebtransport.utils import get_logger

__all__: list[str] = []

logger = get_logger(name=__name__)


class WebTransportClientProtocol(WebTransportCommonProtocol):
    """Adapt aioquic client events and actions for the WebTransportEngine."""

    def __init__(
        self,
        *,
        quic: QuicConnection,
        config: ClientConfig,
        loop: asyncio.AbstractEventLoop | None = None,
        max_event_queue_size: int,
        stream_handler: asyncio.Protocol | None = None,
    ) -> None:
        """Initialize the client protocol adapter."""
        super().__init__(
            quic=quic,
            config=config,
            is_client=True,
            stream_handler=stream_handler,
            loop=loop,
            max_event_queue_size=max_event_queue_size,
        )


async def create_quic_endpoint(
    *, host: str, port: int, config: ClientConfig, loop: asyncio.AbstractEventLoop
) -> tuple[asyncio.DatagramTransport, WebTransportClientProtocol]:
    """Establish the underlying QUIC transport and protocol."""
    quic_config = create_quic_configuration(
        alpn_protocols=config.alpn_protocols,
        ca_certs=config.ca_certs,
        certfile=config.certfile,
        congestion_control_algorithm=config.congestion_control_algorithm,
        idle_timeout=config.connection_idle_timeout,
        is_client=True,
        keyfile=config.keyfile,
        max_datagram_size=config.max_datagram_size,
        server_name=host,
        verify_mode=config.verify_mode,
    )

    quic_connection = QuicConnection(configuration=quic_config)

    def protocol_factory() -> WebTransportClientProtocol:
        return WebTransportClientProtocol(
            quic=quic_connection, config=config, loop=loop, max_event_queue_size=config.max_event_queue_size
        )

    logger.debug("Creating datagram endpoint to %s:%d", host, port)
    transport, protocol = await loop.create_datagram_endpoint(
        protocol_factory=protocol_factory, remote_addr=(host, port)
    )
    logger.debug("Datagram endpoint created.")

    client_protocol = protocol
    client_protocol._quic.connect(addr=(host, port), now=loop.time())
    client_protocol.transmit()

    return transport, client_protocol
