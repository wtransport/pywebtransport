"""Internal aioquic protocol adapter and factory for the server-side."""

from __future__ import annotations

import asyncio
from asyncio import BaseTransport
from collections.abc import Callable
from typing import Any

from aioquic.asyncio.server import QuicServer
from aioquic.asyncio.server import serve as quic_serve
from aioquic.quic.connection import QuicConnection

from pywebtransport._adapter.base import WebTransportCommonProtocol
from pywebtransport._adapter.utils import create_quic_configuration
from pywebtransport.config import ServerConfig
from pywebtransport.utils import get_logger

__all__: list[str] = []

logger = get_logger(name=__name__)

type ConnectionCreator = Callable[[WebTransportServerProtocol, BaseTransport], None]


class WebTransportServerProtocol(WebTransportCommonProtocol):
    """Adapt aioquic server events and actions for the WebTransportEngine."""

    _connection_creator: ConnectionCreator
    _server_config: ServerConfig

    def __init__(
        self,
        *,
        quic: QuicConnection,
        server_config: ServerConfig,
        connection_creator: ConnectionCreator,
        stream_handler: Any = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """Initialize the server protocol adapter."""
        super().__init__(
            quic=quic,
            config=server_config,
            is_client=False,
            stream_handler=stream_handler,
            loop=loop,
            max_event_queue_size=server_config.max_event_queue_size,
        )
        self._server_config = server_config
        self._connection_creator = connection_creator

    def connection_made(self, transport: BaseTransport) -> None:
        """Handle connection establishment."""
        super().connection_made(transport)
        logger.debug("Adapter connection_made, calling connection creator.")
        self._connection_creator(self, transport)


async def create_server(
    *, host: str, port: int, config: ServerConfig, connection_creator: ConnectionCreator
) -> QuicServer:
    """Start an aioquic server with the given configuration."""
    quic_config = create_quic_configuration(
        alpn_protocols=config.alpn_protocols,
        ca_certs=config.ca_certs,
        certfile=config.certfile,
        congestion_control_algorithm=config.congestion_control_algorithm,
        idle_timeout=config.connection_idle_timeout,
        is_client=False,
        keyfile=config.keyfile,
        max_datagram_size=config.max_datagram_size,
        verify_mode=config.verify_mode,
    )

    def protocol_factory(quic: QuicConnection, stream_handler: Any = None, **kwargs: Any) -> WebTransportServerProtocol:
        return WebTransportServerProtocol(
            quic=quic, server_config=config, connection_creator=connection_creator, stream_handler=stream_handler
        )

    return await quic_serve(host=host, port=port, configuration=quic_config, create_protocol=protocol_factory)
