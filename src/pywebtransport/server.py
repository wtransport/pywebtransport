"""WebTransport server."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import AsyncGenerator
from types import TracebackType
from typing import Self

from pywebtransport._controller.controller import EndpointController
from pywebtransport.config import ServerConfig
from pywebtransport.connection import WebTransportConnection
from pywebtransport.exceptions import ServerError
from pywebtransport.types import Address, ConnectionHandle

__all__: list[str] = ["WebTransportServer"]

_logger = logging.getLogger(name=__name__)


class WebTransportServer:
    """Manage WebTransport connections and sessions."""

    def __init__(self, *, config: ServerConfig) -> None:
        """Initialize the instance."""
        self._config = config

        self._closed = False
        self._closing = False
        self._incoming_connections: asyncio.Queue[WebTransportConnection | None] = asyncio.Queue()
        self._serving = False
        self._shutdown_event = asyncio.Event()

        self._close_task: asyncio.Task[None] | None = None
        self._controller: EndpointController | None = None

        _logger.info("app_server create")

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        await self.close()

    @property
    def config(self) -> ServerConfig:
        """Return the server configuration."""
        return self._config

    @property
    def is_closed(self) -> bool:
        """Return True if the server is closed."""
        return self._closed

    @property
    def is_serving(self) -> bool:
        """Return True if the server is serving."""
        return self._serving

    @property
    def local_addresses(self) -> list[Address]:
        """Return the local addresses."""
        if self._controller is not None:
            return self._controller.get_local_addresses()
        return []

    async def close(self) -> None:
        """Terminate the server."""
        if self._close_task is not None and not self._close_task.done():
            await self._close_task
            return

        if self._closed:
            return

        self._close_task = asyncio.create_task(coro=self._close())
        await self._close_task

    async def incoming_connections(self) -> AsyncGenerator[WebTransportConnection, None]:
        """Yield incoming connections until the server is closed."""
        while True:
            connection = await self._incoming_connections.get()
            if connection is None:
                self._incoming_connections.put_nowait(None)
                return
            yield connection

    async def listen(self, *, host: str | None = None, port: int | None = None) -> None:
        """Start listening for connections."""
        if self._closed or self._closing or self._serving:
            raise ServerError(message="app_server validate failed")

        if host is not None or port is not None:
            bind_host = host if host is not None else self._config.bind_host
            bind_port = port if port is not None else self._config.bind_port
            self._config = dataclasses.replace(self._config, bind_host=bind_host, bind_port=bind_port)

        try:
            controller = EndpointController(
                connection_factory=self._create_connection, config=self._config, is_client=False
            )
            await controller.start()

            self._controller = controller
            self._serving = True

            _logger.info("app_server open")

        except FileNotFoundError as e:
            raise ServerError(message="sys_file open failed") from e
        except Exception as e:
            raise ServerError.from_cause(message="app_server open failed", cause=e) from e

    async def serve_forever(self) -> None:
        """Run the server indefinitely until interrupted."""
        if not self._serving:
            raise ServerError(message="app_server validate failed")

        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            raise ServerError.from_cause(message="rt_event resolve failed", cause=e) from e

    async def _close(self) -> None:
        self._serving = False
        self._closing = True

        try:
            if self._controller is not None:
                await self._controller.close()
                self._controller = None
        finally:
            self._incoming_connections.put_nowait(None)
            self._shutdown_event.set()
            self._closing = False
            self._closed = True

            _logger.info("app_server close")

    def _create_connection(self, controller: EndpointController, handle: ConnectionHandle) -> WebTransportConnection:
        connection = WebTransportConnection(handle=handle, controller=controller, config=self._config, is_client=False)
        self._incoming_connections.put_nowait(connection)
        return connection
