"""High-level client for managing a fleet of client instances."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Final, Self

from pywebtransport.client.client import WebTransportClient
from pywebtransport.exceptions import ClientError
from pywebtransport.session import WebTransportSession
from pywebtransport.types import URL
from pywebtransport.utils import get_logger

__all__: list[str] = ["ClientFleet"]

_DEFAULT_MAX_CONCURRENT_HANDSHAKES: Final[int] = 50

_logger = get_logger(name=__name__)


class ClientFleet:
    """Manage a fleet of WebTransportClient instances to distribute load."""

    def __init__(
        self, *, clients: list[WebTransportClient], max_concurrent_handshakes: int = _DEFAULT_MAX_CONCURRENT_HANDSHAKES
    ) -> None:
        """Initialize the instance."""
        if not clients:
            raise ValueError("ClientFleet requires at least one client instance.")

        self._clients = clients
        self._max_concurrent_handshakes = max_concurrent_handshakes

        self._active = False
        self._connect_sem = asyncio.Semaphore(max_concurrent_handshakes)
        self._current_index = 0

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        self._active = True
        successful_clients: list[WebTransportClient] = []

        async def _startup_wrapper(client: WebTransportClient) -> None:
            await client.__aenter__()
            successful_clients.append(client)

        try:
            async with asyncio.TaskGroup() as tg:
                for client in self._clients:
                    tg.create_task(coro=_startup_wrapper(client))
        except* Exception as eg:
            _logger.error("Failed to activate clients in fleet: %s", eg.exceptions, exc_info=eg)
            self._active = False

            if successful_clients:
                try:
                    async with asyncio.TaskGroup() as cleanup_tg:
                        for client in successful_clients:
                            cleanup_tg.create_task(coro=client.__aexit__(None, None, None))
                except* Exception as cleanup_eg:
                    _logger.error(
                        "Error during fleet cleanup after activation failure: %s",
                        cleanup_eg.exceptions,
                        exc_info=cleanup_eg,
                    )
            raise eg

        _logger.info("ClientFleet activated with %d clients", len(self._clients))
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        self._active = False
        try:
            async with asyncio.TaskGroup() as tg:
                for client in self._clients:
                    tg.create_task(coro=client.__aexit__(exc_type, exc_val, exc_tb))
        except* Exception as eg:
            _logger.error("Error closing clients in fleet: %s", eg.exceptions, exc_info=eg)

    async def connect_all(self, *, url: URL) -> list[WebTransportSession]:
        """Connect all clients in the fleet to the specified URL."""
        self._check_active()

        async def safe_connect(client: WebTransportClient) -> WebTransportSession | None:
            try:
                async with self._connect_sem:
                    return await client.connect(url=url)
            except Exception as e:
                _logger.warning("Client failed to connect: %s", e)
                return None

        tasks: list[asyncio.Task[WebTransportSession | None]] = []
        async with asyncio.TaskGroup() as tg:
            for client in self._clients:
                tasks.append(tg.create_task(coro=safe_connect(client)))

        sessions: list[WebTransportSession] = []
        for task in tasks:
            result = task.result()
            if result is not None:
                sessions.append(result)

        return sessions

    def get_client(self) -> WebTransportClient:
        """Return an active client from the fleet using a round-robin strategy."""
        self._check_active()

        client = self._clients[self._current_index]
        self._current_index = (self._current_index + 1) % len(self._clients)
        return client

    def get_client_count(self) -> int:
        """Return the number of clients currently in the fleet."""
        return len(self._clients)

    def _check_active(self) -> None:
        """Verify that the fleet is currently active."""
        if not self._active:
            raise ClientError(
                message=(
                    "ClientFleet has not been activated. It must be used as an "
                    "asynchronous context manager (`async with ...`)."
                )
            )
