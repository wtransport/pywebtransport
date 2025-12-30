"""Manager for handling numerous concurrent connection lifecycles."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, ClassVar

from pywebtransport.connection import WebTransportConnection
from pywebtransport.manager._base import BaseResourceManager
from pywebtransport.types import ConnectionId, EventType
from pywebtransport.utils import get_logger

__all__: list[str] = ["ConnectionManager"]

logger = get_logger(name=__name__)


class ConnectionManager(BaseResourceManager[ConnectionId, WebTransportConnection]):
    """Manage multiple WebTransport connections using event-driven cleanup."""

    _log = logger
    _resource_closed_event_type: ClassVar[EventType] = EventType.CONNECTION_CLOSED

    def __init__(self, *, max_connections: int) -> None:
        """Initialize the connection manager."""
        super().__init__(resource_name="connection", max_resources=max_connections)
        self._closing_tasks: set[asyncio.Task[None]] = set()

    async def shutdown(self) -> None:
        """Shut down the manager and ensure all closing tasks complete."""
        await super().shutdown()

        if self._closing_tasks:
            self._log.debug("Waiting for %d closing tasks to complete", len(self._closing_tasks))
            await asyncio.gather(*self._closing_tasks, return_exceptions=True)

    async def add_connection(self, *, connection: WebTransportConnection) -> ConnectionId:
        """Add a new connection and subscribe to its closure event."""
        await super().add_resource(resource=connection)
        return connection.connection_id

    async def remove_connection(self, *, connection_id: ConnectionId) -> WebTransportConnection | None:
        """Manually remove a connection from management."""
        if self._lock is None:
            return None

        removed_connection: WebTransportConnection | None = None
        async with self._lock:
            if connection_id in self._event_handlers:
                self._event_handlers.pop(connection_id)

            removed_connection = self._resources.pop(connection_id, None)
            if removed_connection is not None:
                self._stats["total_closed"] += 1
                self._update_stats_unsafe()
                self._schedule_close(connection=removed_connection)
                self._log.debug(
                    "Manually removed connection %s (total: %d)", connection_id, self._stats["current_count"]
                )

        return removed_connection

    async def get_stats(self) -> dict[str, Any]:
        """Get detailed statistics about the managed connections."""
        stats = await super().get_stats()
        if self._lock is not None:
            async with self._lock:
                states: defaultdict[str, int] = defaultdict(int)
                for conn in self._resources.values():
                    states[conn.state.value] += 1
                stats["states"] = dict(states)
        return stats

    async def _close_resource(self, *, resource: WebTransportConnection) -> None:
        """Close a single connection resource."""
        if not resource.is_closed:
            await resource.close()

    def _get_resource_id(self, *, resource: WebTransportConnection) -> ConnectionId:
        """Get the unique ID from a connection object."""
        return resource.connection_id

    async def _handle_resource_closed(self, *, resource_id: ConnectionId) -> None:
        """Handle the closure event for a managed resource."""
        if self._lock is None:
            return

        conn: WebTransportConnection | None = None
        async with self._lock:
            if resource_id in self._event_handlers:
                self._event_handlers.pop(resource_id)

            conn = self._resources.pop(resource_id, None)
            if conn is not None:
                self._stats["total_closed"] += 1
                self._update_stats_unsafe()
                self._schedule_close(connection=conn)
                self._log.debug("Passive cleanup: Connection %s removed and close scheduled.", resource_id)

    def _schedule_close(self, *, connection: WebTransportConnection) -> None:
        """Schedule an asynchronous close task for a connection."""
        if connection.is_closed:
            return

        task = asyncio.create_task(coro=connection.close())
        self._closing_tasks.add(task)
        task.add_done_callback(self._closing_tasks.discard)
