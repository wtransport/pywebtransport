"""Reusable base class for managing event-driven resources."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, ClassVar, Protocol, Self, runtime_checkable

from pywebtransport.events import Event, EventEmitter
from pywebtransport.types import EventType

__all__: list[str] = []

_logger = logging.getLogger(name=__name__)


@runtime_checkable
class ManageableResource(Protocol):
    """Define the protocol for a resource manageable by this class."""

    events: EventEmitter

    @property
    def is_closed(self) -> bool:
        """Check if the resource is currently closed."""
        ...


class BaseResourceManager[ResourceId, ResourceType: ManageableResource](ABC):
    """Manage the lifecycle of concurrent resources abstractly via events."""

    _log: ClassVar[logging.Logger] = _logger
    _resource_closed_event_type: ClassVar[EventType]

    def __init__(self, *, resource_name: str, max_resources: int) -> None:
        """Initialize the instance."""
        self._resource_name = resource_name
        self._max_resources = max_resources

        self._active = False
        self._event_handlers: dict[ResourceId, tuple[EventEmitter, Callable[[Event], Awaitable[None]]]] = {}
        self._is_shutting_down = False
        self._lock = asyncio.Lock()
        self._resources: dict[ResourceId, ResourceType] = {}
        self._stats = {"total_created": 0, "total_closed": 0, "current_count": 0, "max_concurrent": 0}

    async def __aenter__(self) -> Self:
        """Enter async context and initialize resources."""
        self._active = True
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit async context and shut down the manager."""
        await self.shutdown()
        self._active = False

    async def add_resource(self, *, resource: ResourceType) -> None:
        """Add a new resource and subscribe to its closure event."""
        if not self._active:
            raise RuntimeError("app_manager validate failed expected=active")

        if resource.is_closed:
            raise RuntimeError(f"app_manager validate invalid component={self._resource_name} expected=open")

        resource_id = self._get_resource_id(resource=resource)
        emitter = resource.events

        async with self._lock:
            if self._is_shutting_down:
                await self._close_resource(resource=resource)
                raise RuntimeError("app_manager validate failed expected=active")

            if resource.is_closed:
                raise RuntimeError(f"app_manager validate invalid component={self._resource_name} expected=open")

            if resource_id in self._resources:
                self._log.debug("app_manager validate invalid component=%s", self._resource_name)
                return

            if 0 < self._max_resources <= len(self._resources):
                await self._close_resource(resource=resource)
                raise RuntimeError(
                    f"app_manager validate exceeded actual={len(self._resources)} component={self._resource_name} "
                    f"limit={self._max_resources}"
                )

            async def closed_handler_wrapper(event: Event) -> None:
                """Handle resource closure event."""
                event_resource_id: ResourceId | None = None
                if isinstance(event.data, dict):
                    event_resource_id = event.data.get(f"{self._resource_name}_id")

                if event_resource_id is not None and event_resource_id != resource_id:
                    self._log.warning(
                        "app_manager validate invalid component=%s expected=identity_match", self._resource_name
                    )

                await self._handle_resource_closed(resource_id=resource_id)

            emitter.once(event_type=self._resource_closed_event_type, handler=closed_handler_wrapper)
            self._event_handlers[resource_id] = (emitter, closed_handler_wrapper)

            if self._check_is_closed(resource=resource):
                try:
                    emitter.off(event_type=self._resource_closed_event_type, handler=closed_handler_wrapper)
                except (KeyError, ValueError):
                    pass
                del self._event_handlers[resource_id]
                raise RuntimeError(f"app_manager validate invalid component={self._resource_name} expected=open")

            self._resources[resource_id] = resource
            self._stats["total_created"] += 1
            self._update_stats_unsafe()
            self._log.debug(
                "app_manager register component=%s count=%d", self._resource_name, self._stats["current_count"]
            )

    async def get_all_resources(self) -> list[ResourceType]:
        """Retrieve a list of all current resources."""
        if not self._active:
            return []
        async with self._lock:
            return list(self._resources.values())

    async def get_resource(self, *, resource_id: ResourceId) -> ResourceType | None:
        """Retrieve a resource by its ID."""
        if not self._active:
            return None
        async with self._lock:
            return self._resources.get(resource_id)

    async def get_stats(self) -> dict[str, Any]:
        """Get detailed statistics about the managed resources."""
        if not self._active:
            return {}
        async with self._lock:
            stats = self._stats.copy()
            stats["current_count"] = len(self._resources)
            stats["active"] = len(self._resources)
            stats[f"max_{self._resource_name}s"] = self._max_resources
            return stats

    async def shutdown(self) -> None:
        """Shut down the manager and all associated resources."""
        if self._is_shutting_down:
            return

        self._is_shutting_down = True
        self._log.info("app_manager drain")

        await self._close_all_resources()
        self._log.info("app_manager close")

    def _check_is_closed(self, *, resource: ResourceType) -> bool:
        """Internal check for resource closure status to aid static analysis."""
        return resource.is_closed

    async def _close_all_resources(self) -> None:
        """Close all currently managed resources concurrently."""
        if not self._active:
            return

        resources_to_close: list[ResourceType] = []
        async with self._lock:
            if not self._resources:
                return
            resources_to_close = list(self._resources.values())
            self._log.debug("app_manager close component=%s count=%d", self._resource_name, len(resources_to_close))

            for _, (emitter, handler) in self._event_handlers.items():
                try:
                    emitter.off(event_type=self._resource_closed_event_type, handler=handler)
                except (KeyError, ValueError):
                    pass
            self._event_handlers.clear()
            self._resources.clear()

        try:
            async with asyncio.TaskGroup() as tg:
                for resource in resources_to_close:
                    tg.create_task(coro=self._close_resource(resource=resource))
        except* Exception as eg:
            self._log.warning(
                "app_manager close failed component=%s err=%s", self._resource_name, eg.exceptions, exc_info=eg
            )

        async with self._lock:
            self._stats["total_closed"] += len(resources_to_close)
            self._update_stats_unsafe()
        self._log.debug("app_manager destroy component=%s", self._resource_name)

    @abstractmethod
    async def _close_resource(self, *, resource: ResourceType) -> None:
        """Close a single resource."""
        raise NotImplementedError

    @abstractmethod
    def _get_resource_id(self, *, resource: ResourceType) -> ResourceId:
        """Get the unique ID from a resource object."""
        raise NotImplementedError

    async def _handle_resource_closed(self, *, resource_id: ResourceId) -> None:
        """Handle the closure event for a managed resource."""
        if not self._active:
            return

        async with self._lock:
            if resource_id in self._event_handlers:
                self._event_handlers.pop(resource_id)

            removed_resource = self._resources.pop(resource_id, None)
            if removed_resource is not None:
                self._stats["total_closed"] += 1
                self._update_stats_unsafe()
                self._log.debug(
                    "app_manager evict component=%s count=%d", self._resource_name, self._stats["current_count"]
                )

    def _update_stats_unsafe(self) -> None:
        """Update internal statistics without locking."""
        current_count = len(self._resources)
        self._stats["current_count"] = current_count
        self._stats["max_concurrent"] = max(self._stats["max_concurrent"], current_count)

    def __len__(self) -> int:
        """Return the current number of managed resources."""
        return len(self._resources)
