"""Event emitter."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pywebtransport.types import EventType

__all__: list[str] = ["Event", "EventEmitter", "EventHandler"]

type EventHandler = Callable[[Event], Awaitable[None] | None]

_logger = logging.getLogger(name=__name__)


@dataclass(kw_only=True, frozen=True, slots=True)
class Event:
    """Encapsulate event data."""

    data: Any | None = None
    type: EventType


class EventEmitter:
    """Manage event subscriptions and dispatching."""

    __slots__ = ("_background_tasks", "_handlers", "_once_handlers")

    def __init__(self) -> None:
        """Initialize the instance."""
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._handlers: dict[EventType, list[EventHandler]] = defaultdict(list)
        self._once_handlers: dict[EventType, list[EventHandler]] = defaultdict(list)

    async def close(self) -> None:
        """Terminate processing and clear handlers."""
        for task in self._background_tasks:
            if not task.done():
                task.cancel()

        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        count = sum(len(handlers) for handlers in self._handlers.values()) + sum(
            len(handlers) for handlers in self._once_handlers.values()
        )
        if count != 0:
            self._handlers.clear()
            self._once_handlers.clear()
            _logger.debug("rt_task destroy count=%d", count)

        _logger.debug("rt_channel close")

    async def emit(self, *, event_type: EventType, data: Any | None = None) -> None:
        """Dispatch an event to registered handlers."""
        event = Event(type=event_type, data=data)
        await self._process_event(event=event)

    def emit_nowait(self, *, event_type: EventType, data: Any | None = None) -> None:
        """Schedule an event dispatch without awaiting."""
        event = Event(type=event_type, data=data)

        try:
            task = asyncio.create_task(coro=self._process_event(event=event))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        except RuntimeError:
            _logger.warning("rt_event send failed event=%s", event_type)

    def off(self, *, event_type: EventType, handler: EventHandler | None = None) -> None:
        """Unregister handlers for an event."""
        if handler is None:
            handlers = self._handlers.get(event_type)
            once_handlers = self._once_handlers.get(event_type)
            count = (len(handlers) if handlers else 0) + (len(once_handlers) if once_handlers else 0)
            if count != 0:
                if handlers:
                    handlers.clear()
                if once_handlers:
                    once_handlers.clear()
                _logger.debug("rt_task destroy count=%d event=%s", count, event_type)
        else:
            removed = False
            handlers = self._handlers.get(event_type)
            if handlers is not None and handler in handlers:
                handlers.remove(handler)
                removed = True
            once_handlers = self._once_handlers.get(event_type)
            if once_handlers is not None and handler in once_handlers:
                once_handlers.remove(handler)
                removed = True
            if removed:
                _logger.debug("rt_task destroy event=%s", event_type)

    def on(self, *, event_type: EventType, handler: EventHandler) -> None:
        """Register a persistent event handler."""
        handlers = self._handlers[event_type]

        if handler not in handlers:
            handlers.append(handler)
            _logger.debug("rt_task create event=%s", event_type)
        else:
            _logger.warning("rt_task create failed event=%s", event_type)

    def once(self, *, event_type: EventType, handler: EventHandler) -> None:
        """Register a one-time event handler."""
        once_handlers = self._once_handlers[event_type]

        if handler not in once_handlers:
            once_handlers.append(handler)
            _logger.debug("rt_task create event=%s", event_type)

    async def wait_for(self, *, event_type: EventType | list[EventType], timeout: float | None = None) -> Event:
        """Await the next matching event."""
        future: asyncio.Future[Event] = asyncio.Future()
        event_types = [event_type] if isinstance(event_type, EventType) else event_type

        async def handler(event: Event) -> None:
            if not future.done():
                future.set_result(event)

        for event_type in event_types:
            self.on(event_type=event_type, handler=handler)

        try:
            try:
                async with asyncio.timeout(delay=timeout):
                    return await future
            except asyncio.TimeoutError:
                future.cancel()
                raise
        finally:
            for event_type in event_types:
                self.off(event_type=event_type, handler=handler)

    async def _process_event(self, *, event: Event) -> None:
        handlers_to_call: list[EventHandler] = self._handlers.get(event.type, [])[:]
        once_handlers_to_call: list[EventHandler] = self._once_handlers.get(event.type, [])[:]
        all_handlers = handlers_to_call + once_handlers_to_call

        count = len(once_handlers_to_call)
        if count != 0:
            self._once_handlers[event.type].clear()
            _logger.debug("rt_task destroy count=%d event=%s", count, event.type)

        if not all_handlers:
            return

        _logger.debug("rt_event send count=%d event=%s", len(all_handlers), event.type)
        for handler in all_handlers:
            try:
                result = handler(event)
                if isinstance(result, Awaitable):
                    await result
            except Exception as e:
                _logger.warning("rt_task resolve failed event=%s err=%s", event.type, e, exc_info=True)
