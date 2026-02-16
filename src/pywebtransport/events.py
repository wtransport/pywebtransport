"""Core components for the library's event-driven architecture."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from pywebtransport.constants import (
    DEFAULT_MAX_EVENT_HISTORY_SIZE,
    DEFAULT_MAX_EVENT_LISTENERS,
    DEFAULT_MAX_EVENT_QUEUE_SIZE,
)
from pywebtransport.types import EventData, EventType, Future, Timeout
from pywebtransport.utils import get_logger, get_timestamp

__all__: list[str] = ["Event", "EventEmitter", "EventHandler"]

type EventHandler = Callable[[Event], Awaitable[None] | None]

_logger = get_logger(name=__name__)


@dataclass(kw_only=True, frozen=True, slots=True)
class Event:
    """Encapsulate system event data."""

    type: EventType | str
    timestamp: float = field(default_factory=get_timestamp)
    data: EventData | None = None
    source: Any | None = None

    def __post_init__(self) -> None:
        """Validate and adjust attributes after initialization."""
        if isinstance(self.type, str):
            try:
                object.__setattr__(self, "type", EventType(self.type))
            except ValueError:
                _logger.warning("Unknown event type string: '%s'", self.type)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the event to a dictionary."""
        return {
            "type": self.type,
            "timestamp": self.timestamp,
            "data": self.data,
            "source": str(self.source) if self.source is not None else None,
        }

    def __repr__(self) -> str:
        """Return the string representation."""
        return f"Event(type={self.type}, timestamp={self.timestamp})"

    def __str__(self) -> str:
        """Return a simplified string representation."""
        return f"Event({self.type})"


class EventEmitter:
    """Manage event subscriptions and dispatching."""

    __slots__ = (
        "_max_listeners",
        "_max_history",
        "_background_tasks",
        "_event_history",
        "_event_queue",
        "_handlers",
        "_once_handlers",
        "_paused",
        "_processing_task",
        "_wildcard_handlers",
    )

    def __init__(
        self,
        *,
        max_listeners: int = DEFAULT_MAX_EVENT_LISTENERS,
        max_history: int = DEFAULT_MAX_EVENT_HISTORY_SIZE,
        max_queue_size: int = DEFAULT_MAX_EVENT_QUEUE_SIZE,
    ) -> None:
        """Initialize the instance."""
        self._max_listeners = max_listeners
        self._max_history = max_history

        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._event_history: deque[Event] = deque(maxlen=max_history) if max_history > 0 else deque()
        self._event_queue: deque[Event] = deque(maxlen=max_queue_size)
        self._handlers: dict[EventType | str, list[EventHandler]] = defaultdict(list)
        self._once_handlers: dict[EventType | str, list[EventHandler]] = defaultdict(list)
        self._paused = False
        self._processing_task: asyncio.Task[None] | None = None
        self._wildcard_handlers: list[EventHandler] = []

    def clear_history(self) -> None:
        """Purge the event history buffer."""
        self._event_history.clear()
        _logger.debug("Event history cleared")

    async def close(self) -> None:
        """Terminate processing and clear listeners."""
        if self._processing_task is not None and not self._processing_task.done():
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass

        for task in self._background_tasks:
            if not task.done():
                task.cancel()

        self.remove_all_listeners()
        _logger.debug("EventEmitter closed and listeners cleared")

    async def emit(self, *, event_type: EventType | str, data: EventData | None = None, source: Any = None) -> None:
        """Dispatch an event to registered listeners."""
        event = Event(type=event_type, data=data, source=source)
        self._add_to_history(event=event)

        if self._paused:
            self._enqueue_event(event)
            return

        await self._process_event(event=event)

    def emit_nowait(self, *, event_type: EventType | str, data: EventData | None = None, source: Any = None) -> None:
        """Schedule an event dispatch without awaiting."""
        event = Event(type=event_type, data=data, source=source)
        self._add_to_history(event=event)

        if self._paused:
            self._enqueue_event(event)
            return

        try:
            task = asyncio.create_task(coro=self._process_event(event=event))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        except RuntimeError:
            _logger.warning("No running event loop, cannot schedule emit for %s", event_type)

    def get_event_history(self, *, event_type: EventType | str | None = None, limit: int = 100) -> list[Event]:
        """Retrieve the recorded event history."""
        if event_type is None:
            return list(self._event_history)[-limit:]

        filtered_events = [event for event in self._event_history if event.type == event_type]
        return filtered_events[-limit:]

    def get_stats(self) -> dict[str, Any]:
        """Retrieve internal statistics."""
        total_handlers = sum(len(handlers) for handlers in self._handlers.values())
        total_once_handlers = sum(len(handlers) for handlers in self._once_handlers.values())
        return {
            "total_handlers": total_handlers,
            "total_once_handlers": total_once_handlers,
            "wildcard_handlers": len(self._wildcard_handlers),
            "event_types": len(self._handlers),
            "history_size": len(self._event_history),
            "queued_events": len(self._event_queue),
            "paused": self._paused,
        }

    def listener_count(self, *, event_type: EventType | str) -> int:
        """Return the number of listeners for an event."""
        return len(self.listeners(event_type=event_type))

    def listeners(self, *, event_type: EventType | str) -> list[EventHandler]:
        """Return the list of handlers for an event."""
        return self._handlers[event_type][:] + self._once_handlers[event_type][:]

    def off(self, *, event_type: EventType | str, handler: EventHandler | None = None) -> None:
        """Unregister specific or all handlers for an event."""
        if handler is None:
            self._handlers[event_type].clear()
            self._once_handlers[event_type].clear()
            _logger.debug("Removed all handlers for event %s", event_type)
        else:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                _logger.debug("Removed handler for event %s", event_type)
            if handler in self._once_handlers[event_type]:
                self._once_handlers[event_type].remove(handler)
                _logger.debug("Removed once handler for event %s", event_type)

    def off_any(self, *, handler: EventHandler | None = None) -> None:
        """Unregister wildcard handlers."""
        if handler is None:
            self._wildcard_handlers.clear()
            _logger.debug("Removed all wildcard handlers")
        elif handler in self._wildcard_handlers:
            self._wildcard_handlers.remove(handler)
            _logger.debug("Removed wildcard handler")

    def on(self, *, event_type: EventType | str, handler: EventHandler) -> None:
        """Register a persistent event handler."""
        handlers = self._handlers[event_type]
        if len(handlers) >= self._max_listeners:
            _logger.warning("Maximum listeners (%d) exceeded for event %s", self._max_listeners, event_type)

        if handler not in handlers:
            handlers.append(handler)
            _logger.debug("Registered handler for event %s", event_type)
        else:
            _logger.warning("Handler already registered for event %s", event_type)

    def on_any(self, *, handler: EventHandler) -> None:
        """Register a wildcard handler for all events."""
        if handler not in self._wildcard_handlers:
            self._wildcard_handlers.append(handler)
            _logger.debug("Registered wildcard handler")

    def once(self, *, event_type: EventType | str, handler: EventHandler) -> None:
        """Register a one-time event handler."""
        once_handlers = self._once_handlers[event_type]

        if handler not in once_handlers:
            once_handlers.append(handler)
            _logger.debug("Registered once handler for event %s", event_type)

    def pause(self) -> None:
        """Suspend event processing."""
        self._paused = True
        _logger.debug("Event processing paused")

    def remove_all_listeners(self, *, event_type: EventType | str | None = None) -> None:
        """Clear listeners for a specific or all events."""
        if event_type is None:
            self._handlers.clear()
            self._once_handlers.clear()
            self._wildcard_handlers.clear()
            _logger.debug("Removed all event listeners")
        else:
            self._handlers[event_type].clear()
            self._once_handlers[event_type].clear()
            _logger.debug("Removed all listeners for event %s", event_type)

    def resume(self) -> asyncio.Task[None] | None:
        """Resume event processing and flush queue."""
        self._paused = False
        _logger.debug("Event processing resumed")

        if self._event_queue and (self._processing_task is None or self._processing_task.done()):
            self._processing_task = asyncio.create_task(coro=self._process_queued_events())
            return self._processing_task
        return None

    def set_max_listeners(self, *, max_listeners: int) -> None:
        """Update the maximum listener limit."""
        self._max_listeners = max_listeners

    async def wait_for(
        self,
        *,
        event_type: EventType | str | list[EventType | str],
        timeout: Timeout | None = None,
        condition: Callable[[Event], bool] | None = None,
    ) -> Event:
        """Await the emission of a specific event."""
        future: Future[Event] = asyncio.Future()
        event_types = [event_type] if isinstance(event_type, (str, EventType)) else event_type

        async def handler(event: Event) -> None:
            try:
                if condition is None or condition(event):
                    if not future.done():
                        future.set_result(event)
            except Exception as e:
                if not future.done():
                    future.set_exception(e)

        for et in event_types:
            self.on(event_type=et, handler=handler)

        try:
            try:
                async with asyncio.timeout(delay=timeout):
                    return await future
            except asyncio.TimeoutError:
                future.cancel()
                raise
        finally:
            for et in event_types:
                self.off(event_type=et, handler=handler)

    def _add_to_history(self, *, event: Event) -> None:
        """Record an event in the history buffer."""
        if self._max_history > 0:
            self._event_history.append(event)

    def _enqueue_event(self, event: Event) -> None:
        """Add an event to the processing queue."""
        if self._event_queue.maxlen is not None and len(self._event_queue) >= self._event_queue.maxlen:
            _logger.warning("Event queue full, dropping oldest event to make room")
        self._event_queue.append(event)

    async def _process_event(self, *, event: Event) -> None:
        """Execute handlers for a specific event."""
        handlers_to_call: list[EventHandler] = self._handlers[event.type][:]
        once_handlers_to_call: list[EventHandler] = self._once_handlers[event.type][:]
        all_handlers = handlers_to_call + once_handlers_to_call + self._wildcard_handlers

        if once_handlers_to_call:
            self._once_handlers[event.type].clear()

        if not all_handlers:
            return

        _logger.debug("Emitting event %s to %d handlers", event.type, len(all_handlers))
        for handler in all_handlers:
            try:
                result = handler(event)
                if isinstance(result, Awaitable):
                    await result
            except Exception as e:
                _logger.error("Error in handler for event %s: %s", event.type, e, exc_info=True)

    async def _process_queued_events(self) -> None:
        """Process pending events in the queue."""
        while self._event_queue and not self._paused:
            event = self._event_queue.popleft()
            await self._process_event(event=event)
