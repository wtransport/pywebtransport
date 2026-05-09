"""Internal manager for tracking and completing asyncio requests."""

from __future__ import annotations

import asyncio
import itertools
from typing import Any

from pywebtransport.types import Future, RequestId

__all__: list[str] = []


class PendingRequestManager:
    """Manage the lifecycle of pending asynchronous requests."""

    __slots__ = ("_counter", "_requests")

    def __init__(self) -> None:
        """Initialize the instance."""
        self._requests: dict[RequestId, Future[Any]] = {}
        self._counter = itertools.count()

    def complete_request(self, *, request_id: RequestId, result: Any) -> None:
        """Resolve a pending request with a successful result."""
        future = self._requests.pop(request_id, None)
        if future is not None and not future.done():
            future.set_result(result)

    def create_request(self) -> tuple[RequestId, Future[Any]]:
        """Create a new tracked request and return its ID and Future."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        request_id = next(self._counter)
        self._requests[request_id] = future
        return request_id, future

    def fail_all(self, *, exception: Exception) -> None:
        """Reject all currently pending requests with the given exception."""
        while self._requests:
            _, future = self._requests.popitem()
            if not future.done():
                future.set_exception(exception)

    def fail_request(self, *, request_id: RequestId, exception: Exception) -> None:
        """Reject a pending request with an exception."""
        future = self._requests.pop(request_id, None)
        if future is not None and not future.done():
            future.set_exception(exception)

    def unregister_request(self, *, request_id: RequestId) -> None:
        """Remove a request from active ownership tracking."""
        self._requests.pop(request_id, None)
