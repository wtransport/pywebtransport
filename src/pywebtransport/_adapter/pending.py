"""Internal manager for pending asyncio requests."""

from __future__ import annotations

import asyncio
import itertools
from typing import Any

from pywebtransport.types import Future, RequestId

__all__: list[str] = []


class PendingRequestManager:
    """Manage lifecycle of pending asynchronous requests."""

    def __init__(self) -> None:
        """Initialize the pending request manager."""
        self._requests: dict[RequestId, Future[Any]] = {}
        self._counter = itertools.count()

    def create_request(self) -> tuple[RequestId, Future[Any]]:
        """Create a new pending request and return its ID and Future."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        request_id = next(self._counter)
        self._requests[request_id] = future
        return request_id, future

    def complete_request(self, *, request_id: RequestId, result: Any) -> None:
        """Complete a pending request with a result."""
        future = self._requests.pop(request_id, None)
        if future is not None and not future.done():
            future.set_result(result)

    def fail_request(self, *, request_id: RequestId, exception: Exception) -> None:
        """Fail a pending request with an exception."""
        future = self._requests.pop(request_id, None)
        if future is not None and not future.done():
            future.set_exception(exception)

    def fail_all(self, *, exception: Exception) -> None:
        """Fail all pending requests with the given exception."""
        while self._requests:
            _, future = self._requests.popitem()
            if not future.done():
                future.set_exception(exception)
