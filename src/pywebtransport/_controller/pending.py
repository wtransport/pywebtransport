"""Pending request manager."""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Callable
from typing import Any

from pywebtransport.types import RequestId

__all__: list[str] = []


class PendingRequestManager:

    __slots__ = ("_counter", "_requests")

    def __init__(self) -> None:
        self._counter = itertools.count()
        self._requests: dict[RequestId, tuple[asyncio.Future[Any], Callable[[Any], Any] | None]] = {}

    def complete_request(self, *, request_id: RequestId, result: Any) -> None:
        request = self._requests.pop(request_id, None)
        if request is None:
            return

        future, resolver = request
        if future.done():
            return

        if resolver is None:
            future.set_result(result)
            return

        try:
            resolved_result = resolver(result)
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(resolved_result)

    def create_request(self, *, resolver: Callable[[Any], Any] | None = None) -> tuple[RequestId, asyncio.Future[Any]]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        request_id = next(self._counter)
        self._requests[request_id] = (future, resolver)
        return request_id, future

    def evict_request(self, *, request_id: RequestId) -> None:
        self._requests.pop(request_id, None)

    def fail_all(self, *, exception: Exception) -> None:
        while self._requests:
            _, (future, _) = self._requests.popitem()
            if not future.done():
                future.set_exception(exception)

    def fail_request(self, *, request_id: RequestId, exception: Exception) -> None:
        request = self._requests.pop(request_id, None)
        if request is None:
            return

        future, _ = request
        if not future.done():
            future.set_exception(exception)
