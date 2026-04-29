"""Core framework and common implementations for server middleware."""

from __future__ import annotations

import asyncio
import fnmatch
import http
import logging
import time
from collections import deque
from types import TracebackType
from typing import Final, Protocol, Self, runtime_checkable

from pywebtransport.exceptions import ServerError
from pywebtransport.types import Headers, SessionProtocol

__all__: list[str] = [
    "AuthHandlerProtocol",
    "MiddlewareManager",
    "MiddlewareProtocol",
    "MiddlewareRejected",
    "RateLimiter",
    "StatefulMiddlewareProtocol",
    "create_auth_middleware",
    "create_cors_middleware",
    "create_rate_limit_middleware",
]

_CLEANUP_INTERVAL: Final[int] = 300
_TRACKED_IP_CAPACITY: Final[int] = 10000
_WINDOW_REQUEST_LIMIT: Final[int] = 100
_WINDOW_TTL: Final[int] = 60

_logger = logging.getLogger(name=__name__)


class MiddlewareRejected(Exception):
    """Indicate a session request rejection by middleware."""

    __slots__ = ("headers", "status_code")

    def __init__(self, *, status_code: int = http.HTTPStatus.FORBIDDEN, headers: Headers | None = None) -> None:
        """Initialize the instance."""
        super().__init__(f"app_middleware reject err={status_code}")

        self.status_code = status_code
        self.headers = headers if headers is not None else {}


@runtime_checkable
class AuthHandlerProtocol(Protocol):
    """Define the authentication handler interface."""

    async def __call__(self, *, headers: Headers) -> bool:
        """Perform authentication check on headers."""
        ...


@runtime_checkable
class MiddlewareProtocol(Protocol):
    """Define the middleware interface."""

    async def __call__(self, *, session: SessionProtocol) -> None:
        """Process a session request."""
        ...


@runtime_checkable
class StatefulMiddlewareProtocol(MiddlewareProtocol, Protocol):
    """Define the stateful middleware interface."""

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        ...


class MiddlewareManager:
    """Manage a chain of server middleware."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self._middleware: list[MiddlewareProtocol] = []

    def add_middleware(self, *, middleware: MiddlewareProtocol) -> None:
        """Add a middleware to the chain."""
        self._middleware.append(middleware)

    def get_middleware_count(self) -> int:
        """Return the number of registered middleware."""
        return len(self._middleware)

    async def process_request(self, *, session: SessionProtocol) -> None:
        """Process a request through the middleware chain."""
        for middleware in self._middleware:
            try:
                await middleware(session=session)
            except MiddlewareRejected:
                raise
            except Exception as e:
                raise MiddlewareRejected(status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR) from e

    def remove_middleware(self, *, middleware: MiddlewareProtocol) -> None:
        """Remove a middleware from the chain."""
        if middleware in self._middleware:
            self._middleware.remove(middleware)


class RateLimiter:
    """Manage stateful, concurrent-safe rate limiting."""

    def __init__(
        self,
        *,
        max_requests: int = _WINDOW_REQUEST_LIMIT,
        window_seconds: int = _WINDOW_TTL,
        cleanup_interval: int = _CLEANUP_INTERVAL,
        max_tracked_ips: int = _TRACKED_IP_CAPACITY,
    ) -> None:
        """Initialize the instance."""
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._cleanup_interval = cleanup_interval
        self._max_tracked_ips = max_tracked_ips

        self._is_closing = False
        self._lock = asyncio.Lock()
        self._requests: dict[str, deque[float]] = {}

        self._cleanup_task: asyncio.Task[None] | None = None
        self._tg: asyncio.TaskGroup | None = None

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        self._is_closing = False
        self._tg = asyncio.TaskGroup()
        await self._tg.__aenter__()
        self._start_cleanup_task()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        self._is_closing = True
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()

        if self._tg is not None:
            await self._tg.__aexit__(exc_type, exc_val, exc_tb)

        self._cleanup_task = None
        self._tg = None

    async def _periodic_cleanup(self) -> None:
        """Remove stale IP entries from the tracker periodically."""
        while True:
            try:
                await asyncio.sleep(delay=self._cleanup_interval)
                if self._is_closing:
                    break

                async with self._lock:
                    current_time = time.perf_counter()
                    cutoff_time = current_time - self._window_seconds
                    ips_to_remove: list[str] = []

                    for ip, timestamps in self._requests.items():
                        while timestamps and timestamps[0] < cutoff_time:
                            timestamps.popleft()
                        if not timestamps:
                            ips_to_remove.append(ip)

                    for ip in ips_to_remove:
                        del self._requests[ip]

                    if ips_to_remove:
                        _logger.debug("app_middleware evict count=%d", len(ips_to_remove))

            except asyncio.CancelledError:
                break
            except Exception as e:
                _logger.warning("rt_task failed err=%s", e, exc_info=True)
                await asyncio.sleep(delay=1.0)

    def _start_cleanup_task(self) -> None:
        """Initiate the periodic cleanup task."""
        if self._tg is not None and (self._cleanup_task is None or self._cleanup_task.done()):
            self._cleanup_task = self._tg.create_task(coro=self._periodic_cleanup())

    async def __call__(self, *, session: SessionProtocol) -> None:
        """Apply rate limiting to an incoming session."""
        if self._tg is None:
            raise ServerError(message="app_middleware validate failed expected=open")

        client_ip = "unknown"
        if session.remote_address is not None:
            client_ip = session.remote_address[0]

        current_time = time.perf_counter()

        async with self._lock:
            if client_ip not in self._requests:
                if len(self._requests) >= self._max_tracked_ips:
                    _logger.warning(
                        "app_middleware validate exceeded actual=%d limit=%d",
                        len(self._requests),
                        self._max_tracked_ips,
                    )
                    self._requests.clear()
                self._requests[client_ip] = deque()

            client_timestamps = self._requests[client_ip]
            cutoff_time = current_time - self._window_seconds

            while client_timestamps and client_timestamps[0] < cutoff_time:
                client_timestamps.popleft()

            if len(client_timestamps) >= self._max_requests:
                raise MiddlewareRejected(
                    status_code=http.HTTPStatus.TOO_MANY_REQUESTS, headers={"retry-after": str(self._window_seconds)}
                )

            client_timestamps.append(current_time)


def create_auth_middleware(*, auth_handler: AuthHandlerProtocol) -> MiddlewareProtocol:
    """Instantiate authentication middleware with a custom handler."""

    async def middleware(*, session: SessionProtocol) -> None:
        try:
            if not await auth_handler(headers=session.headers):
                raise MiddlewareRejected(status_code=http.HTTPStatus.UNAUTHORIZED)
        except MiddlewareRejected:
            raise
        except Exception as e:
            raise MiddlewareRejected(status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR) from e

    return middleware


def create_cors_middleware(*, allowed_origins: list[str]) -> MiddlewareProtocol:
    """Instantiate CORS middleware to validate the Origin header."""

    async def cors_middleware(*, session: SessionProtocol) -> None:
        origin = _find_header_str(headers=session.headers, key="origin")
        if origin is None or not origin:
            raise MiddlewareRejected(status_code=http.HTTPStatus.FORBIDDEN)

        match_found = False
        for pattern in allowed_origins:
            if fnmatch.fnmatch(name=origin, pat=pattern):
                match_found = True
                break

        if not match_found:
            raise MiddlewareRejected(status_code=http.HTTPStatus.FORBIDDEN)

    return cors_middleware


def create_rate_limit_middleware(
    *,
    max_requests: int = _WINDOW_REQUEST_LIMIT,
    window_seconds: int = _WINDOW_TTL,
    cleanup_interval: int = _CLEANUP_INTERVAL,
    max_tracked_ips: int = _TRACKED_IP_CAPACITY,
) -> RateLimiter:
    """Instantiate a stateful rate-limiting middleware."""
    return RateLimiter(
        max_requests=max_requests,
        window_seconds=window_seconds,
        cleanup_interval=cleanup_interval,
        max_tracked_ips=max_tracked_ips,
    )


def _find_header_str(*, headers: Headers, key: str, default: str | None = None) -> str | None:
    """Search for a header value case-insensitively and return as a decoded string."""
    target_key = key.lower()
    target_key_bytes = target_key.encode("utf-8")

    value: str | bytes | None = None

    if isinstance(headers, dict):
        if target_key in headers:
            value = headers[target_key]
        else:
            value = headers.get(target_key_bytes)
    else:
        for k, v in headers:
            if isinstance(k, bytes):
                if k.lower() == target_key_bytes:
                    value = v
                    break
            elif k.lower() == target_key:
                value = v
                break

    if value is None:
        return default

    if isinstance(value, str):
        return value

    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return default
