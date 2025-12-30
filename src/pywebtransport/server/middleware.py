"""Core framework and common implementations for server middleware."""

from __future__ import annotations

import asyncio
import fnmatch
import http
import time
from collections import deque
from types import TracebackType
from typing import Protocol, Self, runtime_checkable

from pywebtransport.exceptions import ServerError
from pywebtransport.types import Headers, SessionProtocol
from pywebtransport.utils import get_header_as_str, get_logger

__all__: list[str] = [
    "AuthHandlerProtocol",
    "MiddlewareManager",
    "MiddlewareProtocol",
    "MiddlewareRejected",
    "RateLimiter",
    "StatefulMiddlewareProtocol",
    "create_auth_middleware",
    "create_cors_middleware",
    "create_logging_middleware",
    "create_rate_limit_middleware",
]

logger = get_logger(name=__name__)

DEFAULT_RATE_LIMIT_MAX_REQUESTS: int = 100
DEFAULT_RATE_LIMIT_WINDOW: int = 60
DEFAULT_RATE_LIMIT_CLEANUP_INTERVAL: int = 300
DEFAULT_RATE_LIMIT_MAX_TRACKED_IPS: int = 10000


class MiddlewareRejected(Exception):
    """Exception raised by middleware to reject a session request with specific details."""

    def __init__(self, status_code: int = http.HTTPStatus.FORBIDDEN, headers: Headers | None = None) -> None:
        """Initialize the rejection exception."""
        super().__init__(f"Request rejected with status {status_code}")
        self.status_code = status_code
        self.headers = headers if headers is not None else {}


@runtime_checkable
class AuthHandlerProtocol(Protocol):
    """A protocol for authentication handlers."""

    async def __call__(self, *, headers: Headers) -> bool:
        """Perform authentication check on headers."""
        ...


@runtime_checkable
class MiddlewareProtocol(Protocol):
    """A protocol for a middleware object."""

    async def __call__(self, *, session: SessionProtocol) -> None:
        """Process a session request. Raise MiddlewareRejected to deny."""
        ...


@runtime_checkable
class StatefulMiddlewareProtocol(MiddlewareProtocol, Protocol):
    """A protocol for middleware that requires lifecycle management."""

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the async context manager."""
        ...


class MiddlewareManager:
    """Manages a chain of server middleware."""

    def __init__(self) -> None:
        """Initialize the middleware manager."""
        self._middleware: list[MiddlewareProtocol] = []

    def add_middleware(self, *, middleware: MiddlewareProtocol) -> None:
        """Add a middleware to the chain."""
        self._middleware.append(middleware)

    def get_middleware_count(self) -> int:
        """Get the number of registered middleware."""
        return len(self._middleware)

    async def process_request(self, *, session: SessionProtocol) -> None:
        """Process a request through the middleware chain."""
        for middleware in self._middleware:
            try:
                await middleware(session=session)
            except MiddlewareRejected:
                raise
            except Exception as e:
                logger.error("Middleware error: %s", e, exc_info=True)
                raise MiddlewareRejected(status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR) from e

    def remove_middleware(self, *, middleware: MiddlewareProtocol) -> None:
        """Remove a middleware from the chain."""
        if middleware in self._middleware:
            self._middleware.remove(middleware)


class RateLimiter:
    """A stateful, concurrent-safe rate-limiting middleware."""

    def __init__(
        self,
        *,
        max_requests: int = DEFAULT_RATE_LIMIT_MAX_REQUESTS,
        window_seconds: int = DEFAULT_RATE_LIMIT_WINDOW,
        cleanup_interval: int = DEFAULT_RATE_LIMIT_CLEANUP_INTERVAL,
        max_tracked_ips: int = DEFAULT_RATE_LIMIT_MAX_TRACKED_IPS,
    ) -> None:
        """Initialize the rate limiter."""
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._cleanup_interval = cleanup_interval
        self._max_tracked_ips = max_tracked_ips
        self._requests: dict[str, deque[float]] = {}
        self._lock = asyncio.Lock()
        self._tg: asyncio.TaskGroup | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._is_closing = False

    async def __aenter__(self) -> Self:
        """Initialize resources and start the cleanup task."""
        self._is_closing = False
        self._tg = asyncio.TaskGroup()
        await self._tg.__aenter__()
        self._start_cleanup_task()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Stop the background cleanup task and release resources."""
        self._is_closing = True
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()

        if self._tg is not None:
            await self._tg.__aexit__(exc_type, exc_val, exc_tb)

        self._cleanup_task = None
        self._tg = None

    async def _periodic_cleanup(self) -> None:
        """Periodically remove stale IP entries from the tracker."""
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
                        logger.debug("Cleaned up %d stale IP entries.", len(ips_to_remove))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in RateLimiter cleanup task: %s", e, exc_info=True)
                await asyncio.sleep(delay=1.0)

    def _start_cleanup_task(self) -> None:
        """Create and start the periodic cleanup task if not already running."""
        if self._tg is not None and (self._cleanup_task is None or self._cleanup_task.done()):
            self._cleanup_task = self._tg.create_task(coro=self._periodic_cleanup())

    async def __call__(self, *, session: SessionProtocol) -> None:
        """Apply rate limiting to an incoming session."""
        if self._tg is None:
            raise ServerError(
                message=(
                    "RateLimiter has not been activated. It must be used as an "
                    "asynchronous context manager (`async with ...`)."
                )
            )

        client_ip = "unknown"
        if session.remote_address is not None:
            client_ip = session.remote_address[0]

        current_time = time.perf_counter()

        async with self._lock:
            if client_ip not in self._requests:
                if len(self._requests) >= self._max_tracked_ips:
                    self._requests.clear()
                    logger.warning(
                        "Rate limiter IP tracking limit (%d) reached. Flushed all records.", self._max_tracked_ips
                    )
                self._requests[client_ip] = deque()

            client_timestamps = self._requests[client_ip]
            cutoff_time = current_time - self._window_seconds

            while client_timestamps and client_timestamps[0] < cutoff_time:
                client_timestamps.popleft()

            if len(client_timestamps) >= self._max_requests:
                logger.warning("Rate limit exceeded for IP %s", client_ip)
                raise MiddlewareRejected(
                    status_code=http.HTTPStatus.TOO_MANY_REQUESTS, headers={"retry-after": str(self._window_seconds)}
                )

            client_timestamps.append(current_time)


def create_auth_middleware(*, auth_handler: AuthHandlerProtocol) -> MiddlewareProtocol:
    """Create an authentication middleware with a custom handler."""

    async def middleware(*, session: SessionProtocol) -> None:
        try:
            if not await auth_handler(headers=session.headers):
                raise MiddlewareRejected(status_code=http.HTTPStatus.UNAUTHORIZED)
        except MiddlewareRejected:
            raise
        except Exception as e:
            logger.error("Authentication handler error: %s", e, exc_info=True)
            raise MiddlewareRejected(status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR) from e

    return middleware


def create_cors_middleware(*, allowed_origins: list[str]) -> MiddlewareProtocol:
    """Create a CORS middleware to validate the Origin header."""

    async def cors_middleware(*, session: SessionProtocol) -> None:
        origin = get_header_as_str(headers=session.headers, key="origin")
        if origin is None or not origin:
            logger.warning("CORS check failed: 'Origin' header missing.")
            raise MiddlewareRejected(status_code=http.HTTPStatus.FORBIDDEN)

        match_found = False
        for pattern in allowed_origins:
            if fnmatch.fnmatch(name=origin, pat=pattern):
                match_found = True
                break

        if not match_found:
            logger.warning("CORS check failed: Origin '%s' not allowed.", origin)
            raise MiddlewareRejected(status_code=http.HTTPStatus.FORBIDDEN)

    return cors_middleware


def create_logging_middleware() -> MiddlewareProtocol:
    """Create a simple request logging middleware."""

    async def middleware(*, session: SessionProtocol) -> None:
        remote_address_str = "unknown"
        if session.remote_address is not None:
            addr = session.remote_address
            remote_address_str = f"{addr[0]}:{addr[1]}"

        logger.info("Session request: path='%s' from=%s", session.path, remote_address_str)

    return middleware


def create_rate_limit_middleware(
    *,
    max_requests: int = DEFAULT_RATE_LIMIT_MAX_REQUESTS,
    window_seconds: int = DEFAULT_RATE_LIMIT_WINDOW,
    cleanup_interval: int = DEFAULT_RATE_LIMIT_CLEANUP_INTERVAL,
    max_tracked_ips: int = DEFAULT_RATE_LIMIT_MAX_TRACKED_IPS,
) -> RateLimiter:
    """Create a stateful rate-limiting middleware instance."""
    return RateLimiter(
        max_requests=max_requests,
        window_seconds=window_seconds,
        cleanup_interval=cleanup_interval,
        max_tracked_ips=max_tracked_ips,
    )
