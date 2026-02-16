"""Request router for path-based session handling."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from typing import Any, Pattern

from pywebtransport.session import WebTransportSession
from pywebtransport.utils import get_logger

__all__: list[str] = ["RequestRouter", "SessionHandler"]

type SessionHandler = Callable[..., Awaitable[None]]

_logger = get_logger(name=__name__)


class RequestRouter:
    """Route session requests to handlers based on path matching."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self._routes: dict[str, SessionHandler] = {}
        self._pattern_routes: list[tuple[Pattern[str], SessionHandler]] = []
        self._default_handler: SessionHandler | None = None

    def add_pattern_route(self, *, pattern: str, handler: SessionHandler) -> None:
        """Register a route for a regular expression pattern."""
        try:
            compiled_pattern = re.compile(pattern)
            self._pattern_routes.append((compiled_pattern, handler))
            _logger.debug("Added pattern route: %s", pattern)
        except re.error as e:
            _logger.error("Invalid regex pattern '%s': %s", pattern, e, exc_info=True)
            raise

    def add_route(self, *, path: str, handler: SessionHandler, override: bool = False) -> None:
        """Register a route for an exact path match."""
        if path in self._routes and not override:
            raise ValueError(f"Route for path '{path}' already exists.")
        self._routes[path] = handler
        _logger.debug("Added route: %s", path)

    def get_all_routes(self) -> dict[str, SessionHandler]:
        """Return a copy of all registered exact-match routes."""
        return self._routes.copy()

    def get_route_handler(self, *, path: str) -> SessionHandler | None:
        """Return the handler for a specific path (exact match only)."""
        return self._routes.get(path)

    def get_route_stats(self) -> dict[str, Any]:
        """Return statistics about the configured routes."""
        return {
            "exact_routes": len(self._routes),
            "pattern_routes": len(self._pattern_routes),
            "has_default_handler": self._default_handler is not None,
        }

    def remove_pattern_route(self, *, pattern: str) -> None:
        """Unregister a route for a regular expression pattern."""
        original_len = len(self._pattern_routes)
        self._pattern_routes = [(p, h) for p, h in self._pattern_routes if p.pattern != pattern]
        if len(self._pattern_routes) < original_len:
            _logger.debug("Removed pattern route: %s", pattern)

    def remove_route(self, *, path: str) -> None:
        """Unregister a route for an exact path match."""
        if path in self._routes:
            del self._routes[path]
            _logger.debug("Removed route: %s", path)

    def route_request(self, *, session: WebTransportSession) -> tuple[SessionHandler, dict[str, Any]] | None:
        """Dispatch a request to the appropriate handler based on the session path."""
        path = session.path

        if path in self._routes:
            return (self._routes[path], {})

        for pattern, pattern_handler in self._pattern_routes:
            match = pattern.fullmatch(path)
            if match is not None:
                return (pattern_handler, match.groupdict())

        if self._default_handler is not None:
            return (self._default_handler, {})

        return None

    def set_default_handler(self, *, handler: SessionHandler) -> None:
        """Configure a default handler for unmatched routes."""
        self._default_handler = handler
        _logger.debug("Set default handler")
