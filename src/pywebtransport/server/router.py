"""Request router for path-based session handling."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from typing import Any, Pattern

from pywebtransport.session import WebTransportSession
from pywebtransport.utils import get_logger

__all__: list[str] = ["RequestRouter", "SessionHandler"]

type SessionHandler = Callable[..., Awaitable[None]]

logger = get_logger(name=__name__)


class RequestRouter:
    """Route session requests to handlers based on path matching."""

    def __init__(self) -> None:
        """Initialize the request router."""
        self._routes: dict[str, SessionHandler] = {}
        self._pattern_routes: list[tuple[Pattern[str], SessionHandler]] = []
        self._default_handler: SessionHandler | None = None

    def route_request(self, *, session: WebTransportSession) -> tuple[SessionHandler, dict[str, Any]] | None:
        """Route a request to the appropriate handler based on the session's path."""
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

    def add_pattern_route(self, *, pattern: str, handler: SessionHandler) -> None:
        """Add a route for a regular expression pattern."""
        try:
            compiled_pattern = re.compile(pattern)
            self._pattern_routes.append((compiled_pattern, handler))
            logger.debug("Added pattern route: %s", pattern)
        except re.error as e:
            logger.error("Invalid regex pattern '%s': %s", pattern, e, exc_info=True)
            raise

    def add_route(self, *, path: str, handler: SessionHandler, override: bool = False) -> None:
        """Add a route for an exact path match."""
        if path in self._routes and not override:
            raise ValueError(f"Route for path '{path}' already exists.")
        self._routes[path] = handler
        logger.debug("Added route: %s", path)

    def remove_pattern_route(self, *, pattern: str) -> None:
        """Remove a route for a regular expression pattern."""
        original_len = len(self._pattern_routes)
        self._pattern_routes = [(p, h) for p, h in self._pattern_routes if p.pattern != pattern]
        if len(self._pattern_routes) < original_len:
            logger.debug("Removed pattern route: %s", pattern)

    def remove_route(self, *, path: str) -> None:
        """Remove a route for an exact path match."""
        if path in self._routes:
            del self._routes[path]
            logger.debug("Removed route: %s", path)

    def set_default_handler(self, *, handler: SessionHandler) -> None:
        """Set a default handler for routes that are not matched."""
        self._default_handler = handler
        logger.debug("Set default handler")

    def get_all_routes(self) -> dict[str, SessionHandler]:
        """Get a copy of all registered exact-match routes."""
        return self._routes.copy()

    def get_route_handler(self, *, path: str) -> SessionHandler | None:
        """Get the handler for a specific path (exact match only)."""
        return self._routes.get(path)

    def get_route_stats(self) -> dict[str, Any]:
        """Get statistics about the configured routes."""
        return {
            "exact_routes": len(self._routes),
            "pattern_routes": len(self._pattern_routes),
            "has_default_handler": self._default_handler is not None,
        }
