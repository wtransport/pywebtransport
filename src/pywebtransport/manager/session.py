"""Manager for concurrent session lifecycles."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, ClassVar

from pywebtransport.constants import ErrorCodes
from pywebtransport.manager._base import BaseResourceManager
from pywebtransport.session import WebTransportSession
from pywebtransport.types import EventType, SessionId, SessionState
from pywebtransport.utils import get_logger

__all__: list[str] = ["SessionManager"]

logger = get_logger(name=__name__)


class SessionManager(BaseResourceManager[SessionId, WebTransportSession]):
    """Manage multiple WebTransport sessions using event-driven cleanup."""

    _log = logger
    _resource_closed_event_type: ClassVar[EventType] = EventType.SESSION_CLOSED

    def __init__(self, *, max_sessions: int) -> None:
        """Initialize the session manager."""
        super().__init__(resource_name="session", max_resources=max_sessions)

    async def add_session(self, *, session: WebTransportSession) -> SessionId:
        """Add a new session and subscribe to its closure event."""
        await super().add_resource(resource=session)
        return session.session_id

    async def remove_session(self, *, session_id: SessionId) -> WebTransportSession | None:
        """Manually remove a session from management."""
        if self._lock is None:
            return None

        removed_session: WebTransportSession | None = None
        async with self._lock:
            if session_id in self._event_handlers:
                emitter, handler = self._event_handlers.pop(session_id)
                try:
                    emitter.off(event_type=self._resource_closed_event_type, handler=handler)
                except (ValueError, KeyError):
                    pass

            removed_session = self._resources.pop(session_id, None)
            if removed_session is not None:
                self._stats["total_closed"] += 1
                self._update_stats_unsafe()
                self._log.debug("Manually removed session %s (total: %d)", session_id, self._stats["current_count"])

        return removed_session

    async def get_sessions_by_state(self, *, state: SessionState) -> list[WebTransportSession]:
        """Retrieve sessions that are in a specific state."""
        if self._lock is None:
            return []
        async with self._lock:
            return [session for session in self._resources.values() if session.state == state]

    async def get_stats(self) -> dict[str, Any]:
        """Get detailed statistics about the managed sessions."""
        stats = await super().get_stats()
        if self._lock is not None:
            async with self._lock:
                states: defaultdict[str, int] = defaultdict(int)
                for session in self._resources.values():
                    states[session.state.value] += 1
                stats["states"] = dict(states)
        return stats

    async def _close_resource(self, *, resource: WebTransportSession) -> None:
        """Close a single session resource."""
        if not resource.is_closed:
            await resource.close(error_code=ErrorCodes.NO_ERROR, reason="Session manager shutdown")

    def _get_resource_id(self, *, resource: WebTransportSession) -> SessionId:
        """Get the unique ID from a session object."""
        return resource.session_id
