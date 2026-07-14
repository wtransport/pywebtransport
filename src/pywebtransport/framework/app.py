"""High-level application framework for building WebTransport servers."""

from __future__ import annotations

import asyncio
import http
import inspect
import logging
import weakref
from collections.abc import Callable
from types import TracebackType
from typing import Any, Self

from pywebtransport.config import ServerConfig
from pywebtransport.connection import WebTransportConnection
from pywebtransport.events import Event
from pywebtransport.exceptions import ConnectionError, ServerError
from pywebtransport.framework.middleware import (
    MiddlewareManager,
    MiddlewareProtocol,
    MiddlewareRejected,
    StatefulMiddlewareProtocol,
)
from pywebtransport.framework.router import RequestRouter, SessionHandler
from pywebtransport.server import WebTransportServer
from pywebtransport.session import WebTransportSession
from pywebtransport.types import EventType

__all__: list[str] = ["ServerApp"]

_logger = logging.getLogger(name=__name__)


class ServerApp:
    """Implement a high-level WebTransport application with routing and middleware."""

    def __init__(self, *, config: ServerConfig) -> None:
        """Initialize the instance."""
        self._handler_tasks: weakref.WeakSet[asyncio.Task[Any]] = weakref.WeakSet()
        self._middleware_manager = MiddlewareManager()
        self._router = RequestRouter()
        self._shutdown_handlers: list[Callable[[], Any]] = []
        self._startup_handlers: list[Callable[[], Any]] = []
        self._stateful_middleware: list[StatefulMiddlewareProtocol] = []

        self._server = WebTransportServer(config=config)
        self._tg: asyncio.TaskGroup | None = None

        self._server.on(event_type=EventType.SESSION_REQUEST, handler=self._handle_session_request)
        _logger.info("app create")

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        await self._server.__aenter__()
        self._tg = asyncio.TaskGroup()
        await self._tg.__aenter__()
        await self.startup()
        _logger.info("app open")
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        try:
            await self.shutdown()
            if self._tg is not None:
                await self._tg.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            await self._server.close()
            _logger.info("app close")

    @property
    def server(self) -> WebTransportServer:
        """Return the underlying WebTransportServer instance."""
        return self._server

    def add_middleware(self, *, middleware: MiddlewareProtocol) -> None:
        """Add a middleware to the processing chain."""
        self._middleware_manager.add_middleware(middleware=middleware)
        if isinstance(middleware, StatefulMiddlewareProtocol):
            self._stateful_middleware.append(middleware)

    def middleware(self, middleware_func: MiddlewareProtocol) -> MiddlewareProtocol:
        """Register a middleware function."""
        self.add_middleware(middleware=middleware_func)
        return middleware_func

    def on_shutdown[F: Callable[..., Any]](self, handler: F) -> F:
        """Register a handler to run on application shutdown."""
        self._shutdown_handlers.append(handler)
        return handler

    def on_startup[F: Callable[..., Any]](self, handler: F) -> F:
        """Register a handler to run on application startup."""
        self._startup_handlers.append(handler)
        return handler

    def pattern_route(self, *, pattern: str) -> Callable[[SessionHandler], SessionHandler]:
        """Register a session handler for a URL pattern."""

        def decorator(handler: SessionHandler) -> SessionHandler:
            self._router.add_pattern_route(pattern=pattern, handler=handler)
            return handler

        return decorator

    def route(self, *, path: str) -> Callable[[SessionHandler], SessionHandler]:
        """Register a session handler for a specific path."""

        def decorator(handler: SessionHandler) -> SessionHandler:
            self._router.add_route(path=path, handler=handler)
            return handler

        return decorator

    def run(self, *, host: str | None = None, port: int | None = None, **kwargs: Any) -> None:
        """Execute the server application in a new asyncio event loop."""
        final_host = host if host is not None else self.server.config.bind_host
        final_port = port if port is not None else self.server.config.bind_port

        async def main() -> None:
            async with self:
                await self.serve(host=final_host, port=final_port, **kwargs)

        try:
            asyncio.run(main=main())
        except KeyboardInterrupt:
            pass

    async def serve(self, *, host: str | None = None, port: int | None = None, **kwargs: Any) -> None:
        """Start the server and listen for connections indefinitely."""
        if self._tg is None:
            raise ServerError(message="app validate failed expected=open")

        final_host = host if host is not None else self.server.config.bind_host
        final_port = port if port is not None else self.server.config.bind_port
        await self._server.listen(host=final_host, port=final_port)
        await self._server.serve_forever()

    async def shutdown(self) -> None:
        """Run shutdown handlers and exit stateful middleware."""
        for handler in self._shutdown_handlers:
            if inspect.iscoroutinefunction(handler):
                await handler()
            else:
                handler()

        for middleware in reversed(self._stateful_middleware):
            await middleware.__aexit__(None, None, None)

        if self._handler_tasks:
            _logger.debug("rt_task abort count=%d", len(self._handler_tasks))
            for task in self._handler_tasks:
                if not task.done():
                    task.cancel()

    async def startup(self) -> None:
        """Run startup handlers and enter stateful middleware."""
        for middleware in self._stateful_middleware:
            await middleware.__aenter__()

        for handler in self._startup_handlers:
            if inspect.iscoroutinefunction(handler):
                await handler()
            else:
                handler()

    async def _dispatch_to_handler(self, *, session: WebTransportSession) -> None:
        """Find the route handler and create a background task to run it."""
        route_result = self._router.route_request(session=session)

        if route_result is None:
            _logger.warning("app_router validate invalid actual=%s session_id=%d", session.path, session.session_id)
            await session.reject(status_code=http.HTTPStatus.METHOD_NOT_ALLOWED)
            return

        handler, params = route_result

        try:
            await session.accept()
        except Exception as e:
            _logger.warning("wt_session open failed session_id=%d err=%s", session.session_id, e, exc_info=True)
            return

        if self._tg is not None:
            handler_task = self._tg.create_task(
                coro=self._run_handler_safely(handler=handler, session=session, params=params)
            )
            self._handler_tasks.add(handler_task)
        else:
            _logger.warning("app validate failed expected=open")

    async def _get_session_from_event(self, *, event: Event) -> WebTransportSession | None:
        """Validate event data and retrieve the existing WebTransportSession handle."""
        if not isinstance(event.data, dict):
            _logger.warning("rt_event validate invalid expected=dict")
            return None

        session = event.data.get("session")
        if not isinstance(session, WebTransportSession):
            _logger.warning("wt_session resolve failed")
            return None

        connection = event.data.get("connection")
        if not isinstance(connection, WebTransportConnection):
            _logger.warning("wt_connection resolve failed")
            return None

        if not connection.is_connected:
            _logger.warning("wt_connection validate failed connection_handle=%d expected=open", connection.handle)
            return None

        return session

    async def _handle_session_request(self, event: Event) -> None:
        """Process an incoming session request event."""
        session: WebTransportSession | None = None
        event_data = event.data if isinstance(event.data, dict) else {}
        session_id_from_data: int | None = event_data.get("session_id")

        try:
            session = await self._get_session_from_event(event=event)

            if session is None:
                return

            await self._middleware_manager.process_request(session=session)
            await self._dispatch_to_handler(session=session)

        except MiddlewareRejected as e:
            sid = session.session_id if session is not None else session_id_from_data
            if e.status_code >= http.HTTPStatus.INTERNAL_SERVER_ERROR:
                _logger.warning("app_middleware validate failed session_id=%d err=%s", sid, e, exc_info=True)
            else:
                _logger.warning("app_middleware validate invalid session_id=%d err=%s", sid, e)
            if session is not None:
                try:
                    await session.reject(status_code=e.status_code)
                except Exception as reject_error:
                    _logger.debug("wt_session reject failed session_id=%d err=%s", session.session_id, reject_error)

        except Exception as e:
            sid = session.session_id if session is not None else session_id_from_data
            _logger.warning("wt_session open failed session_id=%d err=%s", sid, e, exc_info=True)
            if session is not None:
                try:
                    await session.reject(status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR)
                except Exception as cleanup_error:
                    _logger.warning(
                        "wt_session reject failed session_id=%d err=%s",
                        session.session_id,
                        cleanup_error,
                        exc_info=cleanup_error,
                    )

    async def _run_handler_safely(
        self, *, handler: SessionHandler, session: WebTransportSession, params: dict[str, Any]
    ) -> None:
        """Execute the session handler with error handling and resource cleanup."""
        try:
            await handler(session, **params)
        except Exception as handler_error:
            _logger.warning("rt_task failed session_id=%d err=%s", session.session_id, handler_error, exc_info=True)
        finally:
            if not session.is_closed:
                try:
                    await session.close()
                except ConnectionError as e:
                    _logger.debug("wt_session close failed session_id=%d err=%s", session.session_id, e)
                except Exception as close_error:
                    _logger.warning(
                        "wt_session close failed session_id=%d err=%s", session.session_id, close_error, exc_info=True
                    )
