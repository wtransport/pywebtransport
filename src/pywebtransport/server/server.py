"""Core server implementation for accepting WebTransport connections."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import asdict, dataclass
from types import TracebackType
from typing import Any, Self

from pywebtransport._controller.controller import EndpointController
from pywebtransport.config import ServerConfig
from pywebtransport.connection import WebTransportConnection
from pywebtransport.events import Event, EventEmitter
from pywebtransport.exceptions import ServerError
from pywebtransport.manager.connection import ConnectionManager
from pywebtransport.manager.session import SessionManager
from pywebtransport.types import Address, ConnectionState, EventType, SessionState
from pywebtransport.utils import get_logger, get_timestamp

__all__: list[str] = ["ServerDiagnostics", "ServerStats", "WebTransportServer"]

_logger = get_logger(name=__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class ServerDiagnostics:
    """Encapsulate a snapshot of server health."""

    is_serving: bool
    stats: ServerStats
    connection_states: dict[ConnectionState, int]
    max_connections: int
    session_states: dict[SessionState, int]

    @property
    def issues(self) -> list[str]:
        """Return a list of potential issues based on the current diagnostics."""
        issues: list[str] = []
        stats_dict = self.stats.to_dict()

        if not self.is_serving:
            issues.append("Server is not currently serving.")

        total_attempts = stats_dict.get("total_connections_attempted", 0)
        success_rate = stats_dict.get("success_rate", 1.0)
        connections_rejected = stats_dict.get("connections_rejected", 0)

        if total_attempts > 20 and success_rate < 0.9:
            issues.append(f"High connection rejection rate: {connections_rejected}/{total_attempts}")

        active_connections = self.connection_states.get(ConnectionState.CONNECTED, 0)
        if self.max_connections > 0 and (active_connections / max(1, self.max_connections)) > 0.9:
            issues.append(f"High connection usage: {active_connections / self.max_connections:.1%}")

        return issues


@dataclass(kw_only=True, slots=True)
class ServerStats:
    """Encapsulate server statistics."""

    start_time: float | None = None
    connections_accepted: int = 0
    connections_rejected: int = 0
    connection_errors: int = 0
    protocol_errors: int = 0

    @property
    def success_rate(self) -> float:
        """Return the connection success rate."""
        total = self.total_connections_attempted
        if total == 0:
            return 1.0
        return self.connections_accepted / total

    @property
    def total_connections_attempted(self) -> int:
        """Return the total number of connections attempted."""
        return self.connections_accepted + self.connections_rejected

    def to_dict(self) -> dict[str, Any]:
        """Convert statistics to a dictionary."""
        data = asdict(obj=self)
        data["total_connections_attempted"] = self.total_connections_attempted
        data["success_rate"] = self.success_rate
        data["uptime"] = (get_timestamp() - self.start_time) if self.start_time is not None else 0.0
        return data


class WebTransportServer(EventEmitter):
    """Manage the lifecycle and connections for the WebTransport server."""

    def __init__(self, *, config: ServerConfig) -> None:
        """Initialize the instance."""
        config.validate()

        super().__init__(
            max_queue_size=config.max_event_queue_size,
            max_listeners=config.max_event_listeners,
            max_history=config.max_event_history_size,
        )

        self._config = config

        self._closing = False
        self._serving = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._close_task: asyncio.Task[None] | None = None
        self._connection_manager = ConnectionManager(max_connections=config.max_connections)
        self._session_manager = SessionManager(max_sessions=config.max_sessions)

        self._controller: EndpointController | None = None
        self._stats = ServerStats()

        _logger.info("WebTransport server initialized.")

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        await self._connection_manager.__aenter__()
        await self._session_manager.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        await self.close()

    @property
    def config(self) -> ServerConfig:
        """Return the server's configuration object."""
        return self._config

    @property
    def connection_manager(self) -> ConnectionManager:
        """Return the server's connection manager instance."""
        return self._connection_manager

    @property
    def is_serving(self) -> bool:
        """Return True if the server is currently serving."""
        return self._serving

    @property
    def local_addresses(self) -> list[Address]:
        """Return the local addresses the server is bound to."""
        if self._controller is not None:
            return self._controller.get_local_addresses()
        return []

    @property
    def session_manager(self) -> SessionManager:
        """Return the server's session manager instance."""
        return self._session_manager

    async def close(self) -> None:
        """Gracefully shut down the server and its resources."""
        if self._close_task is not None and not self._close_task.done():
            await self._close_task
            return

        if not self._serving:
            return

        self._close_task = asyncio.create_task(coro=self._close_implementation())
        await self._close_task

    async def diagnostics(self) -> ServerDiagnostics:
        """Retrieve a snapshot of the server's diagnostics and statistics."""
        async with asyncio.TaskGroup() as tg:
            conn_task = tg.create_task(coro=self._connection_manager.get_all_resources())
            sess_task = tg.create_task(coro=self._session_manager.get_all_resources())

        connections = conn_task.result()
        sessions = sess_task.result()
        connection_states = Counter(conn.state for conn in connections)
        session_states = Counter(sess.state for sess in sessions)

        return ServerDiagnostics(
            is_serving=self.is_serving,
            stats=self._stats,
            connection_states=dict(connection_states),
            max_connections=self.config.max_connections,
            session_states=dict(session_states),
        )

    async def listen(self, *, host: str | None = None, port: int | None = None) -> None:
        """Start the server and begin listening for connections."""
        if self._serving:
            raise ServerError(message="Server is already serving")

        bind_host = host if host is not None else self._config.bind_host
        bind_port = port if port is not None else self._config.bind_port

        _logger.info("Starting WebTransport server on %s:%s", bind_host, bind_port)

        try:
            listen_config = self._config
            if host is not None or port is not None:
                listen_config = self._config.update(bind_host=bind_host, bind_port=bind_port)

            controller = EndpointController(config=listen_config, is_client=False, loop=asyncio.get_running_loop())

            controller.set_spawn_callback(callback=self._spawn_connection_callback)

            self._controller = controller
            self._serving = True
            self._stats.start_time = get_timestamp()

            addresses = self.local_addresses
            if addresses:
                addr_strs = [f"{ip}:{p}" for ip, p in addresses]
                _logger.info("WebTransport server listening on %s", ", ".join(addr_strs))
            else:
                _logger.info("WebTransport server listening but no addresses acquired.")

        except FileNotFoundError as e:
            _logger.critical("CA/Certificate/Key file error: %s", e)
            raise ServerError(message=f"CA/Certificate/Key file error: {e}") from e
        except Exception as e:
            _logger.critical("Failed to start server: %s", e, exc_info=True)
            raise ServerError(message=f"Failed to start server: {e}") from e

    async def serve_forever(self) -> None:
        """Run the server indefinitely until interrupted."""
        if not self._serving or self._controller is None:
            raise ServerError(message="Server is not listening")

        _logger.info("Server is running. Press Ctrl+C to stop.")
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            _logger.info("serve_forever cancelled.")
        except Exception as e:
            _logger.error("Error during serve_forever wait: %s", e)
        finally:
            _logger.info("serve_forever loop finished.")

    async def _close_implementation(self) -> None:
        """Execute internal server closure logic."""
        _logger.info("Closing WebTransport server...")
        self._serving = False
        self._closing = True

        for task in self._background_tasks:
            if not task.done():
                task.cancel()

        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(coro=self._connection_manager.shutdown())
                tg.create_task(coro=self._session_manager.shutdown())
        except* Exception as eg:
            _logger.error("Errors occurred during manager shutdown: %s", eg.exceptions, exc_info=eg)

        if self._controller is not None:
            self._controller.close()

        self._shutdown_event.set()

        self._closing = False
        _logger.info("WebTransport server closed.")

    def _spawn_connection_callback(self, handle: int) -> None:
        """Instantiate a new WebTransportConnection from the spawned endpoint handle."""
        _logger.debug("Creating WebTransportConnection for handle %d.", handle)

        if self._controller is None:
            _logger.error("Spawn callback triggered but server is not fully initialized.")
            return

        try:
            connection = WebTransportConnection.accept(controller=self._controller, handle=handle, config=self._config)
            task = asyncio.create_task(coro=self._initialize_and_register_connection(connection=connection))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        except Exception as e:
            _logger.error("Error creating WebTransportConnection in callback: %s", e, exc_info=True)

    async def _initialize_and_register_connection(self, connection: WebTransportConnection) -> None:
        """Initialize the connection engine and register it with the manager."""

        async def forward_session_request(event: Event) -> None:
            event_data = event.data.copy() if isinstance(event.data, dict) else {}
            event_data["connection"] = connection

            session = event_data.get("session")
            if session is not None:
                try:
                    await self._session_manager.add_session(session=session)
                except Exception as e:
                    _logger.error("Failed to register session %s: %s", session.session_id, e)

            await self.emit(event_type=EventType.SESSION_REQUEST, data=event_data)

        connection.events.on(event_type=EventType.SESSION_REQUEST, handler=forward_session_request)

        try:
            await self._connection_manager.add_connection(connection=connection)
            self._stats.connections_accepted += 1
            _logger.info("New connection registered: %s", connection.connection_id)
        except Exception as e:
            self._stats.connections_rejected += 1
            self._stats.connection_errors += 1
            _logger.error("Failed to initialize/register new connection: %s", e, exc_info=True)
            connection.events.off(event_type=EventType.SESSION_REQUEST, handler=forward_session_request)
            if not connection.is_closed:
                await connection.close()
        else:

            async def cleanup_listener(event: Event) -> None:
                connection.events.off(event_type=EventType.SESSION_REQUEST, handler=forward_session_request)

            connection.events.once(event_type=EventType.CONNECTION_CLOSED, handler=cleanup_listener)

    def __str__(self) -> str:
        """Return the string representation."""
        status = "serving" if self.is_serving else "stopped"

        addresses = self.local_addresses
        if addresses:
            address_str = "[" + ", ".join(f"{ip}:{port}" for ip, port in addresses) + "]"
        else:
            address_str = "unknown"

        conn_count = len(self._connection_manager)
        sess_count = len(self._session_manager)
        return (
            f"WebTransportServer(status={status}, "
            f"addresses={address_str}, "
            f"connections={conn_count}, "
            f"sessions={sess_count})"
        )
