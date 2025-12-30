"""Client-side entry point for WebTransport connections."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import asdict, dataclass, field
from types import TracebackType
from typing import Any, Self

from pywebtransport.client.utils import normalize_headers, parse_webtransport_url, validate_url
from pywebtransport.config import ClientConfig
from pywebtransport.connection import WebTransportConnection
from pywebtransport.events import EventEmitter
from pywebtransport.exceptions import ClientError, ConnectionError, TimeoutError
from pywebtransport.manager.connection import ConnectionManager
from pywebtransport.session import WebTransportSession
from pywebtransport.types import URL, ConnectionState, EventType, Headers
from pywebtransport.utils import format_duration, get_logger, get_timestamp, merge_headers
from pywebtransport.version import __version__

__all__: list[str] = ["ClientDiagnostics", "ClientStats", "WebTransportClient"]

logger = get_logger(name=__name__)


@dataclass(frozen=True, kw_only=True)
class ClientDiagnostics:
    """An immutable snapshot of the client's health and statistics."""

    stats: ClientStats
    connection_states: dict[ConnectionState, int]

    @property
    def issues(self) -> list[str]:
        """Get a list of potential issues based on current diagnostics."""
        issues: list[str] = []
        stats_dict = self.stats.to_dict()

        connections_attempted = stats_dict.get("connections_attempted", 0)
        success_rate = stats_dict.get("success_rate", 1.0)
        if connections_attempted > 10 and success_rate < 0.9:
            issues.append(f"Low connection success rate: {success_rate:.2%}")

        avg_connect_time = stats_dict.get("avg_connect_time", 0.0)
        if avg_connect_time > 5.0:
            issues.append(f"Slow average connection time: {avg_connect_time:.2f}s")

        return issues


@dataclass(kw_only=True)
class ClientStats:
    """Stores client-wide connection statistics."""

    created_at: float = field(default_factory=get_timestamp)
    connections_attempted: int = 0
    connections_successful: int = 0
    connections_failed: int = 0
    total_connect_time: float = 0.0
    min_connect_time: float = float("inf")
    max_connect_time: float = 0.0

    @property
    def avg_connect_time(self) -> float:
        """Get the average connection time."""
        if self.connections_successful == 0:
            return 0.0

        return self.total_connect_time / self.connections_successful

    @property
    def success_rate(self) -> float:
        """Get the connection success rate."""
        if self.connections_attempted == 0:
            return 1.0

        return self.connections_successful / self.connections_attempted

    def to_dict(self) -> dict[str, Any]:
        """Convert statistics to a dictionary."""
        data = asdict(obj=self)
        data["avg_connect_time"] = self.avg_connect_time
        data["success_rate"] = self.success_rate
        data["uptime"] = get_timestamp() - self.created_at
        if data["min_connect_time"] == float("inf"):
            data["min_connect_time"] = 0.0
        return data


class WebTransportClient(EventEmitter):
    """A client for establishing WebTransport connections and sessions."""

    def __init__(self, *, config: ClientConfig | None = None) -> None:
        """Initialize the WebTransport client."""
        self._config = config if config is not None else ClientConfig()
        super().__init__(
            max_queue_size=self._config.max_event_queue_size,
            max_listeners=self._config.max_event_listeners,
            max_history=self._config.max_event_history_size,
        )
        self._connection_manager = ConnectionManager(max_connections=self._config.max_connections)
        self._default_headers: Headers = []
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._stats = ClientStats()

        logger.info("WebTransport client initialized")

    @property
    def config(self) -> ClientConfig:
        """Get the client's configuration object."""
        return self._config

    @property
    def is_closed(self) -> bool:
        """Check if the client is closed."""
        return self._closed

    async def __aenter__(self) -> Self:
        """Enter the async context for the client."""
        await self._connection_manager.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the async context and close the client."""
        await self.close()

    async def close(self) -> None:
        """Close the client and all underlying connections."""
        if self._close_task is not None and not self._close_task.done():
            await self._close_task
            return

        if self._closed:
            return

        self._close_task = asyncio.create_task(coro=self._close_implementation())
        await self._close_task

    async def connect(
        self, *, url: URL, timeout: float | None = None, headers: Headers | None = None
    ) -> WebTransportSession:
        """Connect to a WebTransport server and return a session."""
        if self._closed:
            raise ClientError(message="Client is closed")

        validate_url(url=url)
        host, port, path = parse_webtransport_url(url=url)
        connect_timeout = timeout if timeout is not None else self._config.connect_timeout
        logger.info("Connecting to %s:%s%s", host, port, path)
        self._stats.connections_attempted += 1

        connection: WebTransportConnection | None = None
        success = False
        start_time = get_timestamp()

        try:
            async with asyncio.timeout(delay=connect_timeout):
                merged_headers = merge_headers(base=self._default_headers, update=headers)
                normalized_headers = normalize_headers(headers=merged_headers)

                has_ua = False
                if isinstance(normalized_headers, dict):
                    has_ua = "user-agent" in normalized_headers
                else:
                    has_ua = any(key == "user-agent" for key, _ in normalized_headers)

                if not has_ua:
                    default_ua = (
                        self._config.user_agent
                        if self._config.user_agent is not None
                        else f"PyWebTransport/{__version__}"
                    )
                    if isinstance(normalized_headers, dict):
                        normalized_headers["user-agent"] = default_ua
                    else:
                        normalized_headers.append(("user-agent", default_ua))

                conn_config = self._config.update(headers=normalized_headers)

                connection = await WebTransportConnection.connect(
                    host=host, port=port, config=conn_config, loop=asyncio.get_running_loop()
                )

                if connection.state != ConnectionState.CONNECTED:
                    logger.debug("Waiting for connection establishment events...")
                    await connection.events.wait_for(
                        event_type=[
                            EventType.CONNECTION_ESTABLISHED,
                            EventType.CONNECTION_FAILED,
                            EventType.CONNECTION_CLOSED,
                        ]
                    )

                if connection.state != ConnectionState.CONNECTED:
                    raise ConnectionError(message=f"Connection failed state={connection.state}")

                await self._connection_manager.add_connection(connection=connection)

                logger.debug("Initiating session creation...")
                session = await connection.create_session(path=path, headers=normalized_headers)
                logger.debug("Session creation successful: %s", session.session_id)

                elapsed = get_timestamp() - start_time
                self._update_success_stats(connect_time=elapsed)
                logger.info("Session established to %s in %s", url, format_duration(seconds=elapsed))
                success = True
                return session

        except asyncio.TimeoutError as e:
            self._stats.connections_failed += 1
            stage = (
                "session negotiation"
                if connection is not None and connection.is_connected
                else "QUIC connection establishment"
            )
            logger.error(
                "Connection timeout to %s during %s after %s", url, stage, format_duration(seconds=connect_timeout)
            )
            raise TimeoutError(message=f"Connection timeout to {url} during {stage}") from e
        except ConnectionRefusedError as e:
            self._stats.connections_failed += 1
            logger.error("Connection refused by %s:%d", host, port)
            raise ConnectionError(message=f"Connection refused by {host}:{port}") from e
        except Exception as e:
            self._stats.connections_failed += 1
            logger.error("Failed to connect to %s: %s", url, e, exc_info=True)
            if "certificate verify failed" in str(e):
                raise ConnectionError(message=f"Certificate verification failed for {url}: {e}") from e
            raise ClientError(message=f"Failed to connect to {url}: {e}") from e
        finally:
            if not success and connection is not None and not connection.is_closed:
                await connection.close()

    async def diagnostics(self) -> ClientDiagnostics:
        """Get a snapshot of the client's diagnostics and statistics."""
        connections = await self._connection_manager.get_all_resources()
        state_counts = Counter(conn.state for conn in connections)

        return ClientDiagnostics(stats=self._stats, connection_states=dict(state_counts))

    def set_default_headers(self, *, headers: Headers) -> None:
        """Set default headers for all subsequent connections."""
        self._default_headers = merge_headers(base=[], update=headers)

    async def _close_implementation(self) -> None:
        """Internal implementation of client closure."""
        logger.info("Closing WebTransport client...")
        self._closed = True
        await self._connection_manager.shutdown()
        logger.info("WebTransport client closed.")

    def _update_success_stats(self, *, connect_time: float) -> None:
        """Update connection statistics on a successful connection."""
        self._stats.connections_successful += 1
        self._stats.total_connect_time += connect_time
        self._stats.min_connect_time = min(self._stats.min_connect_time, connect_time)
        self._stats.max_connect_time = max(self._stats.max_connect_time, connect_time)

    def __str__(self) -> str:
        """Format a concise summary of client information for logging."""
        status = "closed" if self.is_closed else "open"
        conn_count = len(self._connection_manager)
        return f"WebTransportClient(status={status}, connections={conn_count})"
