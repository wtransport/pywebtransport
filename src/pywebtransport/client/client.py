"""Client-side entry point for WebTransport connections."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import asdict, dataclass, field
from types import TracebackType
from typing import Any, Final, Self

from pywebtransport._driver.driver import EndpointDriver, create_client
from pywebtransport.client.utils import normalize_headers, parse_webtransport_url
from pywebtransport.config import ClientConfig
from pywebtransport.connection import WebTransportConnection
from pywebtransport.events import EventEmitter
from pywebtransport.exceptions import ClientError, ConnectionError, TimeoutError
from pywebtransport.manager.connection import ConnectionManager
from pywebtransport.session import WebTransportSession
from pywebtransport.types import URL, ConnectionState, EventType, Headers
from pywebtransport.utils import format_duration, get_logger, get_timestamp, merge_headers, resolve_host
from pywebtransport.version import __version__

__all__: list[str] = ["ClientDiagnostics", "ClientStats", "WebTransportClient"]

_HEALTH_MAX_CONNECT_TIME: Final[float] = 5.0
_HEALTH_MIN_ATTEMPTS: Final[int] = 10
_HEALTH_MIN_SUCCESS_RATE: Final[float] = 0.9

_logger = get_logger(name=__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class ClientDiagnostics:
    """Encapsulate the client's health and statistics."""

    stats: ClientStats
    connection_states: dict[ConnectionState, int]

    @property
    def issues(self) -> list[str]:
        """Return a list of potential health issues."""
        issues: list[str] = []
        stats_dict = self.stats.to_dict()

        connections_attempted = stats_dict.get("connections_attempted", 0)
        success_rate = stats_dict.get("success_rate", 1.0)
        if connections_attempted > _HEALTH_MIN_ATTEMPTS and success_rate < _HEALTH_MIN_SUCCESS_RATE:
            issues.append(f"Low connection success rate: {success_rate:.2%}")

        avg_connect_time = stats_dict.get("avg_connect_time", 0.0)
        if avg_connect_time > _HEALTH_MAX_CONNECT_TIME:
            issues.append(f"Slow average connection time: {avg_connect_time:.2f}s")

        return issues


@dataclass(kw_only=True, slots=True)
class ClientStats:
    """Encapsulate client-wide connection statistics."""

    created_at: float = field(default_factory=get_timestamp)
    connections_attempted: int = 0
    connections_successful: int = 0
    connections_failed: int = 0
    total_connect_time: float = 0.0
    min_connect_time: float = float("inf")
    max_connect_time: float = 0.0

    @property
    def avg_connect_time(self) -> float:
        """Return the average connection time."""
        if self.connections_successful == 0:
            return 0.0

        return self.total_connect_time / self.connections_successful

    @property
    def success_rate(self) -> float:
        """Return the connection success rate."""
        if self.connections_attempted == 0:
            return 1.0

        return self.connections_successful / self.connections_attempted

    def to_dict(self) -> dict[str, Any]:
        """Serialize statistics to a dictionary."""
        data = asdict(obj=self)
        data["avg_connect_time"] = self.avg_connect_time
        data["success_rate"] = self.success_rate
        data["uptime"] = get_timestamp() - self.created_at
        if data["min_connect_time"] == float("inf"):
            data["min_connect_time"] = 0.0
        return data


class WebTransportClient(EventEmitter):
    """Manage WebTransport connections and sessions."""

    def __init__(self, *, config: ClientConfig | None = None) -> None:
        """Initialize the instance."""
        effective_config = config if config is not None else ClientConfig()

        super().__init__(
            max_queue_size=effective_config.max_event_queue_size,
            max_listeners=effective_config.max_event_listeners,
            max_history=effective_config.max_event_history_size,
        )

        self._config = effective_config

        self._closed = False
        self._init_lock = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._connection_manager = ConnectionManager(max_connections=self._config.max_connections)
        self._default_headers: Headers = []

        self._driver: EndpointDriver | None = None
        self._stats = ClientStats()
        self._transport: asyncio.DatagramTransport | None = None

        _logger.info("WebTransport client initialized")

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        await self._connection_manager.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        await self.close()

    @property
    def config(self) -> ClientConfig:
        """Return the client configuration."""
        return self._config

    @property
    def is_closed(self) -> bool:
        """Return True if the client is closed."""
        return self._closed

    async def close(self) -> None:
        """Terminate the client and all underlying connections."""
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
        """Establish a WebTransport session."""
        if self._closed:
            raise ClientError(message="Client is closed")

        host, port, path = parse_webtransport_url(url=url)
        connect_timeout = timeout if timeout is not None else self._config.connect_timeout
        _logger.info("Connecting to %s:%s%s", host, port, path)
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

                if self._driver is None or self._transport is None:
                    async with self._init_lock:
                        if self._driver is None or self._transport is None:
                            self._transport, self._driver = await create_client(
                                config=self._config, loop=asyncio.get_running_loop()
                            )

                assert self._driver is not None
                assert self._transport is not None

                resolved_ips = await resolve_host(host=host, port=port)

                connection = await self._race_addresses(
                    addresses=resolved_ips, port=port, host=host, conn_config=conn_config
                )

                await self._connection_manager.add_connection(connection=connection)

                _logger.debug("Initiating session creation...")
                session = await connection.create_session(path=path, headers=normalized_headers)
                _logger.debug("Session creation successful: %s", session.session_id)

                elapsed = get_timestamp() - start_time
                self._update_success_stats(connect_time=elapsed)
                _logger.info("Session established to %s in %s", url, format_duration(seconds=elapsed))
                success = True
                return session

        except asyncio.TimeoutError as e:
            self._stats.connections_failed += 1
            stage = (
                "session negotiation"
                if connection is not None and connection.is_connected
                else "QUIC connection establishment"
            )
            _logger.error(
                "Connection timeout to %s during %s after %s", url, stage, format_duration(seconds=connect_timeout)
            )
            raise TimeoutError(message=f"Connection timeout to {url} during {stage}") from e
        except ConnectionRefusedError as e:
            self._stats.connections_failed += 1
            _logger.error("Connection refused by %s:%d", host, port)
            raise ConnectionError(message=f"Connection refused by {host}:{port}") from e
        except Exception as e:
            self._stats.connections_failed += 1
            _logger.error("Failed to connect to %s: %s", url, e, exc_info=True)
            if "certificate verify failed" in str(e):
                raise ConnectionError(message=f"Certificate verification failed for {url}: {e}") from e
            raise ClientError(message=f"Failed to connect to {url}: {e}") from e
        finally:
            if not success and connection is not None and not connection.is_closed:
                await connection.close()

    async def diagnostics(self) -> ClientDiagnostics:
        """Retrieve a snapshot of the client's diagnostics and statistics."""
        connections = await self._connection_manager.get_all_resources()
        state_counts = Counter(conn.state for conn in connections)

        return ClientDiagnostics(stats=self._stats, connection_states=dict(state_counts))

    def set_default_headers(self, *, headers: Headers) -> None:
        """Configure default headers for all subsequent connections."""
        self._default_headers = merge_headers(base=[], update=headers)

    async def _close_implementation(self) -> None:
        """Execute the internal client closure process."""
        _logger.info("Closing WebTransport client...")
        self._closed = True
        await self._connection_manager.shutdown()

        if self._transport is not None and not self._transport.is_closing():
            self._transport.close()

        _logger.info("WebTransport client closed.")

    async def _race_addresses(
        self, *, addresses: list[str], port: int, host: str, conn_config: ClientConfig
    ) -> WebTransportConnection:
        """Execute a concurrent connection race across multiple addresses."""
        winner: WebTransportConnection | None = None
        last_error: Exception | None = None
        tasks: set[asyncio.Task[WebTransportConnection]] = set()

        async def _attempt(*, ip: str) -> WebTransportConnection:
            assert self._driver is not None
            assert self._transport is not None
            connection: WebTransportConnection | None = None
            try:
                handle = self._driver.connect(remote_host=ip, remote_port=port, server_name=host)
                connection = WebTransportConnection(
                    config=conn_config, driver=self._driver, handle=handle, transport=self._transport, is_client=True
                )

                if connection.state != ConnectionState.CONNECTED:
                    _logger.debug("Waiting for connection establishment events for IP %s...", ip)
                    await connection.events.wait_for(
                        event_type=[
                            EventType.CONNECTION_ESTABLISHED,
                            EventType.CONNECTION_FAILED,
                            EventType.CONNECTION_CLOSED,
                        ]
                    )

                if connection.state != ConnectionState.CONNECTED:
                    raise ConnectionError(message=f"Connection failed state={connection.state} for IP {ip}")

                return connection
            except BaseException:
                if connection is not None and not connection.is_closed:
                    asyncio.create_task(coro=connection.close())
                raise

        try:
            for i, ip in enumerate(addresses):
                task = asyncio.create_task(coro=_attempt(ip=ip))
                tasks.add(task)

                timeout = conn_config.connection_attempt_delay if i < len(addresses) - 1 else None

                done, pending = await asyncio.wait(fs=tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
                tasks = pending

                for d in done:
                    try:
                        conn = d.result()
                        if winner is None:
                            winner = conn
                        else:
                            asyncio.create_task(coro=conn.close())
                    except Exception as e:
                        if winner is None:
                            last_error = e

                if winner is not None:
                    break

            while tasks and winner is None:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = pending
                for d in done:
                    try:
                        conn = d.result()
                        if winner is None:
                            winner = conn
                        else:
                            asyncio.create_task(coro=conn.close())
                    except Exception as e:
                        if winner is None:
                            last_error = e

            if winner is None:
                raise ConnectionError(message=f"Failed to connect to any resolved IP for {host}") from last_error

            return winner
        finally:
            for task in tasks:
                task.cancel()

    def _update_success_stats(self, *, connect_time: float) -> None:
        """Update internal statistics upon successful connection."""
        self._stats.connections_successful += 1
        self._stats.total_connect_time += connect_time
        self._stats.min_connect_time = min(self._stats.min_connect_time, connect_time)
        self._stats.max_connect_time = max(self._stats.max_connect_time, connect_time)

    def __str__(self) -> str:
        """Return the string representation."""
        status = "closed" if self.is_closed else "open"
        conn_count = len(self._connection_manager)
        return f"WebTransportClient(status={status}, connections={conn_count})"
