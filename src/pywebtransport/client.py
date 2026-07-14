"""Client-side entry point for WebTransport connections."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import time
import urllib.parse
from collections import Counter
from dataclasses import asdict, dataclass, field
from types import TracebackType
from typing import Any, Final, Self

from pywebtransport._controller.controller import EndpointController
from pywebtransport.config import ClientConfig
from pywebtransport.connection import WebTransportConnection
from pywebtransport.events import EventEmitter
from pywebtransport.exceptions import ClientError, ConnectionError, TimeoutError
from pywebtransport.manager.connection import ConnectionManager
from pywebtransport.session import WebTransportSession
from pywebtransport.types import ConnectionState, EventType, Headers
from pywebtransport.version import __version__

__all__: list[str] = ["ClientDiagnostics", "ClientStats", "WebTransportClient"]

_HEALTH_CONNECT_TIME_THRESHOLD: Final[float] = 5.0
_HEALTH_EVALUATION_SAMPLES: Final[int] = 10
_HEALTH_SUCCESS_RATE_THRESHOLD: Final[float] = 0.9

_logger = logging.getLogger(name=__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class ClientDiagnostics:
    """Encapsulate the client's health and statistics."""

    connection_states: dict[ConnectionState, int]
    stats: ClientStats

    @property
    def issues(self) -> list[str]:
        """Return a list of potential health issues."""
        issues: list[str] = []
        stats_dict = self.stats.to_dict()

        connections_attempted = stats_dict.get("connections_attempted", 0)
        success_rate = stats_dict.get("success_rate", 1.0)
        if connections_attempted > _HEALTH_EVALUATION_SAMPLES and success_rate < _HEALTH_SUCCESS_RATE_THRESHOLD:
            issues.append(f"app_client validate exceeded actual={success_rate} expected=health_success_rate_threshold")

        avg_connect_time = stats_dict.get("avg_connect_time", 0.0)
        if avg_connect_time > _HEALTH_CONNECT_TIME_THRESHOLD:
            issues.append(
                f"app_client validate exceeded actual={avg_connect_time} expected=health_connect_time_threshold"
            )

        return issues


@dataclass(kw_only=True, slots=True)
class ClientStats:
    """Encapsulate client-wide connection statistics."""

    connections_attempted: int = 0
    connections_failed: int = 0
    connections_successful: int = 0
    created_at: float = field(default_factory=time.perf_counter)
    max_connect_time: float = 0.0
    min_connect_time: float = float("inf")
    total_connect_time: float = 0.0

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
        data["uptime"] = time.perf_counter() - self.created_at
        if data["min_connect_time"] == float("inf"):
            data["min_connect_time"] = 0.0
        return data


class WebTransportClient(EventEmitter):
    """Manage WebTransport connections and sessions."""

    def __init__(self, *, config: ClientConfig | None = None) -> None:
        """Initialize the instance."""
        effective_config = config if config is not None else ClientConfig()
        effective_config.validate()

        super().__init__(
            max_listeners=effective_config.max_event_listeners,
            max_history=effective_config.event_history_capacity,
            max_queue_size=effective_config.event_queue_capacity,
        )

        self._config = effective_config

        self._closed = False
        self._init_lock = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._connection_manager = ConnectionManager(max_connections=self._config.max_connections)
        self._default_headers: Headers = []

        self._controller: EndpointController | None = None
        self._stats = ClientStats()

        _logger.info("app_client create")

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
        self,
        *,
        url: str,
        headers: Headers | None = None,
        wt_available_protocols: list[str] | None = None,
        timeout: float | None = None,
    ) -> WebTransportSession:
        """Establish a WebTransport session."""
        return await self._establish_connection(
            url=url,
            headers=headers,
            wt_available_protocols=wt_available_protocols,
            optimistic=False,
            timeout=timeout,
        )

    async def connect_optimistic(
        self,
        *,
        url: str,
        headers: Headers | None = None,
        wt_available_protocols: list[str] | None = None,
        timeout: float | None = None,
    ) -> WebTransportSession:
        """Optimistically establish a WebTransport session."""
        return await self._establish_connection(
            url=url,
            headers=headers,
            wt_available_protocols=wt_available_protocols,
            optimistic=True,
            timeout=timeout,
        )

    async def diagnostics(self) -> ClientDiagnostics:
        """Retrieve a snapshot of the client's diagnostics and statistics."""
        connections = await self._connection_manager.get_all_resources()
        state_counts = Counter(conn.state for conn in connections)

        return ClientDiagnostics(connection_states=dict(state_counts), stats=self._stats)

    def set_default_headers(self, *, headers: Headers) -> None:
        """Configure default headers for all subsequent connections."""
        self._default_headers = _merge_headers(base=[], update=headers)

    async def _close_implementation(self) -> None:
        """Execute the internal client closure process."""
        _logger.info("app_client drain")
        self._closed = True
        await self._connection_manager.shutdown()

        if self._controller is not None:
            await self._controller.close()

        _logger.info("app_client close")

    async def _establish_connection(
        self,
        *,
        url: str,
        headers: Headers | None,
        wt_available_protocols: list[str] | None,
        optimistic: bool,
        timeout: float | None,
    ) -> WebTransportSession:
        """Resolve, race, and establish a WebTransport session for the target URL."""
        if self._closed:
            raise ClientError(message="app_client validate failed")

        target = _parse_webtransport_url(url=url)
        connect_timeout = timeout if timeout is not None else self._config.connect_timeout
        self._stats.connections_attempted += 1

        connection: WebTransportConnection | None = None
        success = False
        start_time = time.perf_counter()

        try:
            async with asyncio.timeout(delay=connect_timeout):
                base_headers = _merge_headers(base=self._config.headers or [], update=self._default_headers)
                final_headers = _merge_headers(base=base_headers, update=headers or [])
                normalized_headers = _normalize_headers(headers=final_headers)

                has_ua = False
                if isinstance(normalized_headers, dict):
                    has_ua = "user-agent" in normalized_headers
                else:
                    has_ua = any(key == "user-agent" for key, _ in normalized_headers)

                if not has_ua:
                    default_ua = self._config.user_agent or f"PyWebTransport/{__version__}"
                    if isinstance(normalized_headers, dict):
                        normalized_headers["user-agent"] = default_ua
                    else:
                        normalized_headers.append(("user-agent", default_ua))

                effective_wt_available_protocols = (
                    wt_available_protocols
                    if wt_available_protocols is not None
                    else self._config.wt_available_protocols
                )

                if self._controller is None:
                    async with self._init_lock:
                        if self._controller is None:
                            self._controller = EndpointController(
                                config=self._config, is_client=True, loop=asyncio.get_running_loop()
                            )

                if self._controller is None:
                    raise ClientError(message="rt create failed")

                resolved_ips = await _resolve_host(host=target.host, port=target.port)

                connection = await self._race_addresses(
                    addresses=resolved_ips, port=target.port, host=target.host, conn_config=self._config
                )

                await self._connection_manager.add_connection(connection=connection)

                if optimistic:
                    session = await connection.create_session_optimistic(
                        authority=target.authority,
                        path=target.path,
                        headers=normalized_headers,
                        wt_available_protocols=effective_wt_available_protocols,
                    )
                else:
                    session = await connection.create_session(
                        authority=target.authority,
                        path=target.path,
                        headers=normalized_headers,
                        wt_available_protocols=effective_wt_available_protocols,
                    )

                elapsed = time.perf_counter() - start_time
                self._update_success_stats(connect_time=elapsed)
                success = True
                return session

        except asyncio.TimeoutError:
            self._stats.connections_failed += 1
            if connection is not None and connection.is_connected:
                raise TimeoutError(message=f"wt_session open failed actual={target.host}") from None
            raise TimeoutError(message=f"wt_connection open failed actual={target.host}") from None
        except ConnectionRefusedError as e:
            self._stats.connections_failed += 1
            raise ConnectionError.from_cause(message=f"wt_connection open failed actual={target.host}", cause=e) from e
        except Exception as e:
            self._stats.connections_failed += 1
            if "certificate verify failed" in str(e):
                raise ConnectionError.from_cause(
                    message=f"wt_connection validate failed actual={target.host}", cause=e
                ) from e
            raise ClientError.from_cause(message=f"wt_connection open failed actual={target.host}", cause=e) from e
        finally:
            if not success and connection is not None and not connection.is_closed:
                await connection.close()

    async def _race_addresses(
        self, *, addresses: list[str], port: int, host: str, conn_config: ClientConfig
    ) -> WebTransportConnection:
        """Execute a concurrent connection race across multiple addresses."""
        winner: WebTransportConnection | None = None
        last_error: Exception | None = None
        tasks: set[asyncio.Task[WebTransportConnection]] = set()

        async def _attempt(*, ip: str) -> WebTransportConnection:
            if self._controller is None:
                raise ClientError(message="app_client validate failed")

            connection: WebTransportConnection | None = None
            try:
                handle = await self._controller.connect(remote_host=ip, remote_port=port, server_name=host)
                connection = WebTransportConnection(
                    config=conn_config, controller=self._controller, handle=handle, is_client=True
                )

                if connection.state != ConnectionState.CONNECTED:
                    await connection.events.wait_for(
                        event_type=[EventType.CONNECTION_ESTABLISHED, EventType.CONNECTION_CLOSED]
                    )

                if connection.state != ConnectionState.CONNECTED:
                    raise ConnectionError(
                        message=f"wt_connection open failed actual={connection.state} expected=connected"
                    )

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
                done, pending = await asyncio.wait(fs=tasks, return_when=asyncio.FIRST_COMPLETED)
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
                if last_error is not None:
                    raise ConnectionError.from_cause(
                        message=f"wt_connection open failed actual={host}", cause=last_error
                    ) from last_error
                raise ConnectionError(message=f"wt_connection open failed actual={host}")

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


@dataclass(frozen=True, kw_only=True, slots=True)
class _ConnectionTarget:
    """Standardized target definition for WebTransport connection establishment."""

    authority: str
    host: str
    path: str
    port: int


def _merge_headers(*, base: Headers, update: Headers | None) -> Headers:
    """Combine two header collections."""
    if update is None:
        if isinstance(base, dict):
            return base.copy()
        return list(base)

    if isinstance(base, dict) and isinstance(update, dict):
        new_headers = base.copy()
        new_headers.update(update)
        return new_headers

    base_list = list(base.items()) if isinstance(base, dict) else list(base)
    update_list = list(update.items()) if isinstance(update, dict) else list(update)
    return base_list + update_list


def _normalize_headers(*, headers: Headers) -> Headers:
    """Normalize the header keys to lowercase."""
    if isinstance(headers, dict):
        return {key.lower(): value for key, value in headers.items()}
    return [(key.lower(), value) for key, value in headers]


def _parse_webtransport_url(*, url: str) -> _ConnectionTarget:
    """Parse the WebTransport URL into authority, host, port, and path components."""
    parsed = urllib.parse.urlparse(url=url)
    if parsed.scheme != "https":
        raise ValueError(f"wt_url validate invalid actual={parsed.scheme} expected=https")

    if not parsed.hostname:
        raise ValueError("wt_url validate invalid")

    port = parsed.port if parsed.port is not None else 443

    path = parsed.path if parsed.path else "/"
    if parsed.query:
        path += f"?{parsed.query}"

    return _ConnectionTarget(
        authority=parsed.netloc.split("@")[-1],
        host=parsed.hostname,
        path=path,
        port=port,
    )


async def _resolve_host(*, host: str, port: int = 0) -> list[str]:
    """Resolve a hostname to a list of IP addresses asynchronously."""
    try:
        ipaddress.ip_address(address=host)
        return [host]
    except ValueError:
        pass

    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host=host, port=port, family=socket.AF_UNSPEC, type=socket.SOCK_DGRAM)
        if not infos:
            raise ConnectionError(message=f"sys resolve failed actual={host}")

        resolved_ips: list[str] = []
        for info in infos:
            ip = str(info[4][0])
            if ip not in resolved_ips:
                resolved_ips.append(ip)
        return resolved_ips
    except socket.gaierror as e:
        raise ConnectionError(message=f"sys resolve failed actual={host}") from e
