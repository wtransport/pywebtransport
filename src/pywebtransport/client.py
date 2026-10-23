"""WebTransport client."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from pywebtransport._controller.controller import EndpointController
from pywebtransport.config import ClientConfig
from pywebtransport.connection import WebTransportConnection
from pywebtransport.exceptions import ClientError, ConnectionError, TimeoutError
from pywebtransport.session import WebTransportSession
from pywebtransport.types import Address, ConnectionHandle, Headers
from pywebtransport.version import __version__

__all__: list[str] = ["WebTransportClient"]

_logger = logging.getLogger(name=__name__)


class WebTransportClient:
    """Manage WebTransport connections and sessions."""

    def __init__(self, *, config: ClientConfig | None = None) -> None:
        """Initialize the instance."""
        self._config = config if config is not None else ClientConfig()

        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._closed = False
        self._controller_lock = asyncio.Lock()
        self._default_headers: Headers = []

        self._close_task: asyncio.Task[None] | None = None
        self._controller: EndpointController | None = None

        _logger.info("app_client create")

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
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

    @property
    def local_addresses(self) -> list[Address]:
        """Return the local addresses."""
        if self._controller is not None:
            return self._controller.get_local_addresses()
        return []

    async def close(self) -> None:
        """Terminate the client."""
        if self._close_task is not None and not self._close_task.done():
            await self._close_task
            return

        if self._closed:
            return

        self._close_task = asyncio.create_task(coro=self._close())
        await self._close_task

    async def connect(
        self,
        *,
        url: str,
        headers: Headers | None = None,
        wt_available_protocols: list[str] | None = None,
        timeout: float | None = None,
    ) -> WebTransportSession:
        """Establish a session."""
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
        """Establish a session optimistically."""
        return await self._establish_connection(
            url=url,
            headers=headers,
            wt_available_protocols=wt_available_protocols,
            optimistic=True,
            timeout=timeout,
        )

    def set_default_headers(self, *, headers: Headers) -> None:
        """Set the default headers."""
        self._default_headers = _merge_headers(base=[], update=headers)

    async def _close(self) -> None:
        self._closed = True

        try:
            for task in self._background_tasks:
                if not task.done():
                    task.cancel()

            if self._background_tasks:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)

            if self._controller is not None:
                await self._controller.close()
                self._controller = None
        finally:
            _logger.info("app_client close")

    def _create_connection(self, controller: EndpointController, handle: ConnectionHandle) -> WebTransportConnection:
        return WebTransportConnection(handle=handle, controller=controller, config=self._config, is_client=True)

    async def _establish_connection(
        self,
        *,
        url: str,
        headers: Headers | None,
        wt_available_protocols: list[str] | None,
        optimistic: bool,
        timeout: float | None,
    ) -> WebTransportSession:
        if self._closed:
            raise ClientError(message="app_client validate failed")

        target = _parse_webtransport_url(url=url)
        connect_timeout = timeout if timeout is not None else self._config.connect_timeout

        connection: WebTransportConnection | None = None
        success = False

        try:
            async with asyncio.timeout(delay=connect_timeout):
                base_headers = _merge_headers(base=self._config.headers or [], update=self._default_headers)
                final_headers = _merge_headers(base=base_headers, update=headers or [])
                normalized_headers = _normalize_headers(headers=final_headers)

                if isinstance(normalized_headers, dict):
                    has_user_agent = "user-agent" in normalized_headers
                else:
                    has_user_agent = any(key == "user-agent" for key, _ in normalized_headers)

                if not has_user_agent:
                    default_user_agent = self._config.user_agent or f"PyWebTransport/{__version__}"
                    if isinstance(normalized_headers, dict):
                        normalized_headers["user-agent"] = default_user_agent
                    else:
                        normalized_headers.append(("user-agent", default_user_agent))

                effective_wt_available_protocols = (
                    list(self._config.wt_available_protocols)
                    if self._config.wt_available_protocols is not None
                    else None
                )
                effective_wt_available_protocols = (
                    wt_available_protocols if wt_available_protocols is not None else effective_wt_available_protocols
                )

                if self._controller is None:
                    async with self._controller_lock:
                        if self._controller is None:
                            controller = EndpointController(
                                connection_factory=self._create_connection, config=self._config, is_client=True
                            )
                            await controller.start()
                            self._controller = controller

                resolved_ips = await _resolve_host(host=target.host, port=target.port)

                connection = await self._race_addresses(
                    addresses=resolved_ips, host=target.host, port=target.port, connection_config=self._config
                )

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

                success = True
                return session

        except asyncio.TimeoutError:
            if connection is not None:
                raise TimeoutError(message=f"wt_session open failed actual={target.host}") from None
            raise TimeoutError(message=f"wt_connection open failed actual={target.host}") from None
        except ConnectionRefusedError as e:
            raise ConnectionError.from_cause(message=f"wt_connection open failed actual={target.host}", cause=e) from e
        except Exception as e:
            if "certificate verify failed" in str(e):
                raise ConnectionError.from_cause(
                    message=f"wt_connection validate failed actual={target.host}", cause=e
                ) from e
            raise ClientError.from_cause(message=f"wt_connection open failed actual={target.host}", cause=e) from e
        finally:
            if not success and connection is not None:
                await connection.close()

    async def _race_addresses(
        self, *, addresses: list[str], host: str, port: int, connection_config: ClientConfig
    ) -> WebTransportConnection:
        winner: WebTransportConnection | None = None
        last_error: Exception | None = None
        tasks: set[asyncio.Task[WebTransportConnection]] = set()

        def discard(*, connection: WebTransportConnection) -> None:
            task = asyncio.create_task(coro=connection.close())
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

        async def attempt(*, ip: str) -> WebTransportConnection:
            if self._controller is None:
                raise ClientError(message="app_client validate failed")

            return await self._controller.connect(remote_address=(ip, port), server_name=host)

        def collect(*, done: set[asyncio.Task[WebTransportConnection]]) -> None:
            nonlocal winner, last_error

            for finished_task in done:
                try:
                    connection = finished_task.result()
                    if winner is None:
                        winner = connection
                    else:
                        discard(connection=connection)
                except Exception as e:
                    if winner is None:
                        last_error = e

        def on_attempt_done(task: asyncio.Task[WebTransportConnection]) -> None:
            self._background_tasks.discard(task)
            if not task.cancelled() and task.exception() is None:
                discard(connection=task.result())

        try:
            for index, ip in enumerate(addresses):
                task = asyncio.create_task(coro=attempt(ip=ip))
                tasks.add(task)

                timeout = connection_config.connection_attempt_delay if index < len(addresses) - 1 else None

                done, pending = await asyncio.wait(fs=tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
                tasks = pending
                collect(done=done)

                if winner is not None:
                    break

            while tasks and winner is None:
                done, pending = await asyncio.wait(fs=tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = pending
                collect(done=done)

            if winner is None:
                if last_error is not None:
                    raise ConnectionError.from_cause(
                        message=f"wt_connection open failed actual={host}", cause=last_error
                    ) from last_error
                raise ConnectionError(message=f"wt_connection open failed actual={host}")

            return winner
        finally:
            for task in tasks:
                self._background_tasks.add(task)
                task.add_done_callback(on_attempt_done)


@dataclass(frozen=True, kw_only=True, slots=True)
class _ConnectionTarget:
    authority: str
    host: str
    path: str
    port: int


def _merge_headers(*, base: Headers, update: Headers | None) -> Headers:
    if update is None:
        if isinstance(base, Mapping):
            return dict(base)
        return list(base)

    if isinstance(base, Mapping) and isinstance(update, Mapping):
        new_headers = dict(base)
        new_headers.update(update)
        return new_headers

    base_list = list(base.items()) if isinstance(base, Mapping) else list(base)
    update_list = list(update.items()) if isinstance(update, Mapping) else list(update)
    return base_list + update_list


def _normalize_headers(*, headers: Headers) -> Headers:
    if isinstance(headers, Mapping):
        return {key.lower(): value for key, value in headers.items()}
    return [(key.lower(), value) for key, value in headers]


def _parse_webtransport_url(*, url: str) -> _ConnectionTarget:
    try:
        parsed = urllib.parse.urlparse(url=url)
        port = parsed.port if parsed.port is not None else 443
    except ValueError as e:
        raise ClientError.from_cause(message=f"wt_url validate invalid actual={url}", cause=e) from e

    if parsed.scheme != "https":
        raise ClientError(message=f"wt_url validate invalid actual={parsed.scheme} expected=https")

    if not parsed.hostname:
        raise ClientError(message="wt_url validate invalid")

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
    try:
        ipaddress.ip_address(address=host)
        return [host]
    except ValueError:
        pass

    loop = asyncio.get_running_loop()
    try:
        resolution_results = await loop.getaddrinfo(
            host=host, port=port, family=socket.AF_UNSPEC, type=socket.SOCK_DGRAM
        )
        if not resolution_results:
            raise ConnectionError(message=f"sys resolve failed actual={host}")

        resolved_ips: list[str] = []
        for resolution_result in resolution_results:
            ip = str(resolution_result[4][0])
            if ip not in resolved_ips:
                resolved_ips.append(ip)
        return resolved_ips
    except socket.gaierror as e:
        raise ConnectionError(message=f"sys resolve failed actual={host}") from e
