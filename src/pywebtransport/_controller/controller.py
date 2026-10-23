"""Endpoint controller."""

from __future__ import annotations

import asyncio
import functools
import logging
import socket
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Final

from pywebtransport._controller import conversion
from pywebtransport._controller.events import (
    CommandFailed,
    ConnectionEffects,
    ConnectionSpawned,
    EmitConnectionEvent,
    EmitSessionEvent,
    EmitStreamEvent,
    NotifyRequestDone,
    NotifyRequestFailed,
    ProtocolEvent,
    ReactorShutdown,
)
from pywebtransport._controller.pending import PendingRequestManager
from pywebtransport._pywebtransport import Endpoint, Waker
from pywebtransport.config import ClientConfig, ServerConfig
from pywebtransport.exceptions import ConnectionError
from pywebtransport.types import Address, ConnectionHandle, EventType, RequestId

if TYPE_CHECKING:
    from pywebtransport.connection import WebTransportConnection

__all__: list[str] = []

type _ConnectionFactory = Callable[[EndpointController, ConnectionHandle], WebTransportConnection]

_RUNTIME_SHUTDOWN_TIMEOUT: Final[float] = 5.0
_WAKER_DRAIN_BUFFER_SIZE: Final[int] = 8192

_logger = logging.getLogger(name=__name__)


class EndpointController:

    def __init__(
        self, *, connection_factory: _ConnectionFactory, config: ClientConfig | ServerConfig, is_client: bool
    ) -> None:
        self._connection_factory = connection_factory
        self._config = config
        self._is_client = is_client

        self._connections: dict[ConnectionHandle, WebTransportConnection] = {}
        self._endpoint: Endpoint | None = None
        self._is_closed = False
        self._pending_manager = PendingRequestManager()
        self._remote_addresses: dict[ConnectionHandle, Address] = {}
        self._runtime_lock = asyncio.Lock()
        self._runtime_shutdown_event = asyncio.Event()
        self._read_socket: socket.socket | None = None
        self._waker_task: asyncio.Task[None] | None = None
        self._write_socket: socket.socket | None = None

    async def close(self) -> None:
        if self._is_closed:
            return

        self._is_closed = True

        task = asyncio.create_task(self._release_runtime())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.shield(task)
            raise

    async def connect(self, *, remote_address: Address, server_name: str) -> WebTransportConnection:

        def dispatch_connect(request_id: RequestId) -> None:
            try:
                self._require_endpoint().connect(
                    request_id=request_id, remote_address=remote_address, server_name=server_name
                )
            except ConnectionError:
                raise
            except Exception as e:
                raise ConnectionError.from_cause(message="wt_connection open failed", cause=e) from e

        connection: WebTransportConnection = await self.execute_request(
            dispatcher=dispatch_connect, resolver=self._resolve_connection
        )
        return connection

    async def execute_request(
        self, *, dispatcher: Callable[[RequestId], None], resolver: Callable[[Any], Any] | None = None
    ) -> Any:
        request_id, future = self._pending_manager.create_request(resolver=resolver)

        try:
            dispatcher(request_id)
            return await future
        finally:
            self._pending_manager.evict_request(request_id=request_id)

    def get_local_addresses(self) -> list[Address]:
        return self._require_endpoint().get_local_addresses()

    def get_remote_address(self, *, handle: ConnectionHandle) -> Address | None:
        return self._remote_addresses.get(handle)

    def send_protocol_event(self, *, handle: ConnectionHandle, request_id: RequestId, event: ProtocolEvent) -> None:
        try:
            event_tuple = conversion.convert_protocol_event(event=event)
            self._require_endpoint().handle_protocol_event(handle=handle, request_id=request_id, event=event_tuple)
        except Exception as e:
            raise ConnectionError.from_cause(
                message=f"rt_event send failed actual={type(event).__name__} connection_handle={handle}",
                cause=e,
                connection_handle=handle,
            ) from e

    async def start(self) -> None:
        task = asyncio.create_task(self._acquire_runtime())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.shield(task)
            raise

    async def _acquire_runtime(self) -> None:
        async with self._runtime_lock:
            if self._is_closed:
                raise RuntimeError("rt validate failed")
            if self._endpoint is not None:
                return

            read_socket, write_socket = socket.socketpair()
            read_socket.setblocking(False)
            write_socket.setblocking(False)

            endpoint: Endpoint | None = None

            try:
                waker = Waker(fd=write_socket.fileno())
                endpoint = await asyncio.get_running_loop().run_in_executor(
                    None,
                    functools.partial(Endpoint, config=self._config, is_client=self._is_client, waker=waker),
                )
                waker_task = asyncio.get_running_loop().create_task(
                    self._run_waker_loop(endpoint=endpoint, waker=waker, read_socket=read_socket)
                )
            except BaseException:
                if endpoint is not None:
                    endpoint.close()

                read_socket.close()
                write_socket.close()
                raise

            self._read_socket = read_socket
            self._write_socket = write_socket
            self._endpoint = endpoint
            self._waker_task = waker_task

    def _process_effects(self, *, handle: ConnectionHandle, effects: list[tuple[int, Any]]) -> None:
        for event_tuple in effects:
            try:
                effect = conversion.convert_effect(event_tuple=event_tuple)

                match effect:
                    case EmitConnectionEvent():
                        connection = self._connections.get(handle)
                        if connection is not None:
                            event_data: dict[str, Any] = {"connection_handle": effect.connection_handle}
                            if effect.error_code is not None:
                                event_data["error_code"] = effect.error_code
                            if effect.reason is not None:
                                event_data["reason"] = effect.reason
                            connection._handle_event(event_type=effect.event_type, data=event_data)
                        if effect.event_type == EventType.CONNECTION_CLOSED:
                            self._connections.pop(handle, None)
                            self._remote_addresses.pop(handle, None)
                    case EmitSessionEvent():
                        connection = self._connections.get(handle)
                        if connection is not None:
                            event_data = {"session_id": effect.session_id}
                            if effect.path is not None:
                                event_data["path"] = effect.path
                            if effect.headers is not None:
                                event_data["headers"] = effect.headers
                            if effect.wt_available_protocols is not None:
                                event_data["wt_available_protocols"] = effect.wt_available_protocols
                            if effect.wt_protocol is not None:
                                event_data["wt_protocol"] = effect.wt_protocol
                            if effect.data is not None:
                                event_data["data"] = effect.data
                            if effect.is_unidirectional is not None:
                                event_data["is_unidirectional"] = effect.is_unidirectional
                            if effect.max_data is not None:
                                event_data["max_data"] = effect.max_data
                            if effect.max_streams is not None:
                                event_data["max_streams"] = effect.max_streams
                            if effect.ready_at is not None:
                                event_data["ready_at"] = effect.ready_at
                            if effect.status_code is not None:
                                event_data["status_code"] = effect.status_code
                            if effect.error_code is not None:
                                event_data["error_code"] = effect.error_code
                            if effect.reason is not None:
                                event_data["reason"] = effect.reason
                            connection._handle_event(event_type=effect.event_type, data=event_data)
                    case EmitStreamEvent():
                        connection = self._connections.get(handle)
                        if connection is not None:
                            event_data = {"stream_id": effect.stream_id}
                            if effect.session_id is not None:
                                event_data["session_id"] = effect.session_id
                            if effect.direction is not None:
                                event_data["direction"] = effect.direction
                            if effect.is_peer_initiated is not None:
                                event_data["is_peer_initiated"] = effect.is_peer_initiated
                            if effect.error_code is not None:
                                event_data["error_code"] = effect.error_code
                            connection._handle_event(event_type=effect.event_type, data=event_data)
                    case NotifyRequestDone():
                        self._pending_manager.complete_request(request_id=effect.request_id, result=effect.result)
                    case NotifyRequestFailed():
                        self._pending_manager.fail_request(request_id=effect.request_id, exception=effect.exception)
            except Exception as e:
                _logger.warning(
                    "rt_event receive failed actual=%s ptr=%s err=%s", event_tuple[0], handle, e, exc_info=True
                )

    def _process_runtime_event(self, *, event_tuple: tuple[int, Any]) -> None:
        try:
            runtime_event = conversion.convert_runtime_event(event_tuple=event_tuple)

            match runtime_event:
                case CommandFailed():
                    self._pending_manager.fail_request(
                        request_id=runtime_event.request_id, exception=runtime_event.exception
                    )
                case ConnectionEffects():
                    self._process_effects(handle=runtime_event.handle, effects=runtime_event.effects)
                case ConnectionSpawned():
                    self._remote_addresses[runtime_event.handle] = runtime_event.remote_address
                    self._connections[runtime_event.handle] = self._connection_factory(self, runtime_event.handle)
                    self._process_effects(handle=runtime_event.handle, effects=runtime_event.effects)
                case ReactorShutdown():
                    _logger.debug("rt close")
                    self._runtime_shutdown_event.set()
        except Exception as e:
            _logger.warning("rt_event receive failed err=%s", e, exc_info=True)

    async def _release_runtime(self) -> None:
        async with self._runtime_lock:
            if self._endpoint is not None:
                try:
                    async with asyncio.timeout(delay=_RUNTIME_SHUTDOWN_TIMEOUT):
                        await asyncio.get_running_loop().run_in_executor(None, self._endpoint.close)
                        await self._runtime_shutdown_event.wait()
                except asyncio.TimeoutError:
                    _logger.warning("rt close failed")

            for connection in self._connections.values():
                connection._handle_closed()
            self._connections.clear()
            self._remote_addresses.clear()
            self._pending_manager.fail_all(exception=ConnectionError(message="rt close"))

            if self._waker_task is not None and not self._waker_task.done():
                self._waker_task.cancel()

                try:
                    await self._waker_task
                except asyncio.CancelledError:
                    pass

            for pipe_socket in (self._read_socket, self._write_socket):
                if pipe_socket is not None:
                    try:
                        pipe_socket.close()
                    except OSError as e:
                        _logger.debug("sys_socket destroy failed err=%s", e)

    def _require_endpoint(self) -> Endpoint:
        if self._endpoint is None:
            raise RuntimeError("rt validate failed")
        return self._endpoint

    def _resolve_connection(self, handle: ConnectionHandle) -> WebTransportConnection:
        connection = self._connections.get(handle)
        if connection is None:
            raise ConnectionError(
                message=f"wt_connection resolve failed connection_handle={handle}", connection_handle=handle
            )
        return connection

    async def _run_waker_loop(self, *, endpoint: Endpoint, waker: Waker, read_socket: socket.socket) -> None:
        try:
            while True:
                data = await asyncio.get_running_loop().sock_recv(read_socket, _WAKER_DRAIN_BUFFER_SIZE)
                if not data:
                    break

                try:
                    while True:
                        if not read_socket.recv(_WAKER_DRAIN_BUFFER_SIZE):
                            break
                except BlockingIOError:
                    pass

                waker.clear()

                try:
                    runtime_events = endpoint.poll_runtime_events()
                    for event_tuple in runtime_events:
                        self._process_runtime_event(event_tuple=event_tuple)
                except Exception as e:
                    _logger.warning("rt_event receive failed err=%s", e, exc_info=True)
        except asyncio.CancelledError:
            pass
        except OSError as e:
            if not self._is_closed:
                _logger.debug("sys_socket drain failed err=%s", e)
