"""Orchestrate the unified IPC plane for the threaded reactor."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from typing import Any, Final, cast

from pywebtransport._controller import abi, mapper
from pywebtransport._controller.pending import PendingRequestManager
from pywebtransport._protocol.events import ProtocolEvent
from pywebtransport._pywebtransport import Endpoint, Waker
from pywebtransport.config import ClientConfig, ServerConfig
from pywebtransport.exceptions import ConnectionError
from pywebtransport.types import Address, EventType

__all__: list[str] = []

type _ConnectionCallback = Callable[[EventType, dict[str, Any]], None]
type _SpawnCallback = Callable[[int], None]

_WAKER_DRAIN_BUFFER_SIZE: Final[int] = 8192

_logger = logging.getLogger(name=__name__)


class EndpointController:
    """Orchestrate the unified IPC plane and route background reactor events."""

    def __init__(
        self, *, config: ClientConfig | ServerConfig, is_client: bool, loop: asyncio.AbstractEventLoop | None = None
    ) -> None:
        """Initialize the instance."""
        self._config = config
        self._is_client = is_client
        self._loop = loop or asyncio.get_running_loop()

        self._is_closed = False
        self._connection_callbacks: dict[int, _ConnectionCallback] = {}
        self._remote_addresses: dict[int, Address] = {}
        self._spawn_callback: _SpawnCallback | None = None

        self._r_fd, self._w_fd = os.pipe()
        os.set_blocking(self._r_fd, False)
        os.set_blocking(self._w_fd, False)

        self._pending_manager = PendingRequestManager()
        self._waker = Waker(self._w_fd)
        self._endpoint = Endpoint(is_client=is_client, config=config, waker=self._waker)

        self._loop.add_reader(self._r_fd, self._on_waker_triggered)

    def close(self) -> None:
        """Terminate the controller and shutdown the background reactor."""
        if self._is_closed:
            return

        self._is_closed = True

        self._endpoint.close()
        self._connection_callbacks.clear()
        self._remote_addresses.clear()
        self._spawn_callback = None
        self._pending_manager.fail_all(exception=ConnectionError(message="rt close"))

    async def connect(self, *, remote_host: str, remote_port: int, server_name: str) -> int:
        """Dispatch an outbound connection request to the reactor."""
        request_id, future = self._pending_manager.create_request()

        try:
            self._endpoint.connect(request_id=request_id, remote=(remote_host, remote_port), server_name=server_name)
        except Exception as e:
            self._pending_manager.fail_request(request_id=request_id, exception=e)
            raise ConnectionError.from_cause(message="wt_connection open failed", cause=e) from e

        return cast(int, await future)

    def get_local_addresses(self) -> list[Address]:
        """Return the synchronized OS-allocated local socket addresses."""
        return self._endpoint.get_local_addresses()

    def get_remote_address(self, *, handle: int) -> Address | None:
        """Return the validated remote socket address from local cache."""
        return self._remote_addresses.get(handle)

    def register_connection(self, *, handle: int, callback: _ConnectionCallback) -> None:
        """Register a callback router for the specified connection handle."""
        self._connection_callbacks[handle] = callback

    def send_user_event(self, *, handle: int, event: ProtocolEvent) -> None:
        """Transmit an application event to the background reactor."""
        try:
            ffi_tuple = mapper.pack_user_event(event=event)
            self._endpoint.handle_user_event(handle=handle, event=ffi_tuple)
        except Exception as e:
            raise ConnectionError.from_cause(
                message=f"rt_event send failed actual={type(event).__name__} connection_handle={handle}",
                cause=e,
                connection_handle=handle,
            ) from e

    def set_spawn_callback(self, *, callback: _SpawnCallback) -> None:
        """Assign the callback function for newly accepted connections."""
        self._spawn_callback = callback

    def unregister_connection(self, *, handle: int) -> None:
        """Remove the callback router and state for the specified connection handle."""
        self._connection_callbacks.pop(handle, None)
        self._remote_addresses.pop(handle, None)

    def _execute_effects(self, *, handle: int, effects: list[tuple[int, Any]]) -> None:
        """Process the application layer effects produced by the state machine."""
        for effect_opcode, effect_payload in effects:
            try:
                match effect_opcode:
                    case abi.NOTIFY_REQUEST_DONE:
                        req_id, result = effect_payload
                        self._pending_manager.complete_request(request_id=req_id, result=result)
                    case abi.NOTIFY_REQUEST_FAILED:
                        req_id, exception = effect_payload
                        self._pending_manager.fail_request(request_id=req_id, exception=exception)
                    case abi.EMIT_CONNECTION_EVENT:
                        conn_handle, ev_type, err_code, reason = effect_payload
                        cb = self._connection_callbacks.get(handle)
                        if cb is not None:
                            event_data = {"connection_handle": conn_handle}
                            if err_code is not None:
                                event_data["error_code"] = err_code
                            if reason is not None:
                                event_data["reason"] = reason
                            cb(ev_type, event_data)
                    case abi.EMIT_SESSION_EVENT:
                        sid, ev_type, path, hdrs, wt_avail_protos, wt_proto, data, uni, md, ms, rdy, err, rsn = (
                            effect_payload
                        )
                        cb = self._connection_callbacks.get(handle)
                        if cb is not None:
                            event_data = {"session_id": sid}
                            if path is not None:
                                event_data["path"] = path
                            if hdrs is not None:
                                event_data["headers"] = hdrs
                            if wt_avail_protos is not None:
                                event_data["wt_available_protocols"] = wt_avail_protos
                            if wt_proto is not None:
                                event_data["wt_protocol"] = wt_proto
                            if data is not None:
                                event_data["data"] = data
                            if uni is not None:
                                event_data["is_unidirectional"] = uni
                            if md is not None:
                                event_data["max_data"] = md
                            if ms is not None:
                                event_data["max_streams"] = ms
                            if rdy is not None:
                                event_data["ready_at"] = rdy
                            if err is not None:
                                event_data["error_code"] = err
                            if rsn is not None:
                                event_data["reason"] = rsn
                            cb(ev_type, event_data)
                    case abi.EMIT_STREAM_EVENT:
                        stream_id, ev_type, sid, direction, peer_init, err = effect_payload
                        cb = self._connection_callbacks.get(handle)
                        if cb is not None:
                            event_data = {"stream_id": stream_id}
                            if sid is not None:
                                event_data["session_id"] = sid
                            if direction is not None:
                                event_data["direction"] = direction
                            if peer_init is not None:
                                event_data["is_remote"] = peer_init
                            if err is not None:
                                event_data["error_code"] = err
                            cb(ev_type, event_data)
                    case abi.CLEANUP_H3_STREAM:
                        pass
            except Exception as e:
                _logger.warning(
                    "rt_event receive failed actual=%s ptr=%s err=%s", effect_opcode, handle, e, exc_info=True
                )

    def _on_waker_triggered(self) -> None:
        """Process the edge-triggered wake-up signal from the pipe."""
        try:
            while True:
                try:
                    os.read(self._r_fd, _WAKER_DRAIN_BUFFER_SIZE)
                except BlockingIOError:
                    break
        except OSError as e:
            _logger.debug("sys_pipe drain failed err=%s", e)

        self._waker.clear()

        try:
            events = self._endpoint.poll_runtime_events()
            for event_tuple in events:
                self._process_runtime_event(event_tuple=event_tuple)
        except Exception as e:
            _logger.warning("rt_event receive failed err=%s", e, exc_info=True)

    def _process_runtime_event(self, *, event_tuple: tuple[int, Any]) -> None:
        """Route the IPC runtime events to the corresponding domain handlers."""
        opcode, payload = event_tuple

        match opcode:
            case abi.COMMAND_COMPLETED:
                req_id, handle, remote_address = payload
                self._remote_addresses[handle] = remote_address
                self._pending_manager.complete_request(request_id=req_id, result=handle)
            case abi.COMMAND_FAILED:
                req_id, err_code, reason = payload
                self._pending_manager.fail_request(
                    request_id=req_id, exception=ConnectionError(message=reason, error_code=err_code)
                )
            case abi.CONNECTION_EFFECTS:
                handle, effects = payload
                self._execute_effects(handle=handle, effects=effects)
            case abi.CONNECTION_SPAWNED:
                handle, remote_address, effects = payload
                self._remote_addresses[handle] = remote_address
                if self._spawn_callback is not None:
                    self._spawn_callback(handle)
                self._execute_effects(handle=handle, effects=effects)
            case abi.REACTOR_SHUTDOWN:
                _logger.debug("rt close")

                if not self._is_closed:
                    self._is_closed = True
                    self._connection_callbacks.clear()
                    self._remote_addresses.clear()
                    self._spawn_callback = None
                    self._pending_manager.fail_all(exception=ConnectionError(message="rt failed"))

                self._loop.remove_reader(self._r_fd)

                try:
                    os.close(self._r_fd)
                    os.close(self._w_fd)
                except OSError as e:
                    _logger.debug("sys_pipe destroy failed err=%s", e)
