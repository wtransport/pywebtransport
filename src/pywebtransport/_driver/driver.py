"""Manage the unified multiplexing IO driver for the endpoint."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, cast

from pywebtransport._driver import abi, mapper
from pywebtransport._driver.pending import PendingRequestManager
from pywebtransport._protocol.events import ProtocolEvent
from pywebtransport._wtransport import Endpoint
from pywebtransport.config import ClientConfig, ServerConfig
from pywebtransport.exceptions import ConnectionError
from pywebtransport.types import Address, EventType, Future, RequestId
from pywebtransport.utils import get_logger

__all__: list[str] = []

type _ConnectionCallback = Callable[[EventType, dict[str, Any]], None]
type _SpawnCallback = Callable[[int], None]

_logger = get_logger(name=__name__)


class EndpointDriver(asyncio.DatagramProtocol):
    """Unified multiplexing IO driver for the endpoint."""

    def __init__(
        self, *, config: ClientConfig | ServerConfig, is_client: bool, loop: asyncio.AbstractEventLoop | None = None
    ) -> None:
        """Initialize the instance."""
        self._config = config
        self._is_client = is_client
        self._loop = loop or asyncio.get_running_loop()

        self._endpoint = Endpoint(is_client=is_client, config=config, now=self._loop.time())
        self._pending_manager = PendingRequestManager()
        self._timer_handle: asyncio.TimerHandle | None = None
        self._transport: asyncio.DatagramTransport | None = None

        self._connection_callbacks: dict[int, _ConnectionCallback] = {}
        self._spawn_callback: _SpawnCallback | None = None

    def connect(self, *, remote_host: str, remote_port: int, server_name: str) -> int:
        """Initiate an outbound connection through the endpoint."""
        handle = self._endpoint.connect(
            remote=(remote_host, remote_port), server_name=server_name, now=self._loop.time()
        )
        self._transmit()
        return handle

    def connection_lost(self, exc: Exception | None) -> None:
        """Handle the socket closure and fail pending requests."""
        _logger.debug("Driver socket closed: %s", exc)
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None

        exception = exc if exc is not None else ConnectionError("Socket closed")
        self._pending_manager.fail_all(exception=exception)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Handle the socket binding and start IO polling."""
        self._transport = cast(asyncio.DatagramTransport, transport)
        _logger.debug("Driver socket bound. Initializing transport polling.")
        self._transmit()

    def create_request(self) -> tuple[RequestId, Future[Any]]:
        """Create a tracked asynchronous request."""
        return self._pending_manager.create_request()

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        """Feed incoming datagrams to the endpoint."""
        local_addr: tuple[str, int] | None = None
        if self._transport is not None:
            info = self._transport.get_extra_info("sockname")
            if isinstance(info, tuple) and len(info) >= 2:
                local_addr = (info[0], info[1])

        try:
            event_tuple = self._endpoint.handle_datagram(
                data=data, remote=addr, local=local_addr, now=self._loop.time()
            )
            self._process_endpoint_event(event_tuple=event_tuple)
        except Exception as e:
            _logger.error("Error processing datagram: %s", e, exc_info=True)
        finally:
            self._transmit()

    def error_received(self, exc: Exception) -> None:
        """Handle the socket errors."""
        _logger.error("Driver socket error: %s", exc, exc_info=True)

    def get_remote_address(self, *, handle: int) -> Address | None:
        """Retrieve the validated remote socket address."""
        return self._endpoint.get_remote_address(handle=handle)

    def register_connection(self, *, handle: int, callback: _ConnectionCallback) -> None:
        """Register a connection callback router."""
        self._connection_callbacks[handle] = callback

    def send_user_event(self, *, handle: int, event: ProtocolEvent) -> None:
        """Dispatch an application event to the endpoint."""
        try:
            ffi_tuple = mapper.pack_user_event(event=event)
            event_tuple = self._endpoint.handle_user_event(handle=handle, event=ffi_tuple, now=self._loop.time())
            if event_tuple is not None:
                self._process_endpoint_event(event_tuple=event_tuple)
        except Exception as e:
            _logger.error(
                "Failed to process user event %s for handle %d: %s", type(event).__name__, handle, e, exc_info=True
            )
        finally:
            self._transmit()

    def set_spawn_callback(self, *, callback: _SpawnCallback) -> None:
        """Set the callback for newly accepted connections."""
        self._spawn_callback = callback

    def unregister_connection(self, *, handle: int) -> None:
        """Remove a connection callback router."""
        self._connection_callbacks.pop(handle, None)

    def _execute_effects(self, *, handle: int, effects: list[tuple[int, Any]]) -> None:
        """Execute the application layer effects."""
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
                        conn_id, ev_type, err_code, reason = effect_payload
                        cb = self._connection_callbacks.get(handle)
                        if cb is not None:
                            event_data = {"connection_id": conn_id}
                            if err_code is not None:
                                event_data["error_code"] = err_code
                            if reason is not None:
                                event_data["reason"] = reason
                            cb(ev_type, event_data)
                    case abi.EMIT_SESSION_EVENT:
                        sid, ev_type, path, hdrs, data, uni, md, ms, rdy, err, rsn = effect_payload
                        cb = self._connection_callbacks.get(handle)
                        if cb is not None:
                            event_data = {"session_id": sid}
                            if path is not None:
                                event_data["path"] = path
                            if hdrs is not None:
                                event_data["headers"] = hdrs
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
                _logger.error("Error executing effect %s on handle %d: %s", effect_opcode, handle, e, exc_info=True)

    def _handle_timeout(self) -> None:
        """Process the scheduled endpoint timeouts."""
        self._timer_handle = None
        try:
            results = self._endpoint.handle_timeout(now=self._loop.time())
            for handle, effects in results:
                self._execute_effects(handle=handle, effects=effects)
        except Exception as e:
            _logger.error("Error during timeout processing: %s", e, exc_info=True)
        finally:
            self._transmit()

    def _process_endpoint_event(self, *, event_tuple: tuple[int, Any]) -> None:
        """Route the terminal endpoint events."""
        opcode, payload = event_tuple
        match opcode:
            case abi.CONNECTION_EFFECTS:
                handle, effects = payload
                self._execute_effects(handle=handle, effects=effects)
            case abi.CONNECTION_SPAWNED:
                handle, effects = payload
                if self._spawn_callback is not None:
                    self._spawn_callback(handle)
                self._execute_effects(handle=handle, effects=effects)
            case abi.TRANSMIT:
                (ip, port), contents = payload
                if self._transport is not None and not self._transport.is_closing():
                    self._transport.sendto(contents, (ip, port))
            case abi.CONSUMED:
                pass

    def _schedule_timer(self) -> None:
        """Schedule the next wakeup for the endpoint."""
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None

        wakeup_at = self._endpoint.timeout()
        if wakeup_at is not None:
            self._timer_handle = self._loop.call_at(wakeup_at, self._handle_timeout)

    def _transmit(self) -> None:
        """Exhaust the pending transmission queue."""
        if self._transport is None or self._transport.is_closing():
            return

        try:
            while True:
                tx = self._endpoint.poll_transmit(now=self._loop.time())
                if tx is None:
                    break
                opcode, payload = tx
                if opcode == abi.TRANSMIT:
                    (ip, port), contents = payload
                    self._transport.sendto(contents, (ip, port))
        except Exception as e:
            _logger.error("Error transmitting datagrams: %s", e, exc_info=True)
        finally:
            self._schedule_timer()


async def create_client(
    *, config: ClientConfig, loop: asyncio.AbstractEventLoop | None = None
) -> tuple[asyncio.DatagramTransport, EndpointDriver]:
    """Establish the underlying datagram endpoint for a client."""
    loop = loop or asyncio.get_running_loop()

    def protocol_factory() -> EndpointDriver:
        """Instantiate the endpoint driver."""
        return EndpointDriver(config=config, is_client=True, loop=loop)

    transport, protocol = await loop.create_datagram_endpoint(
        protocol_factory=protocol_factory, local_addr=("0.0.0.0", 0)
    )

    return transport, protocol


async def create_server(
    *, host: str, port: int, config: ServerConfig, loop: asyncio.AbstractEventLoop | None = None
) -> tuple[asyncio.DatagramTransport, EndpointDriver]:
    """Start the datagram endpoint with the server driver."""
    loop = loop or asyncio.get_running_loop()

    def protocol_factory() -> EndpointDriver:
        """Instantiate the endpoint driver."""
        return EndpointDriver(config=config, is_client=False, loop=loop)

    transport, protocol = await loop.create_datagram_endpoint(
        protocol_factory=protocol_factory, local_addr=(host, port)
    )

    return transport, protocol
