"""Unit tests for the pywebtransport._controller.controller module."""

import asyncio
import socket
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pywebtransport import ConnectionError
from pywebtransport._controller.abi import (
    COMMAND_FAILED,
    CONNECTION_EFFECTS,
    CONNECTION_SPAWNED,
    EMIT_CONNECTION_EVENT,
    EMIT_SESSION_EVENT,
    EMIT_STREAM_EVENT,
    NOTIFY_REQUEST_DONE,
    NOTIFY_REQUEST_FAILED,
    REACTOR_SHUTDOWN,
)
from pywebtransport._controller.controller import EndpointController
from pywebtransport._controller.events import EmitConnectionEvent, ProtocolEvent
from pywebtransport._controller.pending import PendingRequestManager
from pywebtransport.types import EventType, StreamDirection


class TestEndpointController:

    @pytest.fixture
    def controller(self, mock_connection_factory: MagicMock, mock_endpoint: MagicMock) -> Iterator[EndpointController]:
        controller = EndpointController(connection_factory=mock_connection_factory, config=MagicMock(), is_client=True)
        controller._endpoint = mock_endpoint
        controller._read_socket, controller._write_socket = socket.socketpair()
        controller._read_socket.setblocking(False)
        controller._write_socket.setblocking(False)
        controller._waker_task = MagicMock()
        controller._waker_task.done.return_value = True

        yield controller

        controller._read_socket.close()
        controller._write_socket.close()

    @pytest.fixture
    def mock_connection(self) -> MagicMock:
        connection = MagicMock()
        connection.handle = 1

        return connection

    @pytest.fixture
    def mock_connection_factory(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def mock_endpoint(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def mock_waker(self) -> MagicMock:
        return MagicMock()

    @pytest.mark.asyncio
    async def test_close_cancelled_while_releasing(
        self, controller: EndpointController, mock_endpoint: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        resume_event = asyncio.Event()

        async def mock_wait() -> None:
            await resume_event.wait()

        monkeypatch.setattr(controller._runtime_shutdown_event, "wait", mock_wait)
        close_task = asyncio.create_task(controller.close())
        await asyncio.sleep(0.01)

        close_task.cancel()
        await asyncio.sleep(0.01)
        resume_event.set()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await close_task

        assert controller._is_closed is True
        mock_endpoint.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_idempotent(self, controller: EndpointController) -> None:
        controller._runtime_shutdown_event.set()
        await controller.close()

        await controller.close()

        assert controller._is_closed is True

    @pytest.mark.asyncio
    async def test_close_not_started(self) -> None:
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        await controller.close()

        assert controller._is_closed is True

    @pytest.mark.asyncio
    async def test_close_oserror(self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch) -> None:
        controller._runtime_shutdown_event.set()
        mock_read_socket = MagicMock()
        mock_read_socket.close.side_effect = OSError("mock close error")
        monkeypatch.setattr(controller, "_read_socket", mock_read_socket)

        await controller.close()

        assert controller._is_closed is True

    @pytest.mark.asyncio
    async def test_close_success(
        self, controller: EndpointController, mock_endpoint: MagicMock, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        controller._remote_addresses[1] = ("127.0.0.1", 443)

        async def pending() -> None:
            await asyncio.Event().wait()

        waker_task = asyncio.create_task(pending())
        controller._waker_task = waker_task
        controller._runtime_shutdown_event.set()

        await controller.close()

        mock_endpoint.close.assert_called_once()
        mock_connection._handle_closed.assert_called_once_with()
        assert controller._is_closed is True
        assert not controller._connections
        assert not controller._remote_addresses
        assert waker_task.cancelled()

    @pytest.mark.asyncio
    async def test_close_timeout(
        self, controller: EndpointController, mock_endpoint: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def mock_wait() -> None:
            raise asyncio.TimeoutError()

        monkeypatch.setattr(controller._runtime_shutdown_event, "wait", mock_wait)

        await controller.close()

        assert controller._is_closed is True
        mock_endpoint.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_failure(self, controller: EndpointController, mock_endpoint: MagicMock) -> None:
        mock_endpoint.connect.side_effect = ValueError("mock connection error")

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection open failed"):
            await controller.connect(remote_address=("127.0.0.1", 443), server_name="localhost")

        assert not controller._pending_manager._requests

    @pytest.mark.asyncio
    async def test_connect_failure_connection_error(
        self, controller: EndpointController, mock_endpoint: MagicMock
    ) -> None:
        exception = ConnectionError(message="native connection error")
        mock_endpoint.connect.side_effect = exception

        with pytest.raises(expected_exception=ConnectionError) as exception_info:
            await controller.connect(remote_address=("127.0.0.1", 443), server_name="localhost")

        assert exception_info.value is exception
        assert not controller._pending_manager._requests

    @pytest.mark.asyncio
    async def test_connect_resolve_failure(self, controller: EndpointController, mock_endpoint: MagicMock) -> None:
        def mock_connect(*args: Any, **kwargs: Any) -> None:
            controller._pending_manager.complete_request(request_id=kwargs["request_id"], result=42)

        mock_endpoint.connect.side_effect = mock_connect

        with pytest.raises(
            expected_exception=ConnectionError, match="wt_connection resolve failed connection_handle=42"
        ):
            await controller.connect(remote_address=("127.0.0.1", 443), server_name="localhost")

        assert not controller._pending_manager._requests

    @pytest.mark.asyncio
    async def test_connect_success(
        self, controller: EndpointController, mock_endpoint: MagicMock, mock_connection: MagicMock
    ) -> None:
        mock_connection.handle = 42
        controller._connections[mock_connection.handle] = mock_connection

        def mock_connect(*args: Any, **kwargs: Any) -> None:
            controller._pending_manager.complete_request(request_id=kwargs["request_id"], result=42)

        mock_endpoint.connect.side_effect = mock_connect

        result = await controller.connect(remote_address=("127.0.0.1", 443), server_name="localhost")

        assert result is mock_connection
        mock_endpoint.connect.assert_called_once()

    def test_get_local_addresses(self, controller: EndpointController, mock_endpoint: MagicMock) -> None:
        mock_endpoint.get_local_addresses.return_value = [("127.0.0.1", 443)]

        result = controller.get_local_addresses()

        assert result == [("127.0.0.1", 443)]
        mock_endpoint.get_local_addresses.assert_called_once()

    def test_get_remote_address_none(self, controller: EndpointController) -> None:
        result = controller.get_remote_address(handle=999)

        assert result is None

    def test_init_state(self) -> None:
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        assert controller._is_closed is False
        assert isinstance(controller._pending_manager, PendingRequestManager)
        assert controller._endpoint is None
        assert controller._read_socket is None
        assert controller._write_socket is None
        assert controller._waker_task is None

    def test_process_effects_emit_connection_event_closed(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        controller._remote_addresses[1] = ("127.0.0.1", 443)
        effects = [(EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_CLOSED, None, None))]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.CONNECTION_CLOSED, data={"connection_handle": 1}
        )
        assert 1 not in controller._connections
        assert controller.get_remote_address(handle=1) is None

    def test_process_effects_emit_connection_event_full(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        effects = [(EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_CLOSED, 100, "timeout"))]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.CONNECTION_CLOSED,
            data={"connection_handle": 1, "error_code": 100, "reason": "timeout"},
        )

    def test_process_effects_emit_connection_event_minimal(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        effects = [(EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_ESTABLISHED, None, None))]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.CONNECTION_ESTABLISHED, data={"connection_handle": 1}
        )

    def test_process_effects_emit_connection_event_unregistered_connection(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        mock_connection.handle = 2
        controller._connections[mock_connection.handle] = mock_connection
        effects = [(EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_ESTABLISHED, None, None))]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_not_called()

    def test_process_effects_emit_session_event_full(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        effects = [
            (
                EMIT_SESSION_EVENT,
                (
                    10,
                    EventType.SESSION_READY,
                    "/path",
                    {"key": "value"},
                    ["h3"],
                    "h3",
                    b"data",
                    True,
                    100,
                    5,
                    0.5,
                    403,
                    0,
                    "done",
                ),
            )
        ]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.SESSION_READY,
            data={
                "session_id": 10,
                "path": "/path",
                "headers": {"key": "value"},
                "wt_available_protocols": ["h3"],
                "wt_protocol": "h3",
                "data": b"data",
                "is_unidirectional": True,
                "max_data": 100,
                "max_streams": 5,
                "ready_at": 0.5,
                "status_code": 403,
                "error_code": 0,
                "reason": "done",
            },
        )

    def test_process_effects_emit_session_event_minimal(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        effects = [
            (
                EMIT_SESSION_EVENT,
                (
                    10,
                    EventType.SESSION_DRAINING,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            )
        ]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.SESSION_DRAINING, data={"session_id": 10}
        )

    def test_process_effects_emit_session_event_unregistered_connection(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        mock_connection.handle = 2
        controller._connections[mock_connection.handle] = mock_connection
        effects = [
            (
                EMIT_SESSION_EVENT,
                (
                    10,
                    EventType.SESSION_DRAINING,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            )
        ]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_not_called()

    def test_process_effects_emit_stream_event_full(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        effects = [
            (EMIT_STREAM_EVENT, (20, EventType.STREAM_RESET_RECEIVED, 10, StreamDirection.BIDIRECTIONAL, True, 1))
        ]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.STREAM_RESET_RECEIVED,
            data={
                "stream_id": 20,
                "session_id": 10,
                "direction": StreamDirection.BIDIRECTIONAL,
                "is_peer_initiated": True,
                "error_code": 1,
            },
        )

    def test_process_effects_emit_stream_event_minimal(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        effects = [(EMIT_STREAM_EVENT, (20, EventType.STREAM_CLOSED, None, None, None, None))]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.STREAM_CLOSED, data={"stream_id": 20}
        )

    def test_process_effects_emit_stream_event_unregistered_connection(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        mock_connection.handle = 2
        controller._connections[mock_connection.handle] = mock_connection
        effects = [(EMIT_STREAM_EVENT, (20, EventType.STREAM_CLOSED, None, None, None, None))]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_not_called()

    def test_process_effects_exception(self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_warning = MagicMock()
        monkeypatch.setattr("pywebtransport._controller.controller._logger.warning", mock_warning)
        effects = [(NOTIFY_REQUEST_DONE, "not_a_tuple"), (NOTIFY_REQUEST_FAILED, "not_a_tuple")]

        controller._process_effects(handle=1, effects=effects)

        assert mock_warning.call_count == 2

    def test_process_effects_multiple_effects_in_batch(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        effects = [
            (EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_ESTABLISHED, None, None)),
            (EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_CLOSED, None, None)),
        ]

        controller._process_effects(handle=1, effects=effects)

        assert mock_connection._handle_event.call_count == 2

    @pytest.mark.asyncio
    async def test_process_effects_notify_request_done(self, controller: EndpointController) -> None:
        request_id, future = controller._pending_manager.create_request()
        effects = [(NOTIFY_REQUEST_DONE, (request_id, "success_result"))]

        controller._process_effects(handle=1, effects=effects)

        assert future.done()
        assert future.result() == "success_result"

    @pytest.mark.asyncio
    async def test_process_effects_notify_request_failed(self, controller: EndpointController) -> None:
        request_id, future = controller._pending_manager.create_request()
        exception = ValueError("fail")
        effects = [(NOTIFY_REQUEST_FAILED, (request_id, exception))]

        controller._process_effects(handle=1, effects=effects)

        assert future.done()
        assert future.exception() == exception

    def test_process_effects_unknown_opcode(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_warning = MagicMock()
        monkeypatch.setattr("pywebtransport._controller.controller._logger.warning", mock_warning)
        effects = [(999, None)]

        controller._process_effects(handle=1, effects=effects)

        mock_warning.assert_called_once()

    def test_process_effects_unmatched_effect_continues_batch(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        mock_convert = MagicMock(
            side_effect=[
                object(),
                EmitConnectionEvent(
                    connection_handle=1, event_type=EventType.CONNECTION_CLOSED, error_code=None, reason=None
                ),
            ]
        )
        monkeypatch.setattr("pywebtransport._controller.controller.conversion.convert_effect", mock_convert)
        effects = [(999, None), (EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_CLOSED, None, None))]

        controller._process_effects(handle=1, effects=effects)

        mock_connection._handle_event.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_runtime_event_command_failed(self, controller: EndpointController) -> None:
        request_id, future = controller._pending_manager.create_request()
        exception = ConnectionError(message="handshake failed", error_code=100)

        controller._process_runtime_event(event_tuple=(COMMAND_FAILED, (request_id, exception)))

        assert future.done()
        assert future.exception() is exception

    def test_process_runtime_event_connection_effects(
        self, controller: EndpointController, mock_connection: MagicMock
    ) -> None:
        controller._connections[mock_connection.handle] = mock_connection
        payload = (1, [(EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_ESTABLISHED, None, None))])

        controller._process_runtime_event(event_tuple=(CONNECTION_EFFECTS, payload))

        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.CONNECTION_ESTABLISHED, data={"connection_handle": 1}
        )

    def test_process_runtime_event_connection_spawned(
        self, controller: EndpointController, mock_connection: MagicMock, mock_connection_factory: MagicMock
    ) -> None:
        mock_connection_factory.return_value = mock_connection
        payload: tuple[Any, ...] = (
            1,
            ("127.0.0.1", 443),
            [(EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_ESTABLISHED, None, None))],
        )

        controller._process_runtime_event(event_tuple=(CONNECTION_SPAWNED, payload))

        assert controller.get_remote_address(handle=1) == ("127.0.0.1", 443)
        mock_connection_factory.assert_called_once_with(controller, 1)
        assert controller._connections[1] is mock_connection
        mock_connection._handle_event.assert_called_once_with(
            event_type=EventType.CONNECTION_ESTABLISHED, data={"connection_handle": 1}
        )

    def test_process_runtime_event_reactor_shutdown(self, controller: EndpointController) -> None:
        mock_event = MagicMock()
        controller._runtime_shutdown_event = mock_event

        controller._process_runtime_event(event_tuple=(REACTOR_SHUTDOWN, None))

        mock_event.set.assert_called_once()

    def test_process_runtime_event_unknown_opcode(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_warning = MagicMock()
        monkeypatch.setattr("pywebtransport._controller.controller._logger.warning", mock_warning)

        controller._process_runtime_event(event_tuple=(999, None))

        mock_warning.assert_called_once()

    def test_process_runtime_event_unmatched_runtime_event(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "pywebtransport._controller.controller.conversion.convert_runtime_event", MagicMock(return_value=object())
        )
        mock_warning = MagicMock()
        monkeypatch.setattr("pywebtransport._controller.controller._logger.warning", mock_warning)

        controller._process_runtime_event(event_tuple=(999, None))

        mock_warning.assert_not_called()
        assert not controller._remote_addresses
        assert not controller._runtime_shutdown_event.is_set()

    def test_require_endpoint_not_started(self) -> None:
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        with pytest.raises(expected_exception=RuntimeError, match="rt validate failed"):
            controller._require_endpoint()

    def test_require_endpoint_started(self, controller: EndpointController, mock_endpoint: MagicMock) -> None:
        result = controller._require_endpoint()

        assert result is mock_endpoint

    @pytest.mark.asyncio
    async def test_run_waker_loop_cancelled(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "sock_recv", AsyncMock(side_effect=asyncio.CancelledError()))
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)
        mock_read_socket = MagicMock()

        await controller._run_waker_loop(endpoint=mock_endpoint, waker=mock_waker, read_socket=mock_read_socket)

        mock_endpoint.poll_runtime_events.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_waker_loop_drain_eof(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "sock_recv", AsyncMock(side_effect=[b"x", b""]))
        mock_read_socket = MagicMock()
        mock_read_socket.recv.side_effect = [b"y", b""]
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        await controller._run_waker_loop(endpoint=mock_endpoint, waker=mock_waker, read_socket=mock_read_socket)

        assert mock_read_socket.recv.call_count == 2

    @pytest.mark.asyncio
    async def test_run_waker_loop_drains_events_after_close(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)
        received = iter([b"x", b""])

        async def mock_sock_recv(*args: Any, **kwargs: Any) -> bytes:
            controller._is_closed = True
            return next(received)

        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "sock_recv", AsyncMock(side_effect=mock_sock_recv))
        mock_read_socket = MagicMock()
        mock_read_socket.recv.side_effect = BlockingIOError()
        mock_endpoint.poll_runtime_events.return_value = [(REACTOR_SHUTDOWN, None)]

        await controller._run_waker_loop(endpoint=mock_endpoint, waker=mock_waker, read_socket=mock_read_socket)

        mock_endpoint.poll_runtime_events.assert_called_once()
        assert controller._runtime_shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_run_waker_loop_oserror(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "sock_recv", AsyncMock(side_effect=OSError("mock error")))
        mock_debug = MagicMock()
        monkeypatch.setattr("pywebtransport._controller.controller._logger.debug", mock_debug)
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)
        mock_read_socket = MagicMock()

        await controller._run_waker_loop(endpoint=mock_endpoint, waker=mock_waker, read_socket=mock_read_socket)

        mock_debug.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_waker_loop_oserror_when_closed(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        async def mock_sock_recv(*args: Any, **kwargs: Any) -> bytes:
            controller._is_closed = True
            raise OSError("mock error")

        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "sock_recv", AsyncMock(side_effect=mock_sock_recv))
        mock_read_socket = MagicMock()
        mock_debug = MagicMock()
        monkeypatch.setattr("pywebtransport._controller.controller._logger.debug", mock_debug)

        await controller._run_waker_loop(endpoint=mock_endpoint, waker=mock_waker, read_socket=mock_read_socket)

        mock_debug.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_waker_loop_poll_exception(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "sock_recv", AsyncMock(side_effect=[b"x", b""]))
        mock_read_socket = MagicMock()
        mock_read_socket.recv.side_effect = BlockingIOError()
        mock_endpoint.poll_runtime_events.side_effect = RuntimeError("poll error")
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        await controller._run_waker_loop(endpoint=mock_endpoint, waker=mock_waker, read_socket=mock_read_socket)

        mock_endpoint.poll_runtime_events.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_waker_loop_processes_multiple_events(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "sock_recv", AsyncMock(side_effect=[b"x", b""]))
        mock_read_socket = MagicMock()
        mock_read_socket.recv.side_effect = BlockingIOError()
        mock_endpoint.poll_runtime_events.return_value = [
            (CONNECTION_SPAWNED, (1, ("127.0.0.1", 443), [])),
            (CONNECTION_SPAWNED, (2, ("127.0.0.1", 444), [])),
        ]
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        await controller._run_waker_loop(endpoint=mock_endpoint, waker=mock_waker, read_socket=mock_read_socket)

        assert controller.get_remote_address(handle=1) == ("127.0.0.1", 443)
        assert controller.get_remote_address(handle=2) == ("127.0.0.1", 444)

    @pytest.mark.asyncio
    async def test_run_waker_loop_success(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "sock_recv", AsyncMock(side_effect=[b"x", b""]))
        mock_read_socket = MagicMock()
        mock_read_socket.recv.side_effect = BlockingIOError()
        mock_endpoint.poll_runtime_events.return_value = [(CONNECTION_SPAWNED, (1, ("127.0.0.1", 443), []))]
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        await controller._run_waker_loop(endpoint=mock_endpoint, waker=mock_waker, read_socket=mock_read_socket)

        mock_waker.clear.assert_called_once()
        mock_endpoint.poll_runtime_events.assert_called_once()

    def test_send_protocol_event_exception(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        event = MagicMock(spec=ProtocolEvent)
        mock_convert = MagicMock(side_effect=ValueError("mock convert error"))
        monkeypatch.setattr("pywebtransport._controller.controller.conversion.convert_protocol_event", mock_convert)

        with pytest.raises(
            expected_exception=ConnectionError, match="rt_event send failed actual=MagicMock"
        ) as exception_info:
            controller.send_protocol_event(handle=1, request_id=7, event=event)

        assert exception_info.value.connection_handle == 1
        mock_convert.assert_called_once_with(event=event)

    def test_send_protocol_event_success(
        self, controller: EndpointController, mock_endpoint: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        event = MagicMock(spec=ProtocolEvent)
        mock_convert = MagicMock(return_value=(99, ()))
        monkeypatch.setattr("pywebtransport._controller.controller.conversion.convert_protocol_event", mock_convert)

        controller.send_protocol_event(handle=1, request_id=7, event=event)

        mock_endpoint.handle_protocol_event.assert_called_once_with(handle=1, request_id=7, event=(99, ()))

    @pytest.mark.asyncio
    async def test_start_already_closed(self) -> None:
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)
        controller._is_closed = True

        with pytest.raises(expected_exception=RuntimeError, match="rt validate failed"):
            await controller.start()

    @pytest.mark.asyncio
    async def test_start_cancelled_while_acquiring(
        self, monkeypatch: pytest.MonkeyPatch, mock_endpoint: MagicMock, mock_waker: MagicMock
    ) -> None:
        monkeypatch.setattr("pywebtransport._controller.controller.Waker", MagicMock(return_value=mock_waker))
        resume_event = asyncio.Event()

        async def mock_run_in_executor(executor: Any, func: Any) -> MagicMock:
            await resume_event.wait()
            return mock_endpoint

        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "run_in_executor", mock_run_in_executor)
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)
        start_task = asyncio.create_task(controller.start())
        await asyncio.sleep(0.01)

        start_task.cancel()
        await asyncio.sleep(0.01)
        resume_event.set()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await start_task

        assert controller._endpoint is mock_endpoint
        assert controller._waker_task is not None
        assert controller._read_socket is not None
        assert controller._write_socket is not None
        controller._waker_task.cancel()
        controller._read_socket.close()
        controller._write_socket.close()

    @pytest.mark.asyncio
    async def test_start_idempotent(self, controller: EndpointController, mock_endpoint: MagicMock) -> None:
        await controller.start()

        assert controller._endpoint is mock_endpoint

    @pytest.mark.asyncio
    async def test_start_rollback_closes_endpoint_on_later_failure(
        self, monkeypatch: pytest.MonkeyPatch, mock_endpoint: MagicMock, mock_waker: MagicMock
    ) -> None:
        monkeypatch.setattr("pywebtransport._controller.controller.Endpoint", MagicMock(return_value=mock_endpoint))
        monkeypatch.setattr("pywebtransport._controller.controller.Waker", MagicMock(return_value=mock_waker))
        loop = asyncio.get_running_loop()
        real_create_task = loop.create_task
        call_count = 0

        def mock_create_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                coro.close()
                raise RuntimeError("mock task error")
            return real_create_task(coro, **kwargs)

        monkeypatch.setattr(loop, "create_task", mock_create_task)
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        with pytest.raises(expected_exception=RuntimeError, match="mock task error"):
            await controller.start()

        mock_endpoint.close.assert_called_once()
        assert controller._endpoint is None

    @pytest.mark.asyncio
    async def test_start_rollback_on_endpoint_failure(
        self, monkeypatch: pytest.MonkeyPatch, mock_waker: MagicMock
    ) -> None:
        monkeypatch.setattr("pywebtransport._controller.controller.Waker", MagicMock(return_value=mock_waker))
        monkeypatch.setattr(
            "pywebtransport._controller.controller.Endpoint", MagicMock(side_effect=ValueError("mock endpoint error"))
        )
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        with pytest.raises(expected_exception=ValueError, match="mock endpoint error"):
            await controller.start()

        assert controller._endpoint is None
        assert controller._read_socket is None
        assert controller._write_socket is None

    @pytest.mark.asyncio
    async def test_start_success(
        self, monkeypatch: pytest.MonkeyPatch, mock_endpoint: MagicMock, mock_waker: MagicMock
    ) -> None:
        monkeypatch.setattr("pywebtransport._controller.controller.Endpoint", MagicMock(return_value=mock_endpoint))
        monkeypatch.setattr("pywebtransport._controller.controller.Waker", MagicMock(return_value=mock_waker))
        controller = EndpointController(connection_factory=MagicMock(), config=MagicMock(), is_client=True)

        await controller.start()

        assert controller._endpoint is mock_endpoint
        assert isinstance(controller._read_socket, socket.socket)
        assert isinstance(controller._write_socket, socket.socket)
        assert controller._waker_task is not None
        assert not controller._waker_task.done()

        controller._waker_task.cancel()
        assert controller._read_socket is not None
        assert controller._write_socket is not None
        controller._read_socket.close()
        controller._write_socket.close()
