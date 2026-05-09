"""Unit tests for the pywebtransport._controller.controller module."""

import asyncio
import socket
from collections.abc import Iterator
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from pywebtransport import ConnectionError
from pywebtransport._controller import abi
from pywebtransport._controller.controller import EndpointController
from pywebtransport._controller.pending import PendingRequestManager
from pywebtransport._protocol.events import ProtocolEvent


class TestEndpointController:

    @pytest.fixture
    def controller(
        self, mock_endpoint: MagicMock, mock_waker: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> Iterator[EndpointController]:
        monkeypatch.setattr("pywebtransport._controller.controller.Endpoint", MagicMock(return_value=mock_endpoint))
        monkeypatch.setattr("pywebtransport._controller.controller.Waker", MagicMock(return_value=mock_waker))
        config = MagicMock()
        mock_loop = MagicMock()

        def mock_create_task(coro: Any) -> MagicMock:
            coro.close()
            return MagicMock()

        mock_loop.create_task.side_effect = mock_create_task

        ctrl = EndpointController(config=config, is_client=True, loop=mock_loop)

        yield ctrl

        if not ctrl._is_closed:
            ctrl.close()

    @pytest.fixture
    def mock_endpoint(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def mock_waker(self) -> MagicMock:
        return MagicMock()

    def test_close_idempotent(self, controller: EndpointController) -> None:
        controller.close()

        controller.close()

        assert controller._is_closed is True

    def test_close_oserror(self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_r_sock = MagicMock()
        mock_r_sock.close.side_effect = OSError("mock close error")
        monkeypatch.setattr(controller, "_r_sock", mock_r_sock)

        controller.close()

        assert controller._is_closed is True

    def test_close_success(
        self, controller: EndpointController, mock_endpoint: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        controller.register_connection(handle=1, callback=MagicMock())
        controller._remote_addresses[1] = ("127.0.0.1", 443)
        controller.set_spawn_callback(callback=MagicMock())

        mock_task = cast(MagicMock, controller._waker_task)
        cast(MagicMock, mock_task.done).return_value = False

        r_sock_mock = MagicMock()
        w_sock_mock = MagicMock()
        monkeypatch.setattr(controller, "_r_sock", r_sock_mock)
        monkeypatch.setattr(controller, "_w_sock", w_sock_mock)

        controller.close()

        mock_endpoint.close.assert_called_once()
        assert controller._is_closed is True
        assert not controller._connection_callbacks
        assert not controller._remote_addresses
        assert controller._spawn_callback is None
        cast(MagicMock, mock_task.cancel).assert_called_once()
        r_sock_mock.close.assert_called_once()
        w_sock_mock.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_failure(self, controller: EndpointController, mock_endpoint: MagicMock) -> None:
        mock_endpoint.connect.side_effect = ValueError("mock connection error")

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection open failed"):
            await controller.connect(remote_host="127.0.0.1", remote_port=443, server_name="localhost")

        assert not controller._pending_manager._requests

    @pytest.mark.asyncio
    async def test_connect_failure_connection_error(
        self, controller: EndpointController, mock_endpoint: MagicMock
    ) -> None:
        exc = ConnectionError(message="native connection error")
        mock_endpoint.connect.side_effect = exc

        with pytest.raises(expected_exception=ConnectionError) as exc_info:
            await controller.connect(remote_host="127.0.0.1", remote_port=443, server_name="localhost")

        assert exc_info.value is exc
        assert not controller._pending_manager._requests

    @pytest.mark.asyncio
    async def test_connect_success(self, controller: EndpointController, mock_endpoint: MagicMock) -> None:
        def mock_connect(*args: Any, **kwargs: Any) -> None:
            req_id = kwargs["request_id"]
            controller._pending_manager.complete_request(request_id=req_id, result=42)

        mock_endpoint.connect.side_effect = mock_connect

        result = await controller.connect(remote_host="127.0.0.1", remote_port=443, server_name="localhost")

        assert result == 42
        mock_endpoint.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_request_dispatcher_exception(self, controller: EndpointController) -> None:
        def dispatcher(req_id: int) -> None:
            raise ValueError("dispatch error")

        with pytest.raises(expected_exception=ValueError, match="dispatch error"):
            await controller.execute_request(dispatcher=dispatcher)

        assert not controller._pending_manager._requests

    @pytest.mark.asyncio
    async def test_execute_request_future_cancelled(self, controller: EndpointController) -> None:
        def dispatcher(req_id: int) -> None:
            pass

        task = asyncio.create_task(controller.execute_request(dispatcher=dispatcher))
        await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await task

        assert not controller._pending_manager._requests

    @pytest.mark.asyncio
    async def test_execute_request_success(self, controller: EndpointController) -> None:
        def dispatcher(req_id: int) -> None:
            controller._pending_manager.complete_request(request_id=req_id, result="done")

        result = await controller.execute_request(dispatcher=dispatcher)

        assert result == "done"
        assert not controller._pending_manager._requests

    def test_execute_effects_cleanup_h3_stream(self, controller: EndpointController) -> None:
        effects = [(abi.CLEANUP_H3_STREAM, None)]

        controller._execute_effects(handle=1, effects=effects)

        assert True

    def test_execute_effects_emit_connection_event_full(self, controller: EndpointController) -> None:
        cb = MagicMock()
        controller.register_connection(handle=1, callback=cb)
        effects = [(abi.EMIT_CONNECTION_EVENT, (1, "CONN_ERR", 100, "timeout"))]

        controller._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with("CONN_ERR", {"connection_handle": 1, "error_code": 100, "reason": "timeout"})

    def test_execute_effects_emit_connection_event_minimal(self, controller: EndpointController) -> None:
        cb = MagicMock()
        controller.register_connection(handle=1, callback=cb)
        effects = [(abi.EMIT_CONNECTION_EVENT, (1, "CONN_OK", None, None))]

        controller._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with("CONN_OK", {"connection_handle": 1})

    def test_execute_effects_emit_connection_event_no_callback(self, controller: EndpointController) -> None:
        effects = [(abi.EMIT_CONNECTION_EVENT, (1, "CONN_OK", None, None))]

        controller._execute_effects(handle=1, effects=effects)

        assert True

    def test_execute_effects_emit_session_event_full(self, controller: EndpointController) -> None:
        cb = MagicMock()
        controller.register_connection(handle=1, callback=cb)
        effects = [
            (
                abi.EMIT_SESSION_EVENT,
                (10, "SESSION_OK", "/p", {"k": "v"}, ["h3"], "h3", b"d", True, 100, 5, 0.5, 0, "ok"),
            )
        ]

        controller._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with(
            "SESSION_OK",
            {
                "session_id": 10,
                "path": "/p",
                "headers": {"k": "v"},
                "wt_available_protocols": ["h3"],
                "wt_protocol": "h3",
                "data": b"d",
                "is_unidirectional": True,
                "max_data": 100,
                "max_streams": 5,
                "ready_at": 0.5,
                "error_code": 0,
                "reason": "ok",
            },
        )

    def test_execute_effects_emit_session_event_minimal(self, controller: EndpointController) -> None:
        cb = MagicMock()
        controller.register_connection(handle=1, callback=cb)
        effects = [
            (
                abi.EMIT_SESSION_EVENT,
                (10, "SESSION_MIN", None, None, None, None, None, None, None, None, None, None, None),
            )
        ]

        controller._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with("SESSION_MIN", {"session_id": 10})

    def test_execute_effects_emit_session_event_no_callback(self, controller: EndpointController) -> None:
        effects = [
            (
                abi.EMIT_SESSION_EVENT,
                (10, "SESSION_MIN", None, None, None, None, None, None, None, None, None, None, None),
            )
        ]

        controller._execute_effects(handle=1, effects=effects)

        assert True

    def test_execute_effects_emit_stream_event_full(self, controller: EndpointController) -> None:
        cb = MagicMock()
        controller.register_connection(handle=1, callback=cb)
        effects = [(abi.EMIT_STREAM_EVENT, (20, "STREAM_EV", 10, "in", True, 1))]

        controller._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with(
            "STREAM_EV", {"stream_id": 20, "session_id": 10, "direction": "in", "is_remote": True, "error_code": 1}
        )

    def test_execute_effects_emit_stream_event_minimal(self, controller: EndpointController) -> None:
        cb = MagicMock()
        controller.register_connection(handle=1, callback=cb)
        effects = [(abi.EMIT_STREAM_EVENT, (20, "STREAM_MIN", None, None, None, None))]

        controller._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with("STREAM_MIN", {"stream_id": 20})

    def test_execute_effects_emit_stream_event_no_callback(self, controller: EndpointController) -> None:
        effects = [(abi.EMIT_STREAM_EVENT, (20, "STREAM_MIN", None, None, None, None))]

        controller._execute_effects(handle=1, effects=effects)

        assert True

    def test_execute_effects_exception(self, controller: EndpointController) -> None:
        effects = [(abi.NOTIFY_REQUEST_DONE, "not_a_tuple"), (abi.CLEANUP_H3_STREAM, None)]

        controller._execute_effects(handle=1, effects=effects)

        assert True

    @pytest.mark.asyncio
    async def test_execute_effects_notify_request_done(self, controller: EndpointController) -> None:
        req_id, future = controller._pending_manager.create_request()
        effects = [(abi.NOTIFY_REQUEST_DONE, (req_id, "success_result"))]

        controller._execute_effects(handle=1, effects=effects)

        assert future.done()
        assert future.result() == "success_result"

    @pytest.mark.asyncio
    async def test_execute_effects_notify_request_failed(self, controller: EndpointController) -> None:
        req_id, future = controller._pending_manager.create_request()
        exc = ValueError("fail")
        effects = [(abi.NOTIFY_REQUEST_FAILED, (req_id, exc))]

        controller._execute_effects(handle=1, effects=effects)

        assert future.done()
        assert future.exception() == exc

    def test_execute_effects_unknown_opcode(self, controller: EndpointController) -> None:
        effects = [(999, None)]

        controller._execute_effects(handle=1, effects=effects)

        assert True

    def test_get_local_addresses(self, controller: EndpointController, mock_endpoint: MagicMock) -> None:
        mock_endpoint.get_local_addresses.return_value = [("127.0.0.1", 443)]

        result = controller.get_local_addresses()

        assert result == [("127.0.0.1", 443)]
        mock_endpoint.get_local_addresses.assert_called_once()

    def test_get_remote_address_none(self, controller: EndpointController) -> None:
        result = controller.get_remote_address(handle=999)

        assert result is None

    def test_init_state(self, controller: EndpointController) -> None:
        assert controller._is_closed is False
        assert isinstance(controller._pending_manager, PendingRequestManager)
        assert isinstance(controller._r_sock, socket.socket)
        assert isinstance(controller._w_sock, socket.socket)
        cast(MagicMock, controller._loop.create_task).assert_called_once()

    @pytest.mark.asyncio
    async def test_process_runtime_event_command_completed(self, controller: EndpointController) -> None:
        req_id, future = controller._pending_manager.create_request()
        payload = (req_id, 1, ("127.0.0.1", 443))

        controller._process_runtime_event(event_tuple=(abi.COMMAND_COMPLETED, payload))

        assert controller.get_remote_address(handle=1) == ("127.0.0.1", 443)
        assert future.done()
        assert future.result() == 1

    @pytest.mark.asyncio
    async def test_process_runtime_event_command_failed(self, controller: EndpointController) -> None:
        req_id, future = controller._pending_manager.create_request()
        payload = (req_id, 100, "handshake failed")

        controller._process_runtime_event(event_tuple=(abi.COMMAND_FAILED, payload))

        assert future.done()
        exc = cast(ConnectionError, future.exception())
        assert isinstance(exc, ConnectionError)
        assert exc.message == "handshake failed"
        assert exc.error_code == 100

    def test_process_runtime_event_connection_effects(self, controller: EndpointController) -> None:
        cb = MagicMock()
        controller.register_connection(handle=1, callback=cb)
        payload = (1, [(abi.EMIT_CONNECTION_EVENT, (1, "OK", None, None))])

        controller._process_runtime_event(event_tuple=(abi.CONNECTION_EFFECTS, payload))

        cb.assert_called_once_with("OK", {"connection_handle": 1})

    def test_process_runtime_event_connection_spawned_no_callback(self, controller: EndpointController) -> None:
        payload: tuple[Any, ...] = (1, ("127.0.0.1", 443), [])

        controller._process_runtime_event(event_tuple=(abi.CONNECTION_SPAWNED, payload))

        assert controller.get_remote_address(handle=1) == ("127.0.0.1", 443)

    def test_process_runtime_event_connection_spawned_with_callback(self, controller: EndpointController) -> None:
        cb = MagicMock()
        controller.set_spawn_callback(callback=cb)
        payload: tuple[Any, ...] = (1, ("127.0.0.1", 443), [])

        controller._process_runtime_event(event_tuple=(abi.CONNECTION_SPAWNED, payload))

        assert controller.get_remote_address(handle=1) == ("127.0.0.1", 443)
        cb.assert_called_once_with(1)

    def test_process_runtime_event_reactor_shutdown(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_close = MagicMock()
        monkeypatch.setattr(controller, "close", mock_close)

        controller._process_runtime_event(event_tuple=(abi.REACTOR_SHUTDOWN, None))

        mock_close.assert_called_once()

    def test_process_runtime_event_reactor_shutdown_already_closed(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        controller._is_closed = True
        mock_close = MagicMock()
        monkeypatch.setattr(controller, "close", mock_close)

        controller._process_runtime_event(event_tuple=(abi.REACTOR_SHUTDOWN, None))

        mock_close.assert_called_once()

    def test_process_runtime_event_unknown_opcode(self, controller: EndpointController) -> None:
        controller._process_runtime_event(event_tuple=(999, None))

        assert True

    def test_register_unregister_connection(self, controller: EndpointController) -> None:
        cb = MagicMock()

        controller.register_connection(handle=1, callback=cb)
        controller._remote_addresses[1] = ("127.0.0.1", 443)
        controller.unregister_connection(handle=1)

        assert 1 not in controller._connection_callbacks
        assert controller.get_remote_address(handle=1) is None

    def test_send_user_event_exception(self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch) -> None:
        event = MagicMock(spec=ProtocolEvent)
        mock_pack = MagicMock(side_effect=ValueError("mock pack error"))
        monkeypatch.setattr("pywebtransport._controller.controller.mapper.pack_user_event", mock_pack)

        with pytest.raises(
            expected_exception=ConnectionError, match="rt_event send failed actual=MagicMock"
        ) as exc_info:
            controller.send_user_event(handle=1, event=event)

        assert exc_info.value.connection_handle == 1
        mock_pack.assert_called_once_with(event=event)

    def test_send_user_event_success(
        self, controller: EndpointController, mock_endpoint: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        event = MagicMock(spec=ProtocolEvent)
        mock_pack = MagicMock(return_value=(99, ()))
        monkeypatch.setattr("pywebtransport._controller.controller.mapper.pack_user_event", mock_pack)

        controller.send_user_event(handle=1, event=event)

        mock_endpoint.handle_user_event.assert_called_once_with(handle=1, event=(99, ()))

    def test_set_spawn_callback(self, controller: EndpointController) -> None:
        cb = MagicMock()

        controller.set_spawn_callback(callback=cb)

        assert controller._spawn_callback is cb

    @pytest.mark.asyncio
    async def test_waker_task_loop_cancelled(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(controller._loop, "sock_recv", AsyncMock(side_effect=asyncio.CancelledError()))

        await controller._waker_task_loop()

        assert True

    @pytest.mark.asyncio
    async def test_waker_task_loop_drain_eof(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(controller._loop, "sock_recv", AsyncMock(side_effect=[b"x", b""]))
        mock_r_sock = MagicMock()
        mock_r_sock.recv.side_effect = [b"y", b""]
        monkeypatch.setattr(controller, "_r_sock", mock_r_sock)

        await controller._waker_task_loop()

        assert mock_r_sock.recv.call_count == 2

    @pytest.mark.asyncio
    async def test_waker_task_loop_graceful_exit(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def mock_sock_recv(*args: Any, **kwargs: Any) -> bytes:
            controller._is_closed = True
            return b"x"

        monkeypatch.setattr(controller._loop, "sock_recv", AsyncMock(side_effect=mock_sock_recv))
        mock_r_sock = MagicMock()
        mock_r_sock.recv.side_effect = BlockingIOError()
        monkeypatch.setattr(controller, "_r_sock", mock_r_sock)
        mock_endpoint = cast(MagicMock, controller._endpoint)
        mock_endpoint.poll_runtime_events.return_value = []

        await controller._waker_task_loop()

        assert controller._is_closed is True

    @pytest.mark.asyncio
    async def test_waker_task_loop_oserror(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(controller._loop, "sock_recv", AsyncMock(side_effect=OSError("mock error")))

        await controller._waker_task_loop()

        assert True

    @pytest.mark.asyncio
    async def test_waker_task_loop_oserror_when_closed(
        self, controller: EndpointController, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def mock_sock_recv(*args: Any, **kwargs: Any) -> bytes:
            controller._is_closed = True
            raise OSError("mock error")

        monkeypatch.setattr(controller._loop, "sock_recv", AsyncMock(side_effect=mock_sock_recv))

        await controller._waker_task_loop()

        assert controller._is_closed is True

    @pytest.mark.asyncio
    async def test_waker_task_loop_poll_exception(
        self, controller: EndpointController, mock_endpoint: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(controller._loop, "sock_recv", AsyncMock(side_effect=[b"x", b""]))
        mock_r_sock = MagicMock()
        mock_r_sock.recv.side_effect = BlockingIOError()
        monkeypatch.setattr(controller, "_r_sock", mock_r_sock)
        mock_endpoint.poll_runtime_events.side_effect = RuntimeError("poll error")

        await controller._waker_task_loop()

        mock_endpoint.poll_runtime_events.assert_called_once()

    @pytest.mark.asyncio
    async def test_waker_task_loop_success(
        self,
        controller: EndpointController,
        mock_endpoint: MagicMock,
        mock_waker: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(controller._loop, "sock_recv", AsyncMock(side_effect=[b"x", b""]))
        mock_r_sock = MagicMock()
        mock_r_sock.recv.side_effect = BlockingIOError()
        monkeypatch.setattr(controller, "_r_sock", mock_r_sock)
        mock_endpoint.poll_runtime_events.return_value = [(abi.COMMAND_COMPLETED, (1, 1, ("127.0.0.1", 443)))]

        await controller._waker_task_loop()

        mock_waker.clear.assert_called_once()
        mock_endpoint.poll_runtime_events.assert_called_once()
