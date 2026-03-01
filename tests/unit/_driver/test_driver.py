"""Unit tests for the pywebtransport._driver.driver module."""

from collections.abc import Generator
from typing import Any
from unittest.mock import Mock, patch

import pytest

from pywebtransport import ClientConfig, ConnectionError, ServerConfig
from pywebtransport._driver import abi
from pywebtransport._driver.driver import EndpointDriver, create_client, create_server
from pywebtransport._protocol.events import ConnectionClose


class TestEndpointDriver:

    @pytest.fixture
    def driver(self, mock_loop: Mock, mock_endpoint: Mock) -> EndpointDriver:
        config = Mock(spec=ClientConfig)
        return EndpointDriver(config=config, is_client=True, loop=mock_loop)

    @pytest.fixture
    def mock_endpoint(self) -> Generator[Mock, None, None]:
        with patch(target="pywebtransport._driver.driver.Endpoint") as mock_cls:
            endpoint_instance = mock_cls.return_value
            yield endpoint_instance

    @pytest.fixture
    def mock_loop(self) -> Mock:
        loop = Mock()
        loop.time.return_value = 1000.0
        return loop

    @pytest.fixture
    def mock_transport(self) -> Mock:
        transport = Mock()
        transport.is_closing.return_value = False
        transport.get_extra_info.return_value = ("127.0.0.1", 12345)
        return transport

    def test_connect(self, driver: EndpointDriver, mock_endpoint: Mock, mock_loop: Mock) -> None:
        mock_endpoint.connect.return_value = 42

        with patch.object(target=driver, attribute="_transmit") as mock_transmit:
            handle = driver.connect(remote_host="127.0.0.1", remote_port=443, server_name="test.local")

            mock_endpoint.connect.assert_called_once_with(
                remote=("127.0.0.1", 443), server_name="test.local", now=1000.0
            )
            mock_transmit.assert_called_once()
            assert handle == 42

    def test_connection_lost_with_exc(self, driver: EndpointDriver) -> None:
        exc = ValueError("test exc")
        timer_mock = Mock()
        setattr(driver, "_timer_handle", timer_mock)

        with patch.object(target=driver, attribute="_pending_manager") as mock_pm:
            driver.connection_lost(exc=exc)

            timer_mock.cancel.assert_called_once()
            assert getattr(driver, "_timer_handle") is None
            mock_pm.fail_all.assert_called_once_with(exception=exc)

    def test_connection_lost_without_exc(self, driver: EndpointDriver) -> None:
        with patch.object(target=driver, attribute="_pending_manager") as mock_pm:
            driver.connection_lost(exc=None)

            mock_pm.fail_all.assert_called_once()
            call_exc = mock_pm.fail_all.call_args.kwargs["exception"]
            assert isinstance(call_exc, ConnectionError)

    def test_connection_made(self, driver: EndpointDriver, mock_transport: Mock) -> None:
        with patch.object(target=driver, attribute="_transmit") as mock_transmit:
            driver.connection_made(transport=mock_transport)

            assert driver._transport is mock_transport
            mock_transmit.assert_called_once()

    def test_create_request(self, driver: EndpointDriver) -> None:
        with patch.object(target=driver, attribute="_pending_manager") as mock_pm:
            mock_pm.create_request.return_value = (1, Mock())

            req_id, fut = driver.create_request()

            assert req_id == 1
            mock_pm.create_request.assert_called_once()

    def test_datagram_received_exception(self, driver: EndpointDriver, mock_endpoint: Mock) -> None:
        mock_endpoint.handle_datagram.side_effect = RuntimeError("endpoint crash")

        with patch.object(target=driver, attribute="_transmit") as mock_transmit:
            driver.datagram_received(data=b"data", addr=("192.168.1.1", 443))

            mock_transmit.assert_called_once()

    def test_datagram_received_full_flow(
        self, driver: EndpointDriver, mock_endpoint: Mock, mock_transport: Mock
    ) -> None:
        driver.connection_made(transport=mock_transport)
        mock_endpoint.handle_datagram.return_value = (abi.CONSUMED, None)

        with (
            patch.object(target=driver, attribute="_process_endpoint_event") as mock_process,
            patch.object(target=driver, attribute="_transmit") as mock_transmit,
        ):
            driver.datagram_received(data=b"data", addr=("192.168.1.1", 443))

            mock_endpoint.handle_datagram.assert_called_once_with(
                data=b"data", remote=("192.168.1.1", 443), local=("127.0.0.1", 12345), now=1000.0
            )
            mock_process.assert_called_once_with(event_tuple=(abi.CONSUMED, None))
            mock_transmit.assert_called_once()

    def test_datagram_received_invalid_sockname(
        self, driver: EndpointDriver, mock_endpoint: Mock, mock_transport: Mock
    ) -> None:
        mock_transport.get_extra_info.return_value = "not a tuple"
        driver.connection_made(transport=mock_transport)

        with patch.object(target=driver, attribute="_transmit"):
            driver.datagram_received(data=b"data", addr=("192.168.1.1", 443))

            mock_endpoint.handle_datagram.assert_called_once_with(
                data=b"data", remote=("192.168.1.1", 443), local=None, now=1000.0
            )

    def test_datagram_received_no_local_info(self, driver: EndpointDriver, mock_endpoint: Mock) -> None:
        with (
            patch.object(target=driver, attribute="_process_endpoint_event"),
            patch.object(target=driver, attribute="_transmit"),
        ):
            driver.datagram_received(data=b"data", addr=("192.168.1.1", 443))

            mock_endpoint.handle_datagram.assert_called_once_with(
                data=b"data", remote=("192.168.1.1", 443), local=None, now=1000.0
            )

    def test_error_received(self, driver: EndpointDriver) -> None:
        driver.error_received(exc=ValueError("test"))

    def test_execute_effects_cleanup_and_missing_cb(self, driver: EndpointDriver) -> None:
        effects = [(abi.CLEANUP_H3_STREAM, None), (abi.EMIT_CONNECTION_EVENT, (100, "connected", None, None))]

        driver._execute_effects(handle=1, effects=effects)

    def test_execute_effects_connection_event_full(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.register_connection(handle=1, callback=cb)

        effects = [(abi.EMIT_CONNECTION_EVENT, (100, "connected", 0, "ok"))]
        driver._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with("connected", {"connection_id": 100, "error_code": 0, "reason": "ok"})

    def test_execute_effects_connection_event_minimal(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.register_connection(handle=1, callback=cb)

        effects = [(abi.EMIT_CONNECTION_EVENT, (100, "connected", None, None))]
        driver._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with("connected", {"connection_id": 100})

    def test_execute_effects_exception_isolation(self, driver: EndpointDriver) -> None:
        effects = [(abi.NOTIFY_REQUEST_DONE, (1, "success")), (abi.CLEANUP_H3_STREAM, None)]

        with patch.object(target=driver, attribute="_pending_manager") as mock_pm:
            mock_pm.complete_request.side_effect = ValueError("isolated error")

            driver._execute_effects(handle=1, effects=effects)

            mock_pm.complete_request.assert_called_once()

    def test_execute_effects_missing_callbacks_and_unmatched_opcode(self, driver: EndpointDriver) -> None:
        session_payload = (10, "opened", None, None, None, None, None, None, None, None, None)
        effects = [
            (abi.EMIT_SESSION_EVENT, session_payload),
            (abi.EMIT_STREAM_EVENT, (20, "data", None, None, None, None)),
            (999, None),
            (abi.CLEANUP_H3_STREAM, None),
        ]

        driver._execute_effects(handle=1, effects=effects)

    def test_execute_effects_multiple_loop_back(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.register_connection(handle=1, callback=cb)
        session_payload = (10, "opened", None, None, None, None, None, None, None, None, None)
        effects = [
            (abi.EMIT_CONNECTION_EVENT, (100, "connected", None, None)),
            (abi.EMIT_SESSION_EVENT, session_payload),
            (abi.EMIT_STREAM_EVENT, (20, "data", None, None, None, None)),
        ]

        driver._execute_effects(handle=1, effects=effects)

        assert cb.call_count == 3

    def test_execute_effects_notify_done_failed(self, driver: EndpointDriver) -> None:
        effects = [(abi.NOTIFY_REQUEST_DONE, (1, "success")), (abi.NOTIFY_REQUEST_FAILED, (2, ValueError()))]

        with patch.object(target=driver, attribute="_pending_manager") as mock_pm:
            driver._execute_effects(handle=1, effects=effects)

            mock_pm.complete_request.assert_called_once_with(request_id=1, result="success")
            mock_pm.fail_request.assert_called_once()

    def test_execute_effects_session_event_full(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.register_connection(handle=1, callback=cb)

        payload = (10, "opened", "/test", {b"a": b"b"}, b"data", True, 1024, 5, 100.0, 1, "err")
        effects = [(abi.EMIT_SESSION_EVENT, payload)]
        driver._execute_effects(handle=1, effects=effects)

        expected_dict = {
            "session_id": 10,
            "path": "/test",
            "headers": {b"a": b"b"},
            "data": b"data",
            "is_unidirectional": True,
            "max_data": 1024,
            "max_streams": 5,
            "ready_at": 100.0,
            "error_code": 1,
            "reason": "err",
        }
        cb.assert_called_once_with("opened", expected_dict)

    def test_execute_effects_session_event_minimal(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.register_connection(handle=1, callback=cb)

        payload = (10, "opened", None, None, None, None, None, None, None, None, None)
        effects = [(abi.EMIT_SESSION_EVENT, payload)]
        driver._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with("opened", {"session_id": 10})

    def test_execute_effects_stream_event_full(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.register_connection(handle=1, callback=cb)

        effects = [(abi.EMIT_STREAM_EVENT, (20, "data", 10, 1, True, 0))]
        driver._execute_effects(handle=1, effects=effects)

        expected_dict = {"stream_id": 20, "session_id": 10, "direction": 1, "is_remote": True, "error_code": 0}
        cb.assert_called_once_with("data", expected_dict)

    def test_execute_effects_stream_event_minimal(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.register_connection(handle=1, callback=cb)

        effects = [(abi.EMIT_STREAM_EVENT, (20, "data", None, None, None, None))]
        driver._execute_effects(handle=1, effects=effects)

        cb.assert_called_once_with("data", {"stream_id": 20})

    def test_get_remote_address(self, driver: EndpointDriver, mock_endpoint: Mock) -> None:
        mock_endpoint.get_remote_address.return_value = ("1.2.3.4", 443)

        assert driver.get_remote_address(handle=1) == ("1.2.3.4", 443)
        mock_endpoint.get_remote_address.assert_called_once_with(handle=1)

    def test_handle_timeout_exception(self, driver: EndpointDriver, mock_endpoint: Mock) -> None:
        mock_endpoint.handle_timeout.side_effect = RuntimeError("timeout crash")

        with patch.object(target=driver, attribute="_transmit") as mock_transmit:
            driver._handle_timeout()
            mock_transmit.assert_called_once()

    def test_handle_timeout_success(self, driver: EndpointDriver, mock_endpoint: Mock) -> None:
        timer_mock = Mock()
        setattr(driver, "_timer_handle", timer_mock)
        mock_endpoint.handle_timeout.return_value = [(1, [(abi.CLEANUP_H3_STREAM, None)])]

        with (
            patch.object(target=driver, attribute="_execute_effects") as mock_exec,
            patch.object(target=driver, attribute="_transmit") as mock_transmit,
        ):
            driver._handle_timeout()

            assert getattr(driver, "_timer_handle") is None
            mock_endpoint.handle_timeout.assert_called_once_with(now=1000.0)
            mock_exec.assert_called_once_with(handle=1, effects=[(abi.CLEANUP_H3_STREAM, None)])
            mock_transmit.assert_called_once()

    def test_init_default_loop(self, mock_endpoint: Mock) -> None:
        with patch(target="pywebtransport._driver.driver.asyncio.get_running_loop") as mock_get_loop:
            mock_loop = Mock()
            mock_get_loop.return_value = mock_loop
            config = Mock(spec=ClientConfig)
            driver = EndpointDriver(config=config, is_client=True)

            assert driver._loop is mock_loop

    def test_process_endpoint_event_consumed(self, driver: EndpointDriver) -> None:
        driver._process_endpoint_event(event_tuple=(abi.CONSUMED, None))

    def test_process_endpoint_event_effects(self, driver: EndpointDriver) -> None:
        with patch.object(target=driver, attribute="_execute_effects") as mock_exec:
            driver._process_endpoint_event(event_tuple=(abi.CONNECTION_EFFECTS, (1, [])))
            mock_exec.assert_called_once_with(handle=1, effects=[])

    def test_process_endpoint_event_spawned(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.set_spawn_callback(callback=cb)

        with patch.object(target=driver, attribute="_execute_effects") as mock_exec:
            driver._process_endpoint_event(event_tuple=(abi.CONNECTION_SPAWNED, (2, [])))

            cb.assert_called_once_with(2)
            mock_exec.assert_called_once_with(handle=2, effects=[])

    def test_process_endpoint_event_spawned_no_callback(self, driver: EndpointDriver) -> None:
        with patch.object(target=driver, attribute="_execute_effects") as mock_exec:
            driver._process_endpoint_event(event_tuple=(abi.CONNECTION_SPAWNED, (2, [])))
            mock_exec.assert_called_once_with(handle=2, effects=[])

    def test_process_endpoint_event_transmit(self, driver: EndpointDriver, mock_transport: Mock) -> None:
        driver.connection_made(transport=mock_transport)

        driver._process_endpoint_event(event_tuple=(abi.TRANSMIT, (("1.1.1.1", 53), b"dns")))

        mock_transport.sendto.assert_called_once_with(b"dns", ("1.1.1.1", 53))

    def test_process_endpoint_event_transmit_closed(self, driver: EndpointDriver, mock_transport: Mock) -> None:
        mock_transport.is_closing.return_value = True
        driver.connection_made(transport=mock_transport)

        driver._process_endpoint_event(event_tuple=(abi.TRANSMIT, (("1.1.1.1", 53), b"dns")))

        mock_transport.sendto.assert_not_called()

    def test_process_endpoint_event_transmit_no_transport(self, driver: EndpointDriver) -> None:
        driver._process_endpoint_event(event_tuple=(abi.TRANSMIT, (("1.1.1.1", 53), b"dns")))

    def test_process_endpoint_event_unmatched_opcode(self, driver: EndpointDriver) -> None:
        driver._process_endpoint_event(event_tuple=(999, None))

    def test_register_unregister_connection(self, driver: EndpointDriver) -> None:
        cb = Mock()
        driver.register_connection(handle=1, callback=cb)

        assert driver._connection_callbacks[1] is cb

        driver.unregister_connection(handle=1)

        assert 1 not in driver._connection_callbacks

        driver.unregister_connection(handle=999)

    def test_schedule_timer(self, driver: EndpointDriver, mock_endpoint: Mock, mock_loop: Mock) -> None:
        old_timer = Mock()
        setattr(driver, "_timer_handle", old_timer)
        mock_endpoint.timeout.return_value = 2000.0
        new_timer = Mock()
        mock_loop.call_at.return_value = new_timer

        driver._schedule_timer()

        old_timer.cancel.assert_called_once()
        mock_loop.call_at.assert_called_once_with(2000.0, driver._handle_timeout)
        assert getattr(driver, "_timer_handle") is new_timer

    def test_schedule_timer_no_timeout(self, driver: EndpointDriver, mock_endpoint: Mock) -> None:
        setattr(driver, "_timer_handle", Mock())
        mock_endpoint.timeout.return_value = None

        driver._schedule_timer()

        assert getattr(driver, "_timer_handle") is None

    def test_send_user_event_exception(self, driver: EndpointDriver) -> None:
        event = ConnectionClose(request_id=1, error_code=0, reason=None)

        with (
            patch.object(target=driver, attribute="_transmit") as mock_transmit,
            patch(target="pywebtransport._driver.driver.mapper.pack_user_event", side_effect=ValueError),
        ):
            driver.send_user_event(handle=1, event=event)

            mock_transmit.assert_called_once()

    def test_send_user_event_no_tuple_returned(self, driver: EndpointDriver, mock_endpoint: Mock) -> None:
        event = ConnectionClose(request_id=1, error_code=0, reason=None)
        mock_endpoint.handle_user_event.return_value = None

        with (
            patch.object(target=driver, attribute="_process_endpoint_event") as mock_process,
            patch.object(target=driver, attribute="_transmit") as mock_transmit,
            patch(target="pywebtransport._driver.driver.mapper.pack_user_event"),
        ):
            driver.send_user_event(handle=1, event=event)

            mock_process.assert_not_called()
            mock_transmit.assert_called_once()

    def test_send_user_event_success(self, driver: EndpointDriver, mock_endpoint: Mock) -> None:
        event = ConnectionClose(request_id=1, error_code=0, reason=None)
        mock_endpoint.handle_user_event.return_value = (abi.CONSUMED, None)

        with (
            patch.object(target=driver, attribute="_process_endpoint_event") as mock_process,
            patch.object(target=driver, attribute="_transmit") as mock_transmit,
            patch(target="pywebtransport._driver.driver.mapper.pack_user_event") as mock_pack,
        ):
            mock_pack.return_value = (abi.CONNECTION_CLOSE, (1, 0, None))
            driver.send_user_event(handle=1, event=event)

            mock_endpoint.handle_user_event.assert_called_once_with(
                handle=1, event=(abi.CONNECTION_CLOSE, (1, 0, None)), now=1000.0
            )
            mock_process.assert_called_once_with(event_tuple=(abi.CONSUMED, None))
            mock_transmit.assert_called_once()

    def test_set_spawn_callback(self, driver: EndpointDriver) -> None:
        cb = Mock()

        driver.set_spawn_callback(callback=cb)

        assert driver._spawn_callback is cb

    def test_transmit_closing_transport(self, driver: EndpointDriver, mock_transport: Mock) -> None:
        mock_transport.is_closing.return_value = True
        driver.connection_made(transport=mock_transport)

        with patch.object(target=driver, attribute="_schedule_timer") as mock_schedule:
            driver._transmit()

            mock_schedule.assert_not_called()

    def test_transmit_exception(self, driver: EndpointDriver, mock_endpoint: Mock, mock_transport: Mock) -> None:
        driver.connection_made(transport=mock_transport)
        mock_endpoint.poll_transmit.side_effect = ValueError("transmit error")

        with patch.object(target=driver, attribute="_schedule_timer") as mock_schedule:
            driver._transmit()

            mock_schedule.assert_called_once()

    def test_transmit_loop_success(self, driver: EndpointDriver, mock_endpoint: Mock, mock_transport: Mock) -> None:
        driver.connection_made(transport=mock_transport)
        mock_endpoint.poll_transmit.side_effect = [
            (abi.TRANSMIT, (("2.2.2.2", 443), b"data1")),
            (abi.CONSUMED, None),
            None,
        ]

        with patch.object(target=driver, attribute="_schedule_timer") as mock_schedule:
            driver._transmit()

            mock_transport.sendto.assert_called_once_with(b"data1", ("2.2.2.2", 443))
            mock_schedule.assert_called_once()

    def test_transmit_no_transport(self, driver: EndpointDriver) -> None:
        with patch.object(target=driver, attribute="_schedule_timer") as mock_schedule:
            driver._transmit()

            mock_schedule.assert_not_called()


class TestFactoryFunctions:

    @pytest.mark.asyncio
    async def test_create_client(self) -> None:
        config = Mock(spec=ClientConfig)
        mock_loop = Mock()
        mock_transport = Mock()
        mock_protocol = Mock()

        async def mock_create(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
            factory = kwargs["protocol_factory"]
            protocol = factory()
            assert isinstance(protocol, EndpointDriver)
            assert protocol._is_client is True
            return mock_transport, mock_protocol

        mock_loop.create_datagram_endpoint = mock_create

        with patch(target="pywebtransport._driver.driver.Endpoint"):
            transport, protocol = await create_client(config=config, loop=mock_loop)

        assert transport is mock_transport
        assert protocol is mock_protocol

    @pytest.mark.asyncio
    async def test_create_server(self) -> None:
        config = Mock(spec=ServerConfig)
        mock_loop = Mock()
        mock_transport = Mock()
        mock_protocol = Mock()

        async def mock_create(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
            assert kwargs["local_addr"] == ("0.0.0.0", 4433)
            factory = kwargs["protocol_factory"]
            protocol = factory()
            assert isinstance(protocol, EndpointDriver)
            assert protocol._is_client is False
            return mock_transport, mock_protocol

        mock_loop.create_datagram_endpoint = mock_create

        with patch(target="pywebtransport._driver.driver.Endpoint"):
            transport, protocol = await create_server(host="0.0.0.0", port=4433, config=config, loop=mock_loop)

        assert transport is mock_transport
        assert protocol is mock_protocol
