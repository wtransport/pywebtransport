import asyncio
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from aioquic.quic.connection import QuicConnection
from pytest_mock import MockerFixture

from pywebtransport import ClientConfig, ErrorCodes
from pywebtransport._adapter.base import WebTransportCommonProtocol
from pywebtransport._protocol.events import (
    CloseQuicConnection,
    CreateH3Session,
    CreateQuicStream,
    Effect,
    EmitConnectionEvent,
    EmitSessionEvent,
    EmitStreamEvent,
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalFailH3Session,
    InternalFailQuicStream,
    InternalReturnStreamData,
    LogH3Frame,
    NotifyRequestDone,
    NotifyRequestFailed,
    ProcessProtocolEvent,
    RescheduleQuicTimer,
    ResetQuicStream,
    SendH3Capsule,
    SendH3Datagram,
    SendH3Goaway,
    SendH3Headers,
    SendQuicData,
    SendQuicDatagram,
    StopQuicStream,
    TransportConnectionTerminated,
    TransportHandshakeCompleted,
    TransportQuicTimerFired,
    TriggerQuicTimer,
)
from pywebtransport.types import EventType


class TestWebTransportCommonProtocol:

    @pytest.fixture
    def mock_config(self) -> ClientConfig:
        config = ClientConfig()
        config.resource_cleanup_interval = 1.0
        config.pending_event_ttl = 1.0
        return config

    @pytest.fixture
    def mock_engine_class(self, mocker: MockerFixture) -> MagicMock:
        return cast(MagicMock, mocker.patch(target="pywebtransport._adapter.base.WebTransportEngine", autospec=True))

    @pytest.fixture
    def mock_loop(self, mocker: MockerFixture) -> MagicMock:
        loop = mocker.Mock(spec=asyncio.AbstractEventLoop)
        loop.time.return_value = 1000.0
        loop.create_future.side_effect = lambda: asyncio.Future(loop=loop)
        mocker.patch(target="asyncio.get_running_loop", return_value=loop)
        return cast(MagicMock, loop)

    @pytest.fixture
    def mock_quic(self, mocker: MockerFixture) -> MagicMock:
        quic = mocker.Mock(spec=QuicConnection)
        quic.host_cid = b"test_cid"
        quic._close_event = None
        quic._quic_logger = MagicMock()
        quic.configuration = mocker.Mock()
        quic.configuration.is_client = True
        quic.get_timer.return_value = 1100.0
        quic.datagrams_to_send.return_value = []
        quic.next_event.return_value = None
        return cast(MagicMock, quic)

    @pytest.fixture
    def protocol(
        self, mock_quic: MagicMock, mock_config: ClientConfig, mock_loop: MagicMock, mock_engine_class: MagicMock
    ) -> WebTransportCommonProtocol:
        return WebTransportCommonProtocol(quic=mock_quic, config=mock_config, is_client=True, loop=mock_loop)

    def test_allocate_stream_id(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.get_next_available_stream_id.return_value = 4

        result = protocol._allocate_stream_id(is_unidirectional=False)

        assert result == 4
        mock_quic.send_stream_data.assert_called_once_with(stream_id=4, data=b"", end_stream=False)

    def test_close_connection_already_closing(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic._close_event = object()

        protocol.close_connection(error_code=ErrorCodes.NO_ERROR)

        mock_quic.close.assert_not_called()

    def test_close_connection_with_reason(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        protocol.close_connection(error_code=ErrorCodes.NO_ERROR, reason_phrase="graceful")

        mock_quic.close.assert_called_once_with(error_code=ErrorCodes.NO_ERROR, reason_phrase="graceful")

    def test_connection_lost_already_closing(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic._close_event = object()
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)

        protocol.connection_lost(exc=None)

        handle_event_mock.assert_not_called()

    def test_connection_lost_full(self, protocol: WebTransportCommonProtocol) -> None:
        timer_handle = MagicMock()
        protocol._timer_handle = timer_handle
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol.connection_lost(exc=None)

        timer_handle.cancel.assert_called_once()
        event = handle_event_mock.call_args.kwargs["event"]
        assert isinstance(event, TransportConnectionTerminated)
        assert event.error_code == ErrorCodes.NO_ERROR

    def test_connection_lost_with_exception(self, protocol: WebTransportCommonProtocol) -> None:
        exc = RuntimeError("network error")
        protocol._setup_maintenance_timers()
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)

        protocol.connection_lost(exc=exc)

        event = handle_event_mock.call_args_list[0].kwargs["event"]
        assert isinstance(event, TransportConnectionTerminated)
        assert event.error_code == ErrorCodes.INTERNAL_ERROR
        assert protocol._resource_gc_timer is None

    def test_connection_made_setup_timers(self, protocol: WebTransportCommonProtocol, mock_loop: MagicMock) -> None:
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = False
        mock_transport.sendto = MagicMock()

        protocol.connection_made(transport=mock_transport)

        assert mock_loop.call_later.call_count == 2

    def test_execute_effects_loop(self, protocol: WebTransportCommonProtocol, mocker: MockerFixture) -> None:
        effect = SendQuicData(stream_id=0, data=b"data", end_stream=False)
        spy_process = mocker.spy(protocol, "_process_single_effect")

        protocol._execute_effects(effects=[effect])

        spy_process.assert_called_once_with(effect=effect)
        assert len(protocol._pending_effects) == 0

    def test_execute_effects_reentrancy(self, protocol: WebTransportCommonProtocol, mocker: MockerFixture) -> None:
        effect = SendQuicData(stream_id=0, data=b"data", end_stream=False)
        protocol._is_processing_effects = True
        spy_process = mocker.spy(protocol, "_process_single_effect")

        protocol._execute_effects(effects=[effect])

        assert len(protocol._pending_effects) == 1
        spy_process.assert_not_called()

    def test_get_next_available_stream_id(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.get_next_available_stream_id.return_value = 8

        result = protocol.get_next_available_stream_id(is_unidirectional=True)

        assert result == 8
        mock_quic.get_next_available_stream_id.assert_called_once_with(is_unidirectional=True)

    def test_get_server_name(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.configuration.server_name = "test.com"

        assert protocol.get_server_name() == "test.com"

    def test_handle_early_event_cleanup_timer_no_ttl(
        self, protocol: WebTransportCommonProtocol, mock_loop: MagicMock
    ) -> None:
        protocol._config.pending_event_ttl = 0
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol._handle_early_event_cleanup_timer()

        assert mock_loop.call_later.call_count == 0

    def test_handle_early_event_cleanup_timer_with_ttl(
        self, protocol: WebTransportCommonProtocol, mock_loop: MagicMock
    ) -> None:
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol._handle_early_event_cleanup_timer()

        event = handle_event_mock.call_args_list[0].kwargs["event"]
        assert isinstance(event, InternalCleanupEarlyEvents)
        mock_loop.call_later.assert_called_once()

    def test_handle_resource_gc_timer_no_interval(
        self, protocol: WebTransportCommonProtocol, mock_loop: MagicMock
    ) -> None:
        protocol._config.resource_cleanup_interval = 0
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol._handle_resource_gc_timer()

        assert mock_loop.call_later.call_count == 0

    def test_handle_resource_gc_timer_with_interval(
        self, protocol: WebTransportCommonProtocol, mock_loop: MagicMock
    ) -> None:
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol._handle_resource_gc_timer()

        event = handle_event_mock.call_args_list[0].kwargs["event"]
        assert isinstance(event, InternalCleanupResources)
        mock_loop.call_later.assert_called_once()

    def test_handle_timer_fired(self, protocol: WebTransportCommonProtocol) -> None:
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol._handle_timer()

        event = handle_event_mock.call_args_list[0].kwargs["event"]
        assert isinstance(event, TransportQuicTimerFired)

    def test_handle_timer_now_with_events(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_event = MagicMock()
        mock_quic.next_event.side_effect = [mock_event, None]
        spy_received = mocker.spy(protocol, "quic_event_received")

        protocol.handle_timer_now()

        mock_quic.handle_timer.assert_called_once_with(now=1000.0)
        spy_received.assert_called_once_with(event=mock_event)

    def test_log_event_no_logger(self, protocol: WebTransportCommonProtocol) -> None:
        protocol._quic_logger = None

        protocol.log_event(category="cat", event="evt", data={})

        assert True

    def test_log_event_with_logger(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_logger = MagicMock()
        protocol._quic_logger = mock_logger

        protocol.log_event(category="cat", event="evt", data={"k": "v"})

        mock_logger.log_event.assert_called_once_with(category="cat", event="evt", data={"k": "v"})

    def test_on_handshake_completed_logic(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.get_next_available_stream_id.side_effect = [1, 2, 3]
        mock_quic._remote_max_datagram_frame_size = 1500
        cast(MagicMock, protocol._engine.initialize_h3_transport).return_value = []
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)

        protocol._on_handshake_completed()

        assert protocol._quic_logger is not None
        assert isinstance(handle_event_mock.call_args_list[0].kwargs["event"], TransportHandshakeCompleted)
        cast(MagicMock, protocol._engine.initialize_h3_transport).assert_called_once_with(
            control_id=1, encoder_id=2, decoder_id=3
        )

    def test_on_handshake_completed_no_datagram_params(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock
    ) -> None:
        mock_quic.get_next_available_stream_id.side_effect = [1, 2, 3]
        del mock_quic._remote_max_datagram_frame_size
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)

        protocol._on_handshake_completed()

        assert handle_event_mock.call_count == 1

    def test_process_single_effect_close_connection(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock
    ) -> None:
        effect = CloseQuicConnection(error_code=ErrorCodes.INTERNAL_ERROR, reason="overload")

        protocol._process_single_effect(effect=effect)

        mock_quic.close.assert_called_once_with(error_code=ErrorCodes.INTERNAL_ERROR, reason_phrase="overload")

    def test_process_single_effect_create_h3_session(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock
    ) -> None:
        mock_quic.get_next_available_stream_id.return_value = 0
        effect = CreateH3Session(request_id=1, path="/test", headers={})

        protocol._process_single_effect(effect=effect)

        assert mock_quic.get_next_available_stream_id.called
        assert cast(MagicMock, protocol._engine.encode_session_request).called

    def test_process_single_effect_create_h3_session_fail(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock
    ) -> None:
        mock_quic.get_next_available_stream_id.side_effect = Exception("fail")
        effect = CreateH3Session(request_id=1, path="/test", headers={})
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)

        protocol._process_single_effect(effect=effect)

        event = handle_event_mock.call_args.kwargs["event"]
        assert isinstance(event, InternalFailH3Session)

    def test_process_single_effect_create_quic_stream(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock
    ) -> None:
        mock_quic.get_next_available_stream_id.return_value = 4
        effect = CreateQuicStream(request_id=1, session_id=0, is_unidirectional=False)

        protocol._process_single_effect(effect=effect)

        assert mock_quic.get_next_available_stream_id.called
        assert cast(MagicMock, protocol._engine.encode_stream_creation).called

    def test_process_single_effect_create_quic_stream_fail(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock
    ) -> None:
        mock_quic.get_next_available_stream_id.side_effect = Exception("fail")
        effect = CreateQuicStream(request_id=1, session_id=0, is_unidirectional=False)
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)

        protocol._process_single_effect(effect=effect)

        assert handle_event_mock.called

    def test_process_single_effect_emit_events(self, protocol: WebTransportCommonProtocol) -> None:
        cb = MagicMock()
        protocol.set_status_callback(callback=cb)
        effects: list[Effect] = [
            EmitConnectionEvent(event_type=cast(EventType, "conn"), data={}),
            EmitSessionEvent(event_type=cast(EventType, "sess"), session_id=0, data={}),
            EmitStreamEvent(event_type=cast(EventType, "stream"), stream_id=0, data={}),
            cast(Effect, InternalReturnStreamData(stream_id=0, data=b"data")),
        ]

        for effect in effects:
            protocol._process_single_effect(effect=effect)

        assert cb.call_count == 3
        assert cast(MagicMock, protocol._engine.handle_event).called

    def test_process_single_effect_emit_events_no_callback(self, protocol: WebTransportCommonProtocol) -> None:
        protocol.set_status_callback(callback=cast(Any, None))
        effects: list[Effect] = [
            EmitConnectionEvent(event_type=cast(EventType, "conn"), data={}),
            EmitSessionEvent(event_type=cast(EventType, "sess"), session_id=0, data={}),
            EmitStreamEvent(event_type=cast(EventType, "stream"), stream_id=0, data={}),
        ]

        for effect in effects:
            protocol._process_single_effect(effect=effect)

        assert True

    def test_process_single_effect_h3_actions(self, protocol: WebTransportCommonProtocol) -> None:
        effects: list[Effect] = [
            SendH3Headers(stream_id=0, status=200, end_stream=False),
            SendH3Headers(stream_id=0, status=200, end_stream=True),
            SendH3Capsule(stream_id=0, capsule_type=0, capsule_data=b"cap", end_stream=False),
            SendH3Datagram(stream_id=0, data=b"dg"),
            SendH3Goaway(),
        ]

        for effect in effects:
            protocol._process_single_effect(effect=effect)

        assert cast(MagicMock, protocol._engine.encode_headers).called
        assert cast(MagicMock, protocol._engine.encode_capsule).called
        assert cast(MagicMock, protocol._engine.encode_datagram).called
        assert cast(MagicMock, protocol._engine.encode_goaway).called

    def test_process_single_effect_log_and_protocol(self, protocol: WebTransportCommonProtocol) -> None:
        mock_evt = MagicMock()
        effects: list[Effect] = [
            LogH3Frame(category="h3", event="frame", data={}),
            ProcessProtocolEvent(event=mock_evt),
        ]
        cast(MagicMock, protocol._engine.handle_event).return_value = []

        for effect in effects:
            protocol._process_single_effect(effect=effect)

        assert len(protocol._pending_effects) == 0

    def test_process_single_effect_quic_io(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        effects: list[Effect] = [
            SendQuicData(stream_id=0, data=b"data", end_stream=False),
            SendQuicDatagram(data=b"dg"),
            ResetQuicStream(stream_id=0, error_code=0),
            StopQuicStream(stream_id=0, error_code=0),
        ]

        for effect in effects:
            protocol._process_single_effect(effect=effect)

        assert mock_quic.send_stream_data.called
        assert mock_quic.send_datagram_frame.called
        assert mock_quic.reset_stream.called
        assert mock_quic.stop_stream.called

    def test_process_single_effect_requests(self, protocol: WebTransportCommonProtocol) -> None:
        rid, fut = protocol.create_request()
        effects: list[Effect] = [
            NotifyRequestDone(request_id=rid, result="done"),
            NotifyRequestFailed(request_id=rid + 1, exception=RuntimeError()),
            cast(Effect, InternalFailH3Session(request_id=rid, exception=RuntimeError())),
            cast(
                Effect,
                InternalFailQuicStream(request_id=rid, session_id=0, is_unidirectional=False, exception=RuntimeError()),
            ),
        ]

        for effect in effects:
            protocol._process_single_effect(effect=effect)

        assert fut.result() == "done"

    def test_process_single_effect_timers(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock, mock_loop: MagicMock
    ) -> None:
        effects: list[Effect] = [RescheduleQuicTimer(), TriggerQuicTimer()]

        for effect in effects:
            protocol._process_single_effect(effect=effect)

        assert mock_loop.call_at.called
        assert cast(MagicMock, mock_quic.handle_timer).called

    def test_quic_event_received_handshake(self, protocol: WebTransportCommonProtocol, mocker: MockerFixture) -> None:
        spy_handshake = mocker.spy(protocol, "_on_handshake_completed")
        from aioquic.quic.events import HandshakeCompleted

        event = HandshakeCompleted(alpn_protocol="h3", early_data_accepted=False, session_resumed=False)

        protocol.quic_event_received(event=event)

        spy_handshake.assert_called_once()

    def test_quic_event_received_mapping(self, protocol: WebTransportCommonProtocol) -> None:
        from aioquic.quic.events import DatagramFrameReceived, StreamDataReceived, StreamReset

        events = [
            DatagramFrameReceived(data=b"dg"),
            StreamDataReceived(data=b"data", end_stream=False, stream_id=0),
            StreamReset(error_code=0, stream_id=0),
        ]
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        for event in events:
            protocol.quic_event_received(event=event)

        assert handle_event_mock.call_count == 3

    def test_quic_event_received_terminated(self, protocol: WebTransportCommonProtocol) -> None:
        from aioquic.quic.events import ConnectionTerminated

        event = ConnectionTerminated(error_code=0, frame_type=0x1D, reason_phrase="done")
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol.quic_event_received(event=event)

        ev = handle_event_mock.call_args.kwargs["event"]
        assert isinstance(ev, TransportHandshakeCompleted) or isinstance(ev, TransportConnectionTerminated)

    def test_quic_event_received_unknown(self, protocol: WebTransportCommonProtocol) -> None:
        event = MagicMock()
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol.quic_event_received(event=event)

        assert handle_event_mock.call_count == 0

    def test_reset_stream_closing(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic._close_event = object()

        protocol.reset_stream(stream_id=0, error_code=0)

        mock_quic.reset_stream.assert_not_called()

    def test_reset_stream_io_conflict(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.reset_stream.side_effect = ValueError("state error")

        protocol.reset_stream(stream_id=0, error_code=0)

        assert mock_quic.reset_stream.called

    def test_schedule_timer_logic(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock, mock_loop: MagicMock
    ) -> None:
        old_handle = MagicMock()
        protocol._timer_handle = old_handle

        protocol.schedule_timer_now()

        old_handle.cancel.assert_called_once()
        assert mock_loop.call_at.called

    def test_schedule_timer_none(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock, mock_loop: MagicMock
    ) -> None:
        mock_quic.get_timer.return_value = None

        protocol.schedule_timer_now()

        assert mock_loop.call_at.call_count == 0

    def test_send_datagram_frame_closing(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic._close_event = object()

        protocol.send_datagram_frame(data=b"dg")

        mock_quic.send_datagram_frame.assert_not_called()

    def test_send_datagram_frame_list_input(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        protocol.send_datagram_frame(data=[b"a", b"b"])

        mock_quic.send_datagram_frame.assert_called_with(data=b"ab")

    def test_send_event(self, protocol: WebTransportCommonProtocol) -> None:
        evt = MagicMock()
        handle_event_mock = cast(MagicMock, protocol._engine.handle_event)
        handle_event_mock.return_value = []

        protocol.send_event(event=evt)

        handle_event_mock.assert_called_once()

    def test_send_stream_data_closing(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic._close_event = object()

        protocol.send_stream_data(stream_id=0, data=b"data")

        mock_quic.send_stream_data.assert_not_called()

    def test_send_stream_data_io_conflict(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.send_stream_data.side_effect = AssertionError("io fail")

        protocol.send_stream_data(stream_id=0, data=b"data")

        assert mock_quic.send_stream_data.called

    def test_send_stream_data_with_fin_checks(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic._close_event = object()

        protocol.send_stream_data(stream_id=0, data=b"", end_stream=True)
        mock_quic.send_stream_data.assert_called_with(stream_id=0, data=b"", end_stream=True)

        protocol.send_stream_data(stream_id=0, data=b"fail", end_stream=False)
        assert mock_quic.send_stream_data.call_count == 1

    def test_setup_maintenance_timers_disabled(
        self, protocol: WebTransportCommonProtocol, mock_loop: MagicMock
    ) -> None:
        protocol._config.resource_cleanup_interval = 0
        protocol._config.pending_event_ttl = 0

        protocol._setup_maintenance_timers()

        assert mock_loop.call_later.call_count == 0

    def test_stop_stream_io_conflict(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.stop_stream.side_effect = ValueError("io fail")

        protocol.stop_stream(stream_id=0, error_code=0)

        assert mock_quic.stop_stream.called

    def test_transmit_client_logic(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.configuration.is_client = True
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = False
        mock_transport.sendto = MagicMock()
        protocol.connection_made(transport=mock_transport)
        mock_quic.datagrams_to_send.return_value = [(b"p1", None)]

        protocol.transmit()

        mock_transport.sendto.assert_called_with(b"p1")

    def test_transmit_generic_exception_handling(
        self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock
    ) -> None:
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = False
        mock_transport.sendto = MagicMock()
        protocol.connection_made(transport=mock_transport)
        mock_quic.datagrams_to_send.return_value = [(b"p1", ("1.1.1.1", 80))]
        mock_transport.sendto.side_effect = RuntimeError("generic failure")

        protocol.transmit()

        assert mock_transport.sendto.called

    def test_transmit_os_error_handling(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = False
        mock_transport.sendto = MagicMock()
        protocol.connection_made(transport=mock_transport)
        mock_quic.datagrams_to_send.return_value = [(b"p1", ("1.1.1.1", 80))]
        mock_transport.sendto.side_effect = OSError("network down")

        protocol.transmit()

        assert mock_transport.sendto.called

    def test_transmit_server_logic(self, protocol: WebTransportCommonProtocol, mock_quic: MagicMock) -> None:
        mock_quic.configuration.is_client = False
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = False
        mock_transport.sendto = MagicMock()
        protocol.connection_made(transport=mock_transport)
        mock_quic.datagrams_to_send.return_value = [(b"p1", ("1.1.1.1", 80))]

        protocol.transmit()

        mock_transport.sendto.assert_called_with(b"p1", ("1.1.1.1", 80))
