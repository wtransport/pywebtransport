"""Unit tests for the pywebtransport._protocol.events module."""

from typing import Any

import pytest

from pywebtransport._protocol.events import (
    CapsuleReceived,
    CleanupH3Stream,
    CloseQuicConnection,
    ConnectionClose,
    ConnectStreamClosed,
    CreateH3Session,
    CreateQuicStream,
    DatagramReceived,
    Effect,
    EmitConnectionEvent,
    EmitSessionEvent,
    EmitStreamEvent,
    GoawayReceived,
    H3Event,
    HeadersReceived,
    InternalBindH3Session,
    InternalBindQuicStream,
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalFailH3Session,
    InternalFailQuicStream,
    InternalReturnStreamData,
    LogH3Frame,
    NotifyRequestDone,
    NotifyRequestFailed,
    ProcessProtocolEvent,
    ProtocolEvent,
    RescheduleQuicTimer,
    ResetQuicStream,
    SendH3Capsule,
    SendH3Datagram,
    SendH3Goaway,
    SendH3Headers,
    SendQuicData,
    SendQuicDatagram,
    SettingsReceived,
    StopQuicStream,
    TransportConnectionTerminated,
    TransportDatagramFrameReceived,
    TransportHandshakeCompleted,
    TransportQuicParametersReceived,
    TransportQuicTimerFired,
    TransportStreamDataReceived,
    TransportStreamReset,
    TriggerQuicTimer,
    UserAcceptSession,
    UserCloseSession,
    UserConnectionGracefulClose,
    UserCreateSession,
    UserCreateStream,
    UserEvent,
    UserGetConnectionDiagnostics,
    UserGetSessionDiagnostics,
    UserGetStreamDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserRejectSession,
    UserResetStream,
    UserSendDatagram,
    UserSendStreamData,
    UserStopStream,
    UserStreamRead,
    WebTransportStreamDataReceived,
)


class TestEffects:

    @pytest.mark.parametrize(
        "effect_class, kwargs, expected_attrs",
        [
            (CleanupH3Stream, {"stream_id": 4}, {"stream_id": 4}),
            (
                CloseQuicConnection,
                {"error_code": 100, "reason": "test close"},
                {"error_code": 100, "reason": "test close"},
            ),
            (
                CreateH3Session,
                {"request_id": 1, "path": "/test", "headers": {b":path": b"/test"}},
                {"request_id": 1, "path": "/test", "headers": {b":path": b"/test"}},
            ),
            (
                CreateQuicStream,
                {"request_id": 1, "session_id": 1, "is_unidirectional": True},
                {"request_id": 1, "session_id": 1, "is_unidirectional": True},
            ),
            (
                EmitConnectionEvent,
                {"event_type": "connected", "data": {"key": "value"}},
                {"event_type": "connected", "data": {"key": "value"}},
            ),
            (
                EmitSessionEvent,
                {"session_id": 1, "event_type": "opened", "data": {}},
                {"session_id": 1, "event_type": "opened", "data": {}},
            ),
            (
                EmitStreamEvent,
                {"stream_id": 4, "event_type": "data_received", "data": {"len": 10}},
                {"stream_id": 4, "event_type": "data_received", "data": {"len": 10}},
            ),
            (
                LogH3Frame,
                {"category": "test", "event": "frame", "data": {"id": 1}},
                {"category": "test", "event": "frame", "data": {"id": 1}},
            ),
            (NotifyRequestDone, {"request_id": 1, "result": "success"}, {"request_id": 1, "result": "success"}),
            (
                NotifyRequestFailed,
                {"request_id": 1, "exception": ValueError("test")},
                {"request_id": 1, "exception": ValueError("test")},
            ),
            (ProcessProtocolEvent, {"event": TransportHandshakeCompleted()}, {"event": TransportHandshakeCompleted()}),
            (RescheduleQuicTimer, {}, {}),
            (ResetQuicStream, {"stream_id": 4, "error_code": 100}, {"stream_id": 4, "error_code": 100}),
            (
                SendH3Capsule,
                {"stream_id": 1, "capsule_type": 0x01, "capsule_data": b"cap", "end_stream": False},
                {"stream_id": 1, "capsule_type": 0x01, "capsule_data": b"cap", "end_stream": False},
            ),
            (SendH3Datagram, {"stream_id": 1, "data": b"dgram"}, {"stream_id": 1, "data": b"dgram"}),
            (SendH3Goaway, {}, {}),
            (
                SendH3Headers,
                {"stream_id": 1, "status": 404, "end_stream": True},
                {"stream_id": 1, "status": 404, "end_stream": True},
            ),
            (
                SendQuicData,
                {"stream_id": 4, "data": b"data", "end_stream": False},
                {"stream_id": 4, "data": b"data", "end_stream": False},
            ),
            (SendQuicDatagram, {"data": b"dgram"}, {"data": b"dgram"}),
            (StopQuicStream, {"stream_id": 4, "error_code": 100}, {"stream_id": 4, "error_code": 100}),
            (TriggerQuicTimer, {}, {}),
        ],
    )
    def test_instantiation(
        self, effect_class: type[Effect], kwargs: dict[str, Any], expected_attrs: dict[str, Any]
    ) -> None:
        if "exception" in kwargs and isinstance(kwargs["exception"], ValueError):
            effect = effect_class(**kwargs)
            assert isinstance(effect, Effect)
            assert getattr(effect, "request_id") == expected_attrs["request_id"]
            assert isinstance(getattr(effect, "exception"), ValueError)
            return

        if "event" in kwargs and isinstance(kwargs["event"], ProtocolEvent):
            effect = effect_class(**kwargs)
            assert isinstance(effect, Effect)
            assert isinstance(getattr(effect, "event"), ProtocolEvent)
            return

        effect = effect_class(**kwargs)

        assert isinstance(effect, Effect)
        for attr, expected_value in expected_attrs.items():
            assert getattr(effect, attr) == expected_value


class TestH3Events:

    @pytest.mark.parametrize(
        "event_class, kwargs, expected_attrs",
        [
            (
                CapsuleReceived,
                {"capsule_data": b"capsule", "capsule_type": 0x01, "stream_id": 1},
                {"capsule_data": b"capsule", "capsule_type": 0x01, "stream_id": 1},
            ),
            (ConnectStreamClosed, {"stream_id": 1}, {"stream_id": 1}),
            (DatagramReceived, {"data": b"datagram", "stream_id": 1}, {"data": b"datagram", "stream_id": 1}),
            (GoawayReceived, {}, {}),
            (
                HeadersReceived,
                {"headers": {b":status": b"200"}, "stream_id": 1, "stream_ended": False},
                {"headers": {b":status": b"200"}, "stream_id": 1, "stream_ended": False},
            ),
            (SettingsReceived, {"settings": {0x01: 0x01}}, {"settings": {0x01: 0x01}}),
            (
                WebTransportStreamDataReceived,
                {"data": b"data", "session_id": 1, "stream_id": 4, "stream_ended": True},
                {"data": b"data", "session_id": 1, "stream_id": 4, "stream_ended": True},
            ),
        ],
    )
    def test_instantiation(
        self, event_class: type[H3Event], kwargs: dict[str, Any], expected_attrs: dict[str, Any]
    ) -> None:
        event = event_class(**kwargs)

        assert isinstance(event, H3Event)
        for attr, expected_value in expected_attrs.items():
            assert getattr(event, attr) == expected_value


class TestInternalProtocolEvents:

    @pytest.mark.parametrize(
        "event_class, kwargs, expected_attrs",
        [
            (InternalBindH3Session, {"request_id": 1, "stream_id": 2}, {"request_id": 1, "stream_id": 2}),
            (
                InternalBindQuicStream,
                {"request_id": 1, "stream_id": 1, "session_id": 1, "is_unidirectional": True},
                {"request_id": 1, "stream_id": 1, "session_id": 1, "is_unidirectional": True},
            ),
            (InternalCleanupEarlyEvents, {}, {}),
            (InternalCleanupResources, {}, {}),
            (
                InternalFailH3Session,
                {"request_id": 1, "exception": ValueError("test")},
                {"request_id": 1, "exception": ValueError("test")},
            ),
            (
                InternalFailQuicStream,
                {"request_id": 1, "session_id": 1, "is_unidirectional": True, "exception": ValueError("test")},
                {"request_id": 1, "session_id": 1, "is_unidirectional": True, "exception": ValueError("test")},
            ),
            (InternalReturnStreamData, {"stream_id": 1, "data": b"returned"}, {"stream_id": 1, "data": b"returned"}),
            (
                TransportConnectionTerminated,
                {"error_code": 100, "reason_phrase": "test reason"},
                {"error_code": 100, "reason_phrase": "test reason"},
            ),
            (TransportDatagramFrameReceived, {"data": b"datagram_data"}, {"data": b"datagram_data"}),
            (TransportHandshakeCompleted, {}, {}),
            (
                TransportQuicParametersReceived,
                {"remote_max_datagram_frame_size": 1500},
                {"remote_max_datagram_frame_size": 1500},
            ),
            (TransportQuicTimerFired, {}, {}),
            (
                TransportStreamDataReceived,
                {"data": b"stream_data", "end_stream": True, "stream_id": 4},
                {"data": b"stream_data", "end_stream": True, "stream_id": 4},
            ),
            (TransportStreamReset, {"error_code": 101, "stream_id": 4}, {"error_code": 101, "stream_id": 4}),
        ],
    )
    def test_instantiation(
        self, event_class: type[ProtocolEvent], kwargs: dict[str, Any], expected_attrs: dict[str, Any]
    ) -> None:
        if "exception" in kwargs and isinstance(kwargs["exception"], ValueError):
            event = event_class(**kwargs)
            assert isinstance(event, ProtocolEvent)
            assert getattr(event, "request_id") == expected_attrs["request_id"]
            assert isinstance(getattr(event, "exception"), ValueError)
            return

        event = event_class(**kwargs)

        assert isinstance(event, ProtocolEvent)
        for attr, expected_value in expected_attrs.items():
            assert getattr(event, attr) == expected_value


class TestUserEvents:

    @pytest.mark.parametrize(
        "event_class, kwargs, expected_attrs",
        [
            (
                ConnectionClose,
                {"request_id": 1, "error_code": 100, "reason": "closing"},
                {"request_id": 1, "error_code": 100, "reason": "closing"},
            ),
            (UserAcceptSession, {"request_id": 1, "session_id": 1}, {"request_id": 1, "session_id": 1}),
            (
                UserCloseSession,
                {"request_id": 1, "session_id": 1, "error_code": 100, "reason": "test"},
                {"request_id": 1, "session_id": 1, "error_code": 100, "reason": "test"},
            ),
            (UserConnectionGracefulClose, {"request_id": 1}, {"request_id": 1}),
            (
                UserCreateSession,
                {"request_id": 1, "path": "/test", "headers": {b":path": b"/test"}},
                {"request_id": 1, "path": "/test", "headers": {b":path": b"/test"}},
            ),
            (
                UserCreateStream,
                {"request_id": 1, "session_id": 1, "is_unidirectional": True},
                {"request_id": 1, "session_id": 1, "is_unidirectional": True},
            ),
            (UserGetConnectionDiagnostics, {"request_id": 1}, {"request_id": 1}),
            (UserGetSessionDiagnostics, {"request_id": 1, "session_id": 1}, {"request_id": 1, "session_id": 1}),
            (UserGetStreamDiagnostics, {"request_id": 1, "stream_id": 4}, {"request_id": 1, "stream_id": 4}),
            (
                UserGrantDataCredit,
                {"request_id": 1, "session_id": 1, "max_data": 1024},
                {"request_id": 1, "session_id": 1, "max_data": 1024},
            ),
            (
                UserGrantStreamsCredit,
                {"request_id": 1, "session_id": 1, "max_streams": 10, "is_unidirectional": False},
                {"request_id": 1, "session_id": 1, "max_streams": 10, "is_unidirectional": False},
            ),
            (
                UserRejectSession,
                {"request_id": 1, "session_id": 1, "status_code": 404},
                {"request_id": 1, "session_id": 1, "status_code": 404},
            ),
            (
                UserResetStream,
                {"request_id": 1, "stream_id": 4, "error_code": 100},
                {"request_id": 1, "stream_id": 4, "error_code": 100},
            ),
            (
                UserSendDatagram,
                {"request_id": 1, "session_id": 1, "data": b"datagram"},
                {"request_id": 1, "session_id": 1, "data": b"datagram"},
            ),
            (
                UserSendStreamData,
                {"request_id": 1, "stream_id": 4, "data": b"data", "end_stream": True},
                {"request_id": 1, "stream_id": 4, "data": b"data", "end_stream": True},
            ),
            (
                UserStopStream,
                {"request_id": 1, "stream_id": 4, "error_code": 100},
                {"request_id": 1, "stream_id": 4, "error_code": 100},
            ),
            (
                UserStreamRead,
                {"request_id": 1, "stream_id": 4, "max_bytes": 1024},
                {"request_id": 1, "stream_id": 4, "max_bytes": 1024},
            ),
        ],
    )
    def test_instantiation(
        self, event_class: type[UserEvent[Any]], kwargs: dict[str, Any], expected_attrs: dict[str, Any]
    ) -> None:
        event = event_class(**kwargs)

        assert isinstance(event, UserEvent)
        for attr, expected_value in expected_attrs.items():
            assert getattr(event, attr) == expected_value
