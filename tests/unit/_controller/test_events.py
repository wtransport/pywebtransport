"""Unit tests for the pywebtransport._controller.events module."""

from typing import Any

import pytest

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
    UserAcceptSession,
    UserCloseConnection,
    UserCloseConnectionGracefully,
    UserCloseSession,
    UserCreateSession,
    UserCreateSessionOptimistic,
    UserCreateStream,
    UserDiagnoseConnection,
    UserDiagnoseSession,
    UserDiagnoseStream,
    UserExportKeyingMaterial,
    UserReadDatagram,
    UserReadStream,
    UserRejectSession,
    UserResetStream,
    UserSendDatagram,
    UserStopSending,
    UserWriteStream,
)
from pywebtransport.types import EventType, StreamDirection


class TestEffect:

    @pytest.mark.parametrize(
        argnames="event_class, kwargs",
        argvalues=[
            (
                EmitConnectionEvent,
                {
                    "connection_handle": 1,
                    "event_type": EventType.CONNECTION_CLOSED,
                    "error_code": 100,
                    "reason": "reason",
                },
            ),
            (
                EmitSessionEvent,
                {
                    "session_id": 10,
                    "event_type": EventType.SESSION_READY,
                    "path": "/p",
                    "headers": {"k": "v"},
                    "wt_available_protocols": ["h3"],
                    "wt_protocol": "h3",
                    "data": b"d",
                    "is_unidirectional": True,
                    "max_data": 100,
                    "max_streams": 5,
                    "ready_at": 0.5,
                    "status_code": 403,
                    "error_code": 0,
                    "reason": "ok",
                },
            ),
            (
                EmitStreamEvent,
                {
                    "stream_id": 20,
                    "event_type": EventType.STREAM_OPENED,
                    "session_id": 10,
                    "direction": StreamDirection.BIDIRECTIONAL,
                    "is_peer_initiated": True,
                    "error_code": 1,
                },
            ),
            (NotifyRequestDone, {"request_id": 5, "result": "result_value"}),
        ],
    )
    def test_instantiation(self, event_class: type[Any], kwargs: dict[str, Any]) -> None:
        event = event_class(**kwargs)

        for attribute, expected_value in kwargs.items():
            assert getattr(event, attribute) == expected_value

    def test_instantiation_notify_request_failed(self) -> None:
        exception = ValueError("boom")

        event = NotifyRequestFailed(request_id=6, exception=exception)

        assert event.request_id == 6
        assert event.exception is exception


class TestProtocolEvent:

    @pytest.mark.parametrize(
        argnames="event_class, kwargs, expected_attributes",
        argvalues=[
            (
                UserAcceptSession,
                {"request_id": 1, "session_id": 1, "headers": {}},
                {"request_id": 1, "session_id": 1, "headers": {}, "wt_protocol": None},
            ),
            (
                UserAcceptSession,
                {"request_id": 1, "session_id": 1, "headers": {b"server": b"pywebtransport"}, "wt_protocol": "h3"},
                {"request_id": 1, "session_id": 1, "headers": {b"server": b"pywebtransport"}, "wt_protocol": "h3"},
            ),
            (
                UserCloseConnection,
                {"request_id": 1, "error_code": 100, "reason": "closing"},
                {"request_id": 1, "error_code": 100, "reason": "closing"},
            ),
            (UserCloseConnectionGracefully, {"request_id": 1}, {"request_id": 1}),
            (
                UserCloseSession,
                {"request_id": 1, "session_id": 1, "error_code": 100, "reason": "test"},
                {"request_id": 1, "session_id": 1, "error_code": 100, "reason": "test"},
            ),
            (
                UserCreateSession,
                {"request_id": 1, "authority": "localhost", "path": "/test", "headers": {b":path": b"/test"}},
                {
                    "request_id": 1,
                    "authority": "localhost",
                    "path": "/test",
                    "headers": {b":path": b"/test"},
                    "wt_available_protocols": None,
                },
            ),
            (
                UserCreateSession,
                {
                    "request_id": 1,
                    "authority": "localhost",
                    "path": "/test",
                    "headers": {},
                    "wt_available_protocols": ["p1", "p2"],
                },
                {
                    "request_id": 1,
                    "authority": "localhost",
                    "path": "/test",
                    "headers": {},
                    "wt_available_protocols": ["p1", "p2"],
                },
            ),
            (
                UserCreateSessionOptimistic,
                {"request_id": 1, "authority": "localhost", "path": "/test", "headers": {b":path": b"/test"}},
                {
                    "request_id": 1,
                    "authority": "localhost",
                    "path": "/test",
                    "headers": {b":path": b"/test"},
                    "wt_available_protocols": None,
                },
            ),
            (
                UserCreateSessionOptimistic,
                {
                    "request_id": 1,
                    "authority": "localhost",
                    "path": "/test",
                    "headers": {},
                    "wt_available_protocols": ["p1", "p2"],
                },
                {
                    "request_id": 1,
                    "authority": "localhost",
                    "path": "/test",
                    "headers": {},
                    "wt_available_protocols": ["p1", "p2"],
                },
            ),
            (
                UserCreateStream,
                {"request_id": 1, "session_id": 1, "is_unidirectional": True},
                {"request_id": 1, "session_id": 1, "is_unidirectional": True},
            ),
            (UserDiagnoseConnection, {"request_id": 1}, {"request_id": 1}),
            (UserDiagnoseSession, {"request_id": 1, "session_id": 1}, {"request_id": 1, "session_id": 1}),
            (UserDiagnoseStream, {"request_id": 1, "stream_id": 4}, {"request_id": 1, "stream_id": 4}),
            (
                UserExportKeyingMaterial,
                {"request_id": 1, "session_id": 10, "label": "test", "context": b"ctx", "length": 32},
                {"request_id": 1, "session_id": 10, "label": "test", "context": b"ctx", "length": 32},
            ),
            (UserReadDatagram, {"request_id": 1, "session_id": 1}, {"request_id": 1, "session_id": 1}),
            (
                UserReadStream,
                {"request_id": 1, "stream_id": 4, "max_size": 1024},
                {"request_id": 1, "stream_id": 4, "max_size": 1024},
            ),
            (
                UserRejectSession,
                {"request_id": 1, "session_id": 1, "headers": {b"retry-after": b"120"}, "status_code": 429},
                {"request_id": 1, "session_id": 1, "headers": {b"retry-after": b"120"}, "status_code": 429},
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
                UserStopSending,
                {"request_id": 1, "stream_id": 4, "error_code": 100},
                {"request_id": 1, "stream_id": 4, "error_code": 100},
            ),
            (
                UserWriteStream,
                {"request_id": 1, "stream_id": 4, "data": b"data", "end_stream": True},
                {"request_id": 1, "stream_id": 4, "data": b"data", "end_stream": True},
            ),
        ],
    )
    def test_instantiation(
        self, event_class: type[ProtocolEvent], kwargs: dict[str, Any], expected_attributes: dict[str, Any]
    ) -> None:
        event = event_class(**kwargs)

        assert isinstance(event, ProtocolEvent)
        for attribute, expected_value in expected_attributes.items():
            assert getattr(event, attribute) == expected_value


class TestRuntimeEvent:

    @pytest.mark.parametrize(
        argnames="event_class, kwargs",
        argvalues=[
            (CommandFailed, {"request_id": 3, "exception": ValueError("failed")}),
            (ConnectionEffects, {"handle": 4, "effects": []}),
            (ConnectionSpawned, {"handle": 5, "remote_address": ("127.0.0.1", 443), "effects": []}),
            (ReactorShutdown, {}),
        ],
    )
    def test_instantiation(self, event_class: type[Any], kwargs: dict[str, Any]) -> None:
        event = event_class(**kwargs)

        for attribute, expected_value in kwargs.items():
            assert getattr(event, attribute) == expected_value
