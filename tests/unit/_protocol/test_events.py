"""Unit tests for the pywebtransport._protocol.events module."""

from typing import Any

import pytest

from pywebtransport._protocol.events import (
    UserAcceptSession,
    UserCloseConnection,
    UserCloseConnectionGracefully,
    UserCloseSession,
    UserCreateSession,
    UserCreateStream,
    UserEvent,
    UserExportKeyingMaterial,
    UserGetConnectionDiagnostics,
    UserGetSessionDiagnostics,
    UserGetStreamDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserReadStream,
    UserRejectSession,
    UserResetStream,
    UserSendDatagram,
    UserSendStreamData,
    UserStopSending,
)


class TestUserEvents:

    @pytest.mark.parametrize(
        argnames="event_class, kwargs, expected_attrs",
        argvalues=[
            (
                UserAcceptSession,
                {"request_id": 1, "session_id": 1},
                {"request_id": 1, "session_id": 1, "wt_protocol": None},
            ),
            (
                UserAcceptSession,
                {"request_id": 1, "session_id": 1, "wt_protocol": "h3"},
                {"request_id": 1, "session_id": 1, "wt_protocol": "h3"},
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
                {"request_id": 1, "path": "/test", "headers": {b":path": b"/test"}},
                {"request_id": 1, "path": "/test", "headers": {b":path": b"/test"}, "wt_available_protocols": None},
            ),
            (
                UserCreateSession,
                {"request_id": 1, "path": "/test", "headers": {}, "wt_available_protocols": ["p1", "p2"]},
                {"request_id": 1, "path": "/test", "headers": {}, "wt_available_protocols": ["p1", "p2"]},
            ),
            (
                UserCreateStream,
                {"request_id": 1, "session_id": 1, "is_unidirectional": True},
                {"request_id": 1, "session_id": 1, "is_unidirectional": True},
            ),
            (
                UserExportKeyingMaterial,
                {"request_id": 1, "session_id": 10, "label": "test", "context": b"ctx", "length": 32},
                {"request_id": 1, "session_id": 10, "label": "test", "context": b"ctx", "length": 32},
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
                {"request_id": 1, "session_id": 1, "is_unidirectional": False, "max_streams": 10},
                {"request_id": 1, "session_id": 1, "is_unidirectional": False, "max_streams": 10},
            ),
            (
                UserReadStream,
                {"request_id": 1, "stream_id": 4, "max_bytes": 1024},
                {"request_id": 1, "stream_id": 4, "max_bytes": 1024},
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
                UserStopSending,
                {"request_id": 1, "stream_id": 4, "error_code": 100},
                {"request_id": 1, "stream_id": 4, "error_code": 100},
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
