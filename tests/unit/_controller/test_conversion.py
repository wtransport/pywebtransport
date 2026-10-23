"""Unit tests for the pywebtransport._controller.conversion module."""

from typing import Any

import pytest

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
    USER_ACCEPT_SESSION,
    USER_CLOSE_CONNECTION,
    USER_CLOSE_CONNECTION_GRACEFULLY,
    USER_CLOSE_SESSION,
    USER_CREATE_SESSION,
    USER_CREATE_SESSION_OPTIMISTIC,
    USER_CREATE_STREAM,
    USER_DIAGNOSE_CONNECTION,
    USER_DIAGNOSE_SESSION,
    USER_DIAGNOSE_STREAM,
    USER_EXPORT_KEYING_MATERIAL,
    USER_READ_DATAGRAM,
    USER_READ_STREAM,
    USER_REJECT_SESSION,
    USER_RESET_STREAM,
    USER_SEND_DATAGRAM,
    USER_STOP_SENDING,
    USER_WRITE_STREAM,
)
from pywebtransport._controller.conversion import convert_effect, convert_protocol_event, convert_runtime_event
from pywebtransport._controller.events import (
    CommandFailed,
    ConnectionEffects,
    ConnectionSpawned,
    Effect,
    EmitConnectionEvent,
    EmitSessionEvent,
    EmitStreamEvent,
    NotifyRequestDone,
    NotifyRequestFailed,
    ProtocolEvent,
    ReactorShutdown,
    RuntimeEvent,
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


class TestConversion:

    @pytest.mark.parametrize(
        argnames="event_tuple, expected",
        argvalues=[
            (
                (EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_CLOSED, 100, "reason")),
                EmitConnectionEvent(
                    connection_handle=1, event_type=EventType.CONNECTION_CLOSED, error_code=100, reason="reason"
                ),
            ),
            (
                (
                    EMIT_SESSION_EVENT,
                    (
                        10,
                        EventType.SESSION_READY,
                        "/p",
                        {"k": "v"},
                        ["h3"],
                        "h3",
                        b"d",
                        True,
                        100,
                        5,
                        0.5,
                        403,
                        0,
                        "ok",
                    ),
                ),
                EmitSessionEvent(
                    session_id=10,
                    event_type=EventType.SESSION_READY,
                    path="/p",
                    headers={"k": "v"},
                    wt_available_protocols=["h3"],
                    wt_protocol="h3",
                    data=b"d",
                    is_unidirectional=True,
                    max_data=100,
                    max_streams=5,
                    ready_at=0.5,
                    status_code=403,
                    error_code=0,
                    reason="ok",
                ),
            ),
            (
                (EMIT_STREAM_EVENT, (20, EventType.STREAM_OPENED, 10, StreamDirection.BIDIRECTIONAL, True, 1)),
                EmitStreamEvent(
                    stream_id=20,
                    event_type=EventType.STREAM_OPENED,
                    session_id=10,
                    direction=StreamDirection.BIDIRECTIONAL,
                    is_peer_initiated=True,
                    error_code=1,
                ),
            ),
            (
                (NOTIFY_REQUEST_DONE, (5, "result_value")),
                NotifyRequestDone(request_id=5, result="result_value"),
            ),
        ],
    )
    def test_convert_effect_success(self, event_tuple: tuple[int, Any], expected: Effect) -> None:
        result = convert_effect(event_tuple=event_tuple)

        assert result == expected

    def test_convert_effect_success_notify_request_failed(self) -> None:
        exception = ValueError("boom")
        event_tuple = (NOTIFY_REQUEST_FAILED, (6, exception))

        result = convert_effect(event_tuple=event_tuple)

        assert result == NotifyRequestFailed(request_id=6, exception=exception)

    def test_convert_effect_unsupported(self) -> None:
        with pytest.raises(expected_exception=ValueError, match="rt_event convert invalid actual=999"):
            convert_effect(event_tuple=(999, None))

    @pytest.mark.parametrize(
        argnames="event, expected",
        argvalues=[
            (
                UserAcceptSession(
                    request_id=2, session_id=10, headers={b"server": b"pywebtransport"}, wt_protocol="h3"
                ),
                (USER_ACCEPT_SESSION, (2, 10, {b"server": b"pywebtransport"}, "h3")),
            ),
            (
                UserCloseConnection(request_id=1, error_code=100, reason="closing"),
                (USER_CLOSE_CONNECTION, (1, 100, "closing")),
            ),
            (
                UserCloseConnectionGracefully(request_id=4),
                (USER_CLOSE_CONNECTION_GRACEFULLY, (4,)),
            ),
            (
                UserCloseSession(request_id=3, session_id=10, error_code=0, reason=None),
                (USER_CLOSE_SESSION, (3, 10, 0, None)),
            ),
            (
                UserCreateSession(
                    request_id=5,
                    authority="localhost",
                    path="/test",
                    headers={b":method": b"CONNECT"},
                    wt_available_protocols=["h3"],
                ),
                (USER_CREATE_SESSION, (5, "localhost", "/test", {b":method": b"CONNECT"}, ["h3"])),
            ),
            (
                UserCreateSessionOptimistic(
                    request_id=55,
                    authority="localhost",
                    path="/test",
                    headers={b":method": b"CONNECT"},
                    wt_available_protocols=["h3"],
                ),
                (USER_CREATE_SESSION_OPTIMISTIC, (55, "localhost", "/test", {b":method": b"CONNECT"}, ["h3"])),
            ),
            (
                UserCreateStream(request_id=6, session_id=10, is_unidirectional=True),
                (USER_CREATE_STREAM, (6, 10, True)),
            ),
            (
                UserDiagnoseConnection(request_id=7),
                (USER_DIAGNOSE_CONNECTION, (7,)),
            ),
            (
                UserDiagnoseSession(request_id=8, session_id=10),
                (USER_DIAGNOSE_SESSION, (8, 10)),
            ),
            (
                UserDiagnoseStream(request_id=9, stream_id=20),
                (USER_DIAGNOSE_STREAM, (9, 20)),
            ),
            (
                UserExportKeyingMaterial(
                    request_id=70, session_id=10, label="EXPORTER-test", context=b"ctx", length=32
                ),
                (USER_EXPORT_KEYING_MATERIAL, (70, 10, "EXPORTER-test", b"ctx", 32)),
            ),
            (
                UserReadDatagram(request_id=18, session_id=10),
                (USER_READ_DATAGRAM, (18, 10)),
            ),
            (
                UserReadStream(request_id=17, stream_id=20, max_size=4096),
                (USER_READ_STREAM, (17, 20, 4096)),
            ),
            (
                UserRejectSession(request_id=12, session_id=10, headers={b"retry-after": b"120"}, status_code=429),
                (USER_REJECT_SESSION, (12, 10, {b"retry-after": b"120"}, 429)),
            ),
            (
                UserResetStream(request_id=13, stream_id=20, error_code=1),
                (USER_RESET_STREAM, (13, 20, 1)),
            ),
            (
                UserSendDatagram(request_id=14, session_id=10, data=b"datagram"),
                (USER_SEND_DATAGRAM, (14, 10, b"datagram")),
            ),
            (
                UserStopSending(request_id=16, stream_id=20, error_code=2),
                (USER_STOP_SENDING, (16, 20, 2)),
            ),
            (
                UserWriteStream(request_id=15, stream_id=20, data=b"chunk", end_stream=True),
                (USER_WRITE_STREAM, (15, 20, b"chunk", True)),
            ),
        ],
    )
    def test_convert_protocol_event_success(self, event: ProtocolEvent, expected: tuple[int, tuple[Any, ...]]) -> None:
        result = convert_protocol_event(event=event)

        assert result == expected

    def test_convert_protocol_event_unsupported(self) -> None:
        class UnsupportedEvent(ProtocolEvent):
            pass

        event = UnsupportedEvent(request_id=0)

        with pytest.raises(expected_exception=ValueError, match="rt_event convert invalid actual=UnsupportedEvent"):
            convert_protocol_event(event=event)

    @pytest.mark.parametrize(
        argnames="event_tuple, expected",
        argvalues=[
            (
                (
                    CONNECTION_EFFECTS,
                    (4, [(EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_CLOSED, None, None))]),
                ),
                ConnectionEffects(
                    handle=4, effects=[(EMIT_CONNECTION_EVENT, (1, EventType.CONNECTION_CLOSED, None, None))]
                ),
            ),
            (
                (CONNECTION_SPAWNED, (5, ("127.0.0.1", 443), [])),
                ConnectionSpawned(handle=5, remote_address=("127.0.0.1", 443), effects=[]),
            ),
            (
                (REACTOR_SHUTDOWN, None),
                ReactorShutdown(),
            ),
        ],
    )
    def test_convert_runtime_event_success(self, event_tuple: tuple[int, Any], expected: RuntimeEvent) -> None:
        result = convert_runtime_event(event_tuple=event_tuple)

        assert result == expected

    def test_convert_runtime_event_success_command_failed(self) -> None:
        exception = ValueError("failed")
        event_tuple = (COMMAND_FAILED, (3, exception))

        result = convert_runtime_event(event_tuple=event_tuple)

        assert result == CommandFailed(request_id=3, exception=exception)

    def test_convert_runtime_event_unsupported(self) -> None:
        with pytest.raises(expected_exception=ValueError, match="rt_event convert invalid actual=999"):
            convert_runtime_event(event_tuple=(999, None))
