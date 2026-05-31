"""Unit tests for the pywebtransport._controller.mapper module."""

from typing import Any

import pytest

from pywebtransport._controller import abi
from pywebtransport._controller.mapper import pack_user_event
from pywebtransport._protocol import events


class TestMapper:

    @pytest.mark.parametrize(
        argnames="event, expected",
        argvalues=[
            (
                events.UserAcceptSession(request_id=2, session_id=10, wt_protocol="h3"),
                (abi.USER_ACCEPT_SESSION, (2, 10, "h3")),
            ),
            (
                events.UserCloseConnection(request_id=1, error_code=100, reason="closing"),
                (abi.USER_CLOSE_CONNECTION, (1, 100, "closing")),
            ),
            (
                events.UserCloseConnectionGracefully(request_id=4),
                (abi.USER_CLOSE_CONNECTION_GRACEFULLY, (4,)),
            ),
            (
                events.UserCloseSession(request_id=3, session_id=10, error_code=0, reason=None),
                (abi.USER_CLOSE_SESSION, (3, 10, 0, None)),
            ),
            (
                events.UserCreateSession(
                    request_id=5,
                    authority="localhost",
                    path="/test",
                    headers={b":method": b"CONNECT"},
                    wt_available_protocols=["h3"],
                ),
                (abi.USER_CREATE_SESSION, (5, "localhost", "/test", {b":method": b"CONNECT"}, ["h3"])),
            ),
            (
                events.UserCreateStream(request_id=6, session_id=10, is_unidirectional=True),
                (abi.USER_CREATE_STREAM, (6, 10, True)),
            ),
            (
                events.UserExportKeyingMaterial(
                    request_id=70, session_id=10, label="EXPORTER-test", context=b"ctx", length=32
                ),
                (abi.USER_EXPORT_KEYING_MATERIAL, (70, 10, "EXPORTER-test", b"ctx", 32)),
            ),
            (
                events.UserGetConnectionDiagnostics(request_id=7),
                (abi.USER_GET_CONNECTION_DIAGNOSTICS, (7,)),
            ),
            (
                events.UserGetSessionDiagnostics(request_id=8, session_id=10),
                (abi.USER_GET_SESSION_DIAGNOSTICS, (8, 10)),
            ),
            (
                events.UserGetStreamDiagnostics(request_id=9, stream_id=20),
                (abi.USER_GET_STREAM_DIAGNOSTICS, (9, 20)),
            ),
            (
                events.UserGrantDataCredit(request_id=10, session_id=10, max_data=1024),
                (abi.USER_GRANT_DATA_CREDIT, (10, 10, 1024)),
            ),
            (
                events.UserGrantStreamsCredit(request_id=11, session_id=10, is_unidirectional=False, max_streams=5),
                (abi.USER_GRANT_STREAMS_CREDIT, (11, 10, False, 5)),
            ),
            (
                events.UserReadStream(request_id=17, stream_id=20, max_bytes=4096),
                (abi.USER_READ_STREAM, (17, 20, 4096)),
            ),
            (
                events.UserRejectSession(request_id=12, session_id=10, status_code=403),
                (abi.USER_REJECT_SESSION, (12, 10, 403)),
            ),
            (
                events.UserResetStream(request_id=13, stream_id=20, error_code=1),
                (abi.USER_RESET_STREAM, (13, 20, 1)),
            ),
            (
                events.UserSendDatagram(request_id=14, session_id=10, data=b"datagram"),
                (abi.USER_SEND_DATAGRAM, (14, 10, b"datagram")),
            ),
            (
                events.UserSendStreamData(request_id=15, stream_id=20, data=b"chunk", end_stream=True),
                (abi.USER_SEND_STREAM_DATA, (15, 20, b"chunk", True)),
            ),
            (
                events.UserStopSending(request_id=16, stream_id=20, error_code=2),
                (abi.USER_STOP_SENDING, (16, 20, 2)),
            ),
        ],
    )
    def test_pack_user_event_success(self, event: events.ProtocolEvent, expected: tuple[int, tuple[Any, ...]]) -> None:
        result = pack_user_event(event=event)

        assert result == expected

    def test_pack_user_event_unsupported(self) -> None:
        class UnsupportedEvent(events.ProtocolEvent):
            pass

        event = UnsupportedEvent()

        with pytest.raises(expected_exception=ValueError, match="rt_event convert invalid actual=UnsupportedEvent"):
            pack_user_event(event=event)
