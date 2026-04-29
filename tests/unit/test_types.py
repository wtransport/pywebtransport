"""Unit tests for the pywebtransport.types module."""

import asyncio
from typing import Any

import pytest

from pywebtransport import Headers
from pywebtransport.types import (
    Buffer,
    ConnectionState,
    EventType,
    SessionProtocol,
    SessionState,
    StreamDirection,
    StreamState,
    WebTransportProtocol,
)


class TestEnumerations:

    @pytest.mark.parametrize(
        argnames="member, expected_value",
        argvalues=[
            (ConnectionState.CLOSED, "closed"),
            (ConnectionState.CLOSING, "closing"),
            (ConnectionState.CONNECTED, "connected"),
            (ConnectionState.CONNECTING, "connecting"),
            (ConnectionState.IDLE, "idle"),
        ],
    )
    def test_connection_state(self, member: ConnectionState, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        argnames="member, expected_value",
        argvalues=[
            (EventType.CONNECTION_CLOSED, "connection_closed"),
            (EventType.CONNECTION_ESTABLISHED, "connection_established"),
            (EventType.DATAGRAM_RECEIVED, "datagram_received"),
            (EventType.SESSION_CLOSED, "session_closed"),
            (EventType.SESSION_DATA_BLOCKED, "session_data_blocked"),
            (EventType.SESSION_DRAINING, "session_draining"),
            (EventType.SESSION_MAX_DATA_UPDATED, "session_max_data_updated"),
            (EventType.SESSION_MAX_STREAMS_BIDI_UPDATED, "session_max_streams_bidi_updated"),
            (EventType.SESSION_MAX_STREAMS_UNI_UPDATED, "session_max_streams_uni_updated"),
            (EventType.SESSION_READY, "session_ready"),
            (EventType.SESSION_REQUEST, "session_request"),
            (EventType.SESSION_STREAMS_BLOCKED, "session_streams_blocked"),
            (EventType.STOP_SENDING_RECEIVED, "stop_sending_received"),
            (EventType.STREAM_CLOSED, "stream_closed"),
            (EventType.STREAM_OPENED, "stream_opened"),
            (EventType.STREAM_RESET_RECEIVED, "stream_reset_received"),
        ],
    )
    def test_event_type(self, member: EventType, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        argnames="member, expected_value",
        argvalues=[
            (SessionState.CLOSED, "closed"),
            (SessionState.CLOSING, "closing"),
            (SessionState.CONNECTED, "connected"),
            (SessionState.CONNECTING, "connecting"),
            (SessionState.DRAINING, "draining"),
        ],
    )
    def test_session_state(self, member: SessionState, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        argnames="member, expected_value",
        argvalues=[
            (StreamDirection.BIDIRECTIONAL, "bidirectional"),
            (StreamDirection.RECEIVE_ONLY, "receive_only"),
            (StreamDirection.SEND_ONLY, "send_only"),
        ],
    )
    def test_stream_direction(self, member: StreamDirection, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        argnames="member, expected_value",
        argvalues=[
            (StreamState.CLOSED, "closed"),
            (StreamState.HALF_CLOSED_LOCAL, "half_closed_local"),
            (StreamState.HALF_CLOSED_REMOTE, "half_closed_remote"),
            (StreamState.OPEN, "open"),
            (StreamState.RESET_RECEIVED, "reset_received"),
            (StreamState.RESET_SENT, "reset_sent"),
        ],
    )
    def test_stream_state(self, member: StreamState, expected_value: str) -> None:
        assert member.value == expected_value


class TestRuntimeCheckableProtocols:

    def test_protocol_slots(self) -> None:
        assert SessionProtocol.__slots__ == ()
        assert WebTransportProtocol.__slots__ == ()

    def test_session_protocol_conformance(self) -> None:
        class GoodSession:
            def __init__(self) -> None:
                self._wt_proto: str | None = None

            async def close(self, *, error_code: int = 0, reason: str | None = None) -> None:
                pass

            @property
            def headers(self) -> Headers:
                return {}

            @property
            def path(self) -> str:
                return "/"

            @property
            def remote_address(self) -> tuple[str, int] | None:
                return ("127.0.0.1", 443)

            @property
            def session_id(self) -> int:
                return 1

            @property
            def state(self) -> SessionState:
                return SessionState.CONNECTED

            @property
            def wt_available_protocols(self) -> list[str] | None:
                return None

            @property
            def wt_protocol(self) -> str | None:
                return self._wt_proto

            @wt_protocol.setter
            def wt_protocol(self, value: str | None) -> None:
                self._wt_proto = value

        assert isinstance(GoodSession(), SessionProtocol)

    def test_session_protocol_non_conformance(self) -> None:
        class BadSession:
            @property
            def headers(self) -> Headers:
                return {}

        assert not isinstance(BadSession(), SessionProtocol)

    def test_web_transport_protocol_conformance(self) -> None:
        class GoodTransport:
            def connection_lost(self, exc: Exception | None) -> None:
                pass

            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                pass

            def datagram_received(self, data: Buffer, addr: tuple[str, int]) -> None:
                pass

            def error_received(self, exc: Exception) -> None:
                pass

        assert isinstance(GoodTransport(), WebTransportProtocol)

    def test_web_transport_protocol_non_conformance(self) -> None:
        class BadTransport:
            def connection_made(self, transport: Any) -> None:
                pass

        assert not isinstance(BadTransport(), WebTransportProtocol)
