"""Unit tests for the pywebtransport.types module."""

import asyncio
from typing import Any

import pytest

from pywebtransport import Headers
from pywebtransport.types import (
    Buffer,
    ConnectionState,
    EventType,
    Serializer,
    SessionProtocol,
    SessionState,
    StreamDirection,
    StreamState,
    WebTransportProtocol,
)


class TestEnumerations:

    @pytest.mark.parametrize(
        "member, expected_value",
        [
            (ConnectionState.IDLE, "idle"),
            (ConnectionState.CONNECTING, "connecting"),
            (ConnectionState.CONNECTED, "connected"),
            (ConnectionState.CLOSING, "closing"),
            (ConnectionState.DRAINING, "draining"),
            (ConnectionState.CLOSED, "closed"),
            (ConnectionState.FAILED, "failed"),
        ],
    )
    def test_connection_state(self, member: ConnectionState, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        "member, expected_value",
        [
            (EventType.CAPSULE_RECEIVED, "capsule_received"),
            (EventType.CONNECTION_CLOSED, "connection_closed"),
            (EventType.CONNECTION_ESTABLISHED, "connection_established"),
            (EventType.CONNECTION_FAILED, "connection_failed"),
            (EventType.CONNECTION_LOST, "connection_lost"),
            (EventType.DATAGRAM_ERROR, "datagram_error"),
            (EventType.DATAGRAM_RECEIVED, "datagram_received"),
            (EventType.DATAGRAM_SENT, "datagram_sent"),
            (EventType.PROTOCOL_ERROR, "protocol_error"),
            (EventType.SESSION_CLOSED, "session_closed"),
            (EventType.SESSION_DATA_BLOCKED, "session_data_blocked"),
            (EventType.SESSION_DRAINING, "session_draining"),
            (EventType.SESSION_MAX_DATA_UPDATED, "session_max_data_updated"),
            (EventType.SESSION_MAX_STREAMS_BIDI_UPDATED, "session_max_streams_bidi_updated"),
            (EventType.SESSION_MAX_STREAMS_UNI_UPDATED, "session_max_streams_uni_updated"),
            (EventType.SESSION_READY, "session_ready"),
            (EventType.SESSION_REQUEST, "session_request"),
            (EventType.SESSION_STREAMS_BLOCKED, "session_streams_blocked"),
            (EventType.SETTINGS_RECEIVED, "settings_received"),
            (EventType.STREAM_CLOSED, "stream_closed"),
            (EventType.STREAM_DATA_RECEIVED, "stream_data_received"),
            (EventType.STREAM_ERROR, "stream_error"),
            (EventType.STREAM_OPENED, "stream_opened"),
            (EventType.TIMEOUT_ERROR, "timeout_error"),
        ],
    )
    def test_event_type(self, member: EventType, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        "member, expected_value",
        [
            (SessionState.CONNECTING, "connecting"),
            (SessionState.CONNECTED, "connected"),
            (SessionState.CLOSING, "closing"),
            (SessionState.DRAINING, "draining"),
            (SessionState.CLOSED, "closed"),
        ],
    )
    def test_session_state(self, member: SessionState, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        "member, expected_value",
        [
            (StreamDirection.BIDIRECTIONAL, "bidirectional"),
            (StreamDirection.SEND_ONLY, "send_only"),
            (StreamDirection.RECEIVE_ONLY, "receive_only"),
        ],
    )
    def test_stream_direction(self, member: StreamDirection, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        "member, expected_value",
        [
            (StreamState.OPEN, "open"),
            (StreamState.HALF_CLOSED_LOCAL, "half_closed_local"),
            (StreamState.HALF_CLOSED_REMOTE, "half_closed_remote"),
            (StreamState.RESET_SENT, "reset_sent"),
            (StreamState.RESET_RECEIVED, "reset_received"),
            (StreamState.CLOSED, "closed"),
        ],
    )
    def test_stream_state(self, member: StreamState, expected_value: str) -> None:
        assert member.value == expected_value


class TestRuntimeCheckableProtocols:

    def test_serializer_protocol_conformance(self) -> None:
        class GoodSerializer:
            def serialize(self, *, obj: Any) -> bytes:
                return b"serialized"

            def deserialize(self, *, data: Buffer, obj_type: Any = None) -> Any:
                return "deserialized"

        assert isinstance(GoodSerializer(), Serializer)

    def test_serializer_protocol_non_conformance(self) -> None:
        class BadSerializer:
            def serialize(self, *, obj: Any) -> bytes:
                return b"serialized"

        assert not isinstance(BadSerializer(), Serializer)

    def test_session_protocol_conformance(self) -> None:
        class GoodSession:
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

            async def close(self, *, error_code: int = 0, reason: str | None = None) -> None:
                pass

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
