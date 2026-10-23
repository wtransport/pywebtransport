"""Unit tests for the pywebtransport.types module."""

import pytest

from pywebtransport.types import (
    ConnectionState,
    EventType,
    SessionState,
    StreamDirection,
    StreamReceiveState,
    StreamSendState,
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
            (EventType.SESSION_CLOSED, "session_closed"),
            (EventType.SESSION_DATA_BLOCKED, "session_data_blocked"),
            (EventType.SESSION_DRAINING, "session_draining"),
            (EventType.SESSION_MAX_DATA_UPDATED, "session_max_data_updated"),
            (EventType.SESSION_MAX_STREAMS_BIDI_UPDATED, "session_max_streams_bidi_updated"),
            (EventType.SESSION_MAX_STREAMS_UNI_UPDATED, "session_max_streams_uni_updated"),
            (EventType.SESSION_PENDING, "session_pending"),
            (EventType.SESSION_READY, "session_ready"),
            (EventType.SESSION_REQUEST_RECEIVED, "session_request_received"),
            (EventType.SESSION_STREAMS_BLOCKED, "session_streams_blocked"),
            (EventType.STREAM_CLOSED, "stream_closed"),
            (EventType.STREAM_OPENED, "stream_opened"),
            (EventType.STREAM_RESET_RECEIVED, "stream_reset_received"),
            (EventType.STREAM_STOP_SENDING_RECEIVED, "stream_stop_sending_received"),
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
            (StreamReceiveState.DATA_READ, "data_read"),
            (StreamReceiveState.DATA_RECEIVED, "data_received"),
            (StreamReceiveState.RECEIVE, "receive"),
            (StreamReceiveState.RESET_READ, "reset_read"),
        ],
    )
    def test_stream_receive_state(self, member: StreamReceiveState, expected_value: str) -> None:
        assert member.value == expected_value

    @pytest.mark.parametrize(
        argnames="member, expected_value",
        argvalues=[
            (StreamSendState.DATA_RECEIVED, "data_received"),
            (StreamSendState.DATA_SENT, "data_sent"),
            (StreamSendState.READY, "ready"),
            (StreamSendState.RESET_SENT, "reset_sent"),
        ],
    )
    def test_stream_send_state(self, member: StreamSendState, expected_value: str) -> None:
        assert member.value == expected_value
