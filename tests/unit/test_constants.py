"""Unit tests for the pywebtransport.constants module."""

from enum import IntEnum

import pytest

from pywebtransport import ErrorCodes
from pywebtransport.constants import (
    DEFAULT_ALPN_PROTOCOLS,
    DEFAULT_BIND_HOST,
    DEFAULT_CLIENT_MAX_CONNECTIONS,
    DEFAULT_CLIENT_MAX_SESSIONS,
    DEFAULT_CLOSE_TIMEOUT,
    DEFAULT_CONGESTION_CONTROL_ALGORITHM,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_CONNECTION_IDLE_TIMEOUT,
    DEFAULT_DEV_PORT,
    DEFAULT_ENABLE_STATELESS_RETRY,
    DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE,
    DEFAULT_FLOW_CONTROL_WINDOW_SIZE,
    DEFAULT_INITIAL_MAX_DATA,
    DEFAULT_INITIAL_MAX_STREAMS_BIDI,
    DEFAULT_INITIAL_MAX_STREAMS_UNI,
    DEFAULT_KEEP_ALIVE,
    DEFAULT_LOG_LEVEL,
    DEFAULT_MAX_CAPSULE_SIZE,
    DEFAULT_MAX_CONNECTION_RETRIES,
    DEFAULT_MAX_DATAGRAM_SIZE,
    DEFAULT_MAX_EVENT_HISTORY_SIZE,
    DEFAULT_MAX_EVENT_LISTENERS,
    DEFAULT_MAX_EVENT_QUEUE_SIZE,
    DEFAULT_MAX_MESSAGE_SIZE,
    DEFAULT_MAX_PENDING_EVENTS_PER_SESSION,
    DEFAULT_MAX_RETRY_DELAY,
    DEFAULT_MAX_STREAM_READ_BUFFER,
    DEFAULT_MAX_STREAM_WRITE_BUFFER,
    DEFAULT_MAX_TOTAL_PENDING_EVENTS,
    DEFAULT_PENDING_EVENT_TTL,
    DEFAULT_READ_TIMEOUT,
    DEFAULT_RESOURCE_CLEANUP_INTERVAL,
    DEFAULT_RETRY_BACKOFF,
    DEFAULT_RETRY_DELAY,
    DEFAULT_SERVER_MAX_CONNECTIONS,
    DEFAULT_SERVER_MAX_SESSIONS,
    DEFAULT_STREAM_CREATION_TIMEOUT,
    DEFAULT_TRANSPORT_STREAMS_CAP,
    DEFAULT_WRITE_TIMEOUT,
    MAX_DATAGRAM_SIZE,
    MAX_PROTOCOL_STREAMS_LIMIT,
    SUPPORTED_CONGESTION_CONTROL_ALGORITHMS,
    USER_AGENT_HEADER,
    WEBTRANSPORT_DEFAULT_PORT,
    WEBTRANSPORT_SCHEME,
)


class TestConstantsValues:

    def test_top_level_constants_values(self) -> None:
        assert USER_AGENT_HEADER == "user-agent"
        assert WEBTRANSPORT_SCHEME == "https"
        assert WEBTRANSPORT_DEFAULT_PORT == 443
        assert MAX_DATAGRAM_SIZE == 65535
        assert MAX_PROTOCOL_STREAMS_LIMIT == 2**60
        assert DEFAULT_ALPN_PROTOCOLS == ["h3"]
        assert DEFAULT_BIND_HOST == "::"
        assert DEFAULT_CLIENT_MAX_CONNECTIONS == 100
        assert DEFAULT_CLIENT_MAX_SESSIONS == 100
        assert DEFAULT_CLOSE_TIMEOUT == 5.0
        assert DEFAULT_CONGESTION_CONTROL_ALGORITHM == "cubic"
        assert DEFAULT_CONNECT_TIMEOUT == 30.0
        assert DEFAULT_CONNECTION_IDLE_TIMEOUT == 60.0
        assert DEFAULT_DEV_PORT == 4433
        assert DEFAULT_ENABLE_STATELESS_RETRY is False
        assert DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE is True
        assert DEFAULT_FLOW_CONTROL_WINDOW_SIZE == 1024 * 1024
        assert DEFAULT_INITIAL_MAX_DATA == 10 * 1024 * 1024
        assert DEFAULT_INITIAL_MAX_STREAMS_BIDI == 100
        assert DEFAULT_INITIAL_MAX_STREAMS_UNI == 100
        assert DEFAULT_KEEP_ALIVE == 30.0
        assert DEFAULT_LOG_LEVEL == "INFO"
        assert DEFAULT_MAX_CAPSULE_SIZE == 65536
        assert DEFAULT_MAX_CONNECTION_RETRIES == 3
        assert DEFAULT_MAX_DATAGRAM_SIZE == 1350
        assert DEFAULT_MAX_EVENT_HISTORY_SIZE == 0
        assert DEFAULT_MAX_EVENT_LISTENERS == 100
        assert DEFAULT_MAX_EVENT_QUEUE_SIZE == 1000
        assert DEFAULT_MAX_MESSAGE_SIZE == 1024 * 1024
        assert DEFAULT_MAX_PENDING_EVENTS_PER_SESSION == 100
        assert DEFAULT_MAX_RETRY_DELAY == 30.0
        assert DEFAULT_MAX_STREAM_READ_BUFFER == 2 * 1024 * 1024
        assert DEFAULT_MAX_STREAM_WRITE_BUFFER == 2 * 1024 * 1024
        assert DEFAULT_MAX_TOTAL_PENDING_EVENTS == 1000
        assert DEFAULT_PENDING_EVENT_TTL == 5.0
        assert DEFAULT_READ_TIMEOUT == 60.0
        assert DEFAULT_RESOURCE_CLEANUP_INTERVAL == 15.0
        assert DEFAULT_RETRY_BACKOFF == 2.0
        assert DEFAULT_RETRY_DELAY == 1.0
        assert DEFAULT_SERVER_MAX_CONNECTIONS == 3000
        assert DEFAULT_SERVER_MAX_SESSIONS == 10000
        assert DEFAULT_STREAM_CREATION_TIMEOUT == 10.0
        assert DEFAULT_TRANSPORT_STREAMS_CAP == 65535
        assert DEFAULT_WRITE_TIMEOUT == 30.0
        assert SUPPORTED_CONGESTION_CONTROL_ALGORITHMS == ["bbr", "cubic", "reno"]


class TestErrorCodes:

    @pytest.mark.parametrize(
        "member, expected_value",
        [
            (ErrorCodes.NO_ERROR, 0x0),
            (ErrorCodes.INTERNAL_ERROR, 0x1),
            (ErrorCodes.CONNECTION_REFUSED, 0x2),
            (ErrorCodes.FLOW_CONTROL_ERROR, 0x3),
            (ErrorCodes.STREAM_LIMIT_ERROR, 0x4),
            (ErrorCodes.STREAM_STATE_ERROR, 0x5),
            (ErrorCodes.FINAL_SIZE_ERROR, 0x6),
            (ErrorCodes.FRAME_ENCODING_ERROR, 0x7),
            (ErrorCodes.TRANSPORT_PARAMETER_ERROR, 0x8),
            (ErrorCodes.CONNECTION_ID_LIMIT_ERROR, 0x9),
            (ErrorCodes.PROTOCOL_VIOLATION, 0xA),
            (ErrorCodes.INVALID_TOKEN, 0xB),
            (ErrorCodes.APPLICATION_ERROR, 0xC),
            (ErrorCodes.CRYPTO_BUFFER_EXCEEDED, 0xD),
            (ErrorCodes.KEY_UPDATE_ERROR, 0xE),
            (ErrorCodes.AEAD_LIMIT_REACHED, 0xF),
            (ErrorCodes.NO_VIABLE_PATH, 0x10),
            (ErrorCodes.H3_DATAGRAM_ERROR, 0x33),
            (ErrorCodes.H3_NO_ERROR, 0x100),
            (ErrorCodes.H3_GENERAL_PROTOCOL_ERROR, 0x101),
            (ErrorCodes.H3_INTERNAL_ERROR, 0x102),
            (ErrorCodes.H3_STREAM_CREATION_ERROR, 0x103),
            (ErrorCodes.H3_CLOSED_CRITICAL_STREAM, 0x104),
            (ErrorCodes.H3_FRAME_UNEXPECTED, 0x105),
            (ErrorCodes.H3_FRAME_ERROR, 0x106),
            (ErrorCodes.H3_EXCESSIVE_LOAD, 0x107),
            (ErrorCodes.H3_ID_ERROR, 0x108),
            (ErrorCodes.H3_SETTINGS_ERROR, 0x109),
            (ErrorCodes.H3_MISSING_SETTINGS, 0x10A),
            (ErrorCodes.H3_REQUEST_REJECTED, 0x10B),
            (ErrorCodes.H3_REQUEST_CANCELLED, 0x10C),
            (ErrorCodes.H3_REQUEST_INCOMPLETE, 0x10D),
            (ErrorCodes.H3_MESSAGE_ERROR, 0x10E),
            (ErrorCodes.H3_CONNECT_ERROR, 0x10F),
            (ErrorCodes.H3_VERSION_FALLBACK, 0x110),
            (ErrorCodes.QPACK_DECODER_STREAM_ERROR, 0x202),
            (ErrorCodes.QPACK_DECOMPRESSION_FAILED, 0x200),
            (ErrorCodes.QPACK_ENCODER_STREAM_ERROR, 0x201),
            (ErrorCodes.WT_SESSION_GONE, 0x170D7B68),
            (ErrorCodes.WT_BUFFERED_STREAM_REJECTED, 0x3994BD84),
            (ErrorCodes.WT_FLOW_CONTROL_ERROR, 0x045D4487),
            (ErrorCodes.WT_APPLICATION_ERROR_FIRST, 0x52E4A40FA8DB),
            (ErrorCodes.WT_APPLICATION_ERROR_LAST, 0x52E5AC983162),
            (ErrorCodes.APP_CONNECTION_TIMEOUT, 0x1000),
            (ErrorCodes.APP_AUTHENTICATION_FAILED, 0x1001),
            (ErrorCodes.APP_PERMISSION_DENIED, 0x1002),
            (ErrorCodes.APP_RESOURCE_EXHAUSTED, 0x1003),
            (ErrorCodes.APP_INVALID_REQUEST, 0x1004),
            (ErrorCodes.APP_SERVICE_UNAVAILABLE, 0x1005),
            (ErrorCodes.LIB_INTERNAL_ERROR, 0x10000001),
            (ErrorCodes.LIB_CONNECTION_STATE_ERROR, 0x11000001),
            (ErrorCodes.LIB_SESSION_STATE_ERROR, 0x12000001),
            (ErrorCodes.LIB_STREAM_STATE_ERROR, 0x13000001),
        ],
    )
    def test_error_code_values(self, member: ErrorCodes, expected_value: int) -> None:
        assert member.value == expected_value

    def test_error_codes_is_int_enum(self) -> None:
        assert issubclass(ErrorCodes, IntEnum)
