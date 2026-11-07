"""Unit tests for the pywebtransport.config module."""

import dataclasses
import math
import ssl
from typing import Any

import pytest

from pywebtransport import ClientConfig, ConfigurationError, Headers, ServerConfig
from pywebtransport.config import _FIELD_SECTION_SIZE_LIMIT
from pywebtransport.constants import (
    DEFAULT_CONNECTION_ATTEMPT_DELAY,
    DEFAULT_CONNECTION_RECEIVE_WINDOW,
    DEFAULT_CONNECTION_SEND_WINDOW,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_INITIAL_MAX_DATA,
    DEFAULT_KEEP_ALIVE_INTERVAL,
    DEFAULT_MAX_CAPSULE_SIZE,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_FIELD_SECTION_SIZE,
    DEFAULT_MAX_PENDING_CAPSULES,
    DEFAULT_MAX_PENDING_DATAGRAMS,
    DEFAULT_MAX_PENDING_STREAMS,
    DEFAULT_MAX_SESSIONS,
    DEFAULT_MAX_STREAMS_BIDI,
    DEFAULT_MAX_STREAMS_UNI,
    DEFAULT_MAX_UDP_PAYLOAD_SIZE,
    DEFAULT_SESSION_RECEIVE_WINDOW,
    DEFAULT_STREAM_RECEIVE_WINDOW,
    H3_MIN_UNI_STREAM_COUNT,
    QUIC_MIN_UDP_PAYLOAD_SIZE_LIMIT,
    QUIC_UDP_PAYLOAD_SIZE_LIMIT,
    QUIC_VARINT_LIMIT,
    WT_SESSION_CONNECT_STREAM_COUNT,
    WT_STREAMS_LIMIT,
)


class TestClientConfig:

    def test_default_initialization(self) -> None:
        config = ClientConfig()

        assert config.congestion_control_algorithm == "cubic"
        assert config.connect_timeout == DEFAULT_CONNECT_TIMEOUT
        assert config.connection_attempt_delay == DEFAULT_CONNECTION_ATTEMPT_DELAY
        assert config.connection_receive_window == DEFAULT_CONNECTION_RECEIVE_WINDOW
        assert config.connection_send_window == DEFAULT_CONNECTION_SEND_WINDOW
        assert config.headers == ()
        assert config.initial_max_data == DEFAULT_INITIAL_MAX_DATA
        assert config.keep_alive_interval == DEFAULT_KEEP_ALIVE_INTERVAL
        assert config.max_capsule_size == DEFAULT_MAX_CAPSULE_SIZE
        assert config.max_connections == DEFAULT_MAX_CONNECTIONS
        assert config.max_field_section_size == DEFAULT_MAX_FIELD_SECTION_SIZE
        assert config.max_pending_capsules == DEFAULT_MAX_PENDING_CAPSULES
        assert config.max_pending_datagrams == DEFAULT_MAX_PENDING_DATAGRAMS
        assert config.max_pending_streams == DEFAULT_MAX_PENDING_STREAMS
        assert config.max_sessions == DEFAULT_MAX_SESSIONS
        assert config.max_streams_bidi == DEFAULT_MAX_STREAMS_BIDI
        assert config.max_streams_uni == DEFAULT_MAX_STREAMS_UNI
        assert config.session_receive_window == DEFAULT_SESSION_RECEIVE_WINDOW
        assert config.stream_receive_window == DEFAULT_STREAM_RECEIVE_WINDOW
        assert config.wt_available_protocols is None
        assert config.verify_mode == ssl.CERT_REQUIRED

    def test_headers_as_list_accepted(self) -> None:
        headers: Headers = [("User-Agent", "Custom/1.0")]

        config = ClientConfig(ca_certs="dummy.pem", headers=headers)

        assert config.headers == (("User-Agent", "Custom/1.0"),)

    def test_headers_values_preserved(self) -> None:
        headers: Headers = {"User-Agent": "Custom/1.0", "X-Custom": "Value"}

        config = ClientConfig(headers=headers)

        assert config.headers == (("User-Agent", "Custom/1.0"), ("X-Custom", "Value"))

    @pytest.mark.parametrize(argnames="duration", argvalues=[0.001, QUIC_VARINT_LIMIT // 1000])
    def test_initialization_with_duration_bounds(self, duration: float) -> None:
        config = ClientConfig(
            early_event_ttl=duration,
            keep_alive_interval=duration,
            max_idle_timeout=duration,
            resource_cleanup_interval=duration,
        )

        assert config.early_event_ttl == duration
        assert config.keep_alive_interval == duration
        assert config.max_idle_timeout == duration
        assert config.resource_cleanup_interval == duration

    def test_initialization_with_infinite_timeout(self) -> None:
        config = ClientConfig(
            close_timeout=math.inf,
            connect_timeout=math.inf,
            connection_attempt_delay=math.inf,
            read_timeout=math.inf,
            stream_create_timeout=math.inf,
        )

        assert config.close_timeout == math.inf
        assert config.connect_timeout == math.inf
        assert config.connection_attempt_delay == math.inf
        assert config.read_timeout == math.inf
        assert config.stream_create_timeout == math.inf

    def test_initialization_with_none_timeout(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem", keep_alive_interval=None, read_timeout=None)

        assert config.keep_alive_interval is None
        assert config.read_timeout is None

    def test_is_frozen(self) -> None:
        config = ClientConfig()

        with pytest.raises(expected_exception=Exception):
            setattr(config, "max_connections", 1)

    def test_replace_preserves_normalized_fields(self) -> None:
        config = ClientConfig(headers={"User-Agent": "Custom/1.0"}, wt_available_protocols=["dummy", "h3"])

        replaced = dataclasses.replace(config, connect_timeout=1.0)

        assert replaced.headers == config.headers
        assert replaced.wt_available_protocols == config.wt_available_protocols

    @pytest.mark.parametrize(
        argnames="invalid_attributes, error_match",
        argvalues=[
            ({"ca_certs": 123}, "cfg_ca_certs validate invalid"),
            ({"certfile": "a.pem", "keyfile": None}, "cfg_keyfile validate invalid"),
            ({"certfile": None, "keyfile": "k.key"}, "cfg_certfile validate invalid"),
            ({"close_timeout": "invalid"}, "cfg_close_timeout validate invalid"),
            ({"close_timeout": -1}, "cfg_close_timeout validate invalid"),
            ({"close_timeout": math.nan}, "cfg_close_timeout validate invalid"),
            ({"congestion_control_algorithm": "invalid_algo"}, "cfg_congestion_control_algorithm validate invalid"),
            ({"connect_timeout": -1}, "cfg_connect_timeout validate invalid"),
            ({"connect_timeout": math.nan}, "cfg_connect_timeout validate invalid"),
            ({"connection_attempt_delay": -0.5}, "cfg_connection_attempt_delay validate invalid"),
            ({"connection_attempt_delay": math.nan}, "cfg_connection_attempt_delay validate invalid"),
            ({"connection_receive_window": 0}, "cfg_connection_receive_window validate invalid"),
            ({"connection_receive_window": 5.5}, "cfg_connection_receive_window validate invalid"),
            ({"connection_receive_window": QUIC_VARINT_LIMIT + 1}, "cfg_connection_receive_window validate invalid"),
            ({"connection_send_window": 0}, "cfg_connection_send_window validate invalid"),
            ({"connection_send_window": 5.5}, "cfg_connection_send_window validate invalid"),
            ({"early_event_ttl": 0}, "cfg_early_event_ttl validate invalid"),
            ({"early_event_ttl": 0.0005}, "cfg_early_event_ttl validate invalid"),
            ({"early_event_ttl": QUIC_VARINT_LIMIT // 1000 + 1}, "cfg_early_event_ttl validate invalid"),
            ({"early_event_ttl": math.nan}, "cfg_early_event_ttl validate invalid"),
            ({"headers": "not-a-dict-or-list"}, "cfg_headers validate invalid"),
            ({"headers": (("User-Agent",),)}, "cfg_headers validate invalid"),
            ({"initial_max_data": -1}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_data": 5.5}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_data": QUIC_VARINT_LIMIT + 1}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_streams_bidi": -1}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_bidi": 5.5}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_bidi": WT_STREAMS_LIMIT + 1}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_uni": -1}, "cfg_initial_max_streams_uni validate invalid"),
            ({"initial_max_streams_uni": 5.5}, "cfg_initial_max_streams_uni validate invalid"),
            ({"initial_max_streams_uni": WT_STREAMS_LIMIT + 1}, "cfg_initial_max_streams_uni validate invalid"),
            ({"keep_alive_interval": -1}, "cfg_keep_alive_interval validate invalid"),
            ({"keep_alive_interval": 0.0005}, "cfg_keep_alive_interval validate invalid"),
            ({"keep_alive_interval": QUIC_VARINT_LIMIT // 1000 + 1}, "cfg_keep_alive_interval validate invalid"),
            ({"keep_alive_interval": math.nan}, "cfg_keep_alive_interval validate invalid"),
            ({"max_capsule_size": 0}, "cfg_max_capsule_size validate invalid"),
            ({"max_capsule_size": 5.5}, "cfg_max_capsule_size validate invalid"),
            ({"max_connection_early_events": 0}, "cfg_max_connection_early_events validate invalid"),
            ({"max_connection_early_events": 5.5}, "cfg_max_connection_early_events validate invalid"),
            ({"max_connections": 0}, "cfg_max_connections validate invalid"),
            ({"max_connections": 5.5}, "cfg_max_connections validate invalid"),
            ({"max_datagram_size": 0}, "cfg_max_datagram_size validate invalid"),
            (
                {"max_datagram_size": 1, "max_udp_payload_size": QUIC_MIN_UDP_PAYLOAD_SIZE_LIMIT - 1},
                "cfg_max_udp_payload_size validate invalid",
            ),
            ({"max_datagram_size": 5.5}, "cfg_max_datagram_size validate invalid"),
            ({"max_datagram_size": DEFAULT_MAX_UDP_PAYLOAD_SIZE + 1}, "cfg_max_datagram_size validate invalid"),
            ({"max_field_section_size": 0}, "cfg_max_field_section_size validate invalid"),
            ({"max_field_section_size": 5.5}, "cfg_max_field_section_size validate invalid"),
            ({"max_field_section_size": _FIELD_SECTION_SIZE_LIMIT + 1}, "cfg_max_field_section_size validate invalid"),
            ({"max_idle_timeout": 0}, "cfg_max_idle_timeout validate invalid"),
            ({"max_idle_timeout": 0.0005}, "cfg_max_idle_timeout validate invalid"),
            ({"max_idle_timeout": QUIC_VARINT_LIMIT // 1000 + 1}, "cfg_max_idle_timeout validate invalid"),
            ({"max_idle_timeout": math.nan}, "cfg_max_idle_timeout validate invalid"),
            ({"max_pending_capsules": 0}, "cfg_max_pending_capsules validate invalid"),
            ({"max_pending_capsules": 5.5}, "cfg_max_pending_capsules validate invalid"),
            ({"max_pending_datagrams": 0}, "cfg_max_pending_datagrams validate invalid"),
            ({"max_pending_datagrams": 5.5}, "cfg_max_pending_datagrams validate invalid"),
            ({"max_pending_streams": 0}, "cfg_max_pending_streams validate invalid"),
            ({"max_pending_streams": 5.5}, "cfg_max_pending_streams validate invalid"),
            ({"max_session_early_events": 0}, "cfg_max_session_early_events validate invalid"),
            ({"max_session_early_events": 5.5}, "cfg_max_session_early_events validate invalid"),
            ({"max_sessions": 0}, "cfg_max_sessions validate invalid"),
            ({"max_sessions": 5.5}, "cfg_max_sessions validate invalid"),
            ({"max_streams_bidi": 5.5}, "cfg_max_streams_bidi validate invalid"),
            (
                {"max_streams_bidi": WT_SESSION_CONNECT_STREAM_COUNT - 1},
                "cfg_max_streams_bidi validate invalid",
            ),
            (
                {"max_streams_bidi": WT_STREAMS_LIMIT + 1},
                "cfg_max_streams_bidi validate invalid",
            ),
            ({"max_streams_uni": 5.5}, "cfg_max_streams_uni validate invalid"),
            (
                {"max_streams_uni": H3_MIN_UNI_STREAM_COUNT - 1},
                "cfg_max_streams_uni validate invalid",
            ),
            (
                {"max_streams_uni": WT_STREAMS_LIMIT + 1},
                "cfg_max_streams_uni validate invalid",
            ),
            ({"max_udp_payload_size": 5000.5}, "cfg_max_udp_payload_size validate invalid"),
            (
                {"max_udp_payload_size": QUIC_UDP_PAYLOAD_SIZE_LIMIT + 1},
                "cfg_max_udp_payload_size validate invalid",
            ),
            ({"max_write_buffer_size": 0}, "cfg_max_write_buffer_size validate invalid"),
            ({"max_write_buffer_size": 5.5}, "cfg_max_write_buffer_size validate invalid"),
            ({"read_timeout": -1}, "cfg_read_timeout validate invalid"),
            ({"read_timeout": math.nan}, "cfg_read_timeout validate invalid"),
            ({"resource_cleanup_interval": -1}, "cfg_resource_cleanup_interval validate invalid"),
            ({"resource_cleanup_interval": 0.0005}, "cfg_resource_cleanup_interval validate invalid"),
            (
                {"resource_cleanup_interval": QUIC_VARINT_LIMIT // 1000 + 1},
                "cfg_resource_cleanup_interval validate invalid",
            ),
            ({"resource_cleanup_interval": math.nan}, "cfg_resource_cleanup_interval validate invalid"),
            ({"session_receive_window": 0}, "cfg_session_receive_window validate invalid"),
            ({"session_receive_window": 5.5}, "cfg_session_receive_window validate invalid"),
            ({"session_receive_window": QUIC_VARINT_LIMIT + 1}, "cfg_session_receive_window validate invalid"),
            ({"stream_create_timeout": -1}, "cfg_stream_create_timeout validate invalid"),
            ({"stream_create_timeout": math.nan}, "cfg_stream_create_timeout validate invalid"),
            ({"stream_receive_window": 0}, "cfg_stream_receive_window validate invalid"),
            ({"stream_receive_window": 5.5}, "cfg_stream_receive_window validate invalid"),
            ({"stream_receive_window": QUIC_VARINT_LIMIT + 1}, "cfg_stream_receive_window validate invalid"),
            ({"verify_mode": "INVALID"}, "cfg_verify_mode validate invalid"),
            ({"wt_available_protocols": "not_a_list"}, "cfg_wt_available_protocols validate invalid"),
            ({"wt_available_protocols": [1, 2]}, "cfg_wt_available_protocols validate invalid"),
        ],
    )
    def test_validation_failures(self, invalid_attributes: dict[str, Any], error_match: str) -> None:
        base_attributes: dict[str, Any] = {"ca_certs": "dummy.pem", "verify_mode": ssl.CERT_REQUIRED}

        with pytest.raises(expected_exception=ConfigurationError, match=error_match):
            ClientConfig(**{**base_attributes, **invalid_attributes})

    def test_wt_available_protocols_values_preserved(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem", wt_available_protocols=["dummy", "h3"])

        assert config.wt_available_protocols == ("dummy", "h3")


class TestServerConfig:

    def test_default_initialization(self) -> None:
        config = ServerConfig(certfile="dummy.crt", keyfile="dummy.key")

        assert config.bind_host == "::"
        assert config.congestion_control_algorithm == "cubic"
        assert config.initial_max_data == DEFAULT_INITIAL_MAX_DATA
        assert config.keep_alive_interval == DEFAULT_KEEP_ALIVE_INTERVAL
        assert config.max_capsule_size == DEFAULT_MAX_CAPSULE_SIZE
        assert config.max_connections == DEFAULT_MAX_CONNECTIONS
        assert config.max_field_section_size == DEFAULT_MAX_FIELD_SECTION_SIZE
        assert config.max_pending_capsules == DEFAULT_MAX_PENDING_CAPSULES
        assert config.max_pending_datagrams == DEFAULT_MAX_PENDING_DATAGRAMS
        assert config.max_pending_streams == DEFAULT_MAX_PENDING_STREAMS
        assert config.session_receive_window == DEFAULT_SESSION_RECEIVE_WINDOW

    def test_initialization_fails_without_bind_host(self) -> None:
        with pytest.raises(expected_exception=ConfigurationError, match="cfg_bind_host validate invalid"):
            ServerConfig(bind_host="", certfile="c", keyfile="k")

    def test_initialization_fails_without_certs(self) -> None:
        with pytest.raises(expected_exception=ConfigurationError, match="cfg_certfile validate invalid"):
            ServerConfig(certfile=None, keyfile="k")  # type: ignore[arg-type]

        with pytest.raises(expected_exception=ConfigurationError, match="cfg_keyfile validate invalid"):
            ServerConfig(certfile="c", keyfile=None)  # type: ignore[arg-type]

    def test_is_frozen(self) -> None:
        config = ServerConfig(certfile="dummy.crt", keyfile="dummy.key")

        with pytest.raises(expected_exception=Exception):
            setattr(config, "bind_port", 1)

    def test_validate_success(self) -> None:
        ServerConfig(certfile="dummy.crt", keyfile="dummy.key")

    def test_validate_success_with_required_verify_mode(self) -> None:
        config = ServerConfig(
            ca_certs="ca.pem", certfile="dummy.crt", keyfile="dummy.key", verify_mode=ssl.CERT_REQUIRED
        )

        assert config.verify_mode == ssl.CERT_REQUIRED

    @pytest.mark.parametrize(
        argnames="invalid_attributes, error_match",
        argvalues=[
            ({"bind_host": ""}, "cfg_bind_host validate invalid"),
            ({"bind_host": 12345}, "cfg_bind_host validate invalid"),
            ({"bind_port": "invalid"}, "cfg_bind_port validate invalid"),
            ({"bind_port": 0}, "cfg_bind_port validate invalid"),
            ({"bind_port": 65536}, "cfg_bind_port validate invalid"),
            ({"ca_certs": None, "verify_mode": ssl.CERT_REQUIRED}, "cfg_ca_certs validate invalid"),
            ({"ca_certs": "ca.pem", "verify_mode": ssl.CERT_OPTIONAL}, "cfg_verify_mode validate invalid"),
            ({"congestion_control_algorithm": "invalid_algo"}, "cfg_congestion_control_algorithm validate invalid"),
            ({"connection_receive_window": 0}, "cfg_connection_receive_window validate invalid"),
            ({"connection_send_window": 0}, "cfg_connection_send_window validate invalid"),
            ({"early_event_ttl": -1.0}, "cfg_early_event_ttl validate invalid"),
            ({"initial_max_data": -1}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_data": QUIC_VARINT_LIMIT + 1}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_streams_bidi": -1}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_bidi": WT_STREAMS_LIMIT + 1}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_uni": -1}, "cfg_initial_max_streams_uni validate invalid"),
            ({"initial_max_streams_uni": WT_STREAMS_LIMIT + 1}, "cfg_initial_max_streams_uni validate invalid"),
            ({"keep_alive_interval": -1}, "cfg_keep_alive_interval validate invalid"),
            ({"max_capsule_size": 0}, "cfg_max_capsule_size validate invalid"),
            ({"max_connection_early_events": 0}, "cfg_max_connection_early_events validate invalid"),
            ({"max_connections": 0}, "cfg_max_connections validate invalid"),
            ({"max_datagram_size": 0}, "cfg_max_datagram_size validate invalid"),
            (
                {"max_datagram_size": 1, "max_udp_payload_size": QUIC_MIN_UDP_PAYLOAD_SIZE_LIMIT - 1},
                "cfg_max_udp_payload_size validate invalid",
            ),
            ({"max_datagram_size": DEFAULT_MAX_UDP_PAYLOAD_SIZE + 1}, "cfg_max_datagram_size validate invalid"),
            ({"max_field_section_size": 0}, "cfg_max_field_section_size validate invalid"),
            ({"max_field_section_size": _FIELD_SECTION_SIZE_LIMIT + 1}, "cfg_max_field_section_size validate invalid"),
            ({"max_pending_capsules": 0}, "cfg_max_pending_capsules validate invalid"),
            ({"max_pending_datagrams": 0}, "cfg_max_pending_datagrams validate invalid"),
            ({"max_pending_streams": 0}, "cfg_max_pending_streams validate invalid"),
            ({"max_session_early_events": 0}, "cfg_max_session_early_events validate invalid"),
            ({"max_sessions": 0}, "cfg_max_sessions validate invalid"),
            (
                {"max_streams_bidi": WT_SESSION_CONNECT_STREAM_COUNT - 1},
                "cfg_max_streams_bidi validate invalid",
            ),
            (
                {"max_streams_bidi": WT_STREAMS_LIMIT + 1},
                "cfg_max_streams_bidi validate invalid",
            ),
            (
                {"max_streams_uni": H3_MIN_UNI_STREAM_COUNT - 1},
                "cfg_max_streams_uni validate invalid",
            ),
            (
                {"max_streams_uni": WT_STREAMS_LIMIT + 1},
                "cfg_max_streams_uni validate invalid",
            ),
            (
                {"max_udp_payload_size": QUIC_UDP_PAYLOAD_SIZE_LIMIT + 1},
                "cfg_max_udp_payload_size validate invalid",
            ),
            ({"max_write_buffer_size": 0}, "cfg_max_write_buffer_size validate invalid"),
            ({"session_receive_window": 0}, "cfg_session_receive_window validate invalid"),
            ({"stream_receive_window": 0}, "cfg_stream_receive_window validate invalid"),
            ({"verify_mode": "INVALID"}, "cfg_verify_mode validate invalid"),
        ],
    )
    def test_validation_failures(self, invalid_attributes: dict[str, Any], error_match: str) -> None:
        base_attributes: dict[str, Any] = {
            "certfile": "dummy.crt",
            "keyfile": "dummy.key",
            "verify_mode": ssl.CERT_NONE,
        }

        with pytest.raises(expected_exception=ConfigurationError, match=error_match):
            ServerConfig(**{**base_attributes, **invalid_attributes})
