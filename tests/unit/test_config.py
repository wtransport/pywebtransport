"""Unit tests for the pywebtransport.config module."""

import ssl
from typing import Any, Union, get_type_hints
from unittest.mock import patch

import pytest

from pywebtransport import ClientConfig, ConfigurationError, Headers, ServerConfig
from pywebtransport.config import _FIELD_SECTION_SIZE_LIMIT
from pywebtransport.constants import (
    DEFAULT_ALPN_PROTOCOLS,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_CONNECTION_ATTEMPT_DELAY,
    DEFAULT_FLOW_CONTROL_WINDOW,
    DEFAULT_INITIAL_MAX_DATA,
    DEFAULT_KEEP_ALIVE_INTERVAL,
    DEFAULT_MAX_CAPSULE_SIZE,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_FIELD_SECTION_SIZE,
    DEFAULT_MAX_PENDING_CAPSULES,
    DEFAULT_MAX_PENDING_DATAGRAMS,
    DEFAULT_MAX_PENDING_STREAMS,
    DEFAULT_MAX_SESSIONS,
    DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS,
    DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS,
    DEFAULT_QUIC_RECEIVE_WINDOW,
    DEFAULT_QUIC_SEND_WINDOW,
    DEFAULT_QUIC_STREAM_RECEIVE_WINDOW,
    H3_MIN_UNI_STREAM_COUNT,
    QUIC_VARINT_LIMIT,
    UDP_MAX_DATAGRAM_SIZE,
    WT_SESSION_CONTROL_BIDI_STREAM_COUNT,
    WT_STREAMS_LIMIT,
)


class TestClientConfig:

    def test_copy_method(self) -> None:
        config1 = ClientConfig(alpn_protocols=["h3"], ca_certs="dummy.pem")

        config2 = config1.copy()
        config2.max_sessions = 99
        config2.alpn_protocols.append("h2")

        assert config1 is not config2
        assert config1.max_sessions != 99
        assert config1.alpn_protocols == ["h3"]
        assert config2.alpn_protocols == ["h3", "h2"]

    def test_default_initialization(self) -> None:
        config = ClientConfig()

        assert config.alpn_protocols == DEFAULT_ALPN_PROTOCOLS
        assert config.congestion_control_algorithm == "cubic"
        assert config.connect_timeout == DEFAULT_CONNECT_TIMEOUT
        assert config.connection_attempt_delay == DEFAULT_CONNECTION_ATTEMPT_DELAY
        assert config.flow_control_window == DEFAULT_FLOW_CONTROL_WINDOW
        assert config.headers == {}
        assert config.initial_max_data == DEFAULT_INITIAL_MAX_DATA
        assert config.keep_alive_interval == DEFAULT_KEEP_ALIVE_INTERVAL
        assert config.max_capsule_size == DEFAULT_MAX_CAPSULE_SIZE
        assert config.max_connections == DEFAULT_MAX_CONNECTIONS
        assert config.max_field_section_size == DEFAULT_MAX_FIELD_SECTION_SIZE
        assert config.max_pending_capsules == DEFAULT_MAX_PENDING_CAPSULES
        assert config.max_pending_datagrams == DEFAULT_MAX_PENDING_DATAGRAMS
        assert config.max_pending_streams == DEFAULT_MAX_PENDING_STREAMS
        assert config.max_sessions == DEFAULT_MAX_SESSIONS
        assert config.quic_max_concurrent_bidi_streams == DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS
        assert config.quic_max_concurrent_uni_streams == DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS
        assert config.quic_receive_window == DEFAULT_QUIC_RECEIVE_WINDOW
        assert config.quic_send_window == DEFAULT_QUIC_SEND_WINDOW
        assert config.quic_stream_receive_window == DEFAULT_QUIC_STREAM_RECEIVE_WINDOW
        assert config.wt_available_protocols is None
        assert config.user_agent is None
        assert config.verify_mode == ssl.CERT_REQUIRED

    def test_from_dict_method(self) -> None:
        config_dict = {"max_sessions": 5, "unknown_field": "should_be_ignored"}

        config = ClientConfig.from_dict(config_dict=config_dict)

        assert config.max_sessions == 5
        assert not hasattr(config, "unknown_field")

    def test_from_dict_missing_type_hint(self) -> None:
        def mock_get_type_hints(obj: Any) -> dict[str, Any]:
            hints = get_type_hints(obj)
            if "max_sessions" in hints:
                del hints["max_sessions"]
            return hints

        with patch(target="pywebtransport.config.get_type_hints", side_effect=mock_get_type_hints):
            config = ClientConfig.from_dict(config_dict={"max_sessions": 5})

            assert config.max_sessions == 5

    def test_from_dict_multi_union_ignored(self) -> None:
        def mock_get_type_hints(obj: Any) -> dict[str, Any]:
            hints = get_type_hints(obj)
            hints["max_connections"] = Union[int, str]
            return hints

        with patch(target="pywebtransport.config.get_type_hints", side_effect=mock_get_type_hints):
            config = ClientConfig.from_dict(config_dict={"max_connections": 5})

            assert config.max_connections == 5

    def test_from_dict_optional_field_resolution(self) -> None:
        config_dict = {"ca_certs": "dummy.pem", "max_sessions": 5}

        config = ClientConfig.from_dict(config_dict=config_dict)

        assert config.ca_certs == "dummy.pem"

    def test_headers_remain_as_provided(self) -> None:
        headers: Headers = {"User-Agent": "Custom/1.0", "X-Custom": "Value"}

        config = ClientConfig(headers=headers)

        assert config.headers == headers
        assert isinstance(config.headers, dict)
        assert config.headers["X-Custom"] == "Value"
        assert config.user_agent is None

    def test_initialization_with_none_timeout(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem", keep_alive_interval=None, read_timeout=None)

        assert config.keep_alive_interval is None
        assert config.read_timeout is None

        config.validate()

    def test_wt_available_protocols_initialization_success(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem", wt_available_protocols=["dummy", "h3"])
        config.validate()
        assert config.wt_available_protocols == ["dummy", "h3"]

    def test_to_dict_method(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem", verify_mode=ssl.CERT_OPTIONAL)

        data = config.to_dict()

        assert data["verify_mode"] == "CERT_OPTIONAL"

    def test_update_method(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem")

        new_config = config.update(connect_timeout=15.0)

        assert config.connect_timeout == DEFAULT_CONNECT_TIMEOUT
        assert new_config.connect_timeout == 15.0
        assert new_config is not config

        with pytest.raises(expected_exception=ConfigurationError, match="cfg validate invalid actual=unknown_key"):
            config.update(unknown_key="value")

    @pytest.mark.parametrize(
        argnames="invalid_attrs, error_match",
        argvalues=[
            ({"alpn_protocols": []}, "cfg_alpn_protocols validate invalid"),
            ({"certfile": None, "keyfile": "k.key"}, "cfg_certfile validate invalid"),
            ({"certfile": "a.pem", "keyfile": None}, "cfg_keyfile validate invalid"),
            ({"close_timeout": -1}, "cfg_close_timeout validate invalid"),
            ({"congestion_control_algorithm": "invalid_algo"}, "cfg_congestion_control_algorithm validate invalid"),
            ({"connect_timeout": -1}, "cfg_connect_timeout validate invalid"),
            ({"connect_timeout": "invalid"}, "cfg_connect_timeout validate invalid"),
            ({"connection_attempt_delay": -0.5}, "cfg_connection_attempt_delay validate invalid"),
            ({"connection_attempt_delay": "invalid"}, "cfg_connection_attempt_delay validate invalid"),
            ({"connection_idle_timeout": 0}, "cfg_connection_idle_timeout validate invalid"),
            ({"event_history_capacity": -1}, "cfg_event_history_capacity validate invalid"),
            ({"event_queue_capacity": 0}, "cfg_event_queue_capacity validate invalid"),
            ({"flow_control_window": 0}, "cfg_flow_control_window validate invalid"),
            ({"initial_max_data": -1}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_data": QUIC_VARINT_LIMIT + 1}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_streams_bidi": -1}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_bidi": WT_STREAMS_LIMIT + 1}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_uni": -1}, "cfg_initial_max_streams_uni validate invalid"),
            ({"initial_max_streams_uni": WT_STREAMS_LIMIT + 1}, "cfg_initial_max_streams_uni validate invalid"),
            ({"keep_alive_interval": -1}, "cfg_keep_alive_interval validate invalid"),
            ({"keep_alive_interval": "invalid"}, "cfg_keep_alive_interval validate invalid"),
            ({"max_capsule_size": 0}, "cfg_max_capsule_size validate invalid"),
            ({"max_connections": 0}, "cfg_max_connections validate invalid"),
            ({"max_datagram_size": 0}, "cfg_max_datagram_size validate invalid"),
            ({"max_datagram_size": UDP_MAX_DATAGRAM_SIZE + 1}, "cfg_max_datagram_size validate invalid"),
            ({"max_event_listeners": 0}, "cfg_max_event_listeners validate invalid"),
            ({"max_field_section_size": 0}, "cfg_max_field_section_size validate invalid"),
            ({"max_field_section_size": _FIELD_SECTION_SIZE_LIMIT + 1}, "cfg_max_field_section_size validate invalid"),
            ({"max_pending_capsules": 0}, "cfg_max_pending_capsules validate invalid"),
            ({"max_pending_datagrams": 0}, "cfg_max_pending_datagrams validate invalid"),
            ({"max_pending_streams": 0}, "cfg_max_pending_streams validate invalid"),
            ({"max_session_pending_events": 0}, "cfg_max_session_pending_events validate invalid"),
            ({"max_sessions": 0}, "cfg_max_sessions validate invalid"),
            ({"max_stream_read_buffer_size": 0}, "cfg_max_stream_read_buffer_size validate invalid"),
            ({"max_stream_write_buffer_size": 0}, "cfg_max_stream_write_buffer_size validate invalid"),
            ({"max_total_pending_events": 0}, "cfg_max_total_pending_events validate invalid"),
            ({"pending_event_ttl": 0}, "cfg_pending_event_ttl validate invalid"),
            (
                {"quic_max_concurrent_bidi_streams": WT_SESSION_CONTROL_BIDI_STREAM_COUNT - 1},
                "cfg_quic_max_concurrent_bidi_streams validate invalid",
            ),
            (
                {"quic_max_concurrent_bidi_streams": WT_STREAMS_LIMIT + 1},
                "cfg_quic_max_concurrent_bidi_streams validate invalid",
            ),
            (
                {"quic_max_concurrent_uni_streams": H3_MIN_UNI_STREAM_COUNT - 1},
                "cfg_quic_max_concurrent_uni_streams validate invalid",
            ),
            (
                {"quic_max_concurrent_uni_streams": WT_STREAMS_LIMIT + 1},
                "cfg_quic_max_concurrent_uni_streams validate invalid",
            ),
            ({"quic_receive_window": 0}, "cfg_quic_receive_window validate invalid"),
            ({"quic_send_window": 0}, "cfg_quic_send_window validate invalid"),
            ({"quic_stream_receive_window": 0}, "cfg_quic_stream_receive_window validate invalid"),
            ({"resource_cleanup_interval": -1}, "cfg_resource_cleanup_interval validate invalid"),
            ({"stream_creation_timeout": -1}, "cfg_stream_creation_timeout validate invalid"),
            ({"verify_mode": "INVALID"}, "cfg_verify_mode validate invalid"),
            ({"write_timeout": -1}, "cfg_write_timeout validate invalid"),
            ({"wt_available_protocols": "not_a_list"}, "cfg_wt_available_protocols validate invalid"),
            ({"wt_available_protocols": [1, 2]}, "cfg_wt_available_protocols validate invalid"),
        ],
    )
    def test_validation_failures(self, invalid_attrs: dict[str, Any], error_match: str) -> None:
        base_config = ClientConfig().to_dict()
        base_config["ca_certs"] = "dummy.pem"
        base_config["verify_mode"] = ssl.CERT_REQUIRED
        test_config = {**base_config, **invalid_attrs}

        config = ClientConfig(**test_config)

        with pytest.raises(expected_exception=ConfigurationError, match=error_match):
            config.validate()


class TestServerConfig:

    def test_default_initialization(self) -> None:
        config = ServerConfig(certfile="dummy.crt", keyfile="dummy.key")

        assert config.alpn_protocols == DEFAULT_ALPN_PROTOCOLS
        assert config.bind_host == "::"
        assert config.congestion_control_algorithm == "cubic"
        assert config.flow_control_window == DEFAULT_FLOW_CONTROL_WINDOW
        assert config.initial_max_data == DEFAULT_INITIAL_MAX_DATA
        assert config.keep_alive_interval == DEFAULT_KEEP_ALIVE_INTERVAL
        assert config.max_capsule_size == DEFAULT_MAX_CAPSULE_SIZE
        assert config.max_connections == DEFAULT_MAX_CONNECTIONS
        assert config.max_field_section_size == DEFAULT_MAX_FIELD_SECTION_SIZE
        assert config.max_pending_capsules == DEFAULT_MAX_PENDING_CAPSULES
        assert config.max_pending_datagrams == DEFAULT_MAX_PENDING_DATAGRAMS
        assert config.max_pending_streams == DEFAULT_MAX_PENDING_STREAMS

    def test_from_dict_coercion(self) -> None:
        config_dict = {"bind_port": "8080", "certfile": "dummy.crt", "keyfile": "dummy.key"}

        config = ServerConfig.from_dict(config_dict=config_dict)

        assert config.bind_port == 8080

    def test_from_dict_enum_conversion_failure_ignored(self) -> None:
        config_dict = {"bind_port": 8080, "certfile": "c", "keyfile": "k", "verify_mode": "INVALID_MODE"}

        config = ServerConfig.from_dict(config_dict=config_dict)

        assert getattr(config, "verify_mode") == "INVALID_MODE"

        with pytest.raises(expected_exception=ConfigurationError, match="cfg_verify_mode validate invalid"):
            config.validate()

    def test_from_dict_enum_conversion_success(self) -> None:
        config_dict = {"bind_port": 8080, "certfile": "c", "keyfile": "k", "verify_mode": "CERT_NONE"}

        config = ServerConfig.from_dict(config_dict=config_dict)

        assert config.verify_mode == ssl.CERT_NONE
        assert isinstance(config.verify_mode, ssl.VerifyMode)

    def test_from_dict_filtering_extra_keys(self) -> None:
        config_dict = {
            "certfile": "dummy.crt",
            "keyfile": "dummy.key",
            "max_connections": 500,
            "unknown_field": "should_be_ignored",
        }

        config = ServerConfig.from_dict(config_dict=config_dict)

        assert config.max_connections == 500
        assert not hasattr(config, "unknown_field")

    def test_from_dict_invalid_port_raises_error(self) -> None:
        config_dict = {"bind_port": "invalid", "certfile": "dummy.crt", "keyfile": "dummy.key"}

        config = ServerConfig.from_dict(config_dict=config_dict)

        with pytest.raises(expected_exception=ConfigurationError, match="cfg_bind_port validate invalid"):
            config.validate()

    def test_from_dict_union_enum_resolution(self) -> None:
        def mock_get_type_hints(obj: Any) -> dict[str, Any]:
            hints = get_type_hints(obj)
            hints["bind_host"] = Union[int, str]
            hints["keyfile"] = Union[int, ssl.VerifyMode]
            hints["verify_mode"] = Union[ssl.VerifyMode, str]
            return hints

        with patch(target="pywebtransport.config.get_type_hints", side_effect=mock_get_type_hints):
            config1 = ServerConfig.from_dict(config_dict={"certfile": "c", "keyfile": "k", "verify_mode": "CERT_NONE"})
            assert config1.verify_mode == ssl.CERT_NONE

            config2 = ServerConfig.from_dict(config_dict={"bind_host": "localhost", "certfile": "c", "keyfile": "k"})
            assert config2.bind_host == "localhost"

            config3 = ServerConfig.from_dict(config_dict={"certfile": "c", "keyfile": "CERT_OPTIONAL"})
            assert config3.keyfile == ssl.CERT_OPTIONAL  # type: ignore[comparison-overlap]

    def test_initialization_fails_without_bind_host(self) -> None:
        config = ServerConfig(bind_host="", certfile="c", keyfile="k")

        with pytest.raises(expected_exception=ConfigurationError, match="cfg_bind_host validate invalid"):
            config.validate()

    def test_initialization_fails_without_certs(self) -> None:
        config1 = ServerConfig(certfile=None, keyfile="k")  # type: ignore[arg-type]

        with pytest.raises(expected_exception=ConfigurationError, match="cfg_certfile validate invalid"):
            config1.validate()

        config2 = ServerConfig(certfile="c", keyfile=None)  # type: ignore[arg-type]

        with pytest.raises(expected_exception=ConfigurationError, match="cfg_keyfile validate invalid"):
            config2.validate()

    def test_to_dict_method(self) -> None:
        config = ServerConfig(certfile="d.crt", keyfile="d.key", verify_mode=ssl.CERT_REQUIRED)

        data = config.to_dict()

        assert data["verify_mode"] == "CERT_REQUIRED"

    def test_update_method_failure(self) -> None:
        config = ServerConfig(certfile="d.crt", keyfile="d.key")

        with pytest.raises(expected_exception=ConfigurationError, match="cfg validate invalid actual=unknown_key"):
            config.update(unknown_key="value")

    def test_update_method_success(self) -> None:
        config = ServerConfig(certfile="d.crt", keyfile="d.key")

        new_config = config.update(max_connections=500)

        assert config.max_connections == DEFAULT_MAX_CONNECTIONS
        assert new_config.max_connections == 500
        assert new_config is not config

    @pytest.mark.parametrize(
        argnames="invalid_attrs, error_match",
        argvalues=[
            ({"alpn_protocols": []}, "cfg_alpn_protocols validate invalid"),
            ({"bind_host": ""}, "cfg_bind_host validate invalid"),
            ({"bind_port": 0}, "cfg_bind_port validate invalid"),
            ({"bind_port": "invalid"}, "cfg_bind_port validate invalid"),
            ({"ca_certs": None, "verify_mode": ssl.CERT_REQUIRED}, "cfg_ca_certs validate invalid"),
            ({"congestion_control_algorithm": "invalid_algo"}, "cfg_congestion_control_algorithm validate invalid"),
            ({"event_history_capacity": -1}, "cfg_event_history_capacity validate invalid"),
            ({"event_queue_capacity": 0}, "cfg_event_queue_capacity validate invalid"),
            ({"flow_control_window": 0}, "cfg_flow_control_window validate invalid"),
            ({"initial_max_data": -1}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_data": QUIC_VARINT_LIMIT + 1}, "cfg_initial_max_data validate invalid"),
            ({"initial_max_streams_bidi": -1}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_bidi": WT_STREAMS_LIMIT + 1}, "cfg_initial_max_streams_bidi validate invalid"),
            ({"initial_max_streams_uni": -1}, "cfg_initial_max_streams_uni validate invalid"),
            ({"initial_max_streams_uni": WT_STREAMS_LIMIT + 1}, "cfg_initial_max_streams_uni validate invalid"),
            ({"keep_alive_interval": -1}, "cfg_keep_alive_interval validate invalid"),
            ({"max_capsule_size": 0}, "cfg_max_capsule_size validate invalid"),
            ({"max_connections": 0}, "cfg_max_connections validate invalid"),
            ({"max_datagram_size": 0}, "cfg_max_datagram_size validate invalid"),
            ({"max_datagram_size": UDP_MAX_DATAGRAM_SIZE + 1}, "cfg_max_datagram_size validate invalid"),
            ({"max_event_listeners": 0}, "cfg_max_event_listeners validate invalid"),
            ({"max_field_section_size": 0}, "cfg_max_field_section_size validate invalid"),
            ({"max_field_section_size": _FIELD_SECTION_SIZE_LIMIT + 1}, "cfg_max_field_section_size validate invalid"),
            ({"max_pending_capsules": 0}, "cfg_max_pending_capsules validate invalid"),
            ({"max_pending_datagrams": 0}, "cfg_max_pending_datagrams validate invalid"),
            ({"max_pending_streams": 0}, "cfg_max_pending_streams validate invalid"),
            ({"max_session_pending_events": 0}, "cfg_max_session_pending_events validate invalid"),
            ({"max_sessions": 0}, "cfg_max_sessions validate invalid"),
            ({"max_stream_read_buffer_size": 0}, "cfg_max_stream_read_buffer_size validate invalid"),
            ({"max_stream_write_buffer_size": 0}, "cfg_max_stream_write_buffer_size validate invalid"),
            ({"max_total_pending_events": 0}, "cfg_max_total_pending_events validate invalid"),
            ({"pending_event_ttl": -1.0}, "cfg_pending_event_ttl validate invalid"),
            (
                {"quic_max_concurrent_bidi_streams": WT_SESSION_CONTROL_BIDI_STREAM_COUNT - 1},
                "cfg_quic_max_concurrent_bidi_streams validate invalid",
            ),
            (
                {"quic_max_concurrent_bidi_streams": WT_STREAMS_LIMIT + 1},
                "cfg_quic_max_concurrent_bidi_streams validate invalid",
            ),
            (
                {"quic_max_concurrent_uni_streams": H3_MIN_UNI_STREAM_COUNT - 1},
                "cfg_quic_max_concurrent_uni_streams validate invalid",
            ),
            (
                {"quic_max_concurrent_uni_streams": WT_STREAMS_LIMIT + 1},
                "cfg_quic_max_concurrent_uni_streams validate invalid",
            ),
            ({"quic_receive_window": 0}, "cfg_quic_receive_window validate invalid"),
            ({"quic_send_window": 0}, "cfg_quic_send_window validate invalid"),
            ({"quic_stream_receive_window": 0}, "cfg_quic_stream_receive_window validate invalid"),
            ({"read_timeout": "invalid"}, "cfg_read_timeout validate invalid"),
            ({"verify_mode": "INVALID"}, "cfg_verify_mode validate invalid"),
        ],
    )
    def test_validation_failures(self, invalid_attrs: dict[str, Any], error_match: str) -> None:
        base_config = ServerConfig(certfile="dummy.crt", keyfile="dummy.key").to_dict()
        base_config["verify_mode"] = ssl.CERT_NONE
        test_config = {**base_config, **invalid_attrs}

        config = ServerConfig(**test_config)

        with pytest.raises(expected_exception=ConfigurationError, match=error_match):
            config.validate()
