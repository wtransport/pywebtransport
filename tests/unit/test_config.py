"""Unit tests for the pywebtransport.config module."""

import ssl
from typing import Any, Union, get_type_hints
from unittest.mock import patch

import pytest

from pywebtransport import ClientConfig, ConfigurationError, Headers, ServerConfig
from pywebtransport.constants import (
    DEFAULT_ALPN_PROTOCOLS,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_CONNECTION_ATTEMPT_DELAY,
    DEFAULT_FLOW_CONTROL_WINDOW_SIZE,
    DEFAULT_INITIAL_MAX_DATA,
    DEFAULT_KEEP_ALIVE,
    DEFAULT_MAX_CAPSULE_SIZE,
    DEFAULT_SERVER_MAX_CONNECTIONS,
    DEFAULT_TRANSPORT_STREAMS_CAP,
    MAX_DATAGRAM_SIZE,
    MAX_PROTOCOL_STREAMS_LIMIT,
)


class TestClientConfig:

    def test_copy_method(self) -> None:
        config1 = ClientConfig(alpn_protocols=["h3"], ca_certs="dummy.pem")

        config2 = config1.copy()
        config2.max_connection_retries = 99
        config2.alpn_protocols.append("h2")

        assert config1 is not config2
        assert config1.max_connection_retries != 99
        assert config1.alpn_protocols == ["h3"]
        assert config2.alpn_protocols == ["h3", "h2"]

    def test_default_initialization(self) -> None:
        config = ClientConfig()

        assert config.connect_timeout == DEFAULT_CONNECT_TIMEOUT
        assert config.connection_attempt_delay == DEFAULT_CONNECTION_ATTEMPT_DELAY
        assert config.verify_mode == ssl.CERT_REQUIRED
        assert config.user_agent is None
        assert config.headers == {}
        assert config.congestion_control_algorithm == "cubic"
        assert config.flow_control_window_size == DEFAULT_FLOW_CONTROL_WINDOW_SIZE
        assert config.initial_max_data == DEFAULT_INITIAL_MAX_DATA
        assert config.keep_alive == DEFAULT_KEEP_ALIVE
        assert config.max_capsule_size == DEFAULT_MAX_CAPSULE_SIZE
        assert config.alpn_protocols == DEFAULT_ALPN_PROTOCOLS
        assert config.transport_streams_cap == DEFAULT_TRANSPORT_STREAMS_CAP
        assert config.subprotocols is None

    def test_from_dict_method(self) -> None:
        config_dict = {"max_connection_retries": 5, "unknown_field": "should_be_ignored"}

        config = ClientConfig.from_dict(config_dict=config_dict)

        assert config.max_connection_retries == 5
        assert not hasattr(config, "unknown_field")

    def test_from_dict_missing_type_hint(self) -> None:
        def mock_get_type_hints(obj: Any) -> dict[str, Any]:
            hints = get_type_hints(obj)
            if "max_connection_retries" in hints:
                del hints["max_connection_retries"]
            return hints

        with patch(target="pywebtransport.config.get_type_hints", side_effect=mock_get_type_hints):
            config = ClientConfig.from_dict(config_dict={"max_connection_retries": 5})

            assert config.max_connection_retries == 5

    def test_from_dict_multi_union_ignored(self) -> None:
        def mock_get_type_hints(obj: Any) -> dict[str, Any]:
            hints = get_type_hints(obj)
            hints["max_connections"] = Union[int, str]
            return hints

        with patch(target="pywebtransport.config.get_type_hints", side_effect=mock_get_type_hints):
            config = ClientConfig.from_dict(config_dict={"max_connections": 5})

            assert config.max_connections == 5

    def test_from_dict_optional_field_resolution(self) -> None:
        config_dict = {"ca_certs": "dummy.pem", "max_connection_retries": 5}

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
        config = ClientConfig(ca_certs="dummy.pem", keep_alive=None, read_timeout=None)

        assert config.read_timeout is None
        assert config.keep_alive is None

        config.validate()

    def test_subprotocols_initialization_success(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem", subprotocols=["dummy", "h3"])
        config.validate()
        assert config.subprotocols == ["dummy", "h3"]

    def test_to_dict_method(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem", verify_mode=ssl.CERT_OPTIONAL)

        data = config.to_dict()

        assert data["verify_mode"] == "CERT_OPTIONAL"

    def test_update_method(self) -> None:
        config = ClientConfig(ca_certs="dummy.pem")

        new_config = config.update(connect_timeout=15.0)

        assert new_config.connect_timeout == 15.0
        assert config.connect_timeout == DEFAULT_CONNECT_TIMEOUT
        assert new_config is not config

        with pytest.raises(expected_exception=ConfigurationError, match="Unknown configuration key"):
            config.update(unknown_key="value")

    @pytest.mark.parametrize(
        argnames="invalid_attrs, error_match",
        argvalues=[
            ({"alpn_protocols": []}, "cannot be empty"),
            ({"certfile": "a.pem", "keyfile": None}, "must be provided together"),
            ({"congestion_control_algorithm": "invalid_algo"}, "must be one of"),
            ({"connect_timeout": -1}, "Timeout must be positive"),
            ({"connect_timeout": "invalid"}, "Timeout must be a number"),
            ({"connection_attempt_delay": -0.5}, "Timeout must be positive"),
            ({"connection_attempt_delay": "invalid"}, "Timeout must be a number"),
            ({"connection_idle_timeout": 0}, "Timeout must be positive"),
            ({"flow_control_window_size": 0}, "must be positive"),
            ({"keep_alive": -1}, "Timeout must be positive"),
            ({"keep_alive": "invalid"}, "Timeout must be a number"),
            ({"max_capsule_size": 0}, "must be positive"),
            ({"max_connections": 0}, "must be positive"),
            ({"max_datagram_size": 0}, "must be between 1 and"),
            ({"max_datagram_size": MAX_DATAGRAM_SIZE + 1}, "must be between 1 and"),
            ({"max_event_history_size": -1}, "must be non-negative"),
            ({"max_event_listeners": 0}, "must be positive"),
            ({"max_event_queue_size": 0}, "must be positive"),
            ({"max_message_size": 0}, "must be positive"),
            ({"max_pending_events_per_session": 0}, "must be positive"),
            ({"max_sessions": 0}, "must be positive"),
            ({"max_stream_read_buffer": 0}, "must be positive"),
            ({"max_stream_write_buffer": 0}, "must be positive"),
            ({"max_total_pending_events": 0}, "must be positive"),
            ({"pending_event_ttl": 0}, "Timeout must be positive"),
            ({"subprotocols": "not_a_list"}, "must be a list of strings"),
            ({"subprotocols": [1, 2]}, "must be a list of strings"),
            ({"transport_streams_cap": 0}, "must be between 1 and"),
            ({"transport_streams_cap": MAX_PROTOCOL_STREAMS_LIMIT + 1}, "must be between 1 and"),
            ({"verify_mode": "INVALID"}, "unknown SSL verify mode"),
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

    @pytest.mark.parametrize(
        argnames="invalid_attrs, error_match",
        argvalues=[
            ({"max_connection_retries": -1}, "must be non-negative"),
            ({"max_retry_delay": -10.0}, "must be positive"),
            ({"retry_backoff": 0.9}, "must be >= 1.0"),
            ({"retry_delay": 0}, "must be positive"),
        ],
    )
    def test_validation_failures_retry_logic(self, invalid_attrs: dict[str, Any], error_match: str) -> None:
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

        assert config.bind_host == "::"
        assert config.max_connections == DEFAULT_SERVER_MAX_CONNECTIONS
        assert config.congestion_control_algorithm == "cubic"
        assert config.flow_control_window_size == DEFAULT_FLOW_CONTROL_WINDOW_SIZE
        assert config.initial_max_data == DEFAULT_INITIAL_MAX_DATA
        assert config.keep_alive == DEFAULT_KEEP_ALIVE
        assert config.max_capsule_size == DEFAULT_MAX_CAPSULE_SIZE
        assert config.alpn_protocols == DEFAULT_ALPN_PROTOCOLS
        assert config.transport_streams_cap == DEFAULT_TRANSPORT_STREAMS_CAP

    def test_from_dict_coercion(self) -> None:
        config_dict = {"bind_port": "8080", "certfile": "dummy.crt", "keyfile": "dummy.key"}

        config = ServerConfig.from_dict(config_dict=config_dict)

        assert config.bind_port == 8080

    def test_from_dict_enum_conversion_failure_ignored(self) -> None:
        config_dict = {"bind_port": 8080, "certfile": "c", "keyfile": "k", "verify_mode": "INVALID_MODE"}

        config = ServerConfig.from_dict(config_dict=config_dict)

        assert config.verify_mode == "INVALID_MODE"  # type: ignore[comparison-overlap]

        with pytest.raises(expected_exception=ConfigurationError, match="unknown SSL verify mode"):
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

        with pytest.raises(expected_exception=ConfigurationError, match="Port must be an integer"):
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

        with pytest.raises(expected_exception=ConfigurationError, match="cannot be empty"):
            config.validate()

    def test_initialization_fails_without_certs(self) -> None:
        config = ServerConfig(certfile=None, keyfile=None)  # type: ignore[arg-type]

        with pytest.raises(
            expected_exception=ConfigurationError, match="Server requires both certificate and key files"
        ):
            config.validate()

    def test_to_dict_method(self) -> None:
        config = ServerConfig(certfile="d.crt", keyfile="d.key", verify_mode=ssl.CERT_REQUIRED)

        data = config.to_dict()

        assert data["verify_mode"] == "CERT_REQUIRED"

    def test_update_method_failure(self) -> None:
        config = ServerConfig(certfile="d.crt", keyfile="d.key")

        with pytest.raises(expected_exception=ConfigurationError, match="Unknown configuration key"):
            config.update(unknown_key="value")

    def test_update_method_success(self) -> None:
        config = ServerConfig(certfile="d.crt", keyfile="d.key")

        new_config = config.update(max_connections=500)

        assert new_config.max_connections == 500
        assert config.max_connections == DEFAULT_SERVER_MAX_CONNECTIONS
        assert new_config is not config

    @pytest.mark.parametrize(
        argnames="invalid_attrs, error_match",
        argvalues=[
            ({"alpn_protocols": []}, "cannot be empty"),
            ({"bind_host": ""}, "cannot be empty"),
            ({"bind_port": 0}, "must be an integer"),
            ({"bind_port": "invalid"}, "must be an integer"),
            ({"ca_certs": None, "verify_mode": ssl.CERT_REQUIRED}, "Server requires 'ca_certs' for mTLS"),
            ({"congestion_control_algorithm": "invalid_algo"}, "must be one of"),
            ({"flow_control_window_size": 0}, "must be positive"),
            ({"keep_alive": -1}, "Timeout must be positive"),
            ({"max_capsule_size": 0}, "must be positive"),
            ({"max_connections": 0}, "must be positive"),
            ({"max_datagram_size": 0}, "must be between 1 and"),
            ({"max_datagram_size": MAX_DATAGRAM_SIZE + 1}, "must be between 1 and"),
            ({"max_event_history_size": -1}, "must be non-negative"),
            ({"max_event_listeners": 0}, "must be positive"),
            ({"max_event_queue_size": 0}, "must be positive"),
            ({"max_message_size": 0}, "must be positive"),
            ({"max_pending_events_per_session": 0}, "must be positive"),
            ({"max_sessions": 0}, "must be positive"),
            ({"max_stream_read_buffer": 0}, "must be positive"),
            ({"max_stream_write_buffer": 0}, "must be positive"),
            ({"max_total_pending_events": 0}, "must be positive"),
            ({"pending_event_ttl": -1.0}, "Timeout must be positive"),
            ({"read_timeout": "invalid"}, "Timeout must be a number"),
            ({"transport_streams_cap": 0}, "must be between 1 and"),
            ({"transport_streams_cap": MAX_PROTOCOL_STREAMS_LIMIT + 1}, "must be between 1 and"),
            ({"verify_mode": "INVALID"}, "unknown SSL verify mode"),
        ],
    )
    def test_validation_failures(self, invalid_attrs: dict[str, Any], error_match: str) -> None:
        base_config = ServerConfig(certfile="dummy.crt", keyfile="dummy.key").to_dict()
        base_config["verify_mode"] = ssl.CERT_NONE
        test_config = {**base_config, **invalid_attrs}

        config = ServerConfig(**test_config)

        with pytest.raises(expected_exception=ConfigurationError, match=error_match):
            config.validate()
