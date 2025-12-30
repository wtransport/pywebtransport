"""Structured configuration objects for clients and servers."""

from __future__ import annotations

import copy
import ssl
import types
from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Self, Union, get_args, get_origin, get_type_hints

from pywebtransport.constants import (
    DEFAULT_ALPN_PROTOCOLS,
    DEFAULT_BIND_HOST,
    DEFAULT_CERTFILE,
    DEFAULT_CLIENT_MAX_CONNECTIONS,
    DEFAULT_CLIENT_MAX_SESSIONS,
    DEFAULT_CLIENT_VERIFY_MODE,
    DEFAULT_CLOSE_TIMEOUT,
    DEFAULT_CONGESTION_CONTROL_ALGORITHM,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_CONNECTION_IDLE_TIMEOUT,
    DEFAULT_DEV_PORT,
    DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE,
    DEFAULT_FLOW_CONTROL_WINDOW_SIZE,
    DEFAULT_INITIAL_MAX_DATA,
    DEFAULT_INITIAL_MAX_STREAMS_BIDI,
    DEFAULT_INITIAL_MAX_STREAMS_UNI,
    DEFAULT_KEEP_ALIVE,
    DEFAULT_KEYFILE,
    DEFAULT_LOG_LEVEL,
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
    DEFAULT_SERVER_VERIFY_MODE,
    DEFAULT_STREAM_CREATION_TIMEOUT,
    DEFAULT_STREAM_FLOW_CONTROL_INCREMENT_BIDI,
    DEFAULT_STREAM_FLOW_CONTROL_INCREMENT_UNI,
    DEFAULT_WRITE_TIMEOUT,
    SUPPORTED_CONGESTION_CONTROL_ALGORITHMS,
)
from pywebtransport.exceptions import ConfigurationError
from pywebtransport.types import Headers

__all__: list[str] = ["BaseConfig", "ClientConfig", "ServerConfig"]


@dataclass(kw_only=True)
class BaseConfig(ABC):
    """Base configuration class sharing common fields and logic."""

    alpn_protocols: list[str] = field(default_factory=lambda: list(DEFAULT_ALPN_PROTOCOLS))
    ca_certs: str | None = None
    certfile: str | None = DEFAULT_CERTFILE
    close_timeout: float = DEFAULT_CLOSE_TIMEOUT
    congestion_control_algorithm: str = DEFAULT_CONGESTION_CONTROL_ALGORITHM
    connection_idle_timeout: float = DEFAULT_CONNECTION_IDLE_TIMEOUT
    flow_control_window_auto_scale: bool = DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE
    flow_control_window_size: int = DEFAULT_FLOW_CONTROL_WINDOW_SIZE
    initial_max_data: int = DEFAULT_INITIAL_MAX_DATA
    initial_max_streams_bidi: int = DEFAULT_INITIAL_MAX_STREAMS_BIDI
    initial_max_streams_uni: int = DEFAULT_INITIAL_MAX_STREAMS_UNI
    keep_alive: bool = DEFAULT_KEEP_ALIVE
    keyfile: str | None = DEFAULT_KEYFILE
    log_level: str = DEFAULT_LOG_LEVEL
    max_connections: int
    max_datagram_size: int = DEFAULT_MAX_DATAGRAM_SIZE
    max_event_history_size: int = DEFAULT_MAX_EVENT_HISTORY_SIZE
    max_event_listeners: int = DEFAULT_MAX_EVENT_LISTENERS
    max_event_queue_size: int = DEFAULT_MAX_EVENT_QUEUE_SIZE
    max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE
    max_pending_events_per_session: int = DEFAULT_MAX_PENDING_EVENTS_PER_SESSION
    max_sessions: int
    max_stream_read_buffer: int = DEFAULT_MAX_STREAM_READ_BUFFER
    max_stream_write_buffer: int = DEFAULT_MAX_STREAM_WRITE_BUFFER
    max_total_pending_events: int = DEFAULT_MAX_TOTAL_PENDING_EVENTS
    pending_event_ttl: float = DEFAULT_PENDING_EVENT_TTL
    read_timeout: float | None = DEFAULT_READ_TIMEOUT
    resource_cleanup_interval: float = DEFAULT_RESOURCE_CLEANUP_INTERVAL
    stream_creation_timeout: float = DEFAULT_STREAM_CREATION_TIMEOUT
    stream_flow_control_increment_bidi: int = DEFAULT_STREAM_FLOW_CONTROL_INCREMENT_BIDI
    stream_flow_control_increment_uni: int = DEFAULT_STREAM_FLOW_CONTROL_INCREMENT_UNI
    write_timeout: float | None = DEFAULT_WRITE_TIMEOUT

    @classmethod
    def from_dict(cls, *, config_dict: dict[str, Any]) -> Self:
        """Create a configuration instance from a dictionary."""
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_dict = {k: v for k, v in config_dict.items() if k in valid_keys}

        type_hints = get_type_hints(cls)

        for key, value in filtered_dict.items():
            if key not in type_hints:
                continue

            target_type = type_hints[key]
            origin = get_origin(target_type)

            if origin is types.UnionType or origin is Union:
                args = [arg for arg in get_args(target_type) if arg is not type(None)]
                if len(args) == 1:
                    target_type = args[0]
                elif isinstance(value, str):
                    for arg in args:
                        if isinstance(arg, type) and issubclass(arg, Enum):
                            target_type = arg
                            break

            if isinstance(value, str) and isinstance(target_type, type) and issubclass(target_type, Enum):
                try:
                    filtered_dict[key] = target_type[value]
                except KeyError:
                    pass

        return cls(**filtered_dict)

    def copy(self) -> Self:
        """Create a deep copy of the configuration."""
        return copy.deepcopy(x=self)

    def to_dict(self) -> dict[str, Any]:
        """Convert the configuration to a dictionary."""
        result = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            match value:
                case ssl.VerifyMode():
                    result[field_name] = value.name
                case _:
                    result[field_name] = value
        return result

    def update(self, **kwargs: Any) -> Self:
        """Create a new config with updated values."""
        new_config = self.copy()
        for key, value in kwargs.items():
            if hasattr(new_config, key):
                setattr(new_config, key, value)
            else:
                raise ConfigurationError(
                    message=f"Unknown configuration key: '{key}'", config_key=key, config_value=value
                )
        new_config.validate()
        return new_config

    def validate(self) -> None:
        """Validate configuration options common to all config types."""
        if not self.alpn_protocols:
            raise ConfigurationError(
                message="Invalid value for 'alpn_protocols': cannot be empty",
                config_key="alpn_protocols",
                config_value=self.alpn_protocols,
            )

        if self.congestion_control_algorithm not in SUPPORTED_CONGESTION_CONTROL_ALGORITHMS:
            raise ConfigurationError(
                message=(
                    f"Invalid value for 'congestion_control_algorithm': "
                    f"must be one of {SUPPORTED_CONGESTION_CONTROL_ALGORITHMS}"
                ),
                config_key="congestion_control_algorithm",
                config_value=self.congestion_control_algorithm,
            )

        timeouts_to_check = [
            "close_timeout",
            "connection_idle_timeout",
            "pending_event_ttl",
            "read_timeout",
            "resource_cleanup_interval",
            "stream_creation_timeout",
            "write_timeout",
        ]

        for timeout_name in timeouts_to_check:
            try:
                _validate_timeout(timeout=getattr(self, timeout_name))
            except (ValueError, TypeError) as e:
                raise ConfigurationError(
                    message=f"Invalid value for '{timeout_name}': {e}",
                    config_key=timeout_name,
                    config_value=getattr(self, timeout_name),
                ) from e

        if self.flow_control_window_size <= 0:
            raise ConfigurationError(
                message="Invalid value for 'flow_control_window_size': must be positive",
                config_key="flow_control_window_size",
                config_value=self.flow_control_window_size,
            )

        if self.max_connections <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_connections': must be positive",
                config_key="max_connections",
                config_value=self.max_connections,
            )

        if self.max_sessions <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_sessions': must be positive",
                config_key="max_sessions",
                config_value=self.max_sessions,
            )

        if self.max_datagram_size <= 0 or self.max_datagram_size > 65535:
            raise ConfigurationError(
                message="Invalid value for 'max_datagram_size': must be between 1 and 65535",
                config_key="max_datagram_size",
                config_value=self.max_datagram_size,
            )

        if self.max_event_history_size < 0:
            raise ConfigurationError(
                message="Invalid value for 'max_event_history_size': must be non-negative",
                config_key="max_event_history_size",
                config_value=self.max_event_history_size,
            )

        if self.max_event_listeners <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_event_listeners': must be positive",
                config_key="max_event_listeners",
                config_value=self.max_event_listeners,
            )

        if self.max_event_queue_size <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_event_queue_size': must be positive",
                config_key="max_event_queue_size",
                config_value=self.max_event_queue_size,
            )

        if self.max_message_size <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_message_size': must be positive",
                config_key="max_message_size",
                config_value=self.max_message_size,
            )

        if self.max_pending_events_per_session <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_pending_events_per_session': must be positive",
                config_key="max_pending_events_per_session",
                config_value=self.max_pending_events_per_session,
            )

        if self.max_total_pending_events <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_total_pending_events': must be positive",
                config_key="max_total_pending_events",
                config_value=self.max_total_pending_events,
            )

        if self.max_stream_read_buffer <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_stream_read_buffer': must be positive",
                config_key="max_stream_read_buffer",
                config_value=self.max_stream_read_buffer,
            )

        if self.max_stream_write_buffer <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_stream_write_buffer': must be positive",
                config_key="max_stream_write_buffer",
                config_value=self.max_stream_write_buffer,
            )

        if self.stream_flow_control_increment_bidi <= 0:
            raise ConfigurationError(
                message="Invalid value for 'stream_flow_control_increment_bidi': must be positive",
                config_key="stream_flow_control_increment_bidi",
                config_value=self.stream_flow_control_increment_bidi,
            )

        if self.stream_flow_control_increment_uni <= 0:
            raise ConfigurationError(
                message="Invalid value for 'stream_flow_control_increment_uni': must be positive",
                config_key="stream_flow_control_increment_uni",
                config_value=self.stream_flow_control_increment_uni,
            )


@dataclass(kw_only=True)
class ClientConfig(BaseConfig):
    """Configuration for the WebTransport client."""

    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    headers: Headers = field(default_factory=dict)
    max_connection_retries: int = DEFAULT_MAX_CONNECTION_RETRIES
    max_connections: int = DEFAULT_CLIENT_MAX_CONNECTIONS
    max_retry_delay: float = DEFAULT_MAX_RETRY_DELAY
    max_sessions: int = DEFAULT_CLIENT_MAX_SESSIONS
    retry_backoff: float = DEFAULT_RETRY_BACKOFF
    retry_delay: float = DEFAULT_RETRY_DELAY
    user_agent: str | None = None
    verify_mode: ssl.VerifyMode | None = DEFAULT_CLIENT_VERIFY_MODE

    def validate(self) -> None:
        """Validate client specific configuration."""
        super().validate()

        try:
            _validate_timeout(timeout=self.connect_timeout)
        except (ValueError, TypeError) as e:
            raise ConfigurationError(
                message=f"Invalid value for 'connect_timeout': {e}",
                config_key="connect_timeout",
                config_value=self.connect_timeout,
            ) from e

        if self.max_connection_retries < 0:
            raise ConfigurationError(
                message="Invalid value for 'max_connection_retries': must be non-negative",
                config_key="max_connection_retries",
                config_value=self.max_connection_retries,
            )
        if self.max_retry_delay <= 0:
            raise ConfigurationError(
                message="Invalid value for 'max_retry_delay': must be positive",
                config_key="max_retry_delay",
                config_value=self.max_retry_delay,
            )
        if self.retry_backoff < 1.0:
            raise ConfigurationError(
                message="Invalid value for 'retry_backoff': must be >= 1.0",
                config_key="retry_backoff",
                config_value=self.retry_backoff,
            )
        if self.retry_delay <= 0:
            raise ConfigurationError(
                message="Invalid value for 'retry_delay': must be positive",
                config_key="retry_delay",
                config_value=self.retry_delay,
            )

        has_certfile = self.certfile is not None
        has_keyfile = self.keyfile is not None
        if has_certfile != has_keyfile:
            raise ConfigurationError(
                message="TLS configuration error: 'certfile' and 'keyfile' must be provided together",
                config_key="certfile/keyfile",
                config_value=f"certfile={self.certfile}, keyfile={self.keyfile}",
            )

        allowed_verify_modes: list[ssl.VerifyMode | None] = [ssl.CERT_NONE, ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED, None]
        if self.verify_mode not in allowed_verify_modes:
            raise ConfigurationError(
                message="Invalid value for 'verify_mode': unknown SSL verify mode",
                config_key="verify_mode",
                config_value=self.verify_mode,
            )


@dataclass(kw_only=True)
class ServerConfig(BaseConfig):
    """Configuration for the WebTransport server."""

    bind_host: str = DEFAULT_BIND_HOST
    bind_port: int = DEFAULT_DEV_PORT
    max_connections: int = DEFAULT_SERVER_MAX_CONNECTIONS
    max_sessions: int = DEFAULT_SERVER_MAX_SESSIONS
    verify_mode: ssl.VerifyMode = DEFAULT_SERVER_VERIFY_MODE

    @classmethod
    def from_dict(cls, *, config_dict: dict[str, Any]) -> Self:
        """Create a ServerConfig instance with type coercion."""
        if "bind_port" in config_dict and isinstance(config_dict["bind_port"], str):
            try:
                config_dict = config_dict.copy()
                config_dict["bind_port"] = int(config_dict["bind_port"])
            except ValueError:
                pass
        return super().from_dict(config_dict=config_dict)

    def validate(self) -> None:
        """Validate server specific configuration."""
        super().validate()

        if not self.bind_host:
            raise ConfigurationError(
                message="Invalid value for 'bind_host': cannot be empty",
                config_key="bind_host",
                config_value=self.bind_host,
            )

        try:
            _validate_port(port=self.bind_port)
        except ValueError as e:
            raise ConfigurationError(
                message=f"Invalid value for 'bind_port': {e}", config_key="bind_port", config_value=self.bind_port
            ) from e

        if self.certfile is None or self.keyfile is None:
            raise ConfigurationError(
                message="TLS configuration error: Server requires both certificate and key files",
                config_key="certfile/keyfile",
                config_value=f"certfile={self.certfile}, keyfile={self.keyfile}",
            )

        allowed_verify_modes: list[ssl.VerifyMode | None] = [ssl.CERT_NONE, ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED]
        if self.verify_mode not in allowed_verify_modes:
            raise ConfigurationError(
                message="Invalid value for 'verify_mode': unknown SSL verify mode",
                config_key="verify_mode",
                config_value=self.verify_mode,
            )


def _validate_port(*, port: Any) -> None:
    """Validate that a value is a valid network port."""
    if not isinstance(port, int) or not (1 <= port <= 65535):
        raise ValueError(f"Port must be an integer between 1 and 65535, got {port}")


def _validate_timeout(*, timeout: float | None) -> None:
    """Validate a timeout value."""
    if timeout is not None:
        if not isinstance(timeout, (int, float)):
            raise TypeError("Timeout must be a number or None")
        if timeout <= 0:
            raise ValueError("Timeout must be positive")
