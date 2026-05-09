"""Structured configuration objects for clients and servers."""

from __future__ import annotations

import copy
import ssl
import types
from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final, Self, Union, get_args, get_origin, get_type_hints

from pywebtransport.constants import (
    DEFAULT_ALPN_PROTOCOLS,
    DEFAULT_BIND_HOST,
    DEFAULT_CLOSE_TIMEOUT,
    DEFAULT_CONGESTION_CONTROL_ALGORITHM,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_CONNECTION_ATTEMPT_DELAY,
    DEFAULT_CONNECTION_IDLE_TIMEOUT,
    DEFAULT_DEV_PORT,
    DEFAULT_EVENT_HISTORY_CAPACITY,
    DEFAULT_EVENT_QUEUE_CAPACITY,
    DEFAULT_FLOW_CONTROL_WINDOW,
    DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE_ENABLED,
    DEFAULT_INITIAL_MAX_DATA,
    DEFAULT_INITIAL_MAX_STREAMS_BIDI,
    DEFAULT_INITIAL_MAX_STREAMS_UNI,
    DEFAULT_KEEP_ALIVE_INTERVAL,
    DEFAULT_LOG_LEVEL,
    DEFAULT_MAX_CAPSULE_SIZE,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_DATAGRAM_SIZE,
    DEFAULT_MAX_EVENT_LISTENERS,
    DEFAULT_MAX_FIELD_SECTION_SIZE,
    DEFAULT_MAX_SESSION_PENDING_EVENTS,
    DEFAULT_MAX_SESSIONS,
    DEFAULT_MAX_STREAM_READ_BUFFER_SIZE,
    DEFAULT_MAX_STREAM_WRITE_BUFFER_SIZE,
    DEFAULT_MAX_TOTAL_PENDING_EVENTS,
    DEFAULT_MAX_TRANSPORT_STREAMS,
    DEFAULT_PENDING_EVENT_TTL,
    DEFAULT_READ_TIMEOUT,
    DEFAULT_RESOURCE_CLEANUP_INTERVAL,
    DEFAULT_STREAM_CREATION_TIMEOUT,
    DEFAULT_WRITE_TIMEOUT,
    UDP_MAX_DATAGRAM_SIZE,
    WT_STREAMS_LIMIT,
)
from pywebtransport.exceptions import ConfigurationError
from pywebtransport.types import Headers

__all__: list[str] = ["BaseConfig", "ClientConfig", "ServerConfig"]

_CONGESTION_CONTROL_ALGORITHMS: Final[list[str]] = ["bbr", "cubic", "reno"]
_MAX_FIELD_SECTION_SIZE: Final[int] = 16 * 1024 * 1024
_VERIFY_MODES: Final[list[ssl.VerifyMode]] = [ssl.CERT_NONE, ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED]


@dataclass(kw_only=True)
class BaseConfig(ABC):
    """Encapsulate common configuration fields and logic."""

    alpn_protocols: list[str] = field(default_factory=lambda: list(DEFAULT_ALPN_PROTOCOLS))
    close_timeout: float = DEFAULT_CLOSE_TIMEOUT
    congestion_control_algorithm: str = DEFAULT_CONGESTION_CONTROL_ALGORITHM
    connection_idle_timeout: float = DEFAULT_CONNECTION_IDLE_TIMEOUT
    event_history_capacity: int = DEFAULT_EVENT_HISTORY_CAPACITY
    event_queue_capacity: int = DEFAULT_EVENT_QUEUE_CAPACITY
    flow_control_window: int = DEFAULT_FLOW_CONTROL_WINDOW
    flow_control_window_auto_scale_enabled: bool = DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE_ENABLED
    initial_max_data: int = DEFAULT_INITIAL_MAX_DATA
    initial_max_streams_bidi: int = DEFAULT_INITIAL_MAX_STREAMS_BIDI
    initial_max_streams_uni: int = DEFAULT_INITIAL_MAX_STREAMS_UNI
    keep_alive_interval: float | None = DEFAULT_KEEP_ALIVE_INTERVAL
    log_level: str = DEFAULT_LOG_LEVEL
    max_capsule_size: int = DEFAULT_MAX_CAPSULE_SIZE
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    max_datagram_size: int = DEFAULT_MAX_DATAGRAM_SIZE
    max_event_listeners: int = DEFAULT_MAX_EVENT_LISTENERS
    max_field_section_size: int = DEFAULT_MAX_FIELD_SECTION_SIZE
    max_session_pending_events: int = DEFAULT_MAX_SESSION_PENDING_EVENTS
    max_sessions: int = DEFAULT_MAX_SESSIONS
    max_stream_read_buffer_size: int = DEFAULT_MAX_STREAM_READ_BUFFER_SIZE
    max_stream_write_buffer_size: int = DEFAULT_MAX_STREAM_WRITE_BUFFER_SIZE
    max_total_pending_events: int = DEFAULT_MAX_TOTAL_PENDING_EVENTS
    max_transport_streams: int = DEFAULT_MAX_TRANSPORT_STREAMS
    pending_event_ttl: float = DEFAULT_PENDING_EVENT_TTL
    read_timeout: float | None = DEFAULT_READ_TIMEOUT
    resource_cleanup_interval: float = DEFAULT_RESOURCE_CLEANUP_INTERVAL
    stream_creation_timeout: float = DEFAULT_STREAM_CREATION_TIMEOUT
    write_timeout: float | None = DEFAULT_WRITE_TIMEOUT

    def copy(self) -> Self:
        """Create a deep copy of the configuration."""
        return copy.deepcopy(x=self)

    @classmethod
    def from_dict(cls, *, config_dict: dict[str, Any]) -> Self:
        """Instantiate configuration from a dictionary."""
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

    def to_dict(self) -> dict[str, Any]:
        """Serialize the configuration to a dictionary."""
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
        """Return a new configuration with updated values."""
        new_config = self.copy()
        for key, value in kwargs.items():
            if hasattr(new_config, key):
                setattr(new_config, key, value)
            else:
                raise ConfigurationError(
                    message=f"cfg validate invalid actual={key}", config_key=key, config_value=value
                )
        new_config.validate()
        return new_config

    def validate(self) -> None:
        """Validate the configuration state."""
        if not self.alpn_protocols:
            raise ConfigurationError(
                message=f"cfg_alpn_protocols validate invalid actual={self.alpn_protocols}",
                config_key="alpn_protocols",
                config_value=self.alpn_protocols,
            )

        try:
            _validate_timeout(timeout=self.close_timeout)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_close_timeout validate invalid actual={self.close_timeout}",
                config_key="close_timeout",
                config_value=self.close_timeout,
            ) from e

        if self.congestion_control_algorithm not in _CONGESTION_CONTROL_ALGORITHMS:
            raise ConfigurationError(
                message=(
                    f"cfg_congestion_control_algorithm validate invalid "
                    f"actual={self.congestion_control_algorithm} expected=congestion_control_algorithms"
                ),
                config_key="congestion_control_algorithm",
                config_value=self.congestion_control_algorithm,
            )

        try:
            _validate_timeout(timeout=self.connection_idle_timeout)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_connection_idle_timeout validate invalid actual={self.connection_idle_timeout}",
                config_key="connection_idle_timeout",
                config_value=self.connection_idle_timeout,
            ) from e

        if self.event_history_capacity < 0:
            raise ConfigurationError(
                message=f"cfg_event_history_capacity validate invalid actual={self.event_history_capacity}",
                config_key="event_history_capacity",
                config_value=self.event_history_capacity,
            )

        if self.event_queue_capacity <= 0:
            raise ConfigurationError(
                message=f"cfg_event_queue_capacity validate invalid actual={self.event_queue_capacity}",
                config_key="event_queue_capacity",
                config_value=self.event_queue_capacity,
            )

        if self.flow_control_window <= 0:
            raise ConfigurationError(
                message=f"cfg_flow_control_window validate invalid actual={self.flow_control_window}",
                config_key="flow_control_window",
                config_value=self.flow_control_window,
            )

        try:
            _validate_timeout(timeout=self.keep_alive_interval)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_keep_alive_interval validate invalid actual={self.keep_alive_interval}",
                config_key="keep_alive_interval",
                config_value=self.keep_alive_interval,
            ) from e

        if self.max_capsule_size <= 0:
            raise ConfigurationError(
                message=f"cfg_max_capsule_size validate invalid actual={self.max_capsule_size}",
                config_key="max_capsule_size",
                config_value=self.max_capsule_size,
            )

        if self.max_connections <= 0:
            raise ConfigurationError(
                message=f"cfg_max_connections validate invalid actual={self.max_connections}",
                config_key="max_connections",
                config_value=self.max_connections,
            )

        if self.max_datagram_size <= 0 or self.max_datagram_size > UDP_MAX_DATAGRAM_SIZE:
            raise ConfigurationError(
                message=f"cfg_max_datagram_size validate invalid actual={self.max_datagram_size}",
                config_key="max_datagram_size",
                config_value=self.max_datagram_size,
            )

        if self.max_event_listeners <= 0:
            raise ConfigurationError(
                message=f"cfg_max_event_listeners validate invalid actual={self.max_event_listeners}",
                config_key="max_event_listeners",
                config_value=self.max_event_listeners,
            )

        if self.max_field_section_size <= 0 or self.max_field_section_size > _MAX_FIELD_SECTION_SIZE:
            raise ConfigurationError(
                message=f"cfg_max_field_section_size validate invalid actual={self.max_field_section_size}",
                config_key="max_field_section_size",
                config_value=self.max_field_section_size,
            )

        if self.max_session_pending_events <= 0:
            raise ConfigurationError(
                message=f"cfg_max_session_pending_events validate invalid actual={self.max_session_pending_events}",
                config_key="max_session_pending_events",
                config_value=self.max_session_pending_events,
            )

        if self.max_sessions <= 0:
            raise ConfigurationError(
                message=f"cfg_max_sessions validate invalid actual={self.max_sessions}",
                config_key="max_sessions",
                config_value=self.max_sessions,
            )

        if self.max_stream_read_buffer_size <= 0:
            raise ConfigurationError(
                message=f"cfg_max_stream_read_buffer_size validate invalid actual={self.max_stream_read_buffer_size}",
                config_key="max_stream_read_buffer_size",
                config_value=self.max_stream_read_buffer_size,
            )

        if self.max_stream_write_buffer_size <= 0:
            raise ConfigurationError(
                message=f"cfg_max_stream_write_buffer_size validate invalid actual={self.max_stream_write_buffer_size}",
                config_key="max_stream_write_buffer_size",
                config_value=self.max_stream_write_buffer_size,
            )

        if self.max_total_pending_events <= 0:
            raise ConfigurationError(
                message=f"cfg_max_total_pending_events validate invalid actual={self.max_total_pending_events}",
                config_key="max_total_pending_events",
                config_value=self.max_total_pending_events,
            )

        if self.max_transport_streams <= 0 or self.max_transport_streams > WT_STREAMS_LIMIT:
            raise ConfigurationError(
                message=f"cfg_max_transport_streams validate invalid actual={self.max_transport_streams}",
                config_key="max_transport_streams",
                config_value=self.max_transport_streams,
            )

        try:
            _validate_timeout(timeout=self.pending_event_ttl)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_pending_event_ttl validate invalid actual={self.pending_event_ttl}",
                config_key="pending_event_ttl",
                config_value=self.pending_event_ttl,
            ) from e

        try:
            _validate_timeout(timeout=self.read_timeout)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_read_timeout validate invalid actual={self.read_timeout}",
                config_key="read_timeout",
                config_value=self.read_timeout,
            ) from e

        try:
            _validate_timeout(timeout=self.resource_cleanup_interval)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_resource_cleanup_interval validate invalid actual={self.resource_cleanup_interval}",
                config_key="resource_cleanup_interval",
                config_value=self.resource_cleanup_interval,
            ) from e

        try:
            _validate_timeout(timeout=self.stream_creation_timeout)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_stream_creation_timeout validate invalid actual={self.stream_creation_timeout}",
                config_key="stream_creation_timeout",
                config_value=self.stream_creation_timeout,
            ) from e

        try:
            _validate_timeout(timeout=self.write_timeout)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_write_timeout validate invalid actual={self.write_timeout}",
                config_key="write_timeout",
                config_value=self.write_timeout,
            ) from e


@dataclass(kw_only=True)
class ClientConfig(BaseConfig):
    """Encapsulate WebTransport client configuration."""

    ca_certs: str | None = None
    certfile: str | None = None
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    connection_attempt_delay: float = DEFAULT_CONNECTION_ATTEMPT_DELAY
    headers: Headers = field(default_factory=dict)
    keyfile: str | None = None
    user_agent: str | None = None
    verify_mode: ssl.VerifyMode = ssl.CERT_REQUIRED
    wt_available_protocols: list[str] | None = None

    def validate(self) -> None:
        """Validate the client configuration state."""
        super().validate()

        if self.keyfile is not None and self.certfile is None:
            raise ConfigurationError(
                message=f"cfg_certfile validate invalid actual={self.certfile}",
                config_key="certfile",
                config_value=self.certfile,
            )

        try:
            _validate_timeout(timeout=self.connect_timeout)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_connect_timeout validate invalid actual={self.connect_timeout}",
                config_key="connect_timeout",
                config_value=self.connect_timeout,
            ) from e

        try:
            _validate_timeout(timeout=self.connection_attempt_delay)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                message=f"cfg_connection_attempt_delay validate invalid actual={self.connection_attempt_delay}",
                config_key="connection_attempt_delay",
                config_value=self.connection_attempt_delay,
            ) from e

        if self.certfile is not None and self.keyfile is None:
            raise ConfigurationError(
                message=f"cfg_keyfile validate invalid actual={self.keyfile}",
                config_key="keyfile",
                config_value=self.keyfile,
            )

        if self.verify_mode not in _VERIFY_MODES:
            raise ConfigurationError(
                message=f"cfg_verify_mode validate invalid actual={self.verify_mode}",
                config_key="verify_mode",
                config_value=self.verify_mode,
            )

        if self.wt_available_protocols is not None:
            if not isinstance(self.wt_available_protocols, list) or not all(
                isinstance(p, str) for p in self.wt_available_protocols
            ):
                raise ConfigurationError(
                    message=f"cfg_wt_available_protocols validate invalid actual={self.wt_available_protocols}",
                    config_key="wt_available_protocols",
                    config_value=self.wt_available_protocols,
                )


@dataclass(kw_only=True)
class ServerConfig(BaseConfig):
    """Encapsulate WebTransport server configuration."""

    bind_host: str = DEFAULT_BIND_HOST
    bind_port: int = DEFAULT_DEV_PORT
    ca_certs: str | None = None
    certfile: str
    keyfile: str
    verify_mode: ssl.VerifyMode = ssl.CERT_NONE

    @classmethod
    def from_dict(cls, *, config_dict: dict[str, Any]) -> Self:
        """Instantiate the server configuration with type coercion."""
        if "bind_port" in config_dict and isinstance(config_dict["bind_port"], str):
            try:
                config_dict = config_dict.copy()
                config_dict["bind_port"] = int(config_dict["bind_port"])
            except ValueError:
                pass
        return super().from_dict(config_dict=config_dict)

    def validate(self) -> None:
        """Validate the server configuration state."""
        super().validate()

        if not self.bind_host:
            raise ConfigurationError(
                message=f"cfg_bind_host validate invalid actual={self.bind_host}",
                config_key="bind_host",
                config_value=self.bind_host,
            )

        try:
            _validate_port(port=self.bind_port)
        except ValueError as e:
            raise ConfigurationError(
                message=f"cfg_bind_port validate invalid actual={self.bind_port}",
                config_key="bind_port",
                config_value=self.bind_port,
            ) from e

        if self.verify_mode in (ssl.CERT_REQUIRED, ssl.CERT_OPTIONAL) and not self.ca_certs:
            raise ConfigurationError(
                message=f"cfg_ca_certs validate invalid actual={self.ca_certs}",
                config_key="ca_certs",
                config_value=self.ca_certs,
            )

        if self.certfile is None:
            raise ConfigurationError(
                message=f"cfg_certfile validate invalid actual={self.certfile}",
                config_key="certfile",
                config_value=self.certfile,
            )

        if self.keyfile is None:
            raise ConfigurationError(
                message=f"cfg_keyfile validate invalid actual={self.keyfile}",
                config_key="keyfile",
                config_value=self.keyfile,
            )

        if self.verify_mode not in _VERIFY_MODES:
            raise ConfigurationError(
                message=f"cfg_verify_mode validate invalid actual={self.verify_mode}",
                config_key="verify_mode",
                config_value=self.verify_mode,
            )


def _validate_port(*, port: Any) -> None:
    """Validate the network port."""
    if not isinstance(port, int) or not (1 <= port <= 65535):
        raise ValueError()


def _validate_timeout(*, timeout: float | None) -> None:
    """Validate the timeout value."""
    if timeout is not None:
        if not isinstance(timeout, (int, float)):
            raise TypeError()
        if timeout <= 0:
            raise ValueError()
