"""Common configuration."""

from __future__ import annotations

import ssl
from abc import ABC
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final

from pywebtransport.constants import (
    DEFAULT_BIND_HOST,
    DEFAULT_BIND_PORT,
    DEFAULT_CLOSE_TIMEOUT,
    DEFAULT_CONGESTION_CONTROL_ALGORITHM,
    DEFAULT_CONNECTION_ATTEMPT_DELAY,
    DEFAULT_CONNECTION_RECEIVE_WINDOW,
    DEFAULT_CONNECTION_SEND_WINDOW,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_INITIAL_MAX_DATA,
    DEFAULT_INITIAL_MAX_STREAMS_BIDI,
    DEFAULT_INITIAL_MAX_STREAMS_UNI,
    DEFAULT_KEEP_ALIVE_INTERVAL,
    DEFAULT_MAX_CAPSULE_SIZE,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_CONNECTION_PENDING_EVENTS,
    DEFAULT_MAX_DATAGRAM_SIZE,
    DEFAULT_MAX_FIELD_SECTION_SIZE,
    DEFAULT_MAX_IDLE_TIMEOUT,
    DEFAULT_MAX_PENDING_CAPSULES,
    DEFAULT_MAX_PENDING_DATAGRAMS,
    DEFAULT_MAX_PENDING_STREAMS,
    DEFAULT_MAX_SESSIONS,
    DEFAULT_MAX_SESSION_PENDING_EVENTS,
    DEFAULT_MAX_STREAMS_BIDI,
    DEFAULT_MAX_STREAMS_UNI,
    DEFAULT_MAX_UDP_PAYLOAD_SIZE,
    DEFAULT_MAX_WRITE_BUFFER_SIZE,
    DEFAULT_PENDING_EVENT_TTL,
    DEFAULT_READ_TIMEOUT,
    DEFAULT_RESOURCE_CLEANUP_INTERVAL,
    DEFAULT_SESSION_RECEIVE_WINDOW,
    DEFAULT_STREAM_CREATE_TIMEOUT,
    DEFAULT_STREAM_RECEIVE_WINDOW,
    H3_MIN_UNI_STREAM_COUNT,
    QUIC_MIN_UDP_PAYLOAD_SIZE_LIMIT,
    QUIC_UDP_PAYLOAD_SIZE_LIMIT,
    QUIC_VARINT_LIMIT,
    WT_SESSION_CONNECT_STREAM_COUNT,
    WT_STREAMS_LIMIT,
)
from pywebtransport.exceptions import ConfigurationError
from pywebtransport.types import Headers

__all__: list[str] = ["ClientConfig", "ServerConfig"]

_CONGESTION_CONTROL_ALGORITHMS: Final[tuple[str, ...]] = ("bbr", "cubic", "reno")
_FIELD_SECTION_SIZE_LIMIT: Final[int] = 16 * 1024 * 1024
_VERIFY_MODES: Final[tuple[ssl.VerifyMode, ...]] = (ssl.CERT_NONE, ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED)


@dataclass(frozen=True, kw_only=True, slots=True)
class _BaseConfig(ABC):
    """Encapsulate the base configuration."""

    close_timeout: float = DEFAULT_CLOSE_TIMEOUT
    congestion_control_algorithm: str = DEFAULT_CONGESTION_CONTROL_ALGORITHM
    connection_receive_window: int = DEFAULT_CONNECTION_RECEIVE_WINDOW
    connection_send_window: int = DEFAULT_CONNECTION_SEND_WINDOW
    initial_max_data: int = DEFAULT_INITIAL_MAX_DATA
    initial_max_streams_bidi: int = DEFAULT_INITIAL_MAX_STREAMS_BIDI
    initial_max_streams_uni: int = DEFAULT_INITIAL_MAX_STREAMS_UNI
    keep_alive_interval: float | None = DEFAULT_KEEP_ALIVE_INTERVAL
    max_capsule_size: int = DEFAULT_MAX_CAPSULE_SIZE
    max_connection_pending_events: int = DEFAULT_MAX_CONNECTION_PENDING_EVENTS
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    max_datagram_size: int = DEFAULT_MAX_DATAGRAM_SIZE
    max_field_section_size: int = DEFAULT_MAX_FIELD_SECTION_SIZE
    max_idle_timeout: float = DEFAULT_MAX_IDLE_TIMEOUT
    max_pending_capsules: int = DEFAULT_MAX_PENDING_CAPSULES
    max_pending_datagrams: int = DEFAULT_MAX_PENDING_DATAGRAMS
    max_pending_streams: int = DEFAULT_MAX_PENDING_STREAMS
    max_session_pending_events: int = DEFAULT_MAX_SESSION_PENDING_EVENTS
    max_sessions: int = DEFAULT_MAX_SESSIONS
    max_streams_bidi: int = DEFAULT_MAX_STREAMS_BIDI
    max_streams_uni: int = DEFAULT_MAX_STREAMS_UNI
    max_udp_payload_size: int = DEFAULT_MAX_UDP_PAYLOAD_SIZE
    max_write_buffer_size: int = DEFAULT_MAX_WRITE_BUFFER_SIZE
    pending_event_ttl: float = DEFAULT_PENDING_EVENT_TTL
    read_timeout: float | None = DEFAULT_READ_TIMEOUT
    resource_cleanup_interval: float = DEFAULT_RESOURCE_CLEANUP_INTERVAL
    session_receive_window: int = DEFAULT_SESSION_RECEIVE_WINDOW
    stream_create_timeout: float = DEFAULT_STREAM_CREATE_TIMEOUT
    stream_receive_window: int = DEFAULT_STREAM_RECEIVE_WINDOW

    def __post_init__(self) -> None:
        """Validate the configuration after initialization."""
        self._validate()

    def _validate(self) -> None:
        if (
            not isinstance(self.close_timeout, (int, float))
            or isinstance(self.close_timeout, bool)
            or self.close_timeout <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_close_timeout validate invalid actual={self.close_timeout}",
                config_key="close_timeout",
                config_value=self.close_timeout,
            )

        if self.congestion_control_algorithm not in _CONGESTION_CONTROL_ALGORITHMS:
            raise ConfigurationError(
                message=f"cfg_congestion_control_algorithm validate invalid actual={self.congestion_control_algorithm}",
                config_key="congestion_control_algorithm",
                config_value=self.congestion_control_algorithm,
            )

        if (
            not isinstance(self.connection_receive_window, int)
            or isinstance(self.connection_receive_window, bool)
            or self.connection_receive_window <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_connection_receive_window validate invalid actual={self.connection_receive_window}",
                config_key="connection_receive_window",
                config_value=self.connection_receive_window,
            )

        if (
            not isinstance(self.connection_send_window, int)
            or isinstance(self.connection_send_window, bool)
            or self.connection_send_window <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_connection_send_window validate invalid actual={self.connection_send_window}",
                config_key="connection_send_window",
                config_value=self.connection_send_window,
            )

        if (
            not isinstance(self.initial_max_data, int)
            or isinstance(self.initial_max_data, bool)
            or self.initial_max_data < 0
            or self.initial_max_data > QUIC_VARINT_LIMIT
        ):
            raise ConfigurationError(
                message=f"cfg_initial_max_data validate invalid actual={self.initial_max_data}",
                config_key="initial_max_data",
                config_value=self.initial_max_data,
            )

        if (
            not isinstance(self.initial_max_streams_bidi, int)
            or isinstance(self.initial_max_streams_bidi, bool)
            or self.initial_max_streams_bidi < 0
            or self.initial_max_streams_bidi > WT_STREAMS_LIMIT
        ):
            raise ConfigurationError(
                message=f"cfg_initial_max_streams_bidi validate invalid actual={self.initial_max_streams_bidi}",
                config_key="initial_max_streams_bidi",
                config_value=self.initial_max_streams_bidi,
            )

        if (
            not isinstance(self.initial_max_streams_uni, int)
            or isinstance(self.initial_max_streams_uni, bool)
            or self.initial_max_streams_uni < 0
            or self.initial_max_streams_uni > WT_STREAMS_LIMIT
        ):
            raise ConfigurationError(
                message=f"cfg_initial_max_streams_uni validate invalid actual={self.initial_max_streams_uni}",
                config_key="initial_max_streams_uni",
                config_value=self.initial_max_streams_uni,
            )

        if self.keep_alive_interval is not None and (
            not isinstance(self.keep_alive_interval, (int, float))
            or isinstance(self.keep_alive_interval, bool)
            or self.keep_alive_interval <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_keep_alive_interval validate invalid actual={self.keep_alive_interval}",
                config_key="keep_alive_interval",
                config_value=self.keep_alive_interval,
            )

        if (
            not isinstance(self.max_capsule_size, int)
            or isinstance(self.max_capsule_size, bool)
            or self.max_capsule_size <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_max_capsule_size validate invalid actual={self.max_capsule_size}",
                config_key="max_capsule_size",
                config_value=self.max_capsule_size,
            )

        if (
            not isinstance(self.max_connection_pending_events, int)
            or isinstance(self.max_connection_pending_events, bool)
            or self.max_connection_pending_events <= 0
        ):
            raise ConfigurationError(
                message=(
                    f"cfg_max_connection_pending_events validate invalid actual={self.max_connection_pending_events}"
                ),
                config_key="max_connection_pending_events",
                config_value=self.max_connection_pending_events,
            )

        if (
            not isinstance(self.max_connections, int)
            or isinstance(self.max_connections, bool)
            or self.max_connections <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_max_connections validate invalid actual={self.max_connections}",
                config_key="max_connections",
                config_value=self.max_connections,
            )

        if (
            not isinstance(self.max_datagram_size, int)
            or isinstance(self.max_datagram_size, bool)
            or self.max_datagram_size <= 0
            or self.max_datagram_size > self.max_udp_payload_size
        ):
            raise ConfigurationError(
                message=f"cfg_max_datagram_size validate invalid actual={self.max_datagram_size}",
                config_key="max_datagram_size",
                config_value=self.max_datagram_size,
            )

        if (
            not isinstance(self.max_field_section_size, int)
            or isinstance(self.max_field_section_size, bool)
            or self.max_field_section_size <= 0
            or self.max_field_section_size > _FIELD_SECTION_SIZE_LIMIT
        ):
            raise ConfigurationError(
                message=f"cfg_max_field_section_size validate invalid actual={self.max_field_section_size}",
                config_key="max_field_section_size",
                config_value=self.max_field_section_size,
            )

        if (
            not isinstance(self.max_idle_timeout, (int, float))
            or isinstance(self.max_idle_timeout, bool)
            or self.max_idle_timeout <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_max_idle_timeout validate invalid actual={self.max_idle_timeout}",
                config_key="max_idle_timeout",
                config_value=self.max_idle_timeout,
            )

        if (
            not isinstance(self.max_pending_capsules, int)
            or isinstance(self.max_pending_capsules, bool)
            or self.max_pending_capsules <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_max_pending_capsules validate invalid actual={self.max_pending_capsules}",
                config_key="max_pending_capsules",
                config_value=self.max_pending_capsules,
            )

        if (
            not isinstance(self.max_pending_datagrams, int)
            or isinstance(self.max_pending_datagrams, bool)
            or self.max_pending_datagrams <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_max_pending_datagrams validate invalid actual={self.max_pending_datagrams}",
                config_key="max_pending_datagrams",
                config_value=self.max_pending_datagrams,
            )

        if (
            not isinstance(self.max_pending_streams, int)
            or isinstance(self.max_pending_streams, bool)
            or self.max_pending_streams <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_max_pending_streams validate invalid actual={self.max_pending_streams}",
                config_key="max_pending_streams",
                config_value=self.max_pending_streams,
            )

        if (
            not isinstance(self.max_session_pending_events, int)
            or isinstance(self.max_session_pending_events, bool)
            or self.max_session_pending_events <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_max_session_pending_events validate invalid actual={self.max_session_pending_events}",
                config_key="max_session_pending_events",
                config_value=self.max_session_pending_events,
            )

        if not isinstance(self.max_sessions, int) or isinstance(self.max_sessions, bool) or self.max_sessions <= 0:
            raise ConfigurationError(
                message=f"cfg_max_sessions validate invalid actual={self.max_sessions}",
                config_key="max_sessions",
                config_value=self.max_sessions,
            )

        if (
            not isinstance(self.max_streams_bidi, int)
            or isinstance(self.max_streams_bidi, bool)
            or self.max_streams_bidi < WT_SESSION_CONNECT_STREAM_COUNT
            or self.max_streams_bidi > WT_STREAMS_LIMIT
        ):
            raise ConfigurationError(
                message=f"cfg_max_streams_bidi validate invalid actual={self.max_streams_bidi}",
                config_key="max_streams_bidi",
                config_value=self.max_streams_bidi,
            )

        if (
            not isinstance(self.max_streams_uni, int)
            or isinstance(self.max_streams_uni, bool)
            or self.max_streams_uni < H3_MIN_UNI_STREAM_COUNT
            or self.max_streams_uni > WT_STREAMS_LIMIT
        ):
            raise ConfigurationError(
                message=f"cfg_max_streams_uni validate invalid actual={self.max_streams_uni}",
                config_key="max_streams_uni",
                config_value=self.max_streams_uni,
            )

        if (
            not isinstance(self.max_udp_payload_size, int)
            or isinstance(self.max_udp_payload_size, bool)
            or self.max_udp_payload_size < QUIC_MIN_UDP_PAYLOAD_SIZE_LIMIT
            or self.max_udp_payload_size > QUIC_UDP_PAYLOAD_SIZE_LIMIT
        ):
            raise ConfigurationError(
                message=f"cfg_max_udp_payload_size validate invalid actual={self.max_udp_payload_size}",
                config_key="max_udp_payload_size",
                config_value=self.max_udp_payload_size,
            )

        if (
            not isinstance(self.max_write_buffer_size, int)
            or isinstance(self.max_write_buffer_size, bool)
            or self.max_write_buffer_size <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_max_write_buffer_size validate invalid actual={self.max_write_buffer_size}",
                config_key="max_write_buffer_size",
                config_value=self.max_write_buffer_size,
            )

        if (
            not isinstance(self.pending_event_ttl, (int, float))
            or isinstance(self.pending_event_ttl, bool)
            or self.pending_event_ttl <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_pending_event_ttl validate invalid actual={self.pending_event_ttl}",
                config_key="pending_event_ttl",
                config_value=self.pending_event_ttl,
            )

        if self.read_timeout is not None and (
            not isinstance(self.read_timeout, (int, float))
            or isinstance(self.read_timeout, bool)
            or self.read_timeout <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_read_timeout validate invalid actual={self.read_timeout}",
                config_key="read_timeout",
                config_value=self.read_timeout,
            )

        if (
            not isinstance(self.resource_cleanup_interval, (int, float))
            or isinstance(self.resource_cleanup_interval, bool)
            or self.resource_cleanup_interval <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_resource_cleanup_interval validate invalid actual={self.resource_cleanup_interval}",
                config_key="resource_cleanup_interval",
                config_value=self.resource_cleanup_interval,
            )

        if (
            not isinstance(self.session_receive_window, int)
            or isinstance(self.session_receive_window, bool)
            or self.session_receive_window <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_session_receive_window validate invalid actual={self.session_receive_window}",
                config_key="session_receive_window",
                config_value=self.session_receive_window,
            )

        if (
            not isinstance(self.stream_create_timeout, (int, float))
            or isinstance(self.stream_create_timeout, bool)
            or self.stream_create_timeout <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_stream_create_timeout validate invalid actual={self.stream_create_timeout}",
                config_key="stream_create_timeout",
                config_value=self.stream_create_timeout,
            )

        if (
            not isinstance(self.stream_receive_window, int)
            or isinstance(self.stream_receive_window, bool)
            or self.stream_receive_window <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_stream_receive_window validate invalid actual={self.stream_receive_window}",
                config_key="stream_receive_window",
                config_value=self.stream_receive_window,
            )


@dataclass(frozen=True, kw_only=True, slots=True)
class ClientConfig(_BaseConfig):
    """Encapsulate client configuration."""

    ca_certs: str | None = None
    certfile: str | None = None
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    connection_attempt_delay: float = DEFAULT_CONNECTION_ATTEMPT_DELAY
    headers: Headers = field(default_factory=dict)
    keyfile: str | None = None
    user_agent: str | None = None
    verify_mode: ssl.VerifyMode = ssl.CERT_REQUIRED
    wt_available_protocols: list[str] | None = None

    def __post_init__(self) -> None:
        """Validate the configuration after initialization."""
        super(ClientConfig, self).__post_init__()

        if isinstance(self.headers, dict):
            object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))
        else:
            object.__setattr__(self, "headers", tuple(self.headers))

        if self.wt_available_protocols is not None:
            object.__setattr__(self, "wt_available_protocols", tuple(self.wt_available_protocols))

    def _validate(self) -> None:
        super(ClientConfig, self)._validate()

        if self.ca_certs is not None and not isinstance(self.ca_certs, str):
            raise ConfigurationError(
                message=f"cfg_ca_certs validate invalid actual={self.ca_certs}",
                config_key="ca_certs",
                config_value=self.ca_certs,
            )

        if (self.keyfile is not None and self.certfile is None) or (
            self.certfile is not None and not isinstance(self.certfile, str)
        ):
            raise ConfigurationError(
                message=f"cfg_certfile validate invalid actual={self.certfile}",
                config_key="certfile",
                config_value=self.certfile,
            )

        if (
            not isinstance(self.connect_timeout, (int, float))
            or isinstance(self.connect_timeout, bool)
            or self.connect_timeout <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_connect_timeout validate invalid actual={self.connect_timeout}",
                config_key="connect_timeout",
                config_value=self.connect_timeout,
            )

        if (
            not isinstance(self.connection_attempt_delay, (int, float))
            or isinstance(self.connection_attempt_delay, bool)
            or self.connection_attempt_delay <= 0
        ):
            raise ConfigurationError(
                message=f"cfg_connection_attempt_delay validate invalid actual={self.connection_attempt_delay}",
                config_key="connection_attempt_delay",
                config_value=self.connection_attempt_delay,
            )

        if not (
            (
                isinstance(self.headers, dict)
                and all(
                    isinstance(key, (str, bytes)) and isinstance(value, (str, bytes))
                    for key, value in self.headers.items()
                )
            )
            or (
                isinstance(self.headers, list)
                and all(
                    isinstance(item, tuple)
                    and len(item) == 2
                    and isinstance(item[0], (str, bytes))
                    and isinstance(item[1], (str, bytes))
                    for item in self.headers
                )
            )
        ):
            raise ConfigurationError(
                message=f"cfg_headers validate invalid actual={self.headers}",
                config_key="headers",
                config_value=self.headers,
            )

        if (self.certfile is not None and self.keyfile is None) or (
            self.keyfile is not None and not isinstance(self.keyfile, str)
        ):
            raise ConfigurationError(
                message=f"cfg_keyfile validate invalid actual={self.keyfile}",
                config_key="keyfile",
                config_value=self.keyfile,
            )

        if self.user_agent is not None and not isinstance(self.user_agent, str):
            raise ConfigurationError(
                message=f"cfg_user_agent validate invalid actual={self.user_agent}",
                config_key="user_agent",
                config_value=self.user_agent,
            )

        if self.verify_mode not in _VERIFY_MODES:
            raise ConfigurationError(
                message=f"cfg_verify_mode validate invalid actual={self.verify_mode}",
                config_key="verify_mode",
                config_value=self.verify_mode,
            )

        if self.wt_available_protocols is not None:
            if not isinstance(self.wt_available_protocols, list) or not all(
                isinstance(protocol, str) for protocol in self.wt_available_protocols
            ):
                raise ConfigurationError(
                    message=f"cfg_wt_available_protocols validate invalid actual={self.wt_available_protocols}",
                    config_key="wt_available_protocols",
                    config_value=self.wt_available_protocols,
                )


@dataclass(frozen=True, kw_only=True, slots=True)
class ServerConfig(_BaseConfig):
    """Encapsulate server configuration."""

    bind_host: str = DEFAULT_BIND_HOST
    bind_port: int = DEFAULT_BIND_PORT
    ca_certs: str | None = None
    certfile: str
    keyfile: str
    verify_mode: ssl.VerifyMode = ssl.CERT_NONE

    def _validate(self) -> None:
        super(ServerConfig, self)._validate()

        if not isinstance(self.bind_host, str) or not self.bind_host:
            raise ConfigurationError(
                message=f"cfg_bind_host validate invalid actual={self.bind_host}",
                config_key="bind_host",
                config_value=self.bind_host,
            )

        if (
            not isinstance(self.bind_port, int)
            or isinstance(self.bind_port, bool)
            or not (1 <= self.bind_port <= 65535)
        ):
            raise ConfigurationError(
                message=f"cfg_bind_port validate invalid actual={self.bind_port}",
                config_key="bind_port",
                config_value=self.bind_port,
            )

        if (self.ca_certs is not None and not isinstance(self.ca_certs, str)) or (
            self.verify_mode in (ssl.CERT_REQUIRED, ssl.CERT_OPTIONAL) and not self.ca_certs
        ):
            raise ConfigurationError(
                message=f"cfg_ca_certs validate invalid actual={self.ca_certs}",
                config_key="ca_certs",
                config_value=self.ca_certs,
            )

        if not isinstance(self.certfile, str) or not self.certfile:
            raise ConfigurationError(
                message=f"cfg_certfile validate invalid actual={self.certfile}",
                config_key="certfile",
                config_value=self.certfile,
            )

        if not isinstance(self.keyfile, str) or not self.keyfile:
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
