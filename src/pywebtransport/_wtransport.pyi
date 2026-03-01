"""Type stubs for the Rust-backed WebTransport extension."""

from __future__ import annotations

from typing import Any, Self, final

from pywebtransport.config import ClientConfig, ServerConfig
from pywebtransport.types import Buffer

ABI_VERSION: int

ALPN_H3: str
USER_AGENT_HEADER: str
WEBTRANSPORT_DEFAULT_PORT: int
WEBTRANSPORT_SCHEME: str
DEFAULT_ALPN_PROTOCOLS: list[str]
DEFAULT_BIND_HOST: str
DEFAULT_CLIENT_MAX_CONNECTIONS: int
DEFAULT_CLIENT_MAX_SESSIONS: int
DEFAULT_CLOSE_TIMEOUT: float
DEFAULT_CONGESTION_CONTROL_ALGORITHM: str
DEFAULT_CONNECTION_ATTEMPT_DELAY: float
DEFAULT_CONNECTION_IDLE_TIMEOUT: float
DEFAULT_CONNECT_TIMEOUT: float
DEFAULT_DEV_PORT: int
DEFAULT_ENABLE_STATELESS_RETRY: bool
DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE: bool
DEFAULT_FLOW_CONTROL_WINDOW_SIZE: int
DEFAULT_INITIAL_MAX_DATA: int
DEFAULT_INITIAL_MAX_STREAMS_BIDI: int
DEFAULT_INITIAL_MAX_STREAMS_UNI: int
DEFAULT_KEEP_ALIVE: float
DEFAULT_LOG_LEVEL: str
DEFAULT_MAX_CAPSULE_SIZE: int
DEFAULT_MAX_CONNECTION_RETRIES: int
DEFAULT_MAX_DATAGRAM_SIZE: int
DEFAULT_MAX_EVENT_HISTORY_SIZE: int
DEFAULT_MAX_EVENT_LISTENERS: int
DEFAULT_MAX_EVENT_QUEUE_SIZE: int
DEFAULT_MAX_MESSAGE_SIZE: int
DEFAULT_MAX_PENDING_EVENTS_PER_SESSION: int
DEFAULT_MAX_RETRY_DELAY: float
DEFAULT_MAX_STREAM_READ_BUFFER: int
DEFAULT_MAX_STREAM_WRITE_BUFFER: int
DEFAULT_MAX_TOTAL_PENDING_EVENTS: int
DEFAULT_PENDING_EVENT_TTL: float
DEFAULT_READ_TIMEOUT: float
DEFAULT_RESOURCE_CLEANUP_INTERVAL: float
DEFAULT_RETRY_BACKOFF: float
DEFAULT_RETRY_DELAY: float
DEFAULT_SERVER_MAX_CONNECTIONS: int
DEFAULT_SERVER_MAX_SESSIONS: int
DEFAULT_STREAM_CREATION_TIMEOUT: float
DEFAULT_TRANSPORT_STREAMS_CAP: int
DEFAULT_WRITE_TIMEOUT: float
SUPPORTED_CONGESTION_CONTROL_ALGORITHMS: list[str]
ERR_AEAD_LIMIT_REACHED: int
ERR_APP_AUTHENTICATION_FAILED: int
ERR_APP_CONNECTION_TIMEOUT: int
ERR_APP_INVALID_REQUEST: int
ERR_APP_PERMISSION_DENIED: int
ERR_APP_RESOURCE_EXHAUSTED: int
ERR_APP_SERVICE_UNAVAILABLE: int
ERR_APPLICATION_ERROR: int
ERR_CONNECTION_ID_LIMIT_ERROR: int
ERR_CONNECTION_REFUSED: int
ERR_CRYPTO_BUFFER_EXCEEDED: int
ERR_FINAL_SIZE_ERROR: int
ERR_FLOW_CONTROL_ERROR: int
ERR_FRAME_ENCODING_ERROR: int
ERR_H3_CLOSED_CRITICAL_STREAM: int
ERR_H3_CONNECT_ERROR: int
ERR_H3_DATAGRAM_ERROR: int
ERR_H3_EXCESSIVE_LOAD: int
ERR_H3_FRAME_ERROR: int
ERR_H3_FRAME_UNEXPECTED: int
ERR_H3_GENERAL_PROTOCOL_ERROR: int
ERR_H3_ID_ERROR: int
ERR_H3_INTERNAL_ERROR: int
ERR_H3_MESSAGE_ERROR: int
ERR_H3_MISSING_SETTINGS: int
ERR_H3_NO_ERROR: int
ERR_H3_REQUEST_CANCELLED: int
ERR_H3_REQUEST_INCOMPLETE: int
ERR_H3_REQUEST_REJECTED: int
ERR_H3_SETTINGS_ERROR: int
ERR_H3_STREAM_CREATION_ERROR: int
ERR_H3_VERSION_FALLBACK: int
ERR_INTERNAL_ERROR: int
ERR_INVALID_TOKEN: int
ERR_KEY_UPDATE_ERROR: int
ERR_LIB_CONNECTION_STATE_ERROR: int
ERR_LIB_INTERNAL_ERROR: int
ERR_LIB_SESSION_STATE_ERROR: int
ERR_LIB_STREAM_STATE_ERROR: int
ERR_NO_ERROR: int
ERR_NO_VIABLE_PATH: int
ERR_PROTOCOL_VIOLATION: int
ERR_QPACK_DECODER_STREAM_ERROR: int
ERR_QPACK_DECOMPRESSION_FAILED: int
ERR_QPACK_ENCODER_STREAM_ERROR: int
ERR_STREAM_LIMIT_ERROR: int
ERR_STREAM_STATE_ERROR: int
ERR_TRANSPORT_PARAMETER_ERROR: int
ERR_WT_APPLICATION_ERROR_FIRST: int
ERR_WT_APPLICATION_ERROR_LAST: int
ERR_WT_BUFFERED_STREAM_REJECTED: int
ERR_WT_FLOW_CONTROL_ERROR: int
ERR_WT_SESSION_GONE: int
MAX_DATAGRAM_SIZE: int
MAX_PROTOCOL_STREAMS_LIMIT: int

def generate_self_signed_cert(
    *, hostname: str, output_dir: str = ".", validity_days: int = 365
) -> tuple[str, str, str]:
    """Generate a self-signed certificate and key for testing."""
    ...

@final
class Endpoint:
    """WebTransport endpoint scheduling state machine."""

    def __new__(cls, is_client: bool, config: ClientConfig | ServerConfig, now: float) -> Self:
        """Initialize a new QUIC endpoint with either server or client behaviors."""
        ...

    def connect(self, remote: tuple[str, int], server_name: str, now: float) -> int:
        """Initiate an outbound QUIC connection and initialize the WebTransport engine."""
        ...

    def get_remote_address(self, handle: int) -> tuple[str, int] | None:
        """Retrieve the current validated remote socket address for a connection."""
        ...

    def handle_datagram(
        self, data: Buffer, remote: tuple[str, int], local: tuple[str, int] | None, now: float
    ) -> tuple[int, Any]:
        """Process an incoming UDP datagram and route it to the appropriate state machine."""
        ...

    def handle_timeout(self, now: float) -> list[tuple[int, Any]]:
        """Evaluate timeouts across the entire multiplexer and trigger necessary logic."""
        ...

    def handle_user_event(self, handle: int, event: tuple[int, Any], now: float) -> tuple[int, Any] | None:
        """Route a user-level application event to the specified connection handle."""
        ...

    def poll_transmit(self, now: float) -> tuple[int, Any] | None:
        """Poll for any pending endpoint-level or connection-level transmission datagrams."""
        ...

    def timeout(self) -> float | None:
        """Compute the earliest wakeup instant required by any entity within the endpoint."""
        ...
