"""Type stubs for the Rust-backed WebTransport extension."""

from __future__ import annotations

from typing import Any, Self, final

from pywebtransport.config import ClientConfig, ServerConfig

ABI_VERSION: int

DEFAULT_ALPN_PROTOCOLS: list[str]
DEFAULT_BIND_HOST: str
DEFAULT_CLOSE_TIMEOUT: float
DEFAULT_CONGESTION_CONTROL_ALGORITHM: str
DEFAULT_CONNECTION_ATTEMPT_DELAY: float
DEFAULT_CONNECTION_IDLE_TIMEOUT: float
DEFAULT_CONNECT_TIMEOUT: float
DEFAULT_DEV_PORT: int
DEFAULT_EVENT_HISTORY_CAPACITY: int
DEFAULT_EVENT_QUEUE_CAPACITY: int
DEFAULT_FLOW_CONTROL_WINDOW: int
DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE_ENABLED: bool
DEFAULT_INITIAL_MAX_DATA: int
DEFAULT_INITIAL_MAX_STREAMS_BIDI: int
DEFAULT_INITIAL_MAX_STREAMS_UNI: int
DEFAULT_KEEP_ALIVE_INTERVAL: float
DEFAULT_LOG_LEVEL: str
DEFAULT_MAX_CAPSULE_SIZE: int
DEFAULT_MAX_CONNECTIONS: int
DEFAULT_MAX_DATAGRAM_SIZE: int
DEFAULT_MAX_EVENT_LISTENERS: int
DEFAULT_MAX_FIELD_SECTION_SIZE: int
DEFAULT_MAX_SESSION_PENDING_EVENTS: int
DEFAULT_MAX_SESSIONS: int
DEFAULT_MAX_STREAM_READ_BUFFER_SIZE: int
DEFAULT_MAX_STREAM_WRITE_BUFFER_SIZE: int
DEFAULT_MAX_TOTAL_PENDING_EVENTS: int
DEFAULT_PENDING_EVENT_TTL: float
DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS: int
DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS: int
DEFAULT_QUIC_RECEIVE_WINDOW: int
DEFAULT_QUIC_SEND_WINDOW: int
DEFAULT_QUIC_STREAM_RECEIVE_WINDOW: int
DEFAULT_READ_TIMEOUT: float
DEFAULT_RESOURCE_CLEANUP_INTERVAL: float
DEFAULT_STREAM_CREATION_TIMEOUT: float
DEFAULT_WRITE_TIMEOUT: float
ERR_APP_AUTHENTICATION_FAILED: int
ERR_APP_CANCELLED: int
ERR_APP_CONNECTION_TIMEOUT: int
ERR_APP_GENERIC_ERROR: int
ERR_APP_INVALID_REQUEST: int
ERR_APP_NO_ERROR: int
ERR_APP_OPERATION_TIMEOUT: int
ERR_APP_PERMISSION_DENIED: int
ERR_APP_RESOURCE_EXHAUSTED: int
ERR_APP_SERVICE_UNAVAILABLE: int
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
ERR_LIB_CONNECTION_STATE_ERROR: int
ERR_LIB_INTERNAL_ERROR: int
ERR_LIB_SESSION_STATE_ERROR: int
ERR_LIB_STREAM_STATE_ERROR: int
ERR_QPACK_DECODER_STREAM_ERROR: int
ERR_QPACK_DECOMPRESSION_FAILED: int
ERR_QPACK_ENCODER_STREAM_ERROR: int
ERR_QUIC_AEAD_LIMIT_REACHED: int
ERR_QUIC_APPLICATION_ERROR: int
ERR_QUIC_CONNECTION_ID_LIMIT_ERROR: int
ERR_QUIC_CONNECTION_REFUSED: int
ERR_QUIC_CRYPTO_BUFFER_EXCEEDED: int
ERR_QUIC_FINAL_SIZE_ERROR: int
ERR_QUIC_FLOW_CONTROL_ERROR: int
ERR_QUIC_FRAME_ENCODING_ERROR: int
ERR_QUIC_INTERNAL_ERROR: int
ERR_QUIC_INVALID_TOKEN: int
ERR_QUIC_KEY_UPDATE_ERROR: int
ERR_QUIC_NO_ERROR: int
ERR_QUIC_NO_VIABLE_PATH: int
ERR_QUIC_PROTOCOL_VIOLATION: int
ERR_QUIC_STREAM_LIMIT_ERROR: int
ERR_QUIC_STREAM_STATE_ERROR: int
ERR_QUIC_TRANSPORT_PARAMETER_ERROR: int
ERR_WT_ALPN_ERROR: int
ERR_WT_APPLICATION_ERROR_FIRST: int
ERR_WT_APPLICATION_ERROR_LAST: int
ERR_WT_BUFFERED_STREAM_REJECTED: int
ERR_WT_FLOW_CONTROL_ERROR: int
ERR_WT_REQUIREMENTS_NOT_MET: int
ERR_WT_SESSION_GONE: int
ERR_WT_STREAM_BUFFER_EXCEEDED: int
H3_MIN_UNI_STREAM_COUNT: int
QUIC_VARINT_LIMIT: int
UDP_MAX_DATAGRAM_SIZE: int
WT_SESSION_CONTROL_BIDI_STREAM_COUNT: int
WT_STREAMS_LIMIT: int

@final
class Endpoint:
    """FFI proxy for the threaded Tokio reactor."""

    def __new__(cls, is_client: bool, config: ClientConfig | ServerConfig, waker: Waker) -> Self:
        """Initialize a new threaded Tokio reactor for QUIC endpoints."""
        ...

    def close(self) -> None:
        """Send a shutdown signal to the threaded Tokio reactor."""
        ...

    def connect(self, request_id: int, remote: tuple[str, int] | tuple[str, int, int, int], server_name: str) -> None:
        """Dispatch an asynchronous outbound QUIC connection command to the reactor."""
        ...

    def get_local_addresses(self) -> list[tuple[str, int]]:
        """Retrieve the synchronized OS-allocated local socket addresses."""
        ...

    def handle_user_event(self, handle: int, event: tuple[int, Any]) -> None:
        """Dispatch a user-level application event to the specific connection handle."""
        ...

    def poll_runtime_events(self) -> list[tuple[int, Any]]:
        """Harvest all pending IPC runtime events from the threaded Tokio reactor."""
        ...

@final
class Waker:
    """Cross-language async waker mechanism using OS-level handles."""

    def __new__(cls, fd: int) -> Self:
        """Initialize the waker with a platform-specific non-blocking OS handle."""
        ...

    def clear(self) -> None:
        """Acknowledge the wake-up and arm the waker for subsequent events."""
        ...

def generate_self_signed_cert(
    *, hostname: str, output_dir: str = ".", validity_days: int = 365
) -> tuple[str, str, str]:
    """Generate a self-signed certificate and key for testing."""
    ...

def init_tracing() -> None:
    """Initialize internal tracing output to stderr."""
    ...
