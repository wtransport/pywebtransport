"""Type stubs for the Rust-backed WebTransport extension."""

from __future__ import annotations

from typing import final

from pywebtransport._protocol.events import Effect, ProtocolEvent
from pywebtransport.config import ClientConfig, ServerConfig
from pywebtransport.types import Buffer

ALPN_H3: str
USER_AGENT_HEADER: str
WEBTRANSPORT_DEFAULT_PORT: int
WEBTRANSPORT_SCHEME: str

BIDIRECTIONAL_STREAM: int
CLOSE_WEBTRANSPORT_SESSION_TYPE: int
DRAIN_WEBTRANSPORT_SESSION_TYPE: int
H3_FRAME_TYPE_CANCEL_PUSH: int
H3_FRAME_TYPE_DATA: int
H3_FRAME_TYPE_GOAWAY: int
H3_FRAME_TYPE_HEADERS: int
H3_FRAME_TYPE_MAX_PUSH_ID: int
H3_FRAME_TYPE_PUSH_PROMISE: int
H3_FRAME_TYPE_SETTINGS: int
H3_FRAME_TYPE_WEBTRANSPORT_STREAM: int
H3_STREAM_TYPE_CONTROL: int
H3_STREAM_TYPE_PUSH: int
H3_STREAM_TYPE_QPACK_DECODER: int
H3_STREAM_TYPE_QPACK_ENCODER: int
H3_STREAM_TYPE_WEBTRANSPORT: int
MAX_CLOSE_REASON_BYTES: int
MAX_DATAGRAM_SIZE: int
MAX_PROTOCOL_STREAMS_LIMIT: int
MAX_STREAM_ID: int
QPACK_DECODER_MAX_BLOCKED_STREAMS: int
QPACK_DECODER_MAX_TABLE_CAPACITY: int
SETTINGS_ENABLE_CONNECT_PROTOCOL: int
SETTINGS_H3_DATAGRAM: int
SETTINGS_QPACK_BLOCKED_STREAMS: int
SETTINGS_QPACK_MAX_TABLE_CAPACITY: int
SETTINGS_WT_INITIAL_MAX_DATA: int
SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI: int
SETTINGS_WT_INITIAL_MAX_STREAMS_UNI: int
UNIDIRECTIONAL_STREAM: int
WT_DATA_BLOCKED_TYPE: int
WT_MAX_DATA_TYPE: int
WT_MAX_STREAM_DATA_TYPE: int
WT_MAX_STREAMS_BIDI_TYPE: int
WT_MAX_STREAMS_UNI_TYPE: int
WT_STREAM_DATA_BLOCKED_TYPE: int
WT_STREAMS_BLOCKED_BIDI_TYPE: int
WT_STREAMS_BLOCKED_UNI_TYPE: int

DEFAULT_ALPN_PROTOCOLS: list[str]
DEFAULT_BIND_HOST: str
DEFAULT_CLIENT_MAX_CONNECTIONS: int
DEFAULT_CLIENT_MAX_SESSIONS: int
DEFAULT_CLOSE_TIMEOUT: float
DEFAULT_CONGESTION_CONTROL_ALGORITHM: str
DEFAULT_CONNECT_TIMEOUT: float
DEFAULT_CONNECTION_IDLE_TIMEOUT: float
DEFAULT_DEV_PORT: int
DEFAULT_FLOW_CONTROL_WINDOW_AUTO_SCALE: bool
DEFAULT_FLOW_CONTROL_WINDOW_SIZE: int
DEFAULT_INITIAL_MAX_DATA: int
DEFAULT_INITIAL_MAX_STREAMS_BIDI: int
DEFAULT_INITIAL_MAX_STREAMS_UNI: int
DEFAULT_KEEP_ALIVE: bool
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

def generate_self_signed_cert(*, hostname: str, output_dir: str = ".", validity_days: int = 365) -> tuple[str, str]:
    """Generate a self-signed certificate and key for testing."""
    ...

@final
class WebTransportEngine:
    """Orchestrates the unified protocol state machine."""

    def __new__(cls, connection_id: str, config: ClientConfig | ServerConfig, is_client: bool) -> WebTransportEngine:
        """Initialize the WebTransport engine."""
        ...

    def cleanup_stream(self, stream_id: int) -> None:
        """Clean up H3 state for a closed stream."""
        ...

    @staticmethod
    def encode_capsule(
        stream_id: int, capsule_type: int, capsule_data: bytes, end_stream: bool = False
    ) -> list[Effect]:
        """Encode a capsule and return effects to send it."""
        ...

    @staticmethod
    def encode_datagram(stream_id: int, data: Buffer | list[Buffer]) -> list[Effect]:
        """Encode a datagram and return effects to send it."""
        ...

    def encode_goaway(self) -> list[Effect]:
        """Encode a GOAWAY frame and return effects to send it."""
        ...

    def encode_headers(self, stream_id: int, status: int, end_stream: bool = False) -> list[Effect]:
        """Encode headers and return effects to send them."""
        ...

    def encode_session_request(
        self,
        stream_id: int,
        path: str,
        authority: str,
        headers: dict[str | bytes, str | bytes] | list[tuple[str | bytes, str | bytes]],
    ) -> list[Effect]:
        """Encode a WebTransport session establishment request."""
        ...

    def encode_stream_creation(self, stream_id: int, control_stream_id: int, is_unidirectional: bool) -> list[Effect]:
        """Encode the preamble for a new WebTransport stream."""
        ...

    def handle_event(self, event: ProtocolEvent, now: float) -> list[Effect]:
        """Process a single event and return resulting effects."""
        ...

    def initialize_h3_transport(self, control_id: int, encoder_id: int, decoder_id: int) -> list[Effect]:
        """Initialize HTTP/3 unidirectional streams and settings."""
        ...
