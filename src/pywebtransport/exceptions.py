"""Custom exception hierarchy for the library."""

from __future__ import annotations

from typing import Any

from pywebtransport.constants import ErrorCodes
from pywebtransport.types import SessionId, SessionState, StreamState

__all__: list[str] = [
    "AuthenticationError",
    "CertificateError",
    "ClientError",
    "ConfigurationError",
    "ConnectionError",
    "DatagramError",
    "FlowControlError",
    "HandshakeError",
    "ProtocolError",
    "SerializationError",
    "ServerError",
    "SessionError",
    "StreamError",
    "TimeoutError",
    "WebTransportError",
]

_FATAL_ERROR_CODES = frozenset(
    {
        ErrorCodes.INTERNAL_ERROR,
        ErrorCodes.H3_INTERNAL_ERROR,
        ErrorCodes.PROTOCOL_VIOLATION,
        ErrorCodes.FRAME_ENCODING_ERROR,
        ErrorCodes.CRYPTO_BUFFER_EXCEEDED,
        ErrorCodes.APP_AUTHENTICATION_FAILED,
        ErrorCodes.APP_PERMISSION_DENIED,
    }
)

_RETRIABLE_ERROR_CODES = frozenset(
    {ErrorCodes.APP_CONNECTION_TIMEOUT, ErrorCodes.APP_SERVICE_UNAVAILABLE, ErrorCodes.FLOW_CONTROL_ERROR}
)


class WebTransportError(Exception):
    """The base exception for all WebTransport errors."""

    def __init__(self, message: str, *, error_code: int | None = None, details: dict[str, Any] | None = None) -> None:
        """Initialize the WebTransport error."""
        super().__init__(message)
        self.message = message
        self.error_code = error_code if error_code is not None else ErrorCodes.INTERNAL_ERROR
        self.details = details if details is not None else {}

    @property
    def category(self) -> str:
        """Return the error category based on the class name."""
        name = self.__class__.__name__
        if name.endswith("Error"):
            name = name[:-5]
        return _to_snake_case(name=name)

    @property
    def is_fatal(self) -> bool:
        """Check if the error is fatal and should terminate the connection."""
        return self.error_code in _FATAL_ERROR_CODES

    @property
    def is_retriable(self) -> bool:
        """Check if the error is transient and the operation can be retried."""
        return self.error_code in _RETRIABLE_ERROR_CODES

    def to_dict(self) -> dict[str, Any]:
        """Convert the exception to a dictionary for serialization."""
        data = {
            "type": self.__class__.__name__,
            "category": self.category,
            "message": self.message,
            "error_code": self.error_code,
            "details": self.details,
            "is_fatal": self.is_fatal,
            "is_retriable": self.is_retriable,
        }

        excluded_keys = {"message", "error_code", "details", "args"}
        for key, value in self.__dict__.items():
            if key not in excluded_keys and not key.startswith("_"):
                data[key] = value if not isinstance(value, Exception) else str(value)
        return data

    def __repr__(self) -> str:
        """Return a detailed string representation of the error."""
        args = [f"message={self.message!r}", f"error_code={hex(self.error_code)}"]
        excluded_keys = {"message", "error_code", "details", "args"}

        for key, value in self.__dict__.items():
            if key not in excluded_keys and not key.startswith("_"):
                args.append(f"{key}={value!r}")

        if self.details:
            args.append(f"details={self.details!r}")

        return f"{self.__class__.__name__}({', '.join(args)})"

    def __str__(self) -> str:
        """Return a simple string representation of the error."""
        return f"[{hex(self.error_code)}] {self.message}"


class AuthenticationError(WebTransportError):
    """An exception for authentication-related errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        auth_method: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the authentication error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_AUTHENTICATION_FAILED,
            details=details,
        )
        self.auth_method = auth_method


class CertificateError(WebTransportError):
    """An exception for certificate-related errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        certificate_path: str | None = None,
        certificate_error: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the certificate error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_AUTHENTICATION_FAILED,
            details=details,
        )
        self.certificate_path = certificate_path
        self.certificate_error = certificate_error


class ClientError(WebTransportError):
    """An exception for client-specific errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        target_url: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the client error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_INVALID_REQUEST,
            details=details,
        )
        self.target_url = target_url


class ConfigurationError(WebTransportError):
    """An exception for configuration-related errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        config_key: str | None = None,
        config_value: Any | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the configuration error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_INVALID_REQUEST,
            details=details,
        )
        self.config_key = config_key
        self.config_value = config_value


class ConnectionError(WebTransportError):
    """An exception for connection-related errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        remote_address: tuple[str, int] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the connection error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.CONNECTION_REFUSED,
            details=details,
        )
        self.remote_address = remote_address


class DatagramError(WebTransportError):
    """An exception for datagram-related errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        datagram_size: int | None = None,
        max_size: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the datagram error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.INTERNAL_ERROR,
            details=details,
        )
        self.datagram_size = datagram_size
        self.max_size = max_size


class FlowControlError(WebTransportError):
    """An exception for flow control errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        stream_id: int | None = None,
        limit_exceeded: int | None = None,
        current_value: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the flow control error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.FLOW_CONTROL_ERROR,
            details=details,
        )
        self.stream_id = stream_id
        self.limit_exceeded = limit_exceeded
        self.current_value = current_value


class HandshakeError(WebTransportError):
    """An exception for handshake-related errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        handshake_stage: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the handshake error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.INTERNAL_ERROR,
            details=details,
        )
        self.handshake_stage = handshake_stage


class ProtocolError(WebTransportError):
    """An exception for protocol violation errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        frame_type: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the protocol error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.PROTOCOL_VIOLATION,
            details=details,
        )
        self.frame_type = frame_type


class SerializationError(WebTransportError):
    """An exception for serialization or deserialization errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        original_exception: Exception | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the serialization error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.INTERNAL_ERROR,
            details=details,
        )
        self.original_exception = original_exception


class ServerError(WebTransportError):
    """An exception for server-specific errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        bind_address: tuple[str, int] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the server error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_SERVICE_UNAVAILABLE,
            details=details,
        )
        self.bind_address = bind_address


class SessionError(WebTransportError):
    """An exception for WebTransport session errors."""

    def __init__(
        self,
        message: str,
        *,
        session_id: SessionId | None = None,
        error_code: int | None = None,
        session_state: SessionState | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the session error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.INTERNAL_ERROR,
            details=details,
        )
        self.session_id = session_id
        self.session_state = session_state


class StreamError(WebTransportError):
    """An exception for stream-related errors."""

    def __init__(
        self,
        message: str,
        *,
        stream_id: int | None = None,
        error_code: int | None = None,
        stream_state: StreamState | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the stream error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.STREAM_STATE_ERROR,
            details=details,
        )
        self.stream_id = stream_id
        self.stream_state = stream_state

    def __str__(self) -> str:
        """Return a simple string representation of the error."""
        base_msg = super().__str__()
        if self.stream_id is not None:
            return f"{base_msg} (stream_id={self.stream_id})"
        return base_msg


class TimeoutError(WebTransportError):
    """An exception for timeout-related errors."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        timeout_duration: float | None = None,
        operation: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the timeout error."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_CONNECTION_TIMEOUT,
            details=details,
        )
        self.timeout_duration = timeout_duration
        self.operation = operation


def _to_snake_case(*, name: str) -> str:
    """Convert a CamelCase string to snake_case."""
    return "".join(["_" + c.lower() if c.isupper() else c for c in name]).lstrip("_")
