"""Custom exception hierarchy for the library."""

from __future__ import annotations

from typing import Any, Final, Self

from pywebtransport.constants import ErrorCodes
from pywebtransport.types import Address, ConnectionHandle, SessionId, StreamId

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
    "ServerError",
    "SessionClosedError",
    "SessionError",
    "StreamError",
    "TimeoutError",
    "WebTransportError",
]

_FATAL_ERROR_CODES: Final[frozenset[int]] = frozenset(
    {
        ErrorCodes.APP_AUTHENTICATION_FAILED,
        ErrorCodes.APP_PERMISSION_DENIED,
        ErrorCodes.H3_CLOSED_CRITICAL_STREAM,
        ErrorCodes.H3_INTERNAL_ERROR,
        ErrorCodes.LIB_CONNECTION_STATE_ERROR,
        ErrorCodes.LIB_INTERNAL_ERROR,
        ErrorCodes.QUIC_AEAD_LIMIT_REACHED,
        ErrorCodes.QUIC_CRYPTO_BUFFER_EXCEEDED,
        ErrorCodes.QUIC_FLOW_CONTROL_ERROR,
        ErrorCodes.QUIC_FRAME_ENCODING_ERROR,
        ErrorCodes.QUIC_INTERNAL_ERROR,
        ErrorCodes.QUIC_PROTOCOL_VIOLATION,
        ErrorCodes.WT_ALPN_ERROR,
    }
)

_RETRIABLE_ERROR_CODES: Final[frozenset[int]] = frozenset(
    {
        ErrorCodes.APP_CONNECTION_TIMEOUT,
        ErrorCodes.APP_OPERATION_TIMEOUT,
        ErrorCodes.APP_RESOURCE_EXHAUSTED,
        ErrorCodes.APP_SERVICE_UNAVAILABLE,
        ErrorCodes.H3_EXCESSIVE_LOAD,
        ErrorCodes.QUIC_CONNECTION_REFUSED,
    }
)


class WebTransportError(Exception):
    """Manage the base exception for all WebTransport errors."""

    def __init__(self, message: str, *, error_code: int | None = None, details: dict[str, Any] | None = None) -> None:
        """Initialize the instance."""
        super().__init__(message)
        self.message = message
        self.error_code = error_code if error_code is not None else ErrorCodes.APP_GENERIC_ERROR
        self.details = details if details is not None else {}

    @property
    def category(self) -> str:
        """Return the error category based on the class name."""
        name = self.__class__.__name__
        if name.endswith("Error"):
            name = name[:-5]
        return _to_snake_case(name=name)

    @classmethod
    def from_cause(
        cls,
        message: str,
        *,
        cause: Exception,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Self:
        """Instantiate a domain exception from a causal exception preserving state traits."""
        inherited_code = getattr(cause, "error_code", None)
        inherited_details = getattr(cause, "details", {})

        final_code = error_code if error_code is not None else inherited_code
        final_details: dict[str, Any] = inherited_details.copy() if isinstance(inherited_details, dict) else {}

        if details is not None:
            final_details.update(details)

        return cls(message=message, error_code=final_code, details=final_details, **kwargs)

    @property
    def is_fatal(self) -> bool:
        """Return True if the error is fatal."""
        return self.error_code in _FATAL_ERROR_CODES

    @property
    def is_retriable(self) -> bool:
        """Return True if the error is transient."""
        return self.error_code in _RETRIABLE_ERROR_CODES

    def to_dict(self) -> dict[str, Any]:
        """Serialize the exception to a dictionary."""
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
        """Return the string representation."""
        args = [f"message={self.message!r}", f"error_code={hex(self.error_code)}"]
        excluded_keys = {"message", "error_code", "details", "args"}

        for key, value in self.__dict__.items():
            if key not in excluded_keys and not key.startswith("_"):
                args.append(f"{key}={value!r}")

        if self.details:
            args.append(f"details={self.details!r}")

        return f"{self.__class__.__name__}({', '.join(args)})"

    def __str__(self) -> str:
        """Return the string representation."""
        return f"{self.message} error_code={hex(self.error_code)}"


class AuthenticationError(WebTransportError):
    """Manage authentication-related errors."""

    def __init__(
        self,
        message: str,
        *,
        auth_scheme: str | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_AUTHENTICATION_FAILED,
            details=details,
        )
        self.auth_scheme = auth_scheme


class CertificateError(WebTransportError):
    """Manage certificate-related errors."""

    def __init__(
        self,
        message: str,
        *,
        path: str | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_AUTHENTICATION_FAILED,
            details=details,
        )
        self.path = path


class ClientError(WebTransportError):
    """Manage client-specific errors."""

    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_INVALID_REQUEST,
            details=details,
        )
        self.url = url


class ConfigurationError(WebTransportError):
    """Manage configuration-related errors."""

    def __init__(
        self,
        message: str,
        *,
        config_key: str | None = None,
        config_value: Any | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_INVALID_REQUEST,
            details=details,
        )
        self.config_key = config_key
        self.config_value = config_value


class ConnectionError(WebTransportError):
    """Manage connection-related errors."""

    def __init__(
        self,
        message: str,
        *,
        connection_handle: ConnectionHandle | None = None,
        remote_address: Address | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_GENERIC_ERROR,
            details=details,
        )
        self.connection_handle = connection_handle
        self.remote_address = remote_address


class DatagramError(WebTransportError):
    """Manage datagram-related errors."""

    def __init__(
        self,
        message: str,
        *,
        datagram_size: int | None = None,
        max_size: int | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_GENERIC_ERROR,
            details=details,
        )
        self.datagram_size = datagram_size
        self.max_size = max_size


class FlowControlError(WebTransportError):
    """Manage flow control errors."""

    def __init__(
        self,
        message: str,
        *,
        stream_id: StreamId | None = None,
        actual: int | None = None,
        limit: int | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.WT_FLOW_CONTROL_ERROR,
            details=details,
        )
        self.stream_id = stream_id
        self.actual = actual
        self.limit = limit


class HandshakeError(WebTransportError):
    """Manage handshake-related errors."""

    def __init__(
        self,
        message: str,
        *,
        stage: str | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_GENERIC_ERROR,
            details=details,
        )
        self.stage = stage


class ProtocolError(WebTransportError):
    """Manage protocol violation errors."""

    def __init__(
        self,
        message: str,
        *,
        frame_type: int | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.H3_GENERAL_PROTOCOL_ERROR,
            details=details,
        )
        self.frame_type = frame_type


class ServerError(WebTransportError):
    """Manage server-specific errors."""

    def __init__(
        self,
        message: str,
        *,
        bind_address: Address | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_SERVICE_UNAVAILABLE,
            details=details,
        )
        self.bind_address = bind_address


class SessionError(WebTransportError):
    """Manage WebTransport session errors."""

    def __init__(
        self,
        message: str,
        *,
        session_id: SessionId | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.LIB_SESSION_STATE_ERROR,
            details=details,
        )
        self.session_id = session_id


class SessionClosedError(SessionError):
    """Signal that the WebTransport session has been closed gracefully."""

    def __init__(
        self,
        message: str = "wt_session close",
        *,
        session_id: SessionId | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            session_id=session_id,
            error_code=error_code if error_code is not None else ErrorCodes.APP_NO_ERROR,
            details=details,
        )


class StreamError(WebTransportError):
    """Manage stream-related errors."""

    def __init__(
        self,
        message: str,
        *,
        stream_id: StreamId | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.LIB_STREAM_STATE_ERROR,
            details=details,
        )
        self.stream_id = stream_id


class TimeoutError(WebTransportError):
    """Manage timeout-related errors."""

    def __init__(
        self,
        message: str,
        *,
        operation: str | None = None,
        error_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message,
            error_code=error_code if error_code is not None else ErrorCodes.APP_OPERATION_TIMEOUT,
            details=details,
        )
        self.operation = operation


def _to_snake_case(*, name: str) -> str:
    """Convert a CamelCase string to snake_case."""
    return "".join(["_" + c.lower() if c.isupper() else c for c in name]).lstrip("_")
