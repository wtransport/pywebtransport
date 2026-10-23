"""Common exceptions."""

from __future__ import annotations

from typing import Any, Self

from pywebtransport.constants import ErrorCodes
from pywebtransport.types import ConnectionHandle, ErrorCode, SessionId, StreamId

__all__: list[str] = [
    "ClientError",
    "ConfigurationError",
    "ConnectionError",
    "DatagramError",
    "ProtocolError",
    "ServerError",
    "SessionError",
    "StreamError",
    "TimeoutError",
    "WebTransportError",
]


class WebTransportError(Exception):
    """Manage the base WebTransport exception."""

    def __init__(self, message: str, *, error_code: ErrorCode | None = None) -> None:
        """Initialize the instance."""
        super().__init__(message)
        self.message = message
        self.error_code = error_code if error_code is not None else ErrorCodes.APP_GENERIC_ERROR

    @classmethod
    def from_cause(cls, message: str, *, cause: Exception, error_code: ErrorCode | None = None, **kwargs: Any) -> Self:
        """Instantiate a domain exception inheriting the cause's error code."""
        inherited_error_code = getattr(cause, "error_code", None)
        final_error_code = error_code if error_code is not None else inherited_error_code
        return cls(message=message, error_code=final_error_code, **kwargs)

    def __repr__(self) -> str:
        """Return the debug representation."""
        repr_parts = [f"message={self.message!r}", f"error_code={hex(self.error_code)}"]
        excluded_keys = {"message", "error_code", "args"}

        for key, value in self.__dict__.items():
            if key not in excluded_keys and not key.startswith("_"):
                repr_parts.append(f"{key}={value!r}")

        return f"{self.__class__.__name__}({', '.join(repr_parts)})"

    def __str__(self) -> str:
        """Return the display string."""
        return f"{self.message} error_code={hex(self.error_code)}"


class ClientError(WebTransportError):
    """Manage client-related errors."""

    def __init__(self, message: str, *, error_code: ErrorCode | None = None) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.APP_INVALID_REQUEST
        )


class ConfigurationError(WebTransportError):
    """Manage configuration-related errors."""

    def __init__(
        self,
        message: str,
        *,
        config_key: str | None = None,
        config_value: Any | None = None,
        error_code: ErrorCode | None = None,
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.APP_INVALID_REQUEST
        )
        self.config_key = config_key
        self.config_value = config_value


class ConnectionError(WebTransportError):
    """Manage connection-related errors."""

    def __init__(
        self, message: str, *, connection_handle: ConnectionHandle | None = None, error_code: ErrorCode | None = None
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.APP_GENERIC_ERROR
        )
        self.connection_handle = connection_handle


class DatagramError(WebTransportError):
    """Manage datagram-related errors."""

    def __init__(self, message: str, *, error_code: ErrorCode | None = None) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.APP_GENERIC_ERROR
        )


class ProtocolError(WebTransportError):
    """Manage protocol violation errors."""

    def __init__(self, message: str, *, error_code: ErrorCode | None = None) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.H3_GENERAL_PROTOCOL_ERROR
        )


class ServerError(WebTransportError):
    """Manage server-related errors."""

    def __init__(self, message: str, *, error_code: ErrorCode | None = None) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.APP_SERVICE_UNAVAILABLE
        )


class SessionError(WebTransportError):
    """Manage session-related errors."""

    def __init__(
        self, message: str, *, session_id: SessionId | None = None, error_code: ErrorCode | None = None
    ) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.LIB_SESSION_STATE_ERROR
        )
        self.session_id = session_id


class StreamError(WebTransportError):
    """Manage stream-related errors."""

    def __init__(self, message: str, *, stream_id: StreamId | None = None, error_code: ErrorCode | None = None) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.LIB_STREAM_STATE_ERROR
        )
        self.stream_id = stream_id


class TimeoutError(WebTransportError):
    """Manage timeout-related errors."""

    def __init__(self, message: str, *, error_code: ErrorCode | None = None) -> None:
        """Initialize the instance."""
        super().__init__(
            message=message, error_code=error_code if error_code is not None else ErrorCodes.APP_OPERATION_TIMEOUT
        )
