"""Unit tests for the pywebtransport.exceptions module."""

from typing import Any

import pytest

from pywebtransport import (
    ClientError,
    ConfigurationError,
    ConnectionError,
    DatagramError,
    ErrorCodes,
    ProtocolError,
    ServerError,
    SessionError,
    StreamError,
    TimeoutError,
    WebTransportError,
)


class TestSubclassExceptions:

    def test_custom_attributes_in_repr(self) -> None:
        exception = ConfigurationError(message="invalid value", config_key="max_sessions", config_value=-1)

        repr_output = repr(exception)

        assert "ConfigurationError" in repr_output
        assert "message='invalid value'" in repr_output
        assert "config_key='max_sessions'" in repr_output
        assert "config_value=-1" in repr_output

    @pytest.mark.parametrize(
        argnames="exception_class, kwargs",
        argvalues=[
            (ConfigurationError, {"config_key": "timeout", "config_value": -1}),
            (ConnectionError, {"connection_handle": 42}),
            (SessionError, {"session_id": 100}),
            (StreamError, {"stream_id": 5}),
        ],
    )
    def test_subclass_specific_attributes(
        self, exception_class: type[WebTransportError], kwargs: dict[str, Any]
    ) -> None:
        exception = exception_class(message="test", **kwargs)

        for key, value in kwargs.items():
            assert getattr(exception, key) == value

    @pytest.mark.parametrize(
        argnames="exception_class, default_code",
        argvalues=[
            (ClientError, ErrorCodes.APP_INVALID_REQUEST),
            (DatagramError, ErrorCodes.APP_GENERIC_ERROR),
            (ProtocolError, ErrorCodes.H3_GENERAL_PROTOCOL_ERROR),
            (ServerError, ErrorCodes.APP_SERVICE_UNAVAILABLE),
            (TimeoutError, ErrorCodes.APP_OPERATION_TIMEOUT),
        ],
    )
    def test_subclass_default_error_code(self, exception_class: type[WebTransportError], default_code: int) -> None:
        exception = exception_class(message="test")

        assert exception.error_code == default_code


class TestWebTransportErrorBase:

    def test_dynamic_repr_generation(self) -> None:
        exception = WebTransportError(message="base error", error_code=0x1)

        assert repr(exception) == "WebTransportError(message='base error', error_code=0x1)"

    def test_from_cause(self) -> None:
        cause = WebTransportError(message="original", error_code=0x10)
        exception = WebTransportError.from_cause("wrapped", cause=cause)

        assert exception.message == "wrapped"
        assert exception.error_code == 0x10

    def test_from_cause_override_code_and_standard_cause(self) -> None:
        cause = ValueError("standard error")
        exception = WebTransportError.from_cause("wrapped standard", cause=cause, error_code=0x20)

        assert exception.message == "wrapped standard"
        assert exception.error_code == 0x20

    def test_from_cause_standard_cause_without_code(self) -> None:
        cause = ValueError("standard error")
        exception = WebTransportError.from_cause("wrapped standard", cause=cause)

        assert exception.error_code == ErrorCodes.APP_GENERIC_ERROR

    def test_initialization_defaults(self) -> None:
        exception = WebTransportError(message="base error")

        assert exception.message == "base error"
        assert exception.error_code == ErrorCodes.APP_GENERIC_ERROR
        assert str(exception) == "base error error_code=0x1001"
