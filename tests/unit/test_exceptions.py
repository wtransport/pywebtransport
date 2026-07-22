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
    SessionClosedError,
    SessionError,
    StreamError,
    TimeoutError,
    WebTransportError,
)
from pywebtransport.exceptions import AuthenticationError, CertificateError, FlowControlError, HandshakeError


class TestSubclassExceptions:

    @pytest.mark.parametrize(
        argnames="exc_class, kwargs, expected_category",
        argvalues=[
            (AuthenticationError, {"auth_scheme": "token"}, "authentication"),
            (CertificateError, {"path": "/c.pem"}, "certificate"),
            (ClientError, {"url": "https://a.com"}, "client"),
            (ConfigurationError, {"config_key": "timeout"}, "configuration"),
            (ConnectionError, {"remote_address": ("1.1.1.1", 443)}, "connection"),
            (DatagramError, {"max_size": 1500}, "datagram"),
            (FlowControlError, {"stream_id": 1}, "flow_control"),
            (HandshakeError, {"stage": "alpn"}, "handshake"),
            (ProtocolError, {"frame_type": 0x41}, "protocol"),
            (ServerError, {"bind_address": ("0.0.0.0", 443)}, "server"),
            (SessionClosedError, {"session_id": 100}, "session_closed"),
            (SessionError, {"session_id": 100}, "session"),
            (StreamError, {"stream_id": 5}, "stream"),
            (TimeoutError, {"operation": "read"}, "timeout"),
        ],
    )
    def test_category_derivation(
        self, exc_class: type[WebTransportError], kwargs: dict[str, Any], expected_category: str
    ) -> None:
        exc = exc_class(message="test", **kwargs)

        assert exc.category == expected_category

    def test_custom_attributes_in_repr(self) -> None:
        exc = ClientError(message="invalid url", url="https://example.com")

        r = repr(exc)

        assert "ClientError" in r
        assert "message='invalid url'" in r
        assert "url='https://example.com'" in r

    def test_custom_attributes_in_to_dict(self) -> None:
        exc = DatagramError(message="too big", datagram_size=9000, max_size=1500)

        data = exc.to_dict()

        assert data["type"] == "DatagramError"
        assert data["datagram_size"] == 9000
        assert data["max_size"] == 1500


class TestWebTransportErrorBase:

    def test_category_without_error_suffix(self) -> None:
        class CustomFault(WebTransportError):
            pass

        exc = CustomFault(message="something failed")

        assert exc.category == "custom_fault"

    def test_dynamic_repr_generation(self) -> None:
        exc = WebTransportError(message="base error", error_code=0x1)

        assert repr(exc) == "WebTransportError(message='base error', error_code=0x1)"

    def test_dynamic_repr_with_details(self) -> None:
        details = {"info": "debug"}
        exc = WebTransportError(message="msg", error_code=0x1, details=details)

        assert "details={'info': 'debug'}" in repr(exc)

    def test_error_properties_fatal(self) -> None:
        exc = WebTransportError(message="fatal", error_code=ErrorCodes.QUIC_INTERNAL_ERROR)

        assert exc.is_fatal is True
        assert exc.is_retriable is False

    def test_error_properties_retriable(self) -> None:
        exc = WebTransportError(message="retry", error_code=ErrorCodes.APP_CONNECTION_TIMEOUT)

        assert exc.is_fatal is False
        assert exc.is_retriable is True

    def test_error_properties_unknown_code(self) -> None:
        exc = WebTransportError(message="unknown", error_code=0x999999)

        assert exc.is_fatal is False
        assert exc.is_retriable is False

    def test_from_cause(self) -> None:
        cause = WebTransportError(message="original", error_code=0x10, details={"k": "v"})
        exc = WebTransportError.from_cause("wrapped", cause=cause, details={"new_k": "new_v"})

        assert exc.message == "wrapped"
        assert exc.error_code == 0x10
        assert exc.details == {"k": "v", "new_k": "new_v"}

    def test_from_cause_override_code_and_standard_cause(self) -> None:
        cause = ValueError("standard error")
        exc = WebTransportError.from_cause("wrapped standard", cause=cause, error_code=0x20)

        assert exc.message == "wrapped standard"
        assert exc.error_code == 0x20
        assert exc.details == {}

    def test_initialization_defaults(self) -> None:
        exc = WebTransportError(message="base error")

        assert exc.message == "base error"
        assert exc.error_code == ErrorCodes.APP_GENERIC_ERROR
        assert exc.details == {}
        assert str(exc) == "base error error_code=0x1001"
        assert exc.category == "web_transport"

    def test_to_dict_structure(self) -> None:
        exc = WebTransportError(message="base error", error_code=ErrorCodes.QUIC_INTERNAL_ERROR)

        data = exc.to_dict()

        assert data["type"] == "WebTransportError"
        assert data["category"] == "web_transport"
        assert data["message"] == "base error"
        assert data["error_code"] == ErrorCodes.QUIC_INTERNAL_ERROR
        assert data["is_fatal"] is True
        assert data["is_retriable"] is False
        assert data["details"] == {}
