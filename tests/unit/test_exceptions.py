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
from pywebtransport.exceptions import (
    AuthenticationError,
    CertificateError,
    FlowControlError,
    HandshakeError,
    SerializationError,
)
from pywebtransport.types import SessionState


class TestSubclassExceptions:

    @pytest.mark.parametrize(
        argnames="exc_class, kwargs, expected_category",
        argvalues=[
            (AuthenticationError, {"auth_method": "token"}, "authentication"),
            (CertificateError, {"certificate_path": "/c.pem"}, "certificate"),
            (ClientError, {"target_url": "https://a.com"}, "client"),
            (ConfigurationError, {"config_key": "timeout"}, "configuration"),
            (ConnectionError, {"remote_address": ("1.1.1.1", 443)}, "connection"),
            (DatagramError, {"max_size": 1500}, "datagram"),
            (FlowControlError, {"stream_id": 1}, "flow_control"),
            (HandshakeError, {"handshake_stage": "alpn"}, "handshake"),
            (ProtocolError, {"frame_type": 0x41}, "protocol"),
            (SerializationError, {}, "serialization"),
            (ServerError, {"bind_address": ("0.0.0.0", 443)}, "server"),
            (SessionError, {"session_id": 100}, "session"),
            (StreamError, {"stream_id": 5}, "stream"),
            (TimeoutError, {"operation": "read"}, "timeout"),
        ],
    )
    def test_category_derivation(
        self, exc_class: type[WebTransportError], kwargs: dict[str, Any], expected_category: str
    ) -> None:
        exc = exc_class(message="Test", **kwargs)

        assert exc.category == expected_category

    def test_custom_attributes_in_repr(self) -> None:
        exc = ClientError(message="Invalid URL", target_url="https://example.com")

        r = repr(exc)

        assert "ClientError" in r
        assert "message='Invalid URL'" in r
        assert "target_url='https://example.com'" in r

    def test_custom_attributes_in_to_dict(self) -> None:
        exc = DatagramError(message="Too big", datagram_size=9000, max_size=1500)

        data = exc.to_dict()

        assert data["type"] == "DatagramError"
        assert data["datagram_size"] == 9000
        assert data["max_size"] == 1500

    def test_serialization_error_handles_exception_object(self) -> None:
        cause = ValueError("Parsing failed")
        exc = SerializationError(message="Bad data", original_exception=cause)

        data = exc.to_dict()

        assert data["original_exception"] == "Parsing failed"
        assert isinstance(exc.original_exception, ValueError)

    def test_session_error_with_enum(self) -> None:
        exc = SessionError(message="Closed", session_state=SessionState.CLOSED)

        data = exc.to_dict()

        assert data["session_state"] == SessionState.CLOSED
        assert "session_state=<SessionState.CLOSED: 'closed'>" in repr(exc)

    def test_stream_error_custom_str(self) -> None:
        exc_no_id = StreamError(message="No ID")
        exc_with_id = StreamError(message="With ID", stream_id=5)

        assert str(exc_no_id) == f"[{hex(exc_no_id.error_code)}] No ID"
        assert str(exc_with_id) == f"[{hex(exc_with_id.error_code)}] With ID (stream_id=5)"


class TestWebTransportErrorBase:

    def test_category_without_error_suffix(self) -> None:
        class CustomFault(WebTransportError):
            pass

        exc = CustomFault(message="Something failed")

        assert exc.category == "custom_fault"

    def test_dynamic_repr_generation(self) -> None:
        exc = WebTransportError(message="Base error", error_code=0x1)

        assert repr(exc) == "WebTransportError(message='Base error', error_code=0x1)"

    def test_dynamic_repr_with_details(self) -> None:
        details = {"info": "debug"}
        exc = WebTransportError(message="Msg", error_code=0x1, details=details)

        assert "details={'info': 'debug'}" in repr(exc)

    def test_error_properties_fatal(self) -> None:
        exc = WebTransportError(message="Fatal", error_code=ErrorCodes.INTERNAL_ERROR)

        assert exc.is_fatal is True
        assert exc.is_retriable is False

    def test_error_properties_retriable(self) -> None:
        exc = WebTransportError(message="Retry", error_code=ErrorCodes.APP_CONNECTION_TIMEOUT)

        assert exc.is_fatal is False
        assert exc.is_retriable is True

    def test_error_properties_unknown_code(self) -> None:
        exc = WebTransportError(message="Unknown", error_code=0x999999)

        assert exc.is_fatal is False
        assert exc.is_retriable is False

    def test_initialization_defaults(self) -> None:
        exc = WebTransportError(message="Base error")

        assert exc.message == "Base error"
        assert exc.error_code == ErrorCodes.INTERNAL_ERROR
        assert exc.details == {}
        assert str(exc) == "[0x1] Base error"
        assert exc.category == "web_transport"

    def test_to_dict_structure(self) -> None:
        exc = WebTransportError(message="Base error", error_code=ErrorCodes.INTERNAL_ERROR)

        data = exc.to_dict()

        assert data["type"] == "WebTransportError"
        assert data["category"] == "web_transport"
        assert data["message"] == "Base error"
        assert data["error_code"] == ErrorCodes.INTERNAL_ERROR
        assert data["is_fatal"] is True
        assert data["is_retriable"] is False
        assert data["details"] == {}
