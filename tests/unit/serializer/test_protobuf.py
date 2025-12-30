"""Unit tests for the pywebtransport.serializer.protobuf module."""

import importlib
import sys
from typing import Any

import pytest

from pywebtransport.exceptions import ConfigurationError, SerializationError

try:
    from google.protobuf.message import DecodeError, Message
except ImportError:
    Message = None
    DecodeError = None


class MockBaseMessage:
    pass


class MockProtoMessage(MockBaseMessage):
    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    def SerializeToString(self) -> bytes:
        if hasattr(self, "_raise_on_serialize"):
            raise RuntimeError("Serialization failed")
        return b"serialized_data"

    def ParseFromString(self, serialized: bytes) -> None:
        if serialized == b"invalid_data":
            raise RuntimeError("Decode failed")
        self.id = 1
        self.name = "parsed"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MockProtoMessage):
            return NotImplemented
        return self.__dict__ == other.__dict__


class NotAMessage:
    pass


def test_environment_missing_dependency() -> None:
    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "google.protobuf.message", None)
        if "pywebtransport.serializer.protobuf" in sys.modules:
            del sys.modules["pywebtransport.serializer.protobuf"]

        import pywebtransport.serializer.protobuf

        assert getattr(pywebtransport.serializer.protobuf, "Message") is None

    if "pywebtransport.serializer.protobuf" in sys.modules:
        del sys.modules["pywebtransport.serializer.protobuf"]
    importlib.import_module("pywebtransport.serializer.protobuf")


class TestProtobufSerializer:

    @pytest.fixture(autouse=True)
    def setup_dependencies(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("pywebtransport.serializer.protobuf.Message", MockBaseMessage)
        monkeypatch.setattr("pywebtransport.serializer.protobuf.DecodeError", RuntimeError)

    @pytest.fixture
    def serializer(self) -> Any:
        from pywebtransport.serializer.protobuf import ProtobufSerializer

        return ProtobufSerializer()

    def test_deserialize_invalid_data_raises_error(self, serializer: Any) -> None:
        data = b"invalid_data"

        with pytest.raises(SerializationError, match="Failed to deserialize data"):
            serializer.deserialize(data=data, obj_type=MockProtoMessage)

    def test_deserialize_invalid_obj_type_raises_error(self, serializer: Any) -> None:
        with pytest.raises(SerializationError, match="is not a valid Protobuf Message"):
            serializer.deserialize(data=b"data", obj_type=NotAMessage)

    def test_deserialize_memoryview_input(self, serializer: Any) -> None:
        data = b"valid_data"
        mv = memoryview(data)

        result = serializer.deserialize(data=mv, obj_type=MockProtoMessage)

        assert result == MockProtoMessage(id=1, name="parsed")

    def test_deserialize_requires_obj_type(self, serializer: Any) -> None:
        with pytest.raises(SerializationError, match="requires a specific 'obj_type'"):
            serializer.deserialize(data=b"data")

    def test_deserialize_success(self, serializer: Any) -> None:
        data = b"valid_data"

        result = serializer.deserialize(data=data, obj_type=MockProtoMessage)

        assert isinstance(result, MockProtoMessage)
        assert result.id == 1
        assert result.name == "parsed"

    def test_init_raises_configuration_error_if_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("pywebtransport.serializer.protobuf.Message", None)
        from pywebtransport.serializer.protobuf import ProtobufSerializer

        with pytest.raises(ConfigurationError, match="library is required"):
            ProtobufSerializer()

    def test_serialize_internal_failure_raises_error(self, serializer: Any) -> None:
        message = MockProtoMessage(_raise_on_serialize=True)

        with pytest.raises(SerializationError, match="Failed to serialize"):
            serializer.serialize(obj=message)

    def test_serialize_success(self, serializer: Any) -> None:
        message = MockProtoMessage(id=1, name="test")

        result = serializer.serialize(obj=message)

        assert result == b"serialized_data"

    def test_serialize_wrong_object_type_raises_error(self, serializer: Any) -> None:
        message = NotAMessage()

        with pytest.raises(SerializationError, match="is not a valid Protobuf Message"):
            serializer.serialize(obj=message)
