"""Unit tests for the pywebtransport.serializer.json module."""

import base64
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import pytest

from pywebtransport.exceptions import SerializationError
from pywebtransport.serializer import JSONSerializer


@dataclass(kw_only=True)
class BinaryData:
    content: bytes


class NonSerializable:
    pass


@dataclass(kw_only=True)
class SimpleData:
    id: int
    name: str


class Status(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


class TestJSONSerializer:

    @pytest.fixture
    def serializer(self) -> JSONSerializer:
        return JSONSerializer()

    def test_deserialize_base64_bytes(self, serializer: JSONSerializer) -> None:
        raw = b"hello world"
        encoded = base64.b64encode(raw).decode("ascii")
        json_data = f'{{"content": "{encoded}"}}'.encode("utf-8")

        result = serializer.deserialize(data=json_data, obj_type=BinaryData)

        assert isinstance(result, BinaryData)
        assert result.content == raw

    def test_deserialize_bytearray(self, serializer: JSONSerializer) -> None:
        raw = b"bytearray content"
        encoded = base64.b64encode(raw).decode("ascii")
        json_data = f'"{encoded}"'.encode("utf-8")

        result = serializer.deserialize(data=json_data, obj_type=bytearray)

        assert isinstance(result, bytearray)
        assert result == raw

    def test_deserialize_invalid_base64_fallback(self, serializer: JSONSerializer) -> None:
        data = b'"invalid-base64!"'

        result = serializer.deserialize(data=data, obj_type=bytes)

        assert result == "invalid-base64!"

    @pytest.mark.parametrize(
        argnames="invalid_data", argvalues=[b"{'id': 1, 'name': 'test'}", b'{"id": 1, "name": "test"', b"not json"]
    )
    def test_deserialize_invalid_json_raises_error(self, serializer: JSONSerializer, invalid_data: bytes) -> None:
        with pytest.raises(expected_exception=SerializationError, match="Data is not valid JSON"):
            serializer.deserialize(data=invalid_data)

    def test_deserialize_memoryview_input(self, serializer: JSONSerializer) -> None:
        data = b'{"id": 1, "name": "test"}'
        mv = memoryview(data)

        result = serializer.deserialize(data=mv, obj_type=SimpleData)

        assert result == SimpleData(id=1, name="test")

    def test_deserialize_to_dataclass(self, serializer: JSONSerializer) -> None:
        data = b'{"id": 1, "name": "test"}'

        result = serializer.deserialize(data=data, obj_type=SimpleData)

        assert result == SimpleData(id=1, name="test")

    def test_deserialize_to_dataclass_with_type_conversion(self, serializer: JSONSerializer) -> None:
        data = b'{"id": "1", "name": 123}'

        result = serializer.deserialize(data=data, obj_type=SimpleData)

        assert result == SimpleData(id=1, name="123")

    def test_deserialize_to_dict(self, serializer: JSONSerializer) -> None:
        data = b'{"id": 1, "name": "test"}'

        result = serializer.deserialize(data=data)

        assert result == {"id": 1, "name": "test"}

    def test_deserialize_type_mismatch_raises_error(self, serializer: JSONSerializer) -> None:
        data = b'{"name": "test"}'

        with pytest.raises(expected_exception=SerializationError, match="Failed to unpack"):
            serializer.deserialize(data=data, obj_type=SimpleData)

    def test_init(self, serializer: JSONSerializer) -> None:
        assert serializer._dump_kwargs == {}
        assert serializer._load_kwargs == {}
        assert serializer._user_default is None

        assert not hasattr(serializer, "__dict__")

    def test_init_with_dump_kwargs(self) -> None:
        serializer = JSONSerializer(dump_kwargs={"indent": 2, "sort_keys": True})
        instance = SimpleData(id=1, name="test")
        expected = b'{\n  "id": 1,\n  "name": "test"\n}'

        result = serializer.serialize(obj=instance)

        assert result == expected

    def test_init_with_load_kwargs(self) -> None:
        serializer = JSONSerializer(load_kwargs={"parse_float": str})
        data = b'{"value": 1.5}'

        result = serializer.deserialize(data=data)

        assert result["value"] == "1.5"

    def test_serialize_custom_default_handler(self) -> None:
        def custom_default(o: Any) -> Any:
            if isinstance(o, complex):
                return f"complex({o.real}, {o.imag})"
            raise TypeError(f"Unknown type {type(o)}")

        serializer = JSONSerializer(dump_kwargs={"default": custom_default})
        data = complex(1, 2)
        expected = b'"complex(1.0, 2.0)"'

        result = serializer.serialize(obj=data)

        assert result == expected

    def test_serialize_dataclass(self, serializer: JSONSerializer) -> None:
        instance = SimpleData(id=1, name="test")
        expected = b'{"id": 1, "name": "test"}'

        result = serializer.serialize(obj=instance)

        assert result == expected

    def test_serialize_dict(self, serializer: JSONSerializer) -> None:
        data = {"key": "value", "items": [1, True, None]}
        expected = b'{"key": "value", "items": [1, true, null]}'

        result = serializer.serialize(obj=data)

        assert result == expected

    def test_serialize_extended_types(self, serializer: JSONSerializer) -> None:
        test_uuid = uuid.uuid4()
        test_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        data = {"bytes": b"test", "uuid": test_uuid, "enum": Status.ACTIVE, "set": {1, 2}, "datetime": test_time}

        json_bytes = serializer.serialize(obj=data)
        result = serializer.deserialize(data=json_bytes)

        assert result["bytes"] == base64.b64encode(b"test").decode("ascii")
        assert result["uuid"] == str(test_uuid)
        assert result["enum"] == "active"
        assert set(result["set"]) == {1, 2}
        assert result["datetime"] == test_time.isoformat()

    def test_serialize_unsupported_type_raises_error(self, serializer: JSONSerializer) -> None:
        with pytest.raises(expected_exception=SerializationError) as exc_info:
            serializer.serialize(obj=NonSerializable())

        assert "is not JSON serializable" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, TypeError)
