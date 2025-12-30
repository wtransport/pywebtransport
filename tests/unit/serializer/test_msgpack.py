"""Unit tests for the pywebtransport.serializer.msgpack module."""

import importlib
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import pytest

from pywebtransport.exceptions import ConfigurationError, SerializationError

try:
    import msgpack
except ImportError:
    msgpack = None


class NonSerializable:
    pass


@dataclass(kw_only=True)
class SimpleData:
    id: int
    name: str


class Status(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


def test_module_import_handles_missing_msgpack() -> None:
    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "msgpack", None)
        if "pywebtransport.serializer.msgpack" in sys.modules:
            del sys.modules["pywebtransport.serializer.msgpack"]

        import pywebtransport.serializer.msgpack

        assert getattr(pywebtransport.serializer.msgpack, "msgpack") is None

    if "pywebtransport.serializer.msgpack" in sys.modules:
        del sys.modules["pywebtransport.serializer.msgpack"]
    importlib.import_module("pywebtransport.serializer.msgpack")


@pytest.mark.skipif(msgpack is None, reason="msgpack library not installed")
class TestMsgPackSerializer:

    @pytest.fixture
    def serializer(self) -> Any:
        from pywebtransport.serializer.msgpack import MsgPackSerializer

        return MsgPackSerializer()

    def test_deserialize_invalid_data_raises_error(self, serializer: Any) -> None:
        with pytest.raises(SerializationError, match="Data is not valid MsgPack"):
            serializer.deserialize(data=b"\xc1")

    def test_deserialize_memoryview_input(self, serializer: Any) -> None:
        data = msgpack.packb({"id": 1, "name": "test"})
        mv = memoryview(data)

        result = serializer.deserialize(data=mv, obj_type=SimpleData)

        assert result == SimpleData(id=1, name="test")

    def test_deserialize_to_dataclass(self, serializer: Any) -> None:
        data = msgpack.packb({"id": 1, "name": "test"})

        result = serializer.deserialize(data=data, obj_type=SimpleData)

        assert result == SimpleData(id=1, name="test")

    def test_deserialize_to_dict(self, serializer: Any) -> None:
        data = msgpack.packb({"id": 1, "name": "test"})

        result = serializer.deserialize(data=data)

        assert result == {"id": 1, "name": "test"}

    def test_deserialize_type_mismatch_raises_error(self, serializer: Any) -> None:
        data = msgpack.packb({"name": "test"})

        with pytest.raises(SerializationError, match="Failed to unpack"):
            serializer.deserialize(data=data, obj_type=SimpleData)

    def test_deserialize_with_unpack_kwargs(self) -> None:
        from pywebtransport.serializer.msgpack import MsgPackSerializer

        serializer = MsgPackSerializer(unpack_kwargs={"use_list": False})
        data = msgpack.packb([1, 2, 3])

        result = serializer.deserialize(data=data)

        assert isinstance(result, tuple)
        assert result == (1, 2, 3)

    def test_init_raises_configuration_error_if_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("pywebtransport.serializer.msgpack.msgpack", None)
        from pywebtransport.serializer.msgpack import MsgPackSerializer

        with pytest.raises(ConfigurationError, match="library is required"):
            MsgPackSerializer()

    def test_serialize_custom_default_handler(self) -> None:
        def custom_default(o: Any) -> Any:
            if isinstance(o, complex):
                return {"real": o.real, "imag": o.imag}
            raise TypeError(f"Unknown type {type(o)}")

        from pywebtransport.serializer.msgpack import MsgPackSerializer

        serializer = MsgPackSerializer(pack_kwargs={"default": custom_default})
        data = complex(1, 2)

        result = serializer.serialize(obj=data)
        unpacked = serializer.deserialize(data=result)

        assert unpacked == {"real": 1.0, "imag": 2.0}

    def test_serialize_dataclass(self, serializer: Any) -> None:
        instance = SimpleData(id=1, name="test")

        result = serializer.serialize(obj=instance)
        unpacked = msgpack.unpackb(result)

        assert unpacked == {"id": 1, "name": "test"}

    def test_serialize_dict(self, serializer: Any) -> None:
        data = {"key": "value", "items": [1, True, None]}

        result = serializer.serialize(obj=data)
        unpacked = msgpack.unpackb(result)

        assert unpacked == data

    def test_serialize_extended_types(self, serializer: Any) -> None:
        test_uuid = uuid.uuid4()
        test_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        data = {
            "uuid": test_uuid,
            "enum": Status.ACTIVE,
            "set": {1, 2},
            "frozenset": frozenset([3, 4]),
            "datetime": test_time,
        }

        packed = serializer.serialize(obj=data)
        result = serializer.deserialize(data=packed)

        assert result["uuid"] == str(test_uuid)
        assert result["enum"] == "active"
        assert set(result["set"]) == {1, 2}
        assert set(result["frozenset"]) == {3, 4}
        assert result["datetime"] == test_time.isoformat()

    def test_serialize_unsupported_type_raises_error(self, serializer: Any) -> None:
        with pytest.raises(SerializationError) as exc_info:
            serializer.serialize(obj=NonSerializable())

        assert "is not MsgPack serializable" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, TypeError)

    def test_serialize_with_pack_kwargs(self) -> None:
        from pywebtransport.serializer.msgpack import MsgPackSerializer

        serializer = MsgPackSerializer(pack_kwargs={"use_single_float": True})
        data = 1.5

        result = serializer.serialize(obj=data)

        assert isinstance(result, bytes)
