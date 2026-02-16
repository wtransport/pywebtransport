"""Serializer implementation using the Protocol Buffers format."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from pywebtransport.exceptions import ConfigurationError, SerializationError
from pywebtransport.types import Buffer, Serializer

try:
    from google.protobuf.message import DecodeError, Message
except ImportError:
    Message = None
    DecodeError = None

if TYPE_CHECKING:
    from google.protobuf.message import Message as MessageType


__all__: list[str] = ["ProtobufSerializer"]


class ProtobufSerializer(Serializer):
    """Serializer for encoding and decoding using the Protobuf format."""

    __slots__ = ()

    def __init__(self) -> None:
        """Initialize the Protobuf serializer."""
        if Message is None:
            raise ConfigurationError(
                message="The 'protobuf' library is required for ProtobufSerializer.",
                config_key="dependency.protobuf",
                details={"installation_guide": "Please install it with: pip install pywebtransport[protobuf]"},
            )

    def deserialize(self, *, data: Buffer, obj_type: Any = None) -> MessageType:
        """Deserialize bytes into an instance of the specified Protobuf message class."""
        if obj_type is None:
            raise SerializationError(message="Protobuf deserialization requires a specific 'obj_type'.")

        if not issubclass(obj_type, Message):
            raise SerializationError(
                message=f"Target type '{obj_type.__name__}' is not a valid Protobuf Message class."
            )

        if isinstance(data, memoryview):
            data = bytes(data)

        instance = obj_type()

        try:
            instance.ParseFromString(serialized=data)
            return instance
        except (DecodeError, Exception) as e:
            raise SerializationError(
                message=f"Failed to deserialize data into '{obj_type.__name__}'.", original_exception=e
            ) from e

    def serialize(self, *, obj: Any) -> bytes:
        """Serialize a Protobuf message object into bytes."""
        if not isinstance(obj, Message):
            raise SerializationError(message=f"Object of type '{type(obj).__name__}' is not a valid Protobuf Message.")

        try:
            return cast(bytes, obj.SerializeToString())
        except Exception as e:
            raise SerializationError(message=f"Failed to serialize Protobuf message: {e}", original_exception=e) from e
