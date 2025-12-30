"""Serializer implementation using the MsgPack format."""

from __future__ import annotations

import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any, cast

from pywebtransport.exceptions import ConfigurationError, SerializationError
from pywebtransport.serializer._base import BaseDataclassSerializer
from pywebtransport.types import Buffer, Serializer

try:
    import msgpack
except ImportError:
    msgpack = None


__all__: list[str] = ["MsgPackSerializer"]


class MsgPackSerializer(BaseDataclassSerializer, Serializer):
    """Serializer for encoding and decoding using the MsgPack format."""

    def __init__(
        self, *, pack_kwargs: dict[str, Any] | None = None, unpack_kwargs: dict[str, Any] | None = None
    ) -> None:
        """Initialize the MsgPack serializer."""
        if msgpack is None:
            raise ConfigurationError(
                message="The 'msgpack' library is required for MsgPackSerializer.",
                config_key="dependency.msgpack",
                details={"installation_guide": "Please install it with: pip install pywebtransport[msgpack]"},
            )

        self._pack_kwargs = pack_kwargs.copy() if pack_kwargs is not None else {}
        self._unpack_kwargs = unpack_kwargs.copy() if unpack_kwargs is not None else {}
        self._user_default = self._pack_kwargs.pop("default", None)

    def deserialize(self, *, data: Buffer, obj_type: Any = None) -> Any:
        """Deserialize a MsgPack byte string into a Python object."""
        try:
            unpack_kwargs = {"raw": False, **self._unpack_kwargs}
            decoded_obj = msgpack.unpackb(packed=data, **unpack_kwargs)

            if obj_type is None:
                return decoded_obj
            return self.convert_to_type(data=decoded_obj, target_type=obj_type)
        except (msgpack.UnpackException, TypeError, ValueError) as e:
            raise SerializationError(
                message="Data is not valid MsgPack or cannot be unpacked.", original_exception=e
            ) from e

    def serialize(self, *, obj: Any) -> bytes:
        """Serialize a Python object into a MsgPack byte string."""
        try:
            return cast(bytes, msgpack.packb(o=obj, default=self._default_handler, **self._pack_kwargs))
        except TypeError as e:
            raise SerializationError(
                message=f"Object of type {type(obj).__name__} is not MsgPack serializable.", original_exception=e
            ) from e

    def _default_handler(self, o: Any) -> Any:
        """Handle types not natively supported by MsgPack."""
        match o:
            case uuid.UUID():
                return str(o)
            case Enum():
                return o.value
            case set() | frozenset():
                return list(o)
            case datetime():
                return o.isoformat()
            case _ if is_dataclass(o) and not isinstance(o, type):
                return asdict(obj=o)
            case _:
                if self._user_default is not None:
                    return self._user_default(o)
                raise TypeError(f"Object of type {type(o).__name__} is not MsgPack serializable")
