"""Serializer implementation using the JSON format."""

from __future__ import annotations

import base64
import json
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from pywebtransport.exceptions import SerializationError
from pywebtransport.serializer._base import BaseDataclassSerializer
from pywebtransport.types import Buffer, Serializer

__all__: list[str] = ["JSONSerializer"]


class JSONSerializer(BaseDataclassSerializer, Serializer):
    """Serializer for encoding and decoding using the JSON format."""

    def __init__(self, *, dump_kwargs: dict[str, Any] | None = None, load_kwargs: dict[str, Any] | None = None) -> None:
        """Initialize the JSON serializer."""
        self._dump_kwargs = dump_kwargs.copy() if dump_kwargs is not None else {}
        self._load_kwargs = load_kwargs.copy() if load_kwargs is not None else {}
        self._user_default = self._dump_kwargs.pop("default", None)

    def convert_to_type(self, *, data: Any, target_type: Any, depth: int = 0) -> Any:
        """Recursively convert a decoded object to a specific target type."""
        if isinstance(data, str) and target_type in (bytes, bytearray):
            try:
                decoded = base64.b64decode(data)
                if target_type is bytearray:
                    return bytearray(decoded)
                return decoded
            except (ValueError, TypeError):
                pass

        return super().convert_to_type(data=data, target_type=target_type, depth=depth)

    def deserialize(self, *, data: Buffer, obj_type: Any = None) -> Any:
        """Deserialize a JSON byte string into a Python object."""
        try:
            if isinstance(data, memoryview):
                data = bytes(data)

            decoded_obj = json.loads(s=data, **self._load_kwargs)

            if obj_type is None:
                return decoded_obj
            return self.convert_to_type(data=decoded_obj, target_type=obj_type)
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            raise SerializationError(
                message="Data is not valid JSON or cannot be unpacked.", original_exception=e
            ) from e

    def serialize(self, *, obj: Any) -> bytes:
        """Serialize a Python object into a JSON byte string."""
        try:
            return json.dumps(obj=obj, default=self._default_handler, **self._dump_kwargs).encode("utf-8")
        except TypeError as e:
            raise SerializationError(
                message=f"Object of type {type(obj).__name__} is not JSON serializable.", original_exception=e
            ) from e

    def _default_handler(self, o: Any) -> Any:
        """Handle types not natively supported by JSON."""
        match o:
            case bytes() | bytearray() | memoryview():
                return base64.b64encode(o).decode("ascii")
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
                raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")
