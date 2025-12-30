"""Base class for serializers handling dataclass conversion."""

from __future__ import annotations

import types
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, Union, get_args, get_origin

from pywebtransport.exceptions import SerializationError

__all__: list[str] = []


_FIELDS_CACHE: dict[type[Any], tuple[Any, ...]] = {}


def _get_cached_fields(*, cls: type[Any]) -> tuple[Any, ...]:
    if cls in _FIELDS_CACHE:
        return _FIELDS_CACHE[cls]

    cls_fields = fields(cls)
    _FIELDS_CACHE[cls] = cls_fields
    return cls_fields


class BaseDataclassSerializer:
    """Base class providing recursive dict-to-dataclass conversion."""

    _MAX_RECURSION_DEPTH = 64

    def convert_to_type(self, *, data: Any, target_type: Any, depth: int = 0) -> Any:
        """Recursively convert a decoded object to a specific target type."""
        if depth > self._MAX_RECURSION_DEPTH:
            raise SerializationError(message="Maximum recursion depth exceeded during deserialization.")

        if target_type is Any:
            return data

        if data is None:
            origin = get_origin(target_type)
            if origin is Union or origin is types.UnionType:
                if type(None) in get_args(target_type):
                    return None
            return None

        origin = get_origin(target_type)
        args = get_args(target_type)

        if origin is Union or origin is types.UnionType:
            non_none_types = [t for t in args if t is not type(None)]

            for candidate in non_none_types:
                candidate_origin = get_origin(candidate) or candidate
                if isinstance(data, candidate_origin):
                    return self.convert_to_type(data=data, target_type=candidate, depth=depth)

            for candidate in non_none_types:
                try:
                    return self.convert_to_type(data=data, target_type=candidate, depth=depth)
                except (TypeError, ValueError, SerializationError):
                    continue

        if isinstance(target_type, type):
            if is_dataclass(target_type) and isinstance(data, dict):
                return self.from_dict_to_dataclass(data=data, cls=target_type, depth=depth + 1)

            if issubclass(target_type, Enum):
                try:
                    return target_type(data)
                except ValueError as e:
                    raise SerializationError(message=f"Invalid value '{data}' for enum {target_type.__name__}") from e

        if origin in (list, tuple, set) or target_type in (list, tuple, set):
            if isinstance(data, (list, tuple, set)):
                container = origin or target_type
                if not args:
                    return container(data)
                inner_type = args[0]
                items = [self.convert_to_type(data=item, target_type=inner_type, depth=depth + 1) for item in data]
                return container(items)

        if (origin is dict or target_type is dict) and isinstance(data, dict):
            if not args:
                return data
            key_type, value_type = args
            return {
                self.convert_to_type(data=k, target_type=key_type, depth=depth + 1): self.convert_to_type(
                    data=v, target_type=value_type, depth=depth + 1
                )
                for k, v in data.items()
            }

        if callable(target_type) and not isinstance(data, target_type):
            try:
                return target_type(data)
            except (TypeError, ValueError):
                pass

        return data

    def from_dict_to_dataclass(self, *, data: dict[str, Any], cls: type[Any], depth: int) -> Any:
        """Recursively convert a dictionary to a dataclass instance."""
        if depth > self._MAX_RECURSION_DEPTH:
            raise SerializationError(message="Maximum recursion depth exceeded during dataclass unpacking.")

        constructor_args = {}
        for field in _get_cached_fields(cls=cls):
            if field.name in data:
                field_value = data[field.name]
                constructor_args[field.name] = self.convert_to_type(
                    data=field_value, target_type=field.type, depth=depth + 1
                )

        try:
            return cls(**constructor_args)
        except TypeError as e:
            raise SerializationError(
                message=f"Failed to unpack dictionary to dataclass {cls.__name__}.", original_exception=e
            ) from e
