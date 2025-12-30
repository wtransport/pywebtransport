"""Unit tests for the pywebtransport.serializer._base module."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Union

import pytest

from pywebtransport.exceptions import SerializationError
from pywebtransport.serializer._base import BaseDataclassSerializer


@dataclass(kw_only=True)
class SimpleDataclass:
    value_int: int
    value_str: str


@dataclass(kw_only=True)
class ComplexDataclass:
    simple: SimpleDataclass
    items: list[int]
    mapping: dict[str, SimpleDataclass]
    optional_value: Any = None
    defaults: str = "default"


@dataclass(kw_only=True)
class DataclassWithMissingField:
    required: str
    optional_with_default: int = 123


class Status(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


class Mode(Enum):
    FAST = "fast"
    SLOW = "slow"


class CustomType:
    def __init__(self, value: str) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CustomType):
            return NotImplemented
        return self.value == other.value


class FailingType:
    def __init__(self, value: Any) -> None:
        raise ValueError("fail")


class IntSubclass(int):
    pass


class TestBaseDataclassSerializer:

    @pytest.fixture
    def serializer(self) -> BaseDataclassSerializer:
        return BaseDataclassSerializer()

    def test_convert_callable_failure_fallback(self, serializer: BaseDataclassSerializer) -> None:
        data = "test"
        result = serializer.convert_to_type(data=data, target_type=FailingType)
        assert result == "test"

    def test_convert_custom_callable(self, serializer: BaseDataclassSerializer) -> None:
        result = serializer.convert_to_type(data="test", target_type=CustomType)
        assert isinstance(result, CustomType)
        assert result.value == "test"

    def test_convert_ignores_extra_fields(self, serializer: BaseDataclassSerializer) -> None:
        data = {"value_int": 1, "value_str": "text", "extra_field": "ignore"}
        result = serializer.convert_to_type(data=data, target_type=SimpleDataclass)
        assert isinstance(result, SimpleDataclass)
        assert not hasattr(result, "extra_field")

    def test_convert_to_complex_dataclass(self, serializer: BaseDataclassSerializer) -> None:
        data = {
            "simple": {"value_int": 1, "value_str": "nested"},
            "items": ["2", "3", "4"],
            "mapping": {
                "first": {"value_int": 100, "value_str": "a"},
                "second": {"value_int": 200, "value_str": "b"},
            },
            "optional_value": "provided",
        }
        result = serializer.convert_to_type(data=data, target_type=ComplexDataclass)
        assert isinstance(result, ComplexDataclass)
        assert isinstance(result.simple, SimpleDataclass)
        assert result.simple.value_int == 1
        assert result.items == [2, 3, 4]
        assert isinstance(result.mapping["first"], SimpleDataclass)
        assert result.mapping["second"].value_int == 200
        assert result.optional_value == "provided"
        assert result.defaults == "default"

    def test_convert_to_simple_dataclass(self, serializer: BaseDataclassSerializer) -> None:
        data = {"value_int": 10, "value_str": "hello"}
        result = serializer.convert_to_type(data=data, target_type=SimpleDataclass)
        assert isinstance(result, SimpleDataclass)
        assert result.value_int == 10
        assert result.value_str == "hello"

    @pytest.mark.parametrize(
        "data, target_type, expected",
        [
            (None, int, None),
            (123, Any, 123),
            ([1], Any, [1]),
            ("42", int, 42),
            (42, str, "42"),
            ("not-an-int", int, "not-an-int"),
            ([1, "a"], list, [1, "a"]),
            ([1, "a"], tuple, (1, "a")),
            ([1, "a", 1], set, {1, "a"}),
            ({"k": "v"}, dict, {"k": "v"}),
            (123, list, 123),
            (123, tuple, 123),
            (123, set, 123),
            (123, dict, 123),
            (["1", "2"], list[int], [1, 2]),
            (("1",), tuple[int], (1,)),
            (["1", "2", "1"], set[int], {1, 2}),
            ({"key": "123"}, dict[str, int], {"key": 123}),
            ({"1": "value"}, dict[int, str], {1: "value"}),
            (
                [{"value_int": "1", "value_str": "a"}],
                list[SimpleDataclass],
                [SimpleDataclass(value_int=1, value_str="a")],
            ),
            (None, int | None, None),
            (None, int | str, None),
            (10, int | str, 10),
            ("hello", int | str, "hello"),
            ("1.5", float | int, 1.5),
            ("123", int | float, 123),
            ("123", Union[int, str], "123"),
            ("not-num", int | float, "not-num"),
        ],
    )
    def test_convert_to_type_various(
        self, serializer: BaseDataclassSerializer, data: Any, target_type: Any, expected: Any
    ) -> None:
        result = serializer.convert_to_type(data=data, target_type=target_type)
        assert result == expected

    def test_enum_conversion_failure(self, serializer: BaseDataclassSerializer) -> None:
        with pytest.raises(SerializationError, match="Invalid value 'invalid' for enum Status"):
            serializer.convert_to_type(data="invalid", target_type=Status)

    def test_enum_conversion_success(self, serializer: BaseDataclassSerializer) -> None:
        result = serializer.convert_to_type(data="active", target_type=Status)
        assert result == Status.ACTIVE

    def test_from_dict_recursion_limit(self, serializer: BaseDataclassSerializer) -> None:
        with pytest.raises(SerializationError, match="Maximum recursion depth exceeded"):
            serializer.from_dict_to_dataclass(data={}, cls=SimpleDataclass, depth=65)

    def test_from_dict_to_dataclass_raises_serialization_error(self, serializer: BaseDataclassSerializer) -> None:
        data = {"optional_with_default": 456}
        with pytest.raises(SerializationError) as exc_info:
            serializer.from_dict_to_dataclass(data=data, cls=DataclassWithMissingField, depth=0)
        assert "Failed to unpack dictionary" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, TypeError)

    def test_non_type_target_passthrough(self, serializer: BaseDataclassSerializer) -> None:
        result_str = serializer.convert_to_type(data="data", target_type="not-a-type-instance")
        result_int = serializer.convert_to_type(data=123, target_type=456)
        assert result_str == "data"
        assert result_int == 123

    def test_recursion_limit_exceeded(self, serializer: BaseDataclassSerializer) -> None:
        data = {"value_int": 1, "value_str": "text"}
        with pytest.raises(SerializationError, match="Maximum recursion depth exceeded"):
            serializer.convert_to_type(data=data, target_type=SimpleDataclass, depth=65)

    def test_regular_class_passthrough(self, serializer: BaseDataclassSerializer) -> None:
        data = 42
        result = serializer.convert_to_type(data=data, target_type=IntSubclass)
        assert isinstance(result, IntSubclass)
        assert result == 42

    def test_union_conversion_with_enum_retry(self, serializer: BaseDataclassSerializer) -> None:
        result = serializer.convert_to_type(data="123", target_type=Union[Status, int])
        assert result == 123

    def test_union_fallthrough_all_failures(self, serializer: BaseDataclassSerializer) -> None:
        data = "invalid"
        result = serializer.convert_to_type(data=data, target_type=Union[Status, Mode])
        assert result == "invalid"
