"""Unit tests for the pywebtransport.messaging.stream module."""

import asyncio
import struct
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ConfigurationError, ErrorCodes, StreamError, StructuredStream, WebTransportStream
from pywebtransport.constants import DEFAULT_MAX_MESSAGE_SIZE
from pywebtransport.exceptions import SerializationError
from pywebtransport.types import Serializer


class MockMsgA:
    pass


class MockMsgB:
    pass


class TestStructuredStream:

    @pytest.fixture
    def mock_serializer(self, mocker: MockerFixture) -> MagicMock:
        return mocker.create_autospec(Serializer, spec_set=True, instance=True)

    @pytest.fixture
    def mock_stream(self, mocker: MockerFixture) -> AsyncMock:
        mock = mocker.create_autospec(WebTransportStream, spec_set=True, instance=True)
        mock.readexactly = AsyncMock()
        mock.write = AsyncMock()
        mock.close = AsyncMock()
        mock.stop_receiving = AsyncMock()
        return mock

    @pytest.fixture
    def registry(self) -> dict[int, type[Any]]:
        return {1: MockMsgA, 2: MockMsgB}

    @pytest.fixture
    def structured_stream(
        self, mock_stream: AsyncMock, mock_serializer: MagicMock, registry: dict[int, type[Any]]
    ) -> StructuredStream:
        return StructuredStream(
            stream=mock_stream, serializer=mock_serializer, registry=registry, max_message_size=DEFAULT_MAX_MESSAGE_SIZE
        )

    @pytest.mark.asyncio
    async def test_anext_raises_on_protocol_error(
        self, structured_stream: StructuredStream, mocker: MockerFixture
    ) -> None:
        error = StreamError(message="Protocol error", error_code=ErrorCodes.H3_MESSAGE_ERROR)
        mocker.patch.object(structured_stream, "receive_obj", side_effect=error)

        with pytest.raises(StreamError) as exc_info:
            await structured_stream.__anext__()
        assert exc_info.value is error

    @pytest.mark.asyncio
    async def test_anext_stops_on_clean_close(self, structured_stream: StructuredStream, mocker: MockerFixture) -> None:
        error = StreamError(message="Clean close", error_code=ErrorCodes.NO_ERROR)
        mocker.patch.object(structured_stream, "receive_obj", side_effect=error)

        with pytest.raises(StopAsyncIteration):
            await structured_stream.__anext__()

    @pytest.mark.asyncio
    async def test_async_iteration(self, structured_stream: StructuredStream, mocker: MockerFixture) -> None:
        obj1, obj2 = MockMsgA(), MockMsgB()
        receive_obj_mock = AsyncMock(
            side_effect=[obj1, obj2, StreamError(message="Done", error_code=ErrorCodes.NO_ERROR)]
        )
        mocker.patch.object(structured_stream, "receive_obj", new=receive_obj_mock)
        received_objs = []

        async for obj in structured_stream:
            received_objs.append(obj)

        assert received_objs == [obj1, obj2]
        assert receive_obj_mock.await_count == 3

    @pytest.mark.asyncio
    async def test_close_method(self, structured_stream: StructuredStream, mock_stream: AsyncMock) -> None:
        await structured_stream.close()

        mock_stream.close.assert_awaited_once()

    def test_init(
        self,
        structured_stream: StructuredStream,
        mock_stream: AsyncMock,
        mock_serializer: MagicMock,
        registry: dict[int, type[Any]],
    ) -> None:
        expected_class_to_id = {MockMsgA: 1, MockMsgB: 2}

        assert structured_stream._stream is mock_stream
        assert structured_stream._serializer is mock_serializer
        assert structured_stream._registry is registry
        assert structured_stream._class_to_id == expected_class_to_id
        assert structured_stream._max_message_size == DEFAULT_MAX_MESSAGE_SIZE
        assert isinstance(structured_stream._write_lock, asyncio.Lock)

    def test_init_with_duplicate_registry_types_raises_error(
        self, mock_stream: AsyncMock, mock_serializer: MagicMock
    ) -> None:
        faulty_registry = {1: MockMsgA, 2: MockMsgA}

        with pytest.raises(ConfigurationError, match="Types in the structured stream registry must be unique"):
            StructuredStream(
                stream=mock_stream, serializer=mock_serializer, registry=faulty_registry, max_message_size=1024
            )

    @pytest.mark.parametrize("closed_status", [True, False])
    def test_is_closed_property(
        self, structured_stream: StructuredStream, mock_stream: AsyncMock, closed_status: bool
    ) -> None:
        type(mock_stream).is_closed = closed_status

        assert structured_stream.is_closed is closed_status

    @pytest.mark.asyncio
    async def test_receive_obj_eof_clean(self, structured_stream: StructuredStream, mock_stream: AsyncMock) -> None:
        mock_stream.readexactly.side_effect = asyncio.IncompleteReadError(b"", 8)

        with pytest.raises(StreamError) as exc_info:
            await structured_stream.receive_obj()

        assert exc_info.value.error_code == ErrorCodes.NO_ERROR
        assert "Stream closed cleanly" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_receive_obj_exceeds_max_message_size_raises_error(
        self, mock_stream: AsyncMock, mock_serializer: MagicMock, registry: dict[int, type[Any]]
    ) -> None:
        stream = StructuredStream(
            stream=mock_stream, serializer=mock_serializer, registry=registry, max_message_size=100
        )
        type_id = 1
        large_payload_len = 101
        header = struct.pack("!HI", type_id, large_payload_len)
        mock_stream.readexactly.return_value = header

        with pytest.raises(SerializationError, match="exceeds the configured limit"):
            await stream.receive_obj()

        mock_stream.stop_receiving.assert_awaited_once_with(error_code=ErrorCodes.APPLICATION_ERROR)

    @pytest.mark.asyncio
    async def test_receive_obj_incomplete_header_raises_stream_error(
        self, structured_stream: StructuredStream, mock_stream: AsyncMock
    ) -> None:
        mock_stream.readexactly.side_effect = asyncio.IncompleteReadError(b"part", 8)

        with pytest.raises(StreamError) as exc_info:
            await structured_stream.receive_obj()

        assert exc_info.value.error_code == ErrorCodes.H3_MESSAGE_ERROR
        assert "waiting for message header" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_receive_obj_incomplete_payload_raises_stream_error(
        self, structured_stream: StructuredStream, mock_stream: AsyncMock
    ) -> None:
        type_id = 1
        payload_len = 100
        header = struct.pack("!HI", type_id, payload_len)
        mock_stream.readexactly.side_effect = [header, asyncio.IncompleteReadError(b"partial", payload_len)]

        with pytest.raises(StreamError) as exc_info:
            await structured_stream.receive_obj()

        assert exc_info.value.error_code == ErrorCodes.H3_MESSAGE_ERROR
        assert "reading payload" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_receive_obj_successful(
        self, structured_stream: StructuredStream, mock_stream: AsyncMock, mock_serializer: MagicMock
    ) -> None:
        type_id = 1
        message_class = MockMsgA
        payload = b"payload_data"
        header = struct.pack("!HI", type_id, len(payload))
        mock_stream.readexactly.side_effect = [header, payload]
        deserialized_obj = MockMsgA()
        mock_serializer.deserialize.return_value = deserialized_obj

        result = await structured_stream.receive_obj()

        assert mock_stream.readexactly.await_count == 2
        mock_stream.readexactly.assert_any_await(n=struct.calcsize("!HI"))
        mock_stream.readexactly.assert_any_await(n=len(payload))
        mock_serializer.deserialize.assert_called_once_with(data=payload, obj_type=message_class)
        assert result is deserialized_obj

    @pytest.mark.asyncio
    async def test_receive_obj_unknown_type_id_raises_error(
        self, structured_stream: StructuredStream, mock_stream: AsyncMock, mock_serializer: MagicMock
    ) -> None:
        unknown_type_id = 99
        header = struct.pack("!HI", unknown_type_id, 10)
        mock_stream.readexactly.return_value = header

        with pytest.raises(SerializationError, match=f"Received unknown message type ID: {unknown_type_id}"):
            await structured_stream.receive_obj()

        mock_stream.readexactly.assert_awaited_once()
        mock_serializer.deserialize.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_obj_concurrency_lock(
        self, structured_stream: StructuredStream, mock_stream: AsyncMock, mock_serializer: MagicMock
    ) -> None:
        obj = MockMsgA()
        mock_serializer.serialize.return_value = b"data"

        async def slow_write(data: bytes) -> None:
            await asyncio.sleep(0.01)

        mock_stream.write.side_effect = slow_write

        await asyncio.gather(structured_stream.send_obj(obj=obj), structured_stream.send_obj(obj=obj))

        assert mock_stream.write.await_count == 2

    @pytest.mark.asyncio
    async def test_send_obj_successful(
        self, structured_stream: StructuredStream, mock_stream: AsyncMock, mock_serializer: MagicMock
    ) -> None:
        obj_to_send = MockMsgB()
        type_id = 2
        serialized_payload = b"some_serialized_data"
        mock_serializer.serialize.return_value = serialized_payload

        await structured_stream.send_obj(obj=obj_to_send)

        mock_serializer.serialize.assert_called_once_with(obj=obj_to_send)
        header = struct.pack("!HI", type_id, len(serialized_payload))
        full_packet = header + serialized_payload

        mock_stream.write.assert_awaited_once_with(data=full_packet)

    @pytest.mark.asyncio
    async def test_send_obj_unregistered_raises_error(
        self, structured_stream: StructuredStream, mock_stream: AsyncMock, mock_serializer: MagicMock
    ) -> None:
        class UnregisteredMsg:
            pass

        with pytest.raises(SerializationError):
            await structured_stream.send_obj(obj=UnregisteredMsg())

        mock_serializer.serialize.assert_not_called()
        mock_stream.write.assert_not_awaited()

    def test_stream_id_property(self, structured_stream: StructuredStream, mock_stream: AsyncMock) -> None:
        expected_id = 123
        type(mock_stream).stream_id = expected_id

        assert structured_stream.stream_id == expected_id
