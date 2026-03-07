"""Unit tests for the pywebtransport.messaging.datagram module."""

import struct
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pytest_asyncio import fixture as asyncio_fixture

from pywebtransport import ConfigurationError, Event, SessionError, StructuredDatagramTransport, TimeoutError
from pywebtransport.exceptions import SerializationError
from pywebtransport.types import EventType


class TestStructuredDatagramTransport:

    @pytest.fixture
    def mock_serializer(self) -> Mock:
        serializer = Mock()
        serializer.serialize.side_effect = lambda obj: str(obj).encode("utf-8")
        serializer.deserialize.side_effect = lambda data, obj_type: int(data.tobytes().decode("utf-8"))
        return serializer

    @pytest.fixture
    def mock_session(self) -> Mock:
        session = Mock()
        session.is_closed = False
        session.session_id = "test_session_id"
        session.events = Mock()
        session.send_datagram = AsyncMock()
        return session

    @pytest.fixture
    def registry(self) -> dict[int, type[Any]]:
        return {1: int, 2: str}

    @asyncio_fixture
    async def transport(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> StructuredDatagramTransport:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)
        transport.initialize()
        return transport

    @pytest.mark.asyncio
    async def test_aenter_aexit(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)

        async with transport as t:
            assert t is transport
            assert t._is_initialized
            mock_session.events.on.assert_called_once()
            assert not t.is_closed

        assert transport.is_closed
        mock_session.events.off.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_behavior(self, mock_session: Mock, transport: StructuredDatagramTransport) -> None:
        await transport.close()

        assert transport.is_closed
        mock_session.events.off.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="error", argvalues=[KeyError("Handler key error"), ValueError("Handler not found")]
    )
    async def test_close_handles_events_off_errors(
        self, mock_session: Mock, transport: StructuredDatagramTransport, error: Exception
    ) -> None:
        mock_session.events.off.side_effect = error

        await transport.close()

        assert transport.is_closed
        mock_session.events.off.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_handles_garbage_collected_session(
        self, mock_session: Mock, transport: StructuredDatagramTransport
    ) -> None:
        with patch.object(target=transport, attribute="_session", return_value=None):
            await transport.close()

        assert transport.is_closed
        mock_session.events.off.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_idempotency(self, mock_session: Mock, transport: StructuredDatagramTransport) -> None:
        await transport.close()
        await transport.close()

        mock_session.events.off.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_uninitialized_transport(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)

        await transport.close()

        assert transport.is_closed
        mock_session.events.off.assert_not_called()

    @pytest.mark.asyncio
    async def test_handler_delegates_to_internal_method(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)
        transport.initialize()
        handler = mock_session.events.on.call_args.kwargs["handler"]

        header = struct.pack("!H", 1)
        payload = b"123"
        event = Event(type=EventType.DATAGRAM_RECEIVED, data={"data": header + payload})

        await handler(event)

        mock_serializer.deserialize.assert_called_once()
        assert transport._incoming_obj_queue is not None
        assert transport._incoming_obj_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_handler_weakref_behavior(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)
        mock_weakref = Mock(return_value=None)

        with patch(target="weakref.ref", return_value=mock_weakref):
            transport.initialize()

        handler = mock_session.events.on.call_args.kwargs["handler"]
        event = Event(type=EventType.DATAGRAM_RECEIVED, data={"data": b"dummy"})

        await handler(event)

        mock_serializer.deserialize.assert_not_called()

    def test_init(self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)

        assert transport._session() is mock_session
        assert transport._registry is registry
        assert transport._serializer is mock_serializer
        assert transport._class_to_id == {int: 1, str: 2}
        assert transport._closed is False
        assert transport._handler_ref is None
        assert transport._incoming_obj_queue is None
        assert transport._is_initialized is False
        assert transport._sentinel is not None

        assert not hasattr(transport, "__dict__")

    def test_init_duplicate_registry_types_raises_error(self, mock_session: Mock, mock_serializer: Mock) -> None:
        registry: dict[int, type[Any]] = {1: int, 2: int}

        with pytest.raises(expected_exception=ConfigurationError):
            StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)

    def test_initialize_garbage_collected_session_raises_error(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)

        with patch.object(target=transport, attribute="_session", return_value=None):
            with pytest.raises(expected_exception=SessionError, match="parent session is already gone"):
                transport.initialize()

    def test_initialize_session_closed_raises_error(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> None:
        mock_session.is_closed = True
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)

        with pytest.raises(expected_exception=SessionError, match="parent session is closed"):
            transport.initialize()

    def test_initialize_success_and_idempotency(
        self, mock_session: Mock, transport: StructuredDatagramTransport
    ) -> None:
        transport.initialize()

        assert mock_session.events.on.call_count == 1
        call_args = mock_session.events.on.call_args
        assert call_args.kwargs["event_type"] == EventType.DATAGRAM_RECEIVED
        assert callable(call_args.kwargs["handler"])

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="session_state, transport_state, expected_closed",
        argvalues=[
            ("active", "open", False),
            ("active", "closed", True),
            ("closed", "open", True),
            ("collected", "open", True),
        ],
    )
    async def test_is_closed_property(
        self,
        mock_session: Mock,
        transport: StructuredDatagramTransport,
        session_state: str,
        transport_state: str,
        expected_closed: bool,
    ) -> None:
        if transport_state == "closed":
            await transport.close()

        if session_state == "closed":
            mock_session.is_closed = True
        elif session_state == "collected":
            with patch.object(target=transport, attribute="_session", return_value=None):
                assert transport.is_closed is expected_closed
                return

        assert transport.is_closed is expected_closed

    @pytest.mark.asyncio
    async def test_on_datagram_received_errors(
        self, transport: StructuredDatagramTransport, mock_serializer: Mock
    ) -> None:
        header = struct.pack("!H", 1)
        payload = b"123"
        event = Event(type=EventType.DATAGRAM_RECEIVED, data={"data": header + payload})
        mock_serializer.deserialize.side_effect = RuntimeError("Generic failure")

        with patch(target="pywebtransport.messaging.datagram._logger") as mock_logger:
            await transport._on_datagram_received(event=event)
            mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="event_data, closed_state, expect_process",
        argvalues=[
            ({"data": struct.pack("!H", 1) + b"valid"}, False, True),
            ({"data": struct.pack("!H", 1) + b"valid"}, True, False),
            ("not a dict", False, False),
            ({}, False, False),
            ({"data": None}, False, False),
            ({"data": b"1"}, False, False),
        ],
    )
    async def test_on_datagram_received_ignored_cases(
        self,
        transport: StructuredDatagramTransport,
        mock_serializer: Mock,
        event_data: Any,
        closed_state: bool,
        expect_process: bool,
    ) -> None:
        if closed_state:
            await transport.close()

        event = Event(type=EventType.DATAGRAM_RECEIVED, data=event_data)
        await transport._on_datagram_received(event=event)

        if expect_process:
            mock_serializer.deserialize.assert_called()
        else:
            mock_serializer.deserialize.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_datagram_received_queue_full(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)
        transport.initialize(queue_size=1)
        header = struct.pack("!H", 1)
        payload = b"123"
        event = Event(type=EventType.DATAGRAM_RECEIVED, data={"data": header + payload})

        with patch(target="pywebtransport.messaging.datagram._logger") as mock_logger:
            await transport._on_datagram_received(event=event)
            await transport._on_datagram_received(event=event)
            mock_logger.warning.assert_called()

    @pytest.mark.asyncio
    async def test_on_datagram_received_queue_full_session_collected(
        self, mock_session: Mock, mock_serializer: Mock, registry: dict[int, type[Any]]
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)
        transport.initialize(queue_size=1)
        header = struct.pack("!H", 1)
        payload = b"123"
        event = Event(type=EventType.DATAGRAM_RECEIVED, data={"data": header + payload})

        with patch(target="pywebtransport.messaging.datagram._logger") as mock_logger:
            await transport._on_datagram_received(event=event)
            with patch.object(target=transport, attribute="_session", return_value=None):
                await transport._on_datagram_received(event=event)

            assert mock_logger.warning.call_count == 1
            args, _ = mock_logger.warning.call_args
            assert "unknown" in args

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="header_val, payload, error_type",
        argvalues=[(1, b"invalid", SerializationError), (999, b"123", SerializationError)],
    )
    async def test_receive_obj_drops_bad_datagrams(
        self,
        transport: StructuredDatagramTransport,
        mock_serializer: Mock,
        header_val: int,
        payload: bytes,
        error_type: type[Exception],
    ) -> None:
        if header_val == 999:
            pass
        else:
            mock_serializer.deserialize.side_effect = error_type("fail")

        header = struct.pack("!H", header_val)
        event = Event(type=EventType.DATAGRAM_RECEIVED, data={"data": header + payload})

        with patch(target="pywebtransport.messaging.datagram._logger") as mock_logger:
            await transport._on_datagram_received(event=event)
            mock_logger.warning.assert_called()

        with pytest.raises(expected_exception=TimeoutError):
            await transport.receive_obj(timeout=0.01)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="scenario, expected_error, match",
        argvalues=[
            ("uninitialized", SessionError, "not been initialized"),
            ("closed_transport", SessionError, "is closed"),
            ("poison_pill", SessionError, "closed while receiving"),
            ("timeout", TimeoutError, "Receive object timeout"),
        ],
    )
    async def test_receive_obj_errors(
        self,
        mock_session: Mock,
        mock_serializer: Mock,
        registry: dict[int, type[Any]],
        scenario: str,
        expected_error: type[Exception],
        match: str,
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)

        if scenario != "uninitialized":
            transport.initialize()

        if scenario == "closed_transport":
            await transport.close()

        if scenario == "poison_pill":
            assert transport._incoming_obj_queue is not None
            transport._incoming_obj_queue.put_nowait(item=transport._sentinel)

        if scenario == "timeout":
            kwargs = {"timeout": 0.01}
        else:
            kwargs = {}

        with pytest.raises(expected_exception=expected_error, match=match):
            await transport.receive_obj(**kwargs)

    @pytest.mark.asyncio
    async def test_receive_obj_success(self, transport: StructuredDatagramTransport) -> None:
        header = struct.pack("!H", 1)
        payload = b"123"
        event = Event(type=EventType.DATAGRAM_RECEIVED, data={"data": header + payload})

        await transport._on_datagram_received(event=event)
        obj = await transport.receive_obj()

        assert obj == 123

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="scenario, expected_error, match",
        argvalues=[
            ("uninitialized", SessionError, "not been initialized"),
            ("session_closed", SessionError, "Session is closed"),
            ("session_collected", SessionError, "Session is closed"),
            ("unregistered_type", SerializationError, "not registered"),
        ],
    )
    async def test_send_obj_errors(
        self,
        mock_session: Mock,
        mock_serializer: Mock,
        registry: dict[int, type[Any]],
        scenario: str,
        expected_error: type[Exception],
        match: str,
    ) -> None:
        transport = StructuredDatagramTransport(session=mock_session, registry=registry, serializer=mock_serializer)

        if scenario != "uninitialized":
            transport.initialize()

        obj: Any = 123
        patcher = None

        if scenario == "session_closed":
            mock_session.is_closed = True
        elif scenario == "session_collected":
            patcher = patch.object(target=transport, attribute="_session", return_value=None)
            patcher.start()
        elif scenario == "unregistered_type":
            obj = 1.23

        try:
            with pytest.raises(expected_exception=expected_error, match=match):
                await transport.send_obj(obj=obj)
        finally:
            if patcher:
                patcher.stop()

    @pytest.mark.asyncio
    async def test_send_obj_success(
        self, mock_session: Mock, mock_serializer: Mock, transport: StructuredDatagramTransport
    ) -> None:
        obj = 123
        expected_header = struct.pack("!H", 1)
        expected_payload = b"123"
        expected_data = b"".join((expected_header, expected_payload))

        await transport.send_obj(obj=obj)

        mock_serializer.serialize.assert_called_once_with(obj=obj)
        mock_session.send_datagram.assert_awaited_once_with(data=expected_data)
