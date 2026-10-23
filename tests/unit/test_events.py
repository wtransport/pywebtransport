"""Unit tests for the pywebtransport.events module."""

import asyncio
from collections import defaultdict
from typing import Any

import pytest
from pytest_mock import MockerFixture

from pywebtransport import Event
from pywebtransport.events import EventEmitter
from pywebtransport.types import EventType


@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Any:
    return mocker.patch(target="pywebtransport.events._logger")


class TestEvent:

    def test_event_equality(self) -> None:
        event1 = Event(type=EventType.SESSION_READY, data={"a": 1})
        event2 = Event(type=EventType.SESSION_READY, data={"a": 1})
        event3 = Event(type=EventType.SESSION_READY, data={"a": 2})

        assert event1 == event2
        assert event1 != event3

    def test_event_explicit_init(self) -> None:
        event = Event(type=EventType.SESSION_READY, data={"foo": "bar"})

        assert event.data == {"foo": "bar"}
        assert not hasattr(event, "__dict__")

    def test_initialization_with_enum(self) -> None:
        event = Event(type=EventType.CONNECTION_ESTABLISHED)

        assert event.type == EventType.CONNECTION_ESTABLISHED
        assert event.data is None


class TestEventEmitter:

    @pytest.fixture
    def emitter(self) -> EventEmitter:
        return EventEmitter()

    @pytest.mark.asyncio
    async def test_close(self, emitter: EventEmitter, mocker: MockerFixture, mock_logger: Any) -> None:
        handler = mocker.Mock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        await emitter.close()

        assert len(emitter._handlers[EventType.SESSION_READY]) == 0
        mock_logger.debug.assert_any_call("rt_task destroy count=%d", 1)

    @pytest.mark.asyncio
    async def test_close_cancels_background_tasks(self, emitter: EventEmitter) -> None:
        async def hang() -> None:
            await asyncio.sleep(delay=10)

        async def quick() -> None:
            pass

        task1 = asyncio.create_task(coro=hang())
        task2 = asyncio.create_task(coro=quick())

        await task2

        emitter._background_tasks.add(task1)
        emitter._background_tasks.add(task2)

        await asyncio.sleep(delay=0)

        await emitter.close()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await task1

        assert task1.cancelled()
        assert task2.done()
        assert not task2.cancelled()

    @pytest.mark.asyncio
    async def test_close_no_handlers_registered(self, emitter: EventEmitter, mock_logger: Any) -> None:
        await emitter.close()

        destroy_log_found = any(
            call.args and call.args[0] == "rt_task destroy count=%d" for call in mock_logger.debug.call_args_list
        )
        assert not destroy_log_found

    @pytest.mark.asyncio
    async def test_emit_nowait_no_loop(self, emitter: EventEmitter, mocker: MockerFixture, mock_logger: Any) -> None:
        mocker.patch(target="asyncio.create_task", side_effect=RuntimeError)
        mocker.patch.object(target=EventEmitter, attribute="_process_event", new_callable=mocker.Mock)
        handler = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        emitter.emit_nowait(event_type=EventType.SESSION_READY)

        mock_logger.warning.assert_called_once_with("rt_event send failed event=%s", EventType.SESSION_READY)
        handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_emit_nowait_success(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        assert len(emitter._background_tasks) == 0

        emitter.emit_nowait(event_type=EventType.SESSION_READY, data={"k": "v"})

        assert len(emitter._background_tasks) == 1

        await asyncio.sleep(delay=0.01)

        handler.assert_awaited_once()
        call_args = handler.call_args[0][0]
        assert call_args.data == {"k": "v"}

    @pytest.mark.asyncio
    async def test_emit_with_awaitable_non_coroutine(self, emitter: EventEmitter) -> None:
        future: asyncio.Future[None] = asyncio.Future()

        def handler(event: Event) -> asyncio.Future[None]:
            future.set_result(None)
            return future

        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        await emitter.emit(event_type=EventType.SESSION_READY)

        assert future.done()

    @pytest.mark.asyncio
    async def test_emit_with_no_handlers(self, emitter: EventEmitter, mock_logger: Any) -> None:
        await emitter.emit(event_type=EventType.SESSION_CLOSED)

        emit_log_found = any(call.args and "rt_event send" in call.args[0] for call in mock_logger.debug.call_args_list)
        assert not emit_log_found

    @pytest.mark.asyncio
    async def test_emit_with_sync_handler(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        await emitter.emit(event_type=EventType.SESSION_READY)

        handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_handler_raises_exception(
        self, emitter: EventEmitter, mocker: MockerFixture, mock_logger: Any
    ) -> None:
        handler1 = mocker.AsyncMock(side_effect=ValueError("handler failed"))
        handler2 = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler1)
        emitter.on(event_type=EventType.SESSION_READY, handler=handler2)

        await emitter.emit(event_type=EventType.SESSION_READY)

        mock_logger.warning.assert_called_once_with(
            "rt_task resolve failed event=%s err=%s", EventType.SESSION_READY, mocker.ANY, exc_info=True
        )
        handler1.assert_awaited_once()
        handler2.assert_awaited_once()

    def test_init(self) -> None:
        emitter = EventEmitter()

        assert isinstance(emitter._background_tasks, set)
        assert len(emitter._background_tasks) == 0

        assert isinstance(emitter._handlers, defaultdict)
        assert isinstance(emitter._once_handlers, defaultdict)

        assert not hasattr(emitter, "__dict__")

    @pytest.mark.asyncio
    async def test_off(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.on(event_type=EventType.CONNECTION_CLOSED, handler=handler)

        emitter.off(event_type=EventType.CONNECTION_CLOSED, handler=handler)
        await emitter.emit(event_type=EventType.CONNECTION_CLOSED)

        handler.assert_not_called()

    def test_off_all_handlers_empty(self, emitter: EventEmitter, mock_logger: Any) -> None:
        emitter.off(event_type=EventType.SESSION_READY, handler=None)

        destroy_log_found = any(
            call.args and call.args[0] == "rt_task destroy count=%d event=%s"
            for call in mock_logger.debug.call_args_list
        )
        assert not destroy_log_found

    @pytest.mark.asyncio
    async def test_off_all_handlers_for_event(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler1 = mocker.AsyncMock()
        handler2 = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler1)
        emitter.once(event_type=EventType.SESSION_READY, handler=handler2)

        emitter.off(event_type=EventType.SESSION_READY, handler=None)
        await emitter.emit(event_type=EventType.SESSION_READY)

        handler1.assert_not_awaited()
        handler2.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_off_all_handlers_only_once_registered(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        emitter.once(event_type=EventType.SESSION_READY, handler=handler)

        emitter.off(event_type=EventType.SESSION_READY, handler=None)
        await emitter.emit(event_type=EventType.SESSION_READY)

        handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_off_all_handlers_only_persistent_registered(
        self, emitter: EventEmitter, mocker: MockerFixture
    ) -> None:
        handler = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        emitter.off(event_type=EventType.SESSION_READY, handler=None)
        await emitter.emit(event_type=EventType.SESSION_READY)

        handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_off_removes_once_handler(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.once(event_type=EventType.CONNECTION_CLOSED, handler=handler)

        emitter.off(event_type=EventType.CONNECTION_CLOSED, handler=handler)
        await emitter.emit(event_type=EventType.CONNECTION_CLOSED)

        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_off_unknown_handler(self, emitter: EventEmitter, mocker: MockerFixture, mock_logger: Any) -> None:
        handler = mocker.Mock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)
        unknown = mocker.Mock()

        emitter.off(event_type=EventType.SESSION_READY, handler=unknown)

        destroy_log_found = any(
            call.args and call.args[0] == "rt_task destroy event=%s" for call in mock_logger.debug.call_args_list
        )
        assert not destroy_log_found
        assert handler in emitter._handlers[EventType.SESSION_READY]

    @pytest.mark.asyncio
    async def test_on_and_emit(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        await emitter.emit(event_type=EventType.SESSION_READY, data={"test": 1})

        handler.assert_awaited_once()
        event = handler.call_args[0][0]
        assert isinstance(event, Event)
        assert event.type == EventType.SESSION_READY
        assert event.data == {"test": 1}

    @pytest.mark.asyncio
    async def test_once(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        emitter.once(event_type=EventType.STREAM_OPENED, handler=handler)

        await emitter.emit(event_type=EventType.STREAM_OPENED)
        await emitter.emit(event_type=EventType.STREAM_OPENED)

        handler.assert_awaited_once()
        assert len(emitter._once_handlers[EventType.STREAM_OPENED]) == 0

    @pytest.mark.asyncio
    async def test_once_duplicate_handler(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        emitter.once(event_type=EventType.SESSION_READY, handler=handler)

        emitter.once(event_type=EventType.SESSION_READY, handler=handler)

        assert len(emitter._once_handlers[EventType.SESSION_READY]) == 1

    @pytest.mark.asyncio
    async def test_registering_same_handler_twice_warning(
        self, emitter: EventEmitter, mocker: MockerFixture, mock_logger: Any
    ) -> None:
        handler = mocker.AsyncMock()

        emitter.on(event_type=EventType.SESSION_READY, handler=handler)
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        assert len(emitter._handlers[EventType.SESSION_READY]) == 1
        mock_logger.warning.assert_called_once_with("rt_task create failed event=%s", EventType.SESSION_READY)

    @pytest.mark.asyncio
    async def test_wait_for_is_cancelled(self, emitter: EventEmitter) -> None:
        event_type = EventType.SESSION_READY
        wait_task = asyncio.create_task(coro=emitter.wait_for(event_type=event_type))
        await asyncio.sleep(delay=0.01)

        assert len(emitter._handlers[event_type]) == 1
        wait_task.cancel()
        with pytest.raises(expected_exception=asyncio.CancelledError):
            await wait_task

        assert len(emitter._handlers[event_type]) == 0

    @pytest.mark.asyncio
    async def test_wait_for_list_of_events(self, emitter: EventEmitter) -> None:
        wait_task = asyncio.create_task(
            coro=emitter.wait_for(event_type=[EventType.SESSION_READY, EventType.STREAM_OPENED])
        )
        await asyncio.sleep(delay=0.01)

        await emitter.emit(event_type=EventType.STREAM_OPENED, data={"id": 1})
        await emitter.emit(event_type=EventType.SESSION_READY)

        try:
            async with asyncio.timeout(delay=1):
                result = await wait_task
        except TimeoutError:
            wait_task.cancel()
            raise

        assert result.type == EventType.STREAM_OPENED

    @pytest.mark.asyncio
    async def test_wait_for_success(self, emitter: EventEmitter) -> None:
        wait_task = asyncio.create_task(coro=emitter.wait_for(event_type=EventType.SESSION_READY))
        await asyncio.sleep(delay=0.01)

        await emitter.emit(event_type=EventType.SESSION_READY, data={"id": "abc"})
        try:
            async with asyncio.timeout(delay=1):
                result = await wait_task
        except TimeoutError:
            wait_task.cancel()
            raise

        assert result.data == {"id": "abc"}

    @pytest.mark.asyncio
    async def test_wait_for_timeout(self, emitter: EventEmitter) -> None:
        with pytest.raises(expected_exception=asyncio.TimeoutError):
            await emitter.wait_for(event_type=EventType.SESSION_READY, timeout=0.01)
