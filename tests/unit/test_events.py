"""Unit tests for the pywebtransport.events module."""

import asyncio
from collections import defaultdict, deque
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
        event1 = Event(type=EventType.SESSION_READY, timestamp=100.0)
        event2 = Event(type=EventType.SESSION_READY, timestamp=100.0)
        event3 = Event(type=EventType.SESSION_READY, timestamp=200.0)

        assert event1 == event2
        assert event1 != event3

    def test_event_explicit_init(self) -> None:
        event = Event(type=EventType.SESSION_READY, timestamp=999.99, data={"foo": "bar"}, source="src")

        assert event.timestamp == 999.99
        assert event.data == {"foo": "bar"}
        assert event.source == "src"
        assert not hasattr(event, "__dict__")

    def test_init_with_non_string_type(self) -> None:
        event = Event(type=123)  # type: ignore[arg-type]
        assert event.type == 123  # type: ignore[comparison-overlap]

    def test_initialization_with_enum(self) -> None:
        event = Event(type=EventType.CONNECTION_ESTABLISHED)

        assert event.type == EventType.CONNECTION_ESTABLISHED
        assert isinstance(event.timestamp, float)
        assert event.timestamp > 0.0
        assert event.data is None

    def test_post_init_str_to_enum_conversion(self) -> None:
        event = Event(type="connection_established")

        assert event.type == EventType.CONNECTION_ESTABLISHED

    def test_post_init_unknown_str_logs_warning(self, mock_logger: Any) -> None:
        event = Event(type="custom_event")

        assert event.type == "custom_event"
        mock_logger.warning.assert_called_once_with("rt_event validate invalid actual=%s", "custom_event")

    def test_repr_and_str(self) -> None:
        event = Event(type=EventType.CONNECTION_CLOSED, timestamp=12345.6789)

        assert repr(event) == "Event(type=connection_closed, timestamp=12345.6789)"
        assert str(event) == "Event(connection_closed)"

    def test_to_dict(self) -> None:
        event = Event(type=EventType.SESSION_READY, timestamp=12345.6789, data={"id": 1}, source="test_source")
        expected_dict = {
            "type": EventType.SESSION_READY,
            "timestamp": 12345.6789,
            "data": {"id": 1},
            "source": "test_source",
        }

        event_dict = event.to_dict()

        assert event_dict == expected_dict

    def test_to_dict_with_none_source(self) -> None:
        event = Event(type=EventType.SESSION_READY, data={"id": 1}, source=None)

        event_dict = event.to_dict()

        assert event_dict["source"] is None


class TestEventEmitter:

    @pytest.fixture
    def emitter(self) -> EventEmitter:
        return EventEmitter(max_listeners=3, max_history=10)

    @pytest.mark.asyncio
    async def test_clear_history(self, emitter: EventEmitter) -> None:
        await emitter.emit(event_type=EventType.SESSION_READY)
        assert len(emitter.get_event_history()) == 1

        emitter.clear_history()

        assert len(emitter.get_event_history()) == 0

    @pytest.mark.asyncio
    async def test_close(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        real_task = asyncio.create_task(coro=asyncio.sleep(delay=10))
        cancel_spy = mocker.spy(obj=real_task, name="cancel")
        emitter._processing_task = real_task
        emitter.on(event_type=EventType.SESSION_READY, handler=mocker.Mock())

        await emitter.close()

        cancel_spy.assert_called_once()
        assert real_task.cancelled()
        assert emitter.get_stats()["total_handlers"] == 0

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
    async def test_close_with_done_task(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        mock_task = mocker.Mock(done=lambda: True)
        emitter._processing_task = mock_task

        await emitter.close()

        mock_task.cancel.assert_not_called()

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
    async def test_emit_nowait_paused(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)
        emitter.pause()

        emitter.emit_nowait(event_type=EventType.SESSION_READY)
        await asyncio.sleep(delay=0)

        handler.assert_not_awaited()
        assert emitter.get_stats()["queued_events"] == 1

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
    async def test_event_history_and_filtering(self, emitter: EventEmitter) -> None:
        await emitter.emit(event_type=EventType.SESSION_READY)
        await emitter.emit(event_type=EventType.STREAM_OPENED)
        await emitter.emit(event_type=EventType.DATAGRAM_RECEIVED)

        history = emitter.get_event_history()
        assert len(history) == 3

        limited_history = emitter.get_event_history(limit=1)
        assert len(limited_history) == 1
        assert limited_history[0].type == EventType.DATAGRAM_RECEIVED

        filtered_history = emitter.get_event_history(event_type=EventType.STREAM_OPENED)
        assert len(filtered_history) == 1
        assert filtered_history[0].type == EventType.STREAM_OPENED

    @pytest.mark.asyncio
    async def test_event_queue_full_warning(self, mocker: MockerFixture, mock_logger: Any) -> None:
        emitter = EventEmitter(max_queue_size=1)
        emitter.pause()

        emitter.emit_nowait(event_type=EventType.SESSION_READY)
        emitter.emit_nowait(event_type=EventType.SESSION_READY)

        mock_logger.warning.assert_called_once_with("rt_channel validate exceeded actual=%d limit=%d", 1, 1)

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
        emitter = EventEmitter(max_listeners=5, max_history=10, max_queue_size=20)

        assert emitter._max_listeners == 5
        assert emitter._max_history == 10

        assert isinstance(emitter._background_tasks, set)
        assert len(emitter._background_tasks) == 0

        assert isinstance(emitter._event_history, deque)
        assert emitter._event_history.maxlen == 10

        assert isinstance(emitter._event_queue, deque)
        assert emitter._event_queue.maxlen == 20

        assert isinstance(emitter._handlers, defaultdict)
        assert isinstance(emitter._once_handlers, defaultdict)

        assert emitter._paused is False
        assert emitter._processing_task is None

        assert isinstance(emitter._wildcard_handlers, list)
        assert len(emitter._wildcard_handlers) == 0

        assert not hasattr(emitter, "__dict__")

    def test_init_is_idempotent(self) -> None:
        emitter = EventEmitter(max_listeners=5)
        assert emitter._max_listeners == 5

        emitter.__init__(max_listeners=10)  # type: ignore[misc]

        assert emitter._max_listeners == 10

    @pytest.mark.asyncio
    async def test_max_listeners_warning(self, emitter: EventEmitter, mocker: MockerFixture, mock_logger: Any) -> None:
        emitter.set_max_listeners(max_listeners=1)
        emitter.on(event_type=EventType.SESSION_READY, handler=mocker.AsyncMock())

        emitter.on(event_type=EventType.SESSION_READY, handler=mocker.AsyncMock())

        mock_logger.warning.assert_called_once_with(
            "rt_task validate exceeded actual=%d event=%s limit=%d", 1, EventType.SESSION_READY, 1
        )

    @pytest.mark.asyncio
    async def test_off(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.on(event_type=EventType.CONNECTION_CLOSED, handler=handler)

        emitter.off(event_type=EventType.CONNECTION_CLOSED, handler=handler)
        await emitter.emit(event_type=EventType.CONNECTION_CLOSED)

        handler.assert_not_called()

    def test_off_all_handlers_empty(self, emitter: EventEmitter) -> None:
        emitter.off(event_type=EventType.SESSION_READY, handler=None)

        assert emitter.listener_count(event_type=EventType.SESSION_READY) == 0

    @pytest.mark.asyncio
    async def test_off_all_handlers_for_event(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler1 = mocker.AsyncMock()
        handler2 = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler1)
        emitter.on(event_type=EventType.SESSION_READY, handler=handler2)
        assert emitter.listener_count(event_type=EventType.SESSION_READY) == 2

        emitter.off(event_type=EventType.SESSION_READY, handler=None)

        assert emitter.listener_count(event_type=EventType.SESSION_READY) == 0

    def test_off_any_empty(self, emitter: EventEmitter) -> None:
        emitter.off_any()

        assert emitter.get_stats()["wildcard_handlers"] == 0

    @pytest.mark.asyncio
    async def test_off_any_no_args_removes_all(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.on_any(handler=handler)

        emitter.off_any()
        await emitter.emit(event_type=EventType.SESSION_READY)

        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_off_any_unknown_handler(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.on_any(handler=handler)
        unknown = mocker.Mock()

        emitter.off_any(handler=unknown)

        assert emitter.get_stats()["wildcard_handlers"] == 1

    @pytest.mark.asyncio
    async def test_off_removes_once_handler(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.once(event_type=EventType.CONNECTION_CLOSED, handler=handler)

        emitter.off(event_type=EventType.CONNECTION_CLOSED, handler=handler)
        await emitter.emit(event_type=EventType.CONNECTION_CLOSED)

        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_off_unknown_handler(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)
        unknown = mocker.Mock()

        emitter.off(event_type=EventType.SESSION_READY, handler=unknown)

        assert emitter.listener_count(event_type=EventType.SESSION_READY) == 1

    @pytest.mark.asyncio
    async def test_on_and_emit(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        await emitter.emit(event_type=EventType.SESSION_READY, data={"test": 1})

        handler.assert_awaited_once()
        call_arg = handler.call_args[0][0]
        assert isinstance(call_arg, Event)
        assert call_arg.type == EventType.SESSION_READY
        assert call_arg.data == {"test": 1}

    @pytest.mark.asyncio
    async def test_on_any_and_off_any(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        wildcard_handler = mocker.AsyncMock()
        emitter.on_any(handler=wildcard_handler)

        await emitter.emit(event_type=EventType.CONNECTION_ESTABLISHED)
        emitter.off_any(handler=wildcard_handler)
        await emitter.emit(event_type=EventType.DATAGRAM_RECEIVED)

        wildcard_handler.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_on_any_duplicate_handler(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.on_any(handler=handler)
        emitter.on_any(handler=handler)

        await emitter.emit(event_type=EventType.SESSION_READY)

        handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_once(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        emitter.once(event_type=EventType.STREAM_OPENED, handler=handler)
        assert emitter.listener_count(event_type=EventType.STREAM_OPENED) == 1

        await emitter.emit(event_type=EventType.STREAM_OPENED)
        await emitter.emit(event_type=EventType.STREAM_OPENED)

        handler.assert_awaited_once()
        assert emitter.listener_count(event_type=EventType.STREAM_OPENED) == 0

    @pytest.mark.asyncio
    async def test_once_duplicate_handler(self, emitter: EventEmitter, mocker: MockerFixture, mock_logger: Any) -> None:
        handler = mocker.AsyncMock()
        emitter.once(event_type=EventType.SESSION_READY, handler=handler)

        emitter.once(event_type=EventType.SESSION_READY, handler=handler)

        assert emitter.listener_count(event_type=EventType.SESSION_READY) == 1
        assert len(emitter._once_handlers[EventType.SESSION_READY]) == 1

    @pytest.mark.asyncio
    async def test_pause_and_resume(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.AsyncMock()
        spy_create_task = mocker.spy(obj=asyncio, name="create_task")

        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        emitter.pause()
        await emitter.emit(event_type=EventType.SESSION_READY)
        handler.assert_not_awaited()

        task = emitter.resume()

        assert task is not None
        spy_create_task.assert_called()
        await task
        handler.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_registering_same_handler_twice_warning(
        self, emitter: EventEmitter, mocker: MockerFixture, mock_logger: Any
    ) -> None:
        handler = mocker.AsyncMock()

        emitter.on(event_type=EventType.SESSION_READY, handler=handler)
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        assert emitter.listener_count(event_type=EventType.SESSION_READY) == 1
        mock_logger.warning.assert_called_once_with("rt_task create failed event=%s", EventType.SESSION_READY)

    def test_remove_all_listeners_defaults(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler = mocker.Mock()
        emitter.on(event_type=EventType.SESSION_READY, handler=handler)

        emitter.remove_all_listeners()

        assert emitter.listener_count(event_type=EventType.SESSION_READY) == 0

    def test_remove_all_listeners_defaults_empty(self, emitter: EventEmitter) -> None:
        emitter.remove_all_listeners()

        assert emitter.get_stats()["total_handlers"] == 0

    def test_remove_all_listeners_for_specific_event(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        handler1 = mocker.Mock()
        handler2 = mocker.Mock()
        ev_a = EventType.SESSION_READY
        ev_b = EventType.STREAM_OPENED

        emitter.on(event_type=ev_a, handler=handler1)
        emitter.once(event_type=ev_a, handler=handler2)
        emitter.on(event_type=ev_b, handler=handler1)
        assert emitter.listener_count(event_type=ev_a) == 2
        assert emitter.listener_count(event_type=ev_b) == 1

        emitter.remove_all_listeners(event_type=ev_a)

        assert emitter.listener_count(event_type=ev_a) == 0
        assert emitter.listener_count(event_type=ev_b) == 1

    def test_remove_all_listeners_specific_empty(self, emitter: EventEmitter) -> None:
        emitter.remove_all_listeners(event_type=EventType.SESSION_READY)

        assert emitter.listener_count(event_type=EventType.SESSION_READY) == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(argnames="queue_empty, task_running", argvalues=[(True, False), (False, True)])
    async def test_resume_returns_none(
        self, emitter: EventEmitter, mocker: MockerFixture, queue_empty: bool, task_running: bool
    ) -> None:
        if not queue_empty:
            emitter._event_queue.append(Event(type=EventType.SESSION_READY))
        if task_running:
            emitter._processing_task = mocker.Mock(done=lambda: False)

        task = emitter.resume()

        assert task is None

    @pytest.mark.asyncio
    async def test_resume_task_cancelled(self, emitter: EventEmitter, mocker: MockerFixture) -> None:
        started = asyncio.Event()

        async def blocking_handler(event: Event) -> None:
            started.set()
            try:
                await asyncio.sleep(delay=2)
            except asyncio.CancelledError:
                raise

        emitter.on(event_type=EventType.SESSION_READY, handler=blocking_handler)

        emitter.pause()
        await emitter.emit(event_type=EventType.SESSION_READY)

        task = emitter.resume()
        assert task is not None

        await asyncio.wait_for(started.wait(), timeout=1.0)

        task.cancel()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_wait_for_condition(self, emitter: EventEmitter) -> None:
        wait_task = asyncio.create_task(
            coro=emitter.wait_for(
                event_type=EventType.DATAGRAM_RECEIVED, condition=lambda e: bool(e.data and e.data["stream_id"] == 3)
            )
        )
        await asyncio.sleep(delay=0.01)

        await emitter.emit(event_type=EventType.DATAGRAM_RECEIVED, data={"stream_id": 1})
        assert not wait_task.done()
        await emitter.emit(event_type=EventType.DATAGRAM_RECEIVED, data={"stream_id": 3})

        try:
            async with asyncio.timeout(delay=1):
                result = await wait_task
        except TimeoutError:
            wait_task.cancel()
            raise

        assert result.data
        assert result.data["stream_id"] == 3

    @pytest.mark.asyncio
    async def test_wait_for_condition_raises_exception(self, emitter: EventEmitter) -> None:
        class ConditionError(Exception):
            pass

        def faulty_condition(event: Event) -> bool:
            raise ConditionError("condition failed")

        wait_task = asyncio.create_task(
            coro=emitter.wait_for(event_type=EventType.SESSION_READY, condition=faulty_condition)
        )
        await asyncio.sleep(delay=0.01)

        await emitter.emit(event_type=EventType.SESSION_READY)

        with pytest.raises(expected_exception=ConditionError):
            await wait_task

    @pytest.mark.asyncio
    async def test_wait_for_is_cancelled(self, emitter: EventEmitter) -> None:
        event_type = EventType.SESSION_READY
        wait_task = asyncio.create_task(coro=emitter.wait_for(event_type=event_type))
        await asyncio.sleep(delay=0.01)

        assert emitter.listener_count(event_type=event_type) == 1
        wait_task.cancel()
        with pytest.raises(expected_exception=asyncio.CancelledError):
            await wait_task

        assert emitter.listener_count(event_type=event_type) == 0

    @pytest.mark.asyncio
    async def test_wait_for_list_of_events(self, emitter: EventEmitter) -> None:
        wait_task = asyncio.create_task(
            coro=emitter.wait_for(event_type=[EventType.SESSION_READY, EventType.STREAM_OPENED])
        )
        await asyncio.sleep(delay=0.01)

        await emitter.emit(event_type=EventType.STREAM_OPENED, data={"id": 1})

        try:
            async with asyncio.timeout(delay=1):
                result = await wait_task
        except TimeoutError:
            wait_task.cancel()
            raise

        assert result.type == EventType.STREAM_OPENED

    @pytest.mark.asyncio
    async def test_wait_for_race_condition_error(self, emitter: EventEmitter) -> None:
        class RaceError(Exception):
            pass

        def faulty_condition(event: Event) -> bool:
            raise RaceError("failed")

        emitter.pause()
        wait_task = asyncio.create_task(
            coro=emitter.wait_for(event_type=EventType.SESSION_READY, condition=faulty_condition)
        )
        await asyncio.sleep(delay=0.01)

        emitter.emit_nowait(event_type=EventType.SESSION_READY)
        emitter.emit_nowait(event_type=EventType.SESSION_READY)

        emitter.resume()
        try:
            async with asyncio.timeout(delay=1):
                with pytest.raises(expected_exception=RaceError):
                    await wait_task
        except TimeoutError:
            wait_task.cancel()
            raise

    @pytest.mark.asyncio
    async def test_wait_for_race_condition_success(self, emitter: EventEmitter) -> None:
        emitter.pause()
        wait_task = asyncio.create_task(coro=emitter.wait_for(event_type=EventType.SESSION_READY))
        await asyncio.sleep(delay=0.01)
        emitter.emit_nowait(event_type=EventType.SESSION_READY, data={"id": 1})
        emitter.emit_nowait(event_type=EventType.SESSION_READY, data={"id": 2})

        emitter.resume()
        try:
            async with asyncio.timeout(delay=1):
                result = await wait_task
        except TimeoutError:
            wait_task.cancel()
            raise

        assert result.data == {"id": 1}

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
