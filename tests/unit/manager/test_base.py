"""Unit tests for the pywebtransport.manager._base module."""

import asyncio
import logging
from typing import cast
from unittest.mock import MagicMock

import pytest
from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture

from pywebtransport import Event
from pywebtransport.events import EventEmitter
from pywebtransport.manager._base import BaseResourceManager
from pywebtransport.types import EventType


class MockResource:

    def __init__(self, resource_id: str) -> None:
        self.resource_id = resource_id
        self.events = EventEmitter()
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    @property
    def is_closed(self) -> bool:
        return self.closed


class FalsyResource(MockResource):

    def __bool__(self) -> bool:
        return False


class ConcreteResourceManager(BaseResourceManager[str, MockResource]):

    _resource_closed_event_type = EventType.CONNECTION_CLOSED

    async def _close_resource(self, *, resource: MockResource) -> None:
        await resource.close()

    def _get_resource_id(self, *, resource: MockResource) -> str:
        return resource.resource_id


@pytest.mark.asyncio
class TestBaseResourceManager:

    @pytest.fixture
    def manager(self) -> ConcreteResourceManager:
        return ConcreteResourceManager(resource_name="test_item", max_resources=5)

    async def test_abstract_methods_raise(self) -> None:
        resource = MockResource(resource_id="r1")

        class PartialManager(BaseResourceManager[str, MockResource]):
            _resource_closed_event_type = EventType.CONNECTION_CLOSED

            async def _close_resource(self, *, resource: MockResource) -> None:
                await getattr(BaseResourceManager, "_close_resource")(self, resource=resource)

            def _get_resource_id(self, *, resource: MockResource) -> str:
                return cast(str, getattr(BaseResourceManager, "_get_resource_id")(self, resource=resource))

        manager = PartialManager(resource_name="test", max_resources=1)

        with pytest.raises(expected_exception=NotImplementedError):
            await manager._close_resource(resource=resource)

        with pytest.raises(expected_exception=NotImplementedError):
            manager._get_resource_id(resource=resource)

    async def test_add_resource(self, manager: ConcreteResourceManager) -> None:
        resource = MockResource(resource_id="r1")

        async with manager:
            await manager.add_resource(resource=resource)

            assert len(manager) == 1
            stats = await manager.get_stats()
            assert stats["total_created"] == 1
            assert stats["current_count"] == 1

            retrieved = await manager.get_resource(resource_id="r1")
            assert retrieved is resource

    async def test_add_resource_closed_during_registration(
        self, manager: ConcreteResourceManager, mocker: MockerFixture
    ) -> None:
        resource = MockResource(resource_id="r1")
        resource.events = mocker.MagicMock(spec=EventEmitter)
        cast(MagicMock, resource.events.off).side_effect = ValueError("handler not found")

        mocker.patch.object(target=manager, attribute="_check_is_closed", return_value=True)

        async with manager:
            with pytest.raises(
                expected_exception=RuntimeError, match="app_manager validate invalid component=test_item expected=open"
            ):
                await manager.add_resource(resource=resource)

        cast(MagicMock, resource.events.off).assert_called_once()
        assert len(manager) == 0

    async def test_add_resource_closed_handler_invalid_data(
        self, manager: ConcreteResourceManager, mocker: MockerFixture
    ) -> None:
        resource = MockResource(resource_id="r1")

        async with manager:
            await manager.add_resource(resource=resource)

            event = Event(type=EventType.CONNECTION_CLOSED, data="invalid")
            resource.events.emit_nowait(event_type=EventType.CONNECTION_CLOSED, data=event.data)
            await asyncio.sleep(delay=0.01)

            assert len(manager) == 0

    async def test_add_resource_closed_handler_mismatch_id(
        self, manager: ConcreteResourceManager, caplog: LogCaptureFixture
    ) -> None:
        resource = MockResource(resource_id="r1")

        async with manager:
            await manager.add_resource(resource=resource)

            event = Event(type=EventType.CONNECTION_CLOSED, data={"test_item_id": "other"})
            resource.events.emit_nowait(event_type=EventType.CONNECTION_CLOSED, data=event.data)
            await asyncio.sleep(delay=0.01)

            assert len(manager) == 0
            assert "app_manager validate invalid component=test_item expected=identity_match" in caplog.text

    async def test_add_resource_closed_inside_lock(self, manager: ConcreteResourceManager) -> None:
        class FlakyClosedResource(MockResource):
            def __init__(self, resource_id: str) -> None:
                super().__init__(resource_id=resource_id)
                self._access_count = 0

            @property
            def is_closed(self) -> bool:
                self._access_count += 1
                return self._access_count > 1

        resource = FlakyClosedResource(resource_id="r1")

        async with manager:
            with pytest.raises(
                expected_exception=RuntimeError, match="app_manager validate invalid component=test_item expected=open"
            ):
                await manager.add_resource(resource=resource)

        assert len(manager) == 0

    async def test_add_resource_duplicate(self, manager: ConcreteResourceManager, caplog: LogCaptureFixture) -> None:
        caplog.set_level(level=logging.DEBUG)
        resource = MockResource(resource_id="r1")

        async with manager:
            await manager.add_resource(resource=resource)
            await manager.add_resource(resource=resource)

            assert len(manager) == 1
            stats = await manager.get_stats()
            assert stats["total_created"] == 1

            assert "app_manager validate invalid component=test_item" in caplog.text

    async def test_add_resource_initially_closed(self, manager: ConcreteResourceManager) -> None:
        resource = MockResource(resource_id="r1")
        await resource.close()

        async with manager:
            with pytest.raises(
                expected_exception=RuntimeError, match="app_manager validate invalid component=test_item expected=open"
            ):
                await manager.add_resource(resource=resource)

        assert len(manager) == 0

    async def test_add_resource_limit_reached(self, manager: ConcreteResourceManager) -> None:
        manager._max_resources = 1
        r1 = MockResource(resource_id="r1")
        r2 = MockResource(resource_id="r2")

        async with manager:
            await manager.add_resource(resource=r1)

            with pytest.raises(
                expected_exception=RuntimeError,
                match="app_manager validate exceeded actual=1 component=test_item limit=1",
            ):
                await manager.add_resource(resource=r2)

            assert len(manager) == 1
            assert r2.closed is True

    async def test_add_resource_not_activated(self, manager: ConcreteResourceManager) -> None:
        resource = MockResource(resource_id="r1")

        with pytest.raises(expected_exception=RuntimeError, match="app_manager validate failed expected=active"):
            await manager.add_resource(resource=resource)

    async def test_add_resource_shutting_down(self, manager: ConcreteResourceManager) -> None:
        resource = MockResource(resource_id="r1")

        async with manager:
            await manager.shutdown()
            with pytest.raises(expected_exception=RuntimeError, match="app_manager validate failed expected=active"):
                await manager.add_resource(resource=resource)
            assert resource.closed is True

    async def test_close_all_resources_event_off_error(
        self, manager: ConcreteResourceManager, mocker: MockerFixture
    ) -> None:
        resource = MockResource(resource_id="r1")
        resource.events = mocker.MagicMock(spec=EventEmitter)
        cast(MagicMock, resource.events.off).side_effect = ValueError("cleanup error")

        async with manager:
            await manager.add_resource(resource=resource)
            await manager.shutdown()

        assert len(manager) == 0

    async def test_close_all_resources_no_lock(self, manager: ConcreteResourceManager) -> None:
        await manager._close_all_resources()

        assert len(manager) == 0

    async def test_close_all_resources_stats_update(self, manager: ConcreteResourceManager) -> None:
        async with manager:
            pass

        assert manager._stats["total_closed"] == 0

    async def test_context_manager_lifecycle(self, manager: ConcreteResourceManager) -> None:
        assert not manager._active

        async def run_context() -> None:
            async with manager:
                assert manager._active
                assert not manager._is_shutting_down

        await run_context()

        assert not manager._active
        assert manager._is_shutting_down

    async def test_get_all_resources(self, manager: ConcreteResourceManager) -> None:
        r1 = MockResource(resource_id="r1")
        r2 = MockResource(resource_id="r2")

        async with manager:
            await manager.add_resource(resource=r1)
            await manager.add_resource(resource=r2)

            all_res = await manager.get_all_resources()
            assert len(all_res) == 2
            assert r1 in all_res
            assert r2 in all_res

    async def test_get_all_resources_no_lock(self, manager: ConcreteResourceManager) -> None:
        assert await manager.get_all_resources() == []

    async def test_get_resource_no_lock(self, manager: ConcreteResourceManager) -> None:
        assert await manager.get_resource(resource_id="r1") is None

    async def test_get_stats_no_lock(self, manager: ConcreteResourceManager) -> None:
        assert await manager.get_stats() == {}

    async def test_handle_resource_closed_allowed_during_shutdown(
        self, manager: ConcreteResourceManager, mocker: MockerFixture
    ) -> None:
        manager._is_shutting_down = True
        manager._active = True
        manager._resources["r1"] = MockResource(resource_id="r1")

        await manager._handle_resource_closed(resource_id="r1")

        assert "r1" not in manager._resources
        assert manager._stats["total_closed"] == 1

    async def test_handle_resource_closed_falsy_resource(self, manager: ConcreteResourceManager) -> None:
        resource = FalsyResource(resource_id="r_falsy")

        async with manager:
            await manager.add_resource(resource=resource)
            await manager._handle_resource_closed(resource_id="r_falsy")

            assert len(manager) == 0
            stats = await manager.get_stats()
            assert stats["total_closed"] == 1

    async def test_handle_resource_closed_no_lock(self, manager: ConcreteResourceManager) -> None:
        await manager._handle_resource_closed(resource_id="r1")

    async def test_handle_resource_closed_resource_not_found(self, manager: ConcreteResourceManager) -> None:
        manager._active = True

        await manager._handle_resource_closed(resource_id="non_existent")

        assert manager._stats["total_closed"] == 0

    async def test_resource_closed_event_handling(self, manager: ConcreteResourceManager) -> None:
        resource = MockResource(resource_id="r1")

        async with manager:
            await manager.add_resource(resource=resource)
            assert len(manager) == 1

            event = Event(type=EventType.CONNECTION_CLOSED, data={"test_item_id": "r1"})
            resource.events.emit_nowait(event_type=EventType.CONNECTION_CLOSED, data=event.data)

            await asyncio.sleep(delay=0.01)

            assert len(manager) == 0
            stats = await manager.get_stats()
            assert stats["total_closed"] == 1

    async def test_shutdown_closes_resources(self, manager: ConcreteResourceManager) -> None:
        r1 = MockResource(resource_id="r1")
        r2 = MockResource(resource_id="r2")

        async with manager:
            await manager.add_resource(resource=r1)
            await manager.add_resource(resource=r2)

        assert r1.closed
        assert r2.closed
        assert len(manager) == 0
        assert manager._stats["total_closed"] == 2

    async def test_shutdown_handles_multiple_close_errors(
        self, manager: ConcreteResourceManager, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        r1 = MockResource(resource_id="r1")
        r2 = MockResource(resource_id="r2")
        mocker.patch.object(target=r1, attribute="close", side_effect=ValueError("fail 1"))
        mocker.patch.object(target=r2, attribute="close", side_effect=RuntimeError("fail 2"))

        try:
            async with manager:
                await manager.add_resource(resource=r1)
                await manager.add_resource(resource=r2)
        except BaseException:
            pass

        assert "app_manager close failed component=test_item err=" in caplog.text
        assert "fail 1" in caplog.text or "fail 2" in caplog.text
        assert len(manager) == 0

    async def test_shutdown_idempotent(self, manager: ConcreteResourceManager) -> None:
        async with manager:
            await manager.shutdown()
            await manager.shutdown()

        assert manager._is_shutting_down
