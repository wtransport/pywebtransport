"""Unit tests for the pywebtransport._controller.pending module."""

import pytest

from pywebtransport._controller.pending import PendingRequestManager


class TestPendingRequestManager:

    @pytest.fixture
    def manager(self) -> PendingRequestManager:
        return PendingRequestManager()

    @pytest.mark.asyncio
    async def test_complete_request_already_done(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()
        future.set_result("initial")

        manager.complete_request(request_id=request_id, result="new")

        assert future.result() == "initial"
        assert request_id not in manager._requests

    @pytest.mark.asyncio
    async def test_complete_request_nonexistent(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()

        manager.complete_request(request_id=999, result="data")

        assert not future.done()
        assert request_id in manager._requests

    @pytest.mark.asyncio
    async def test_complete_request_resolver_failure(self, manager: PendingRequestManager) -> None:
        exception = ValueError("resolve failed")

        def resolver(result: object) -> object:
            raise exception

        request_id, future = manager.create_request(resolver=resolver)

        manager.complete_request(request_id=request_id, result="raw")

        assert future.exception() is exception
        assert request_id not in manager._requests

    @pytest.mark.asyncio
    async def test_complete_request_resolver_success(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request(resolver=lambda result: f"resolved_{result}")

        manager.complete_request(request_id=request_id, result="raw")

        assert future.result() == "resolved_raw"
        assert request_id not in manager._requests

    @pytest.mark.asyncio
    async def test_complete_request_success(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()

        manager.complete_request(request_id=request_id, result="success_payload")

        assert future.done()
        assert future.result() == "success_payload"
        assert request_id not in manager._requests

    @pytest.mark.asyncio
    async def test_create_request_generates_unique_ids(self, manager: PendingRequestManager) -> None:
        request_id1, _ = manager.create_request()
        request_id2, _ = manager.create_request()

        assert request_id1 != request_id2
        assert len(manager._requests) == 2

    @pytest.mark.asyncio
    async def test_evict_request(self, manager: PendingRequestManager) -> None:
        request_id, _ = manager.create_request()

        manager.evict_request(request_id=request_id)

        assert request_id not in manager._requests
        manager.evict_request(request_id=request_id)

    @pytest.mark.asyncio
    async def test_fail_all_with_mixed_states(self, manager: PendingRequestManager) -> None:
        request_id1, future1 = manager.create_request()
        request_id2, future2 = manager.create_request()
        future1.set_result("already_done")
        exception = RuntimeError("connection lost")

        manager.fail_all(exception=exception)

        assert future1.result() == "already_done"
        assert future2.done()
        assert future2.exception() == exception
        assert len(manager._requests) == 0

    @pytest.mark.asyncio
    async def test_fail_request_already_done(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()
        future.set_result("done")
        exception = ValueError("error")

        manager.fail_request(request_id=request_id, exception=exception)

        assert future.result() == "done"
        assert request_id not in manager._requests

    @pytest.mark.asyncio
    async def test_fail_request_nonexistent(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()

        manager.fail_request(request_id=888, exception=RuntimeError())

        assert not future.done()
        assert request_id in manager._requests

    @pytest.mark.asyncio
    async def test_fail_request_success(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()
        exception = ValueError("invalid request")

        manager.fail_request(request_id=request_id, exception=exception)

        assert future.done()
        assert future.exception() == exception
        assert request_id not in manager._requests

    def test_init(self, manager: PendingRequestManager) -> None:
        assert manager._requests == {}
        assert not hasattr(manager, "__dict__")
