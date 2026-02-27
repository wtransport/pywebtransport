"""Unit tests for the pywebtransport._driver.pending module."""

import pytest

from pywebtransport._driver.pending import PendingRequestManager


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
        manager.complete_request(request_id=999, result="data")

        assert True

    @pytest.mark.asyncio
    async def test_complete_request_success(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()

        manager.complete_request(request_id=request_id, result="success_payload")

        assert future.done()
        assert future.result() == "success_payload"
        assert request_id not in manager._requests

    @pytest.mark.asyncio
    async def test_create_request_generates_unique_ids(self, manager: PendingRequestManager) -> None:
        id1, _ = manager.create_request()
        id2, _ = manager.create_request()

        assert id1 != id2
        assert len(manager._requests) == 2

    @pytest.mark.asyncio
    async def test_fail_all_with_mixed_states(self, manager: PendingRequestManager) -> None:
        id1, fut1 = manager.create_request()
        id2, fut2 = manager.create_request()
        fut1.set_result("already_done")
        exc = RuntimeError("connection lost")

        manager.fail_all(exception=exc)

        assert fut1.result() == "already_done"
        assert fut2.done()
        assert fut2.exception() == exc
        assert len(manager._requests) == 0

    @pytest.mark.asyncio
    async def test_fail_request_already_done(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()
        future.set_result("done")
        exc = ValueError("error")

        manager.fail_request(request_id=request_id, exception=exc)

        assert future.result() == "done"
        assert request_id not in manager._requests

    @pytest.mark.asyncio
    async def test_fail_request_nonexistent(self, manager: PendingRequestManager) -> None:
        manager.fail_request(request_id=888, exception=RuntimeError())

        assert True

    @pytest.mark.asyncio
    async def test_fail_request_success(self, manager: PendingRequestManager) -> None:
        request_id, future = manager.create_request()
        exc = ValueError("invalid request")

        manager.fail_request(request_id=request_id, exception=exc)

        assert future.done()
        assert future.exception() == exc
        assert request_id not in manager._requests

    def test_init(self, manager: PendingRequestManager) -> None:
        assert manager._requests == {}
        assert not hasattr(manager, "__dict__")
