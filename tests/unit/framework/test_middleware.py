"""Unit tests for the pywebtransport.framework.middleware module."""

import asyncio
import logging
from collections import deque
from collections.abc import AsyncGenerator
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from _pytest.logging import LogCaptureFixture
from pytest_asyncio import fixture as asyncio_fixture
from pytest_mock import MockerFixture

from pywebtransport import Headers, ServerError, WebTransportSession
from pywebtransport.framework import (
    MiddlewareManager,
    MiddlewareRejected,
    create_auth_middleware,
    create_cors_middleware,
    create_rate_limit_middleware,
)
from pywebtransport.framework.middleware import RateLimiter, _find_header_str


class TestMiddlewareFactories:

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> Any:
        session = mocker.Mock(spec=WebTransportSession)
        session.path = "/test"
        session.headers = {"origin": "https://example.com", "x-auth": "good-token"}
        session.remote_address = ("1.2.3.4", 12345)
        return session

    @pytest.mark.asyncio
    async def test_create_auth_middleware_failure(self, mock_session: Any, mocker: MockerFixture) -> None:
        auth_handler = mocker.AsyncMock(return_value=False)
        auth_middleware = create_auth_middleware(auth_handler=auth_handler)

        with pytest.raises(expected_exception=MiddlewareRejected) as exc_info:
            await auth_middleware(session=mock_session)

        assert exc_info.value.status_code == 401
        assert exc_info.value.headers == {}
        auth_handler.assert_awaited_once_with(headers=mock_session.headers)

    @pytest.mark.asyncio
    async def test_create_auth_middleware_handler_exception(self, mock_session: Any, mocker: MockerFixture) -> None:
        auth_handler = mocker.AsyncMock(side_effect=ValueError("auth error"))
        auth_middleware = create_auth_middleware(auth_handler=auth_handler)

        with pytest.raises(expected_exception=MiddlewareRejected) as exc_info:
            await auth_middleware(session=mock_session)

        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_create_auth_middleware_success(self, mock_session: Any, mocker: MockerFixture) -> None:
        auth_handler = mocker.AsyncMock(return_value=True)
        auth_middleware = create_auth_middleware(auth_handler=auth_handler)

        await auth_middleware(session=mock_session)

        auth_handler.assert_awaited_once_with(headers=mock_session.headers)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="origin, allowed_origins, should_pass",
        argvalues=[
            ("https://example.com", ["https://example.com"], True),
            ("https://evil.com", ["https://example.com"], False),
            ("https://any.com", ["*"], True),
            ("https://sub.example.com", ["*.example.com"], True),
            (None, ["https://example.com"], False),
        ],
    )
    async def test_create_cors_middleware(
        self, mock_session: Any, origin: str | None, allowed_origins: list[str], should_pass: bool
    ) -> None:
        mock_session.headers = {"origin": origin} if origin else {}
        cors_middleware = create_cors_middleware(allowed_origins=allowed_origins)

        if should_pass:
            await cors_middleware(session=mock_session)
        else:
            with pytest.raises(expected_exception=MiddlewareRejected) as exc_info:
                await cors_middleware(session=mock_session)
            assert exc_info.value.status_code == 403

    def test_create_rate_limit_middleware(self) -> None:
        limiter = create_rate_limit_middleware(
            max_requests=50, window_seconds=30, cleanup_interval=100, max_tracked_ips=500
        )

        assert isinstance(limiter, RateLimiter)
        assert limiter._max_requests == 50
        assert limiter._window_seconds == 30
        assert limiter._cleanup_interval == 100
        assert limiter._max_tracked_ips == 500

    @pytest.mark.asyncio
    async def test_middleware_with_generic_session(self) -> None:
        generic_session = MagicMock()
        generic_session.path = "/generic"
        generic_session.headers = {}
        generic_session.remote_address = None

        async with RateLimiter() as rate_limiter:
            await rate_limiter(session=generic_session)
            assert rate_limiter._lock is not None
            async with rate_limiter._lock:
                assert "unknown" in rate_limiter._requests


@pytest.mark.asyncio
class TestMiddlewareManager:

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> Any:
        return mocker.create_autospec(spec=WebTransportSession, instance=True)

    async def test_add_remove_middleware(self) -> None:
        manager = MiddlewareManager()
        assert manager.get_middleware_count() == 0

        async def middleware1(*, session: Any) -> None:
            pass

        manager.add_middleware(middleware=middleware1)
        assert manager.get_middleware_count() == 1

        manager.remove_middleware(middleware=middleware1)
        assert manager.get_middleware_count() == 0

        manager.remove_middleware(middleware=middleware1)
        assert manager.get_middleware_count() == 0

    async def test_process_request_all_pass(self, mock_session: Any, mocker: MockerFixture) -> None:
        manager = MiddlewareManager()
        middleware1 = mocker.AsyncMock(return_value=None)
        middleware2 = mocker.AsyncMock(return_value=None)
        manager.add_middleware(middleware=middleware1)
        manager.add_middleware(middleware=middleware2)

        await manager.process_request(session=mock_session)

        middleware1.assert_awaited_once_with(session=mock_session)
        middleware2.assert_awaited_once_with(session=mock_session)

    async def test_process_request_exception(self, mock_session: Any, mocker: MockerFixture) -> None:
        manager = MiddlewareManager()
        middleware1 = mocker.AsyncMock(side_effect=ValueError("middleware error"))
        manager.add_middleware(middleware=middleware1)

        with pytest.raises(expected_exception=MiddlewareRejected) as exc_info:
            await manager.process_request(session=mock_session)

        assert exc_info.value.status_code == 500

    async def test_process_request_rejection(self, mock_session: Any, mocker: MockerFixture) -> None:
        manager = MiddlewareManager()
        middleware1 = mocker.AsyncMock(return_value=None)
        middleware2 = mocker.AsyncMock(side_effect=MiddlewareRejected(status_code=403))
        middleware3 = mocker.AsyncMock(return_value=None)
        manager.add_middleware(middleware=middleware1)
        manager.add_middleware(middleware=middleware2)
        manager.add_middleware(middleware=middleware3)

        with pytest.raises(expected_exception=MiddlewareRejected) as exc_info:
            await manager.process_request(session=mock_session)

        assert exc_info.value.status_code == 403
        middleware1.assert_awaited_once_with(session=mock_session)
        middleware2.assert_awaited_once_with(session=mock_session)
        middleware3.assert_not_called()


@pytest.mark.asyncio
class TestRateLimiter:

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> Any:
        session = mocker.Mock(spec=WebTransportSession)
        session.remote_address = ("1.2.3.4", 12345)
        return session

    @asyncio_fixture
    async def rate_limiter(self) -> AsyncGenerator[RateLimiter, None]:
        limiter = RateLimiter(max_requests=2, window_seconds=10)
        async with limiter as activated_limiter:
            yield activated_limiter

    async def test_aexit_no_cleanup_task(self) -> None:
        limiter = RateLimiter()
        limiter._cleanup_task = None

        await limiter.__aexit__(None, None, None)

        assert limiter._is_closing

    async def test_call_existing_ip(self, mock_session: Any, rate_limiter: RateLimiter) -> None:
        assert rate_limiter._lock is not None
        async with rate_limiter._lock:
            rate_limiter._requests["1.2.3.4"] = deque()

        await rate_limiter(session=mock_session)

        async with rate_limiter._lock:
            assert len(rate_limiter._requests["1.2.3.4"]) == 1

    async def test_call_no_remote_address(self, mocker: MockerFixture, rate_limiter: RateLimiter) -> None:
        session = mocker.Mock(spec=WebTransportSession)
        session.remote_address = None

        await rate_limiter(session=session)

    async def test_ip_limit_flush(self, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        caplog.set_level(level=logging.WARNING)
        mocker.patch(target="time.perf_counter", return_value=100.0)
        rate_limiter = RateLimiter(max_tracked_ips=2)

        async with rate_limiter as rl:
            rl._requests["1.1.1.1"] = deque([100.0])
            rl._requests["2.2.2.2"] = deque([100.0])

            session = mocker.Mock(spec=WebTransportSession)
            session.remote_address = ("3.3.3.3", 12345)

            await rl(session=session)

            assert "app_middleware validate exceeded actual=2 limit=2" in caplog.text
            assert "1.1.1.1" not in rl._requests
            assert "3.3.3.3" in rl._requests

    async def test_lifecycle_and_cleanup(self, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        caplog.set_level(level=logging.DEBUG)
        original_sleep = asyncio.sleep
        proceed_event = asyncio.Event()

        async def sleep_mock(delay: float) -> None:
            if delay > 0:
                proceed_event.set()
            await original_sleep(delay=0)

        mocker.patch(target="asyncio.sleep", side_effect=sleep_mock)
        mock_time = mocker.patch(target="time.perf_counter")
        rate_limiter = RateLimiter(window_seconds=10, cleanup_interval=30)

        async with rate_limiter as rl:
            assert rl._lock is not None
            async with rl._lock:
                rl._requests["active_ip"] = deque([210.0])
                rl._requests["stale_ip"] = deque([100.0])
                rl._requests["empty_ip"] = deque()

            mock_time.return_value = 215.0
            await proceed_event.wait()
            await asyncio.sleep(delay=0)

            assert "app_middleware evict count=2" in caplog.text
            assert rl._lock is not None
            async with rl._lock:
                assert "active_ip" in rl._requests
                assert "stale_ip" not in rl._requests
                assert "empty_ip" not in rl._requests

    async def test_periodic_cleanup_empty_timestamps(self, mocker: MockerFixture) -> None:
        mocker.patch(target="asyncio.sleep", side_effect=[None, asyncio.CancelledError])
        mocker.patch(target="time.perf_counter", return_value=100.0)

        async with RateLimiter() as rate_limiter:
            assert rate_limiter._lock is not None
            async with rate_limiter._lock:
                rate_limiter._requests["empty_ip"] = deque()

            await rate_limiter._periodic_cleanup()

            assert "empty_ip" not in rate_limiter._requests

    async def test_periodic_cleanup_exit_on_closing(self, mocker: MockerFixture) -> None:
        mocker.patch(target="asyncio.sleep", return_value=None)
        rate_limiter = RateLimiter()
        rate_limiter._is_closing = True
        rate_limiter._lock = mocker.MagicMock()

        await rate_limiter._periodic_cleanup()

        cast(MagicMock, rate_limiter._lock).__aenter__.assert_not_called()

    async def test_periodic_cleanup_no_stale_ips(self, mocker: MockerFixture) -> None:
        mocker.patch(target="asyncio.sleep", side_effect=[None, asyncio.CancelledError])
        mock_time = mocker.patch(target="time.perf_counter")

        async with RateLimiter() as rate_limiter:
            assert rate_limiter._lock is not None
            async with rate_limiter._lock:
                rate_limiter._requests["active_ip"] = deque([100.0])
            mock_time.return_value = 105.0

            await rate_limiter._periodic_cleanup()

            assert "active_ip" in rate_limiter._requests

    async def test_periodic_cleanup_task_error(self, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        mocker.patch(target="asyncio.sleep", side_effect=[None, asyncio.CancelledError])

        async with RateLimiter() as rate_limiter:
            assert rate_limiter._lock is not None

            mock_lock = mocker.MagicMock()
            mock_lock.__aenter__.side_effect = ValueError("cleanup error")
            rate_limiter._lock = mock_lock

            with pytest.raises(expected_exception=asyncio.CancelledError):
                await rate_limiter._periodic_cleanup()

        assert "rt_task failed err=cleanup error" in caplog.text

    async def test_rate_limiter_call_not_activated(self, mock_session: Any) -> None:
        limiter = RateLimiter()

        with pytest.raises(expected_exception=ServerError, match="app_middleware validate failed expected=open"):
            await limiter(session=mock_session)

    async def test_rate_limiting_logic(
        self, mock_session: Any, mocker: MockerFixture, rate_limiter: RateLimiter
    ) -> None:
        mock_time = mocker.patch(target="time.perf_counter")

        mock_time.return_value = 100.0
        await rate_limiter(session=mock_session)

        mock_time.return_value = 101.0
        await rate_limiter(session=mock_session)

        mock_time.return_value = 102.0
        with pytest.raises(expected_exception=MiddlewareRejected) as exc_info:
            await rate_limiter(session=mock_session)

        assert exc_info.value.status_code == 429
        headers = cast(dict[str, str], exc_info.value.headers)
        assert headers["retry-after"] == "10"

        mock_time.return_value = 110.1
        await rate_limiter(session=mock_session)

    async def test_start_cleanup_task_idempotent(self, mocker: MockerFixture) -> None:
        rate_limiter = RateLimiter()
        rate_limiter._start_cleanup_task()
        assert rate_limiter._cleanup_task is None

        mock_cleanup = mocker.Mock(return_value="dummy_coro")
        mocker.patch.object(target=rate_limiter, attribute="_periodic_cleanup", new=mock_cleanup)

        mock_tg = mocker.Mock()
        mock_task = mocker.Mock()
        mock_tg.create_task.return_value = mock_task
        rate_limiter._tg = mock_tg

        rate_limiter._start_cleanup_task()

        mock_cleanup.assert_called_once()
        mock_tg.create_task.assert_called_once_with(coro="dummy_coro")

        mock_task.done.return_value = False
        rate_limiter._start_cleanup_task()

        assert mock_tg.create_task.call_count == 1

        mock_task.done.return_value = True
        rate_limiter._start_cleanup_task()

        assert mock_tg.create_task.call_count == 2


def test_find_header_str_decoding() -> None:
    headers: Headers = {b"content-type": b"application/json"}

    result = _find_header_str(headers=headers, key="content-type")

    assert result == "application/json"


def test_find_header_str_default() -> None:
    headers: Headers = {"host": "example.com"}

    result = _find_header_str(headers=headers, key="missing", default="default")

    assert result == "default"


def test_find_header_str_dual_mode_dict() -> None:
    headers: Headers = {b"content-length": b"123", "server": "test"}

    val_1 = _find_header_str(headers=headers, key="content-length")
    val_2 = _find_header_str(headers=headers, key="server")

    assert val_1 == "123"
    assert val_2 == "test"


def test_find_header_str_dual_mode_list() -> None:
    headers: Headers = [(b"content-length", b"123"), ("server", "test")]

    val_1 = _find_header_str(headers=headers, key="content-length")
    val_2 = _find_header_str(headers=headers, key="server")

    assert val_1 == "123"
    assert val_2 == "test"


def test_find_header_str_existing_string() -> None:
    headers: Headers = {"user-agent": "test-client"}

    result = _find_header_str(headers=headers, key="user-agent")

    assert result == "test-client"


def test_find_header_str_from_dict() -> None:
    headers: Headers = {"content-type": "application/json"}

    assert _find_header_str(headers=headers, key="content-type") == "application/json"
    assert _find_header_str(headers=headers, key="unknown") is None
    assert _find_header_str(headers=headers, key="unknown", default="default") == "default"


def test_find_header_str_from_list() -> None:
    headers: Headers = [("Content-Type", "application/json")]

    assert _find_header_str(headers=headers, key="content-type") == "application/json"
    assert _find_header_str(headers=headers, key="unknown") is None


def test_find_header_str_invalid_utf8() -> None:
    headers: Headers = {b"key": b"\xff\xfe"}

    result = _find_header_str(headers=headers, key="key", default="fallback")

    assert result == "fallback"
