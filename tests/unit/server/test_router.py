"""Unit tests for the pywebtransport.server.router module."""

import re
from typing import Any

import pytest
from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture

from pywebtransport import WebTransportSession
from pywebtransport.server import RequestRouter


class TestRequestRouter:

    @pytest.fixture
    def mock_handler(self, mocker: MockerFixture) -> Any:
        return mocker.AsyncMock()

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> Any:
        return mocker.create_autospec(spec=WebTransportSession, instance=True)

    @pytest.fixture
    def router(self) -> RequestRouter:
        return RequestRouter()

    def test_add_and_get_route(self, router: RequestRouter, mock_handler: Any) -> None:
        router.add_route(path="/home", handler=mock_handler)

        assert router.get_route_handler(path="/home") is mock_handler
        assert router.get_route_handler(path="/not-found") is None
        assert router.get_all_routes() == {"/home": mock_handler}
        assert router.get_route_stats()["exact_routes"] == 1

    def test_add_invalid_pattern_route(
        self, router: RequestRouter, mock_handler: Any, caplog: LogCaptureFixture
    ) -> None:
        invalid_pattern = r"/users/(\d+"

        with pytest.raises(expected_exception=re.error):
            router.add_pattern_route(pattern=invalid_pattern, handler=mock_handler)

        assert router.get_route_stats()["pattern_routes"] == 0
        assert f"Invalid regex pattern '{invalid_pattern}'" in caplog.text

    def test_add_pattern_route(self, router: RequestRouter, mock_handler: Any) -> None:
        router.add_pattern_route(pattern=r"/users/(\d+)", handler=mock_handler)

        stats = router.get_route_stats()

        assert stats["pattern_routes"] == 1

    def test_add_route_duplicate_raises_error(self, router: RequestRouter, mock_handler: Any) -> None:
        router.add_route(path="/home", handler=mock_handler)

        with pytest.raises(expected_exception=ValueError, match="Route for path '/home' already exists"):
            router.add_route(path="/home", handler=mock_handler)

    def test_add_route_override(self, router: RequestRouter, mock_handler: Any, mocker: MockerFixture) -> None:
        router.add_route(path="/home", handler=mock_handler)
        new_handler = mocker.AsyncMock()

        router.add_route(path="/home", handler=new_handler, override=True)

        assert router.get_route_handler(path="/home") is new_handler

    def test_init(self, router: RequestRouter) -> None:
        stats = router.get_route_stats()

        assert not router.get_all_routes()
        assert stats["exact_routes"] == 0
        assert stats["pattern_routes"] == 0
        assert stats["has_default_handler"] is False

    def test_remove_pattern_route(self, router: RequestRouter, mock_handler: Any) -> None:
        pattern = r"/users/(\d+)"
        router.add_pattern_route(pattern=pattern, handler=mock_handler)

        router.remove_pattern_route(pattern=pattern)

        assert router.get_route_stats()["pattern_routes"] == 0

        router.remove_pattern_route(pattern=r"/not/found")

    def test_remove_route(self, router: RequestRouter, mock_handler: Any) -> None:
        router.add_route(path="/temp", handler=mock_handler)
        assert router.get_route_handler(path="/temp") is not None

        router.remove_route(path="/temp")

        assert router.get_route_handler(path="/temp") is None

        router.remove_route(path="/non-existent")

    def test_route_request_no_match(self, router: RequestRouter, mock_session: Any, mocker: MockerFixture) -> None:
        router.add_route(path="/home", handler=mocker.AsyncMock())
        mock_session.path = "/about"

        result = router.route_request(session=mock_session)

        assert result is None

    def test_route_request_precedence(self, router: RequestRouter, mock_session: Any, mocker: MockerFixture) -> None:
        exact_handler = mocker.AsyncMock()
        pattern_handler = mocker.AsyncMock()
        router.add_route(path="/users/profile", handler=exact_handler)
        router.add_pattern_route(pattern=r"/users/(\w+)", handler=pattern_handler)
        mock_session.path = "/users/profile"

        result = router.route_request(session=mock_session)

        assert result is not None
        handler, params = result
        assert handler is exact_handler
        assert params == {}

    @pytest.mark.parametrize(
        argnames="path, should_find",
        argvalues=[
            ("/exact", "exact_handler"),
            ("/users/123", "pattern_handler"),
            ("/items/abc/456", "multi_capture_handler"),
            ("/not-found", "default_handler"),
        ],
    )
    def test_route_request_scenarios(
        self, router: RequestRouter, mock_session: Any, path: str, should_find: str, mocker: MockerFixture
    ) -> None:
        handlers = {
            "exact_handler": mocker.AsyncMock(name="exact"),
            "pattern_handler": mocker.AsyncMock(name="pattern"),
            "multi_capture_handler": mocker.AsyncMock(name="multi_capture"),
            "default_handler": mocker.AsyncMock(name="default"),
        }
        router.add_route(path="/exact", handler=handlers["exact_handler"])
        router.add_pattern_route(pattern=r"/users/(\d+)", handler=handlers["pattern_handler"])
        router.add_pattern_route(pattern=r"/items/([a-z]+)/(\d+)", handler=handlers["multi_capture_handler"])
        router.set_default_handler(handler=handlers["default_handler"])
        mock_session.path = path

        result = router.route_request(session=mock_session)

        assert result is not None
        handler, params = result
        assert handler is handlers[should_find]
        assert params == {}
