"""Unit tests for the pywebtransport.server.app module."""

import asyncio
import http
import weakref
from typing import Any, cast

import pytest
from pytest_asyncio import fixture as asyncio_fixture
from pytest_mock import MockerFixture

from pywebtransport import ConnectionError, Event, ServerApp, ServerConfig, ServerError, WebTransportSession
from pywebtransport._protocol.events import UserAcceptSession, UserCloseSession, UserRejectSession
from pywebtransport.connection import WebTransportConnection
from pywebtransport.server import MiddlewareProtocol, MiddlewareRejected, StatefulMiddlewareProtocol, WebTransportServer
from pywebtransport.types import EventType


class TestServerApp:

    @pytest.fixture
    def app(self, mock_server: Any, mock_router: Any, mock_middleware_manager: Any) -> ServerApp:
        return ServerApp()

    @asyncio_fixture
    async def mock_connection(self, mocker: MockerFixture, mock_future: asyncio.Future[Any]) -> Any:
        conn = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        conn.is_connected = True
        conn.connection_id = "conn_1"
        conn._handle = 42

        mock_driver = mocker.Mock()
        mock_driver.create_request.return_value = (123, mock_future)
        mock_driver.send_user_event = mocker.Mock()
        conn._driver = mock_driver

        return conn

    @asyncio_fixture
    async def mock_future(self) -> asyncio.Future[Any]:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        fut.set_result(None)

        return fut

    @pytest.fixture
    def mock_middleware_manager(self, mocker: MockerFixture) -> Any:
        manager_instance = mocker.MagicMock()
        manager_instance.process_request = mocker.AsyncMock(return_value=None)
        mocker.patch(target="pywebtransport.server.app.MiddlewareManager", return_value=manager_instance)

        return manager_instance

    @pytest.fixture
    def mock_router(self, mocker: MockerFixture) -> Any:
        router_instance = mocker.MagicMock()
        mocker.patch(target="pywebtransport.server.app.RequestRouter", return_value=router_instance)

        return router_instance

    @pytest.fixture
    def mock_server(self, mocker: MockerFixture) -> Any:
        server_instance = mocker.create_autospec(spec=WebTransportServer, instance=True)
        server_instance.session_manager = mocker.MagicMock()
        server_instance.session_manager.add_session = mocker.AsyncMock()
        server_instance.config = ServerConfig(bind_host="0.0.0.0", bind_port=4433)
        server_instance.close = mocker.AsyncMock()
        mocker.patch(target="pywebtransport.server.app.WebTransportServer", return_value=server_instance)

        return server_instance

    @asyncio_fixture
    async def mock_session(self, mocker: MockerFixture, mock_connection: Any) -> Any:
        session_instance = mocker.MagicMock(name="WebTransportSession")
        session_instance.__class__ = WebTransportSession
        session_instance.session_id = 100
        session_instance.path = "/"
        session_instance.is_closed = False
        session_instance.close = mocker.AsyncMock()
        session_instance._connection = mocker.Mock(return_value=mock_connection)

        return session_instance

    @pytest.mark.asyncio
    async def test_aexit_cleanup_without_startup(self, app: ServerApp, mock_server: Any) -> None:
        app._tg = None

        await app.__aexit__(None, None, None)

        mock_server.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_context_manager(self, app: ServerApp, mock_server: Any, mocker: MockerFixture) -> None:
        mock_startup = mocker.patch.object(target=app, attribute="startup", new_callable=mocker.AsyncMock)
        mock_shutdown = mocker.patch.object(target=app, attribute="shutdown", new_callable=mocker.AsyncMock)

        async with app as a:
            assert a is app
            assert app._tg is not None
            mock_server.__aenter__.assert_awaited_once()
            mock_startup.assert_awaited_once()

        mock_shutdown.assert_awaited_once()
        mock_server.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_context_manager_with_exception(
        self, app: ServerApp, mock_server: Any, mocker: MockerFixture
    ) -> None:
        mock_startup = mocker.patch.object(target=app, attribute="startup", new_callable=mocker.AsyncMock)
        mock_shutdown = mocker.patch.object(target=app, attribute="shutdown", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=(ValueError, ExceptionGroup)):
            async with app:
                mock_server.__aenter__.assert_awaited_once()
                mock_startup.assert_awaited_once()
                raise ValueError("Test error")

        mock_shutdown.assert_awaited_once()
        mock_server.close.assert_awaited_once()

    def test_decorators(self, app: ServerApp, mock_router: Any, mock_middleware_manager: Any) -> None:

        @app.route(path="/test")
        async def handler1(session: WebTransportSession) -> None:
            pass

        @app.pattern_route(pattern="/other/.*")
        async def handler2(session: WebTransportSession) -> None:
            pass

        mock_router.add_route.assert_called_once_with(path="/test", handler=handler1)
        mock_router.add_pattern_route.assert_called_once_with(pattern="/other/.*", handler=handler2)

        async def middleware(session: WebTransportSession) -> None:
            pass

        middleware_proto = cast(MiddlewareProtocol, middleware)
        registered_middleware = app.middleware(middleware_func=middleware_proto)

        assert registered_middleware is middleware_proto
        mock_middleware_manager.add_middleware.assert_called_once_with(middleware=middleware_proto)

        @app.on_startup
        def startup_handler() -> None:
            pass

        @app.on_shutdown
        def shutdown_handler() -> None:
            pass

        assert startup_handler in app._startup_handlers
        assert shutdown_handler in app._shutdown_handlers

    @pytest.mark.asyncio
    async def test_dispatch_to_handler_accept_exception(
        self, app: ServerApp, mock_session: Any, mock_router: Any, mock_connection: Any, mocker: MockerFixture
    ) -> None:
        mock_handler = mocker.AsyncMock()
        mock_router.route_request.return_value = (mock_handler, {})
        error_future: asyncio.Future[None] = asyncio.Future()
        error_future.set_exception(ValueError("Accept failed"))
        mock_connection._driver.create_request.return_value = (1, error_future)
        mock_logger_error = mocker.patch(target="pywebtransport.server.app._logger.error")

        await app._dispatch_to_handler(session=mock_session)

        mock_logger_error.assert_called()
        assert not app._handler_tasks

    @pytest.mark.asyncio
    async def test_dispatch_to_handler_connection_missing(
        self, app: ServerApp, mock_session: Any, mock_router: Any, mocker: MockerFixture
    ) -> None:
        mock_session._connection.return_value = None
        mock_logger_error = mocker.patch(target="pywebtransport.server.app._logger.error")

        await app._dispatch_to_handler(session=mock_session)

        mock_logger_error.assert_called_with("Cannot dispatch handler, connection is missing.")
        mock_router.route_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_dispatch_to_handler_no_route(
        self,
        app: ServerApp,
        mock_session: Any,
        mock_router: Any,
        mock_connection: Any,
        mock_future: asyncio.Future[Any],
    ) -> None:
        mock_router.route_request.return_value = None
        req_id = 99
        mock_connection._driver.create_request.return_value = (req_id, mock_future)

        await app._dispatch_to_handler(session=mock_session)

        mock_connection._driver.create_request.assert_called_once()
        mock_connection._driver.send_user_event.assert_called_once()

        call_args = mock_connection._driver.send_user_event.call_args
        assert call_args.kwargs["handle"] == 42
        event = call_args.kwargs["event"]

        assert isinstance(event, UserRejectSession)
        assert event.request_id == req_id
        assert event.session_id == mock_session.session_id
        assert event.status_code == http.HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_dispatch_to_handler_no_task_group(
        self, app: ServerApp, mock_session: Any, mock_router: Any, mock_connection: Any, mocker: MockerFixture
    ) -> None:
        mock_handler = mocker.AsyncMock()
        mock_router.route_request.return_value = (mock_handler, {})
        mock_logger_error = mocker.patch(target="pywebtransport.server.app._logger.error")
        app._tg = None

        await app._dispatch_to_handler(session=mock_session)

        mock_logger_error.assert_called_with("TaskGroup not initialized. Handler cannot be dispatched.")

    @pytest.mark.asyncio
    async def test_dispatch_to_handler_success(
        self,
        app: ServerApp,
        mock_session: Any,
        mock_router: Any,
        mock_connection: Any,
        mocker: MockerFixture,
        mock_future: asyncio.Future[Any],
    ) -> None:
        mock_handler = mocker.AsyncMock()
        mock_router.route_request.return_value = (mock_handler, {"id": "123"})
        req_id = 55
        mock_connection._driver.create_request.return_value = (req_id, mock_future)

        mock_tg = mocker.Mock()
        mock_task = mocker.Mock(spec=asyncio.Task)
        mock_tg.create_task.return_value = mock_task
        app._tg = mock_tg

        await app._dispatch_to_handler(session=mock_session)

        mock_connection._driver.create_request.assert_called_once()
        mock_connection._driver.send_user_event.assert_called_once()

        call_args = mock_connection._driver.send_user_event.call_args
        assert call_args.kwargs["handle"] == 42
        event = call_args.kwargs["event"]

        assert isinstance(event, UserAcceptSession)
        assert event.request_id == req_id
        assert event.session_id == mock_session.session_id

        mock_tg.create_task.assert_called_once()
        assert mock_task in app._handler_tasks

        coro = mock_tg.create_task.call_args.kwargs["coro"]
        coro.close()

    @pytest.mark.asyncio
    async def test_get_session_from_event_disconnected(
        self, app: ServerApp, mocker: MockerFixture, mock_connection: Any, mock_session: Any
    ) -> None:
        mock_connection.is_connected = False
        mock_logger_warning = mocker.patch(target="pywebtransport.server.app._logger.warning")
        event = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session": mock_session, "session_id": 100},
        )

        session = await app._get_session_from_event(event=event)

        assert session is None
        mock_logger_warning.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="data_override, description",
        argvalues=[
            ({"session": "not_a_session"}, "invalid_session_type"),
            ({"connection": "not_a_connection"}, "invalid_connection_type"),
            ({"session": None}, "missing_session_obj"),
            ({"connection": None}, "missing_connection"),
        ],
    )
    async def test_get_session_from_event_failures(
        self, app: ServerApp, mocker: MockerFixture, data_override: dict[str, Any], description: str
    ) -> None:
        mock_conn = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        mock_session = mocker.MagicMock(name="WebTransportSession")
        mock_session.__class__ = WebTransportSession
        mock_session._connection.return_value = mock_conn

        base_data = {"connection": mock_conn, "session": mock_session, "session_id": 100}
        base_data.update(data_override)

        event = Event(type=EventType.SESSION_REQUEST, data=base_data)

        session = await app._get_session_from_event(event=event)

        assert session is None

    @pytest.mark.asyncio
    async def test_get_session_from_event_invalid_data_type(self, app: ServerApp, mocker: MockerFixture) -> None:
        mock_logger_warning = mocker.patch(target="pywebtransport.server.app._logger.warning")
        event = Event(type=EventType.SESSION_REQUEST, data="not_a_dict")

        session = await app._get_session_from_event(event=event)

        assert session is None
        mock_logger_warning.assert_called_with("Session request event data is not a dictionary")

    @pytest.mark.asyncio
    async def test_get_session_from_event_manager_exception(
        self, app: ServerApp, mock_connection: Any, mock_session: Any, mock_server: Any, mocker: MockerFixture
    ) -> None:
        mock_server.session_manager.add_session.side_effect = ValueError("Manager error")
        mock_logger_error = mocker.patch(target="pywebtransport.server.app._logger.error")
        event = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session": mock_session, "session_id": 100},
        )

        session = await app._get_session_from_event(event=event)

        assert session is mock_session
        mock_logger_error.assert_called()

    @pytest.mark.asyncio
    async def test_get_session_from_event_mismatched_connection(
        self, app: ServerApp, mocker: MockerFixture, mock_connection: Any
    ) -> None:
        other_conn = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        other_conn.connection_id = "conn_other"
        mock_session = mocker.MagicMock(name="WebTransportSession")
        mock_session.__class__ = WebTransportSession
        mock_session._connection.return_value = other_conn

        event = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session": mock_session, "session_id": 100},
        )

        session = await app._get_session_from_event(event=event)

        assert session is None

    @pytest.mark.asyncio
    async def test_get_session_from_event_no_session_manager(
        self, app: ServerApp, mock_connection: Any, mock_session: Any
    ) -> None:
        cast(Any, app.server).session_manager = None
        event = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session": mock_session, "session_id": 100},
        )

        session = await app._get_session_from_event(event=event)

        assert session is mock_session

    @pytest.mark.asyncio
    async def test_get_session_from_event_success(
        self, app: ServerApp, mock_connection: Any, mock_session: Any, mock_server: Any
    ) -> None:
        event = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session": mock_session, "session_id": 100},
        )

        session = await app._get_session_from_event(event=event)

        assert session is mock_session
        mock_server.session_manager.add_session.assert_awaited_once_with(session=mock_session)

    @pytest.mark.asyncio
    async def test_handle_session_request_exception_cleanup(
        self,
        app: ServerApp,
        mocker: MockerFixture,
        mock_connection: Any,
        mock_session: Any,
        mock_future: asyncio.Future[Any],
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", side_effect=ValueError("Unexpected"))
        req_id = 77
        mock_connection._driver.create_request.return_value = (req_id, mock_future)

        event = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session_id": 100, "session": mock_session},
        )

        await app._handle_session_request(event=event)

        mock_connection._driver.send_user_event.assert_called_once()
        call_args = mock_connection._driver.send_user_event.call_args

        assert call_args.kwargs["handle"] == 42
        event_sent = call_args.kwargs["event"]

        assert isinstance(event_sent, UserCloseSession)
        assert event_sent.request_id == req_id
        assert event_sent.session_id == 100

    @pytest.mark.asyncio
    async def test_handle_session_request_exception_cleanup_branches(
        self, app: ServerApp, mocker: MockerFixture, mock_connection: Any, mock_session: Any
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", side_effect=ValueError("Unexpected"))

        event_no_id = Event(type=EventType.SESSION_REQUEST, data={"connection": mock_connection})

        await app._handle_session_request(event=event_no_id)

        mock_connection._driver.send_user_event.assert_not_called()
        mock_connection.reset_mock()

        event_no_conn = Event(type=EventType.SESSION_REQUEST, data={"session_id": 100})

        await app._handle_session_request(event=event_no_conn)

        mock_connection._driver.send_user_event.assert_not_called()

        mock_session.is_closed = True
        event_closed = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session_id": 100, "session": mock_session},
        )

        await app._handle_session_request(event=event_closed)

        mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_session_request_exception_cleanup_error(
        self, app: ServerApp, mocker: MockerFixture, mock_connection: Any
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", side_effect=ValueError("Unexpected"))
        mock_connection._driver.create_request.side_effect = ValueError("Cleanup error")
        mock_logger_error = mocker.patch(target="pywebtransport.server.app._logger.error")

        event = Event(type=EventType.SESSION_REQUEST, data={"connection": mock_connection, "session_id": 100})

        await app._handle_session_request(event=event)

        mock_logger_error.assert_any_call(
            "Error during session request error cleanup: %s", mocker.ANY, exc_info=mocker.ANY
        )

    @pytest.mark.asyncio
    async def test_handle_session_request_exception_cleanup_with_session(
        self,
        app: ServerApp,
        mocker: MockerFixture,
        mock_session: Any,
        mock_connection: Any,
        mock_future: asyncio.Future[Any],
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mocker.patch.object(target=app, attribute="_middleware_manager")
        mocker.patch.object(target=app, attribute="_dispatch_to_handler", side_effect=ValueError("Dispatch error"))
        mock_session.close.side_effect = ValueError("Session close error")
        mock_logger_error = mocker.patch(target="pywebtransport.server.app._logger.error")
        req_id = 88
        mock_connection._driver.create_request.return_value = (req_id, mock_future)

        event = Event(type=EventType.SESSION_REQUEST, data={})

        await app._handle_session_request(event=event)

        mock_session.close.assert_awaited_once()
        mock_logger_error.assert_any_call(
            "Error during session request error cleanup: %s", mocker.ANY, exc_info=mocker.ANY
        )

    @pytest.mark.asyncio
    async def test_handle_session_request_happy_path(
        self, app: ServerApp, mock_middleware_manager: Any, mocker: MockerFixture, mock_session: Any
    ) -> None:
        mock_get_session = mocker.patch.object(
            target=app, attribute="_get_session_from_event", new_callable=mocker.AsyncMock, return_value=mock_session
        )
        mock_dispatch = mocker.patch.object(target=app, attribute="_dispatch_to_handler", new_callable=mocker.AsyncMock)

        event = Event(type=EventType.SESSION_REQUEST, data={})

        await app._handle_session_request(event=event)

        mock_get_session.assert_awaited_once_with(event=event)
        mock_middleware_manager.process_request.assert_awaited_once_with(session=mock_session)
        mock_dispatch.assert_awaited_once_with(session=mock_session)

    @pytest.mark.asyncio
    async def test_handle_session_request_middleware_rejection(
        self,
        app: ServerApp,
        mock_middleware_manager: Any,
        mocker: MockerFixture,
        mock_session: Any,
        mock_connection: Any,
        mock_future: asyncio.Future[Any],
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mock_middleware_manager.process_request.side_effect = MiddlewareRejected(status_code=403)
        req_id = 66
        mock_connection._driver.create_request.return_value = (req_id, mock_future)

        event = Event(type=EventType.SESSION_REQUEST, data={"connection": mock_connection})

        await app._handle_session_request(event=event)

        mock_connection._driver.create_request.assert_called_once()
        mock_connection._driver.send_user_event.assert_called_once()

        call_args = mock_connection._driver.send_user_event.call_args
        assert call_args.kwargs["handle"] == 42
        event_sent = call_args.kwargs["event"]

        assert isinstance(event_sent, UserRejectSession)
        assert event_sent.request_id == req_id
        assert event_sent.status_code == 403

        mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handle_session_request_middleware_rejection_branches(
        self,
        app: ServerApp,
        mock_middleware_manager: Any,
        mocker: MockerFixture,
        mock_session: Any,
        mock_connection: Any,
        mock_future: asyncio.Future[Any],
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mock_middleware_manager.process_request.side_effect = MiddlewareRejected(status_code=403)
        req_id = 66
        mock_connection._driver.create_request.return_value = (req_id, mock_future)

        event = Event(type=EventType.SESSION_REQUEST, data={"connection": mock_connection})

        await app._handle_session_request(event=event)

        mock_session.close.assert_awaited_once()

        mock_session.close.reset_mock()
        mock_connection._driver.create_request.reset_mock()

        mock_session.is_closed = True

        await app._handle_session_request(event=event)

        mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_session_request_middleware_rejection_no_connection(
        self, app: ServerApp, mock_middleware_manager: Any, mocker: MockerFixture, mock_session: Any
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mock_middleware_manager.process_request.side_effect = MiddlewareRejected(status_code=403)

        event = Event(type=EventType.SESSION_REQUEST, data={})

        await app._handle_session_request(event=event)

        mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handle_session_request_no_session(self, app: ServerApp, mocker: MockerFixture) -> None:
        mocker.patch.object(
            target=app, attribute="_get_session_from_event", new_callable=mocker.AsyncMock, return_value=None
        )
        mock_middleware = mocker.patch.object(target=app, attribute="_middleware_manager")

        event = Event(type=EventType.SESSION_REQUEST, data={})

        await app._handle_session_request(event=event)

        mock_middleware.process_request.assert_not_called()

    def test_init(self, app: ServerApp, mock_server: Any, mock_middleware_manager: Any, mock_router: Any) -> None:
        assert isinstance(app._handler_tasks, weakref.WeakSet)
        assert not app._handler_tasks
        assert app._middleware_manager is mock_middleware_manager
        assert app._router is mock_router
        assert app._shutdown_handlers == []
        assert app._startup_handlers == []
        assert app._stateful_middleware == []
        assert app.server is mock_server
        assert app._tg is None

        mock_server.on.assert_called_once_with(
            event_type=EventType.SESSION_REQUEST, handler=app._handle_session_request
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="run_kwargs, serve_kwargs",
        argvalues=[
            ({"host": "localhost", "port": 1234}, {"host": "localhost", "port": 1234}),
            ({}, {"host": "0.0.0.0", "port": 4433}),
        ],
        ids=["with_args", "with_defaults"],
    )
    async def test_run(
        self, app: ServerApp, mocker: MockerFixture, run_kwargs: dict[str, Any], serve_kwargs: dict[str, Any]
    ) -> None:
        mocker.patch.object(target=app, attribute="serve", new_callable=mocker.AsyncMock)
        mock_asyncio_run = mocker.patch(target="asyncio.run")

        app.run(**run_kwargs)

        mock_asyncio_run.assert_called_once()
        call_args = mock_asyncio_run.call_args
        main_coro = call_args.args[0] if call_args.args else call_args.kwargs.get("main")

        if asyncio.iscoroutine(main_coro):
            await main_coro

    @pytest.mark.asyncio
    async def test_run_handler_exception(self, app: ServerApp, mocker: MockerFixture, mock_session: Any) -> None:
        handler_mock = mocker.AsyncMock(side_effect=ValueError("Handler error"))

        await app._run_handler_safely(handler=handler_mock, session=mock_session, params={})

        handler_mock.assert_awaited_once_with(mock_session)
        mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_handler_safely_close_fails(
        self, app: ServerApp, mocker: MockerFixture, mock_session: Any
    ) -> None:
        handler_mock = mocker.AsyncMock()
        mock_session.is_closed = False
        mock_session.close.side_effect = RuntimeError("Close failed")

        await app._run_handler_safely(handler=handler_mock, session=mock_session, params={})

        handler_mock.assert_awaited_once()
        mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_handler_safely_connection_error(
        self, app: ServerApp, mocker: MockerFixture, mock_session: Any
    ) -> None:
        handler_mock = mocker.AsyncMock()
        mock_session.is_closed = False
        mock_session.close.side_effect = ConnectionError("Engine stopped")
        mock_logger_debug = mocker.patch(target="pywebtransport.server.app._logger.debug")

        await app._run_handler_safely(handler=handler_mock, session=mock_session, params={})

        mock_session.close.assert_awaited_once()
        mock_logger_debug.assert_any_call(
            "Session %s cleanup: Connection closed implicitly or Engine stopped (%s).",
            mock_session.session_id,
            mocker.ANY,
        )

    @pytest.mark.asyncio
    async def test_run_handler_session_already_closed(
        self, app: ServerApp, mocker: MockerFixture, mock_session: Any
    ) -> None:
        handler_mock = mocker.AsyncMock()
        mock_session.is_closed = True

        await app._run_handler_safely(handler=handler_mock, session=mock_session, params={})

        handler_mock.assert_awaited_once()
        mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_handler_with_params(self, app: ServerApp, mocker: MockerFixture, mock_session: Any) -> None:
        handler_mock = mocker.AsyncMock()
        params = {"id": "123", "action": "test"}

        await app._run_handler_safely(handler=handler_mock, session=mock_session, params=params)

        handler_mock.assert_awaited_once_with(mock_session, id="123", action="test")
        mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_with_exception(self, app: ServerApp, mocker: MockerFixture) -> None:
        def consume_coro_and_raise(*args: Any, **kwargs: Any) -> Any:
            coro = kwargs.get("main") or (args[0] if args else None)
            if asyncio.iscoroutine(coro):
                coro.close()
            raise ValueError("Run error")

        mock_asyncio_run = mocker.patch(target="asyncio.run", side_effect=consume_coro_and_raise)

        with pytest.raises(expected_exception=ValueError, match="Run error"):
            app.run()

        mock_asyncio_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_with_keyboard_interrupt(self, app: ServerApp, mocker: MockerFixture) -> None:
        mocker.patch.object(target=app, attribute="serve", new_callable=mocker.AsyncMock)
        mock_logger_info = mocker.patch(target="pywebtransport.server.app._logger.info")

        def consume_coro_and_raise(*args: Any, **kwargs: Any) -> Any:
            coro = kwargs.get("main") or (args[0] if args else None)
            if asyncio.iscoroutine(coro):
                coro.close()
            raise KeyboardInterrupt

        mock_asyncio_run = mocker.patch(target="asyncio.run", side_effect=consume_coro_and_raise)

        app.run()

        mock_asyncio_run.assert_called_once()
        mock_logger_info.assert_called_with("Server stopped by user.")

    @pytest.mark.asyncio
    async def test_serve(self, app: ServerApp, mock_server: Any, mocker: MockerFixture) -> None:
        app._tg = mocker.Mock()

        await app.serve(host="localhost", port=8080)

        mock_server.listen.assert_awaited_once_with(host="localhost", port=8080)
        mock_server.serve_forever.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_serve_not_activated(self, app: ServerApp) -> None:
        with pytest.raises(expected_exception=ServerError, match="ServerApp has not been activated"):
            await app.serve()

    @pytest.mark.asyncio
    async def test_serve_with_default_host_port(self, app: ServerApp, mock_server: Any, mocker: MockerFixture) -> None:
        app._tg = mocker.Mock()

        await app.serve()

        mock_server.listen.assert_awaited_once_with(host="0.0.0.0", port=4433)
        mock_server.serve_forever.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_active_tasks(self, app: ServerApp, mocker: MockerFixture) -> None:
        mock_task_1 = mocker.MagicMock(spec=asyncio.Task)
        mock_task_1.done.return_value = False
        mock_task_2 = mocker.MagicMock(spec=asyncio.Task)
        mock_task_2.done.return_value = True

        app._handler_tasks.add(mock_task_1)
        app._handler_tasks.add(mock_task_2)

        await app.shutdown()

        mock_task_1.cancel.assert_called_once()
        mock_task_2.cancel.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(argnames="is_async", argvalues=[True, False])
    async def test_startup_and_shutdown_handlers(self, app: ServerApp, mocker: MockerFixture, is_async: bool) -> None:
        mocker.patch(target="asyncio.iscoroutinefunction", return_value=is_async)

        startup_handler = mocker.AsyncMock() if is_async else mocker.MagicMock()
        shutdown_handler = mocker.AsyncMock() if is_async else mocker.MagicMock()

        app.on_startup(handler=startup_handler)
        app.on_shutdown(handler=shutdown_handler)

        await app.startup()

        if is_async:
            startup_handler.assert_awaited_once()
        else:
            startup_handler.assert_called_once()

        await app.shutdown()

        if is_async:
            shutdown_handler.assert_awaited_once()
        else:
            shutdown_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_startup_and_shutdown_stateful_middleware(self, app: ServerApp, mocker: MockerFixture) -> None:
        stateful_middleware = mocker.MagicMock(spec=StatefulMiddlewareProtocol)
        stateful_middleware.__aenter__ = mocker.AsyncMock()
        stateful_middleware.__aexit__ = mocker.AsyncMock()
        app.add_middleware(middleware=stateful_middleware)

        await app.startup()

        stateful_middleware.__aenter__.assert_awaited_once()

        await app.shutdown()

        stateful_middleware.__aexit__.assert_awaited_once_with(None, None, None)

    @pytest.mark.asyncio
    async def test_startup_shutdown_defensive_checks(self, app: ServerApp, mocker: MockerFixture) -> None:
        middleware = mocker.MagicMock(spec=StatefulMiddlewareProtocol)
        middleware.__aenter__ = mocker.AsyncMock()
        middleware.__aexit__ = mocker.AsyncMock()

        app._stateful_middleware.append(middleware)

        await app.startup()

        middleware.__aenter__.assert_awaited_once()

        await app.shutdown()

        middleware.__aexit__.assert_awaited_once()
