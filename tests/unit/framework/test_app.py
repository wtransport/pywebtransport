"""Unit tests for the pywebtransport.framework.app module."""

import asyncio
import weakref
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pytest_asyncio import fixture as asyncio_fixture
from pytest_mock import MockerFixture

from pywebtransport import (
    ConnectionError,
    Event,
    ServerApp,
    ServerConfig,
    ServerError,
    WebTransportServer,
    WebTransportSession,
)
from pywebtransport.connection import WebTransportConnection
from pywebtransport.framework import MiddlewareProtocol, MiddlewareRejected, StatefulMiddlewareProtocol
from pywebtransport.types import EventType


class TestServerApp:

    @pytest.fixture
    def app(self, mock_server: Any, mock_router: Any, mock_middleware_manager: Any) -> ServerApp:
        config = ServerConfig(certfile="dummy.crt", keyfile="dummy.key")
        return ServerApp(config=config)

    @asyncio_fixture
    async def mock_connection(self, mocker: MockerFixture) -> Any:
        conn = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        conn.is_connected = True
        conn.handle = 42

        return conn

    @pytest.fixture
    def mock_middleware_manager(self, mocker: MockerFixture) -> Any:
        manager_instance = mocker.MagicMock()
        manager_instance.process_request = mocker.AsyncMock(return_value=None)
        mocker.patch(target="pywebtransport.framework.app.MiddlewareManager", return_value=manager_instance)

        return manager_instance

    @pytest.fixture
    def mock_router(self, mocker: MockerFixture) -> Any:
        router_instance = mocker.MagicMock()
        mocker.patch(target="pywebtransport.framework.app.RequestRouter", return_value=router_instance)

        return router_instance

    @pytest.fixture
    def mock_server(self, mocker: MockerFixture) -> Any:
        server_instance = mocker.create_autospec(spec=WebTransportServer, instance=True)
        server_instance.session_manager = mocker.MagicMock()
        server_instance.session_manager.add_session = mocker.AsyncMock()
        server_instance.config = ServerConfig(
            bind_host="0.0.0.0", bind_port=4433, certfile="dummy.crt", keyfile="dummy.key"
        )
        server_instance.close = mocker.AsyncMock()
        mocker.patch(target="pywebtransport.framework.app.WebTransportServer", return_value=server_instance)

        return server_instance

    @asyncio_fixture
    async def mock_session(self, mocker: MockerFixture) -> Any:
        session_instance = mocker.MagicMock(name="WebTransportSession")
        session_instance.__class__ = WebTransportSession
        session_instance.session_id = 100
        session_instance.path = "/"
        session_instance.is_closed = False
        session_instance.accept = mocker.AsyncMock()
        session_instance.close = mocker.AsyncMock()
        session_instance.reject = mocker.AsyncMock()

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
        self, app: ServerApp, mock_session: Any, mock_router: Any, mocker: MockerFixture
    ) -> None:
        mock_handler = mocker.AsyncMock()
        mock_router.route_request.return_value = (mock_handler, {})
        mock_session.accept.side_effect = ValueError("Accept failed")
        mock_logger_warning = mocker.patch(target="pywebtransport.framework.app._logger.warning")

        await app._dispatch_to_handler(session=mock_session)

        mock_logger_warning.assert_called()
        assert not app._handler_tasks

    @pytest.mark.asyncio
    async def test_dispatch_to_handler_no_route(self, app: ServerApp, mock_session: Any, mock_router: Any) -> None:
        mock_router.route_request.return_value = None

        await app._dispatch_to_handler(session=mock_session)

        mock_session.reject.assert_awaited_once_with(status_code=404)

    @pytest.mark.asyncio
    async def test_dispatch_to_handler_no_task_group(
        self, app: ServerApp, mock_session: Any, mock_router: Any, mocker: MockerFixture
    ) -> None:
        mock_handler = mocker.AsyncMock()
        mock_router.route_request.return_value = (mock_handler, {})
        mock_logger_warning = mocker.patch(target="pywebtransport.framework.app._logger.warning")
        app._tg = None

        await app._dispatch_to_handler(session=mock_session)

        mock_session.accept.assert_awaited_once()
        mock_logger_warning.assert_called_with("app validate failed expected=open")

    @pytest.mark.asyncio
    async def test_dispatch_to_handler_success(
        self, app: ServerApp, mock_session: Any, mock_router: Any, mocker: MockerFixture
    ) -> None:
        mock_handler = mocker.AsyncMock()
        mock_router.route_request.return_value = (mock_handler, {"id": "123"})

        mock_tg = mocker.Mock()
        mock_task = mocker.Mock(spec=asyncio.Task)
        mock_tg.create_task.return_value = mock_task
        app._tg = mock_tg

        await app._dispatch_to_handler(session=mock_session)

        mock_session.accept.assert_awaited_once()
        mock_tg.create_task.assert_called_once()
        assert mock_task in app._handler_tasks

        coro = mock_tg.create_task.call_args.kwargs["coro"]
        coro.close()

    @pytest.mark.asyncio
    async def test_get_session_from_event_disconnected(
        self, app: ServerApp, mocker: MockerFixture, mock_connection: Any, mock_session: Any
    ) -> None:
        mock_connection.is_connected = False
        mock_logger_warning = mocker.patch(target="pywebtransport.framework.app._logger.warning")
        event = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session": mock_session, "session_id": 100},
        )

        session = await app._get_session_from_event(event=event)

        assert session is None
        mock_logger_warning.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="data_override",
        argvalues=[
            {"session": "not_a_session"},
            {"connection": "not_a_connection"},
            {"session": None},
            {"connection": None},
        ],
    )
    async def test_get_session_from_event_failures(
        self, app: ServerApp, mocker: MockerFixture, data_override: dict[str, Any]
    ) -> None:
        mock_conn = mocker.create_autospec(spec=WebTransportConnection, instance=True)
        mock_session = mocker.MagicMock(name="WebTransportSession")
        mock_session.__class__ = WebTransportSession

        base_data = {"connection": mock_conn, "session": mock_session, "session_id": 100}
        base_data.update(data_override)

        event = Event(type=EventType.SESSION_REQUEST, data=base_data)

        session = await app._get_session_from_event(event=event)

        assert session is None

    @pytest.mark.asyncio
    async def test_get_session_from_event_invalid_data_type(self, app: ServerApp, mocker: MockerFixture) -> None:
        mock_logger_warning = mocker.patch(target="pywebtransport.framework.app._logger.warning")
        event = Event(type=EventType.SESSION_REQUEST, data="not_a_dict")

        session = await app._get_session_from_event(event=event)

        assert session is None
        mock_logger_warning.assert_called_with("rt_event validate invalid expected=dict")

    @pytest.mark.asyncio
    async def test_get_session_from_event_success(
        self, app: ServerApp, mock_connection: Any, mock_session: Any
    ) -> None:
        event = Event(
            type=EventType.SESSION_REQUEST,
            data={"connection": mock_connection, "session": mock_session, "session_id": 100},
        )

        session = await app._get_session_from_event(event=event)

        assert session is mock_session

    @pytest.mark.asyncio
    async def test_handle_session_request_exception_cleanup(
        self, app: ServerApp, mocker: MockerFixture, mock_session: Any
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mocker.patch.object(target=app, attribute="_dispatch_to_handler", side_effect=ValueError("Dispatch error"))

        event = Event(type=EventType.SESSION_REQUEST, data={"session_id": 100})

        await app._handle_session_request(event=event)

        mock_session.reject.assert_awaited_once_with(status_code=500)

    @pytest.mark.asyncio
    async def test_handle_session_request_exception_cleanup_branches(
        self, app: ServerApp, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", side_effect=ValueError("Unexpected"))
        mock_logger_warning = mocker.patch(target="pywebtransport.framework.app._logger.warning")

        event_no_id = Event(type=EventType.SESSION_REQUEST, data={"session_id": 999})

        await app._handle_session_request(event=event_no_id)

        mock_logger_warning.assert_called()

    @pytest.mark.asyncio
    async def test_handle_session_request_exception_cleanup_error(
        self, app: ServerApp, mocker: MockerFixture, mock_session: Any
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mocker.patch.object(target=app, attribute="_dispatch_to_handler", side_effect=ValueError("Dispatch error"))
        mock_session.reject.side_effect = ValueError("Reject failed")
        mock_logger_warning = mocker.patch(target="pywebtransport.framework.app._logger.warning")

        event = Event(type=EventType.SESSION_REQUEST, data={"session_id": 100})

        await app._handle_session_request(event=event)

        mock_logger_warning.assert_any_call(
            "wt_session reject failed session_id=%d err=%s", mocker.ANY, mocker.ANY, exc_info=mocker.ANY
        )

    @pytest.mark.asyncio
    async def test_handle_session_request_happy_path(
        self, app: ServerApp, mock_middleware_manager: Any, mocker: MockerFixture, mock_session: Any
    ) -> None:
        mock_get_session = mocker.patch.object(
            target=app, attribute="_get_session_from_event", new_callable=mocker.AsyncMock, return_value=mock_session
        )
        mock_dispatch = mocker.patch.object(target=app, attribute="_dispatch_to_handler", new_callable=mocker.AsyncMock)

        event = Event(type=EventType.SESSION_REQUEST, data={"session_id": 100})

        await app._handle_session_request(event=event)

        mock_get_session.assert_awaited_once_with(event=event)
        mock_middleware_manager.process_request.assert_awaited_once_with(session=mock_session)
        mock_dispatch.assert_awaited_once_with(session=mock_session)

    @pytest.mark.asyncio
    async def test_handle_session_request_middleware_rejection(
        self, app: ServerApp, mock_middleware_manager: Any, mocker: MockerFixture, mock_session: Any
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mock_middleware_manager.process_request.side_effect = MiddlewareRejected(status_code=403)

        event = Event(type=EventType.SESSION_REQUEST, data={"session_id": 100})

        await app._handle_session_request(event=event)

        mock_session.reject.assert_awaited_once_with(status_code=403)

    @pytest.mark.asyncio
    async def test_handle_session_request_middleware_rejection_error(
        self, app: ServerApp, mock_middleware_manager: Any, mocker: MockerFixture, mock_session: Any
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mock_middleware_manager.process_request.side_effect = MiddlewareRejected(status_code=403)
        mock_session.reject.side_effect = ValueError("Reject failed")
        mock_logger_debug = mocker.patch(target="pywebtransport.framework.app._logger.debug")

        event = Event(type=EventType.SESSION_REQUEST, data={"session_id": 100})

        await app._handle_session_request(event=event)

        mock_session.reject.assert_awaited_once_with(status_code=403)
        mock_logger_debug.assert_called_with(
            "wt_session reject failed session_id=%d err=%s", mock_session.session_id, mocker.ANY
        )

    @pytest.mark.asyncio
    async def test_handle_session_request_middleware_rejection_no_session(
        self, app: ServerApp, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(
            target=app,
            attribute="_get_session_from_event",
            side_effect=MiddlewareRejected(status_code=403),
        )

        event = Event(type=EventType.SESSION_REQUEST, data={"session_id": 999})

        await app._handle_session_request(event=event)

    @pytest.mark.asyncio
    async def test_handle_session_request_middleware_rejection_server_error(
        self, app: ServerApp, mock_middleware_manager: Any, mocker: MockerFixture, mock_session: Any
    ) -> None:
        mocker.patch.object(target=app, attribute="_get_session_from_event", return_value=mock_session)
        mock_middleware_manager.process_request.side_effect = MiddlewareRejected(status_code=500)

        event = Event(type=EventType.SESSION_REQUEST, data={"session_id": 100})

        await app._handle_session_request(event=event)

        mock_session.reject.assert_awaited_once_with(status_code=500)

    @pytest.mark.asyncio
    async def test_handle_session_request_no_session(self, app: ServerApp, mocker: MockerFixture) -> None:
        mocker.patch.object(
            target=app, attribute="_get_session_from_event", new_callable=mocker.AsyncMock, return_value=None
        )
        mock_middleware = mocker.patch.object(target=app, attribute="_middleware_manager")

        event = Event(type=EventType.SESSION_REQUEST, data={"session_id": 999})

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

        cast(MagicMock, mock_server.on).assert_called_once_with(
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
        mock_session.close.side_effect = ConnectionError(message="Engine stopped")
        mock_logger_debug = mocker.patch(target="pywebtransport.framework.app._logger.debug")

        await app._run_handler_safely(handler=handler_mock, session=mock_session, params={})

        mock_session.close.assert_awaited_once()
        mock_logger_debug.assert_any_call(
            "wt_session close failed session_id=%d err=%s", mock_session.session_id, mocker.ANY
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

        def consume_coro_and_raise(*args: Any, **kwargs: Any) -> Any:
            coro = kwargs.get("main") or (args[0] if args else None)
            if asyncio.iscoroutine(coro):
                coro.close()
            raise KeyboardInterrupt

        mock_asyncio_run = mocker.patch(target="asyncio.run", side_effect=consume_coro_and_raise)

        app.run()

        mock_asyncio_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_serve(self, app: ServerApp, mock_server: Any, mocker: MockerFixture) -> None:
        app._tg = mocker.Mock()

        await app.serve(host="localhost", port=8080)

        mock_server.listen.assert_awaited_once_with(host="localhost", port=8080)
        mock_server.serve_forever.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_serve_not_activated(self, app: ServerApp) -> None:
        with pytest.raises(expected_exception=ServerError, match="app validate failed expected=open"):
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
        if is_async:
            mock_handler = mocker.AsyncMock()

            async def real_async_handler() -> None:
                await mock_handler()

            app.on_startup(handler=real_async_handler)
            app.on_shutdown(handler=real_async_handler)

            await app.startup()
            mock_handler.assert_awaited_once()

            await app.shutdown()
            assert mock_handler.await_count == 2
        else:
            mock_handler = mocker.MagicMock()

            def real_sync_handler() -> None:
                mock_handler()

            app.on_startup(handler=real_sync_handler)
            app.on_shutdown(handler=real_sync_handler)

            await app.startup()
            mock_handler.assert_called_once()

            await app.shutdown()
            assert mock_handler.call_count == 2

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
