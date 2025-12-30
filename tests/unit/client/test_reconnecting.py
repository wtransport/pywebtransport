"""Unit tests for the pywebtransport.client.reconnecting module."""

import asyncio
import logging
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture

from pywebtransport import (
    ClientConfig,
    ClientError,
    ConnectionError,
    TimeoutError,
    WebTransportClient,
    WebTransportSession,
)
from pywebtransport.client import ReconnectingClient
from pywebtransport.types import EventType, SessionState


class TestReconnectingClient:

    URL = "https://example.com"

    @pytest.fixture
    def client(self, mock_underlying_client: Any) -> ReconnectingClient:
        return ReconnectingClient(url=self.URL, client=mock_underlying_client)

    @pytest.fixture
    def mock_session(self, mocker: MockerFixture) -> Any:
        session = mocker.create_autospec(WebTransportSession, instance=True)
        type(session).state = mocker.PropertyMock(return_value=SessionState.CONNECTED)
        session.is_closed = False
        session.close = mocker.AsyncMock(return_value=None)
        session._connection = mocker.Mock(return_value=mocker.AsyncMock())
        session.events = mocker.MagicMock()
        session.events.wait_for = mocker.AsyncMock()
        return session

    @pytest.fixture
    def mock_underlying_client(self, mocker: MockerFixture, mock_session: Any) -> Any:
        client = mocker.create_autospec(WebTransportClient, instance=True)
        client.connect = mocker.AsyncMock(return_value=mock_session)
        client.config = ClientConfig()
        return client

    @pytest.mark.asyncio
    async def test_aenter_and_aexit_lifecycle(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        mock_tg = mocker.AsyncMock(spec=asyncio.TaskGroup)
        mock_tg.create_task.return_value = mocker.Mock(spec=asyncio.Task)
        mocker.patch("asyncio.TaskGroup", return_value=mock_tg)

        connect_mock = cast(MagicMock, client._client.connect)
        connect_mock.side_effect = asyncio.CancelledError

        async with client:
            assert client._is_initialized
            assert client._tg is mock_tg
            mock_tg.__aenter__.assert_awaited_once()
            mock_tg.create_task.assert_called_once()

            call_args = mock_tg.create_task.call_args
            if call_args and "coro" in call_args.kwargs:
                call_args.kwargs["coro"].close()

        mock_tg.__aexit__.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aenter_is_idempotent(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        mock_tg = mocker.AsyncMock(spec=asyncio.TaskGroup)
        mocker.patch("asyncio.TaskGroup", return_value=mock_tg)

        async with client:
            mock_tg.create_task.assert_called_once()

            call_args = mock_tg.create_task.call_args
            if call_args and "coro" in call_args.kwargs:
                call_args.kwargs["coro"].close()

            async with client as new_client:
                assert new_client is client

            mock_tg.create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_aenter_on_closed_client(self, client: ReconnectingClient) -> None:
        await client.close()

        with pytest.raises(ClientError, match="Client is already closed"):
            async with client:
                pass

    @pytest.mark.asyncio
    async def test_aexit_closes_on_exception(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        close_spy = mocker.spy(client, "close")
        mock_tg = mocker.AsyncMock(spec=asyncio.TaskGroup)
        mocker.patch("asyncio.TaskGroup", return_value=mock_tg)

        with pytest.raises(RuntimeError, match="Test exception"):
            async with client:
                call_args = mock_tg.create_task.call_args
                if call_args and "coro" in call_args.kwargs:
                    call_args.kwargs["coro"].close()
                raise RuntimeError("Test exception")

        close_spy.assert_awaited_once()
        mock_tg.__aexit__.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aexit_without_aenter(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        client._tg = None
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_close_idempotency(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        mock_tg = mocker.AsyncMock(spec=asyncio.TaskGroup)
        mock_task = mocker.Mock(spec=asyncio.Task)
        mock_task.done.return_value = False
        mock_tg.create_task.return_value = mock_task
        mocker.patch("asyncio.TaskGroup", return_value=mock_tg)

        await client.__aenter__()
        mock_tg.create_task.call_args.kwargs["coro"].close()

        assert client._reconnect_task is not None

        await client.close()
        mock_task.cancel.assert_called_once()

        mock_task.reset_mock()
        await client.close()
        mock_task.cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_logs_session_close_error(
        self, client: ReconnectingClient, mock_session: Any, caplog: LogCaptureFixture
    ) -> None:
        client._session = mock_session
        mock_session.close.side_effect = RuntimeError("Close failed")

        await client.close()

        assert "Error closing session: Close failed" in caplog.text
        assert client._session is None

    @pytest.mark.asyncio
    async def test_close_with_active_session(
        self, client: ReconnectingClient, mock_session: Any, mocker: MockerFixture
    ) -> None:
        mock_tg = mocker.AsyncMock(spec=asyncio.TaskGroup)
        mocker.patch("asyncio.TaskGroup", return_value=mock_tg)

        async with client:
            mock_tg.create_task.call_args.kwargs["coro"].close()
            client._session = mock_session
            await client.close()
            mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_session_client_closed_while_waiting(
        self, client: ReconnectingClient, mocker: MockerFixture
    ) -> None:
        client._connected_event = asyncio.Event()
        client._tg = mocker.AsyncMock(spec=asyncio.TaskGroup)

        mock_task = mocker.Mock(spec=asyncio.Task)
        cast(MagicMock, mock_task.done).return_value = False
        client._reconnect_task = mock_task

        async def simulate_close_during_wait() -> None:
            client._closed = True
            assert client._connected_event is not None
            client._connected_event.set()

        mocker.patch.object(client._connected_event, "wait", side_effect=simulate_close_during_wait)

        with pytest.raises(ClientError, match="Client closed while waiting for session"):
            await client.get_session()

    @pytest.mark.asyncio
    async def test_get_session_crashes_propagation(self, client: ReconnectingClient) -> None:
        client._closed = False
        crash_error = ValueError("Unexpected crash")
        client._crashed_exception = crash_error
        client._connected_event = asyncio.Event()

        with pytest.raises(ClientError, match="Background reconnection task crashed") as exc_info:
            await client.get_session()

        assert exc_info.value.__cause__ is crash_error

    @pytest.mark.asyncio
    async def test_get_session_crashes_propagation_in_loop(
        self, client: ReconnectingClient, mocker: MockerFixture
    ) -> None:
        client._connected_event = asyncio.Event()
        client._tg = mocker.AsyncMock(spec=asyncio.TaskGroup)

        crash_error = ValueError("Crash during wait")

        async def wait_side_effect() -> None:
            client._crashed_exception = crash_error

        mocker.patch.object(client._connected_event, "wait", side_effect=wait_side_effect)

        with pytest.raises(ClientError, match="Background task crashed") as exc_info:
            await client.get_session()

        assert exc_info.value.__cause__ is crash_error

    @pytest.mark.asyncio
    async def test_get_session_defensive_task_check(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        client._connected_event = asyncio.Event()
        client._tg = mocker.AsyncMock(spec=asyncio.TaskGroup)
        client._reconnect_task = None
        call_count = 0

        async def side_effect() -> None:
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                client._closed = True

        mocker.patch.object(client._connected_event, "wait", side_effect=side_effect)

        with pytest.raises(ClientError, match="Client closed while waiting"):
            await client.get_session(wait_timeout=0.1)

    @pytest.mark.asyncio
    async def test_get_session_fails_if_task_done(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        client._connected_event = asyncio.Event()
        client._tg = mocker.AsyncMock(spec=asyncio.TaskGroup)

        mock_task = mocker.Mock(spec=asyncio.Task)
        cast(MagicMock, mock_task.done).return_value = True
        cast(MagicMock, mock_task.cancelled).return_value = False
        cast(MagicMock, mock_task.exception).return_value = None

        client._reconnect_task = mock_task

        async def simulate_event_wait() -> None:
            return

        mocker.patch.object(client._connected_event, "wait", side_effect=simulate_event_wait)

        with pytest.raises(ClientError, match="Reconnection task finished unexpectedly"):
            await client.get_session()

    @pytest.mark.asyncio
    async def test_get_session_on_closed_client(self, client: ReconnectingClient) -> None:
        await client.close()

        with pytest.raises(ClientError, match="Client is closed"):
            await client.get_session()

    @pytest.mark.asyncio
    async def test_get_session_succeeds_when_connected(
        self, client: ReconnectingClient, mock_session: Any, mocker: MockerFixture
    ) -> None:
        client._session = mock_session
        client._connected_event = asyncio.Event()
        client._connected_event.set()
        client._tg = mocker.AsyncMock(spec=asyncio.TaskGroup)

        session = await client.get_session(wait_timeout=0.1)

        assert session is mock_session

    @pytest.mark.asyncio
    async def test_get_session_task_cancelled_error(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        client._connected_event = asyncio.Event()
        client._tg = mocker.AsyncMock(spec=asyncio.TaskGroup)

        mock_task = mocker.Mock(spec=asyncio.Task)
        cast(MagicMock, mock_task.done).return_value = True
        cast(MagicMock, mock_task.cancelled).return_value = True
        client._reconnect_task = mock_task

        async def simulate_event_wait() -> None:
            return

        mocker.patch.object(client._connected_event, "wait", side_effect=simulate_event_wait)

        with pytest.raises(ClientError, match="Reconnection task cancelled"):
            await client.get_session()

    @pytest.mark.asyncio
    async def test_get_session_task_exception(self, client: ReconnectingClient, mocker: MockerFixture) -> None:
        client._connected_event = asyncio.Event()
        client._tg = mocker.AsyncMock(spec=asyncio.TaskGroup)

        mock_task = mocker.Mock(spec=asyncio.Task)
        cast(MagicMock, mock_task.done).return_value = True
        cast(MagicMock, mock_task.cancelled).return_value = False
        original_error = RuntimeError("Task died")
        cast(MagicMock, mock_task.exception).return_value = original_error

        client._reconnect_task = mock_task

        async def simulate_event_wait() -> None:
            return

        mocker.patch.object(client._connected_event, "wait", side_effect=simulate_event_wait)

        with pytest.raises(ClientError, match="Reconnection task failed: Task died") as exc_info:
            await client.get_session()

        assert exc_info.value.__cause__ is original_error

    @pytest.mark.asyncio
    async def test_get_session_timeout(self, client: ReconnectingClient, mock_underlying_client: Any) -> None:
        async def sleep_forever(*args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(10)

        mock_underlying_client.connect.side_effect = sleep_forever
        mock_underlying_client.config.max_connection_retries = 5

        async with client:
            with pytest.raises(asyncio.TimeoutError):
                await client.get_session(wait_timeout=0.01)

    @pytest.mark.asyncio
    async def test_get_session_uninitialized(self, client: ReconnectingClient) -> None:
        with pytest.raises(ClientError, match="ReconnectingClient has not been activated"):
            await client.get_session()

    @pytest.mark.asyncio
    async def test_get_session_waits_and_succeeds(
        self, client: ReconnectingClient, mock_session: Any, mocker: MockerFixture
    ) -> None:
        session_available = asyncio.Event()

        async def wait_side_effect(*args: Any, **kwargs: Any) -> None:
            await session_available.wait()
            client._session = mock_session

        client._connected_event = asyncio.Event()
        client._tg = mocker.AsyncMock(spec=asyncio.TaskGroup)
        mocker.patch.object(client._connected_event, "wait", side_effect=wait_side_effect)

        task = asyncio.create_task(client.get_session(wait_timeout=1.0))
        await asyncio.sleep(0.01)
        session_available.set()
        session = await task

        assert session is mock_session

    def test_init(self, mock_underlying_client: Any) -> None:
        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)

        assert client._url == self.URL
        assert client._client is mock_underlying_client
        assert client._config is mock_underlying_client.config
        assert client._session is None
        assert not client._closed
        assert not client._is_initialized

    @pytest.mark.parametrize(
        ("session_state", "event_is_set", "expected"),
        [
            (SessionState.CONNECTED, True, True),
            (SessionState.CONNECTING, True, False),
            (SessionState.CONNECTED, False, False),
        ],
    )
    def test_is_connected_property(
        self,
        client: ReconnectingClient,
        mock_session: Any,
        mocker: MockerFixture,
        session_state: SessionState,
        event_is_set: bool,
        expected: bool,
    ) -> None:
        type(mock_session).state = mocker.PropertyMock(return_value=session_state)
        client._session = mock_session
        client._connected_event = asyncio.Event()
        if event_is_set:
            client._connected_event.set()

        assert client.is_connected is expected

    def test_is_connected_property_when_uninitialized(self, client: ReconnectingClient) -> None:
        assert not client.is_connected

    @pytest.mark.asyncio
    async def test_reconnect_loop_cancels_during_sleep(
        self, mock_underlying_client: Any, mocker: MockerFixture
    ) -> None:
        sleep_started = asyncio.Event()
        config = ClientConfig(retry_delay=10, max_connection_retries=1)
        mock_underlying_client.config = config
        mock_underlying_client.connect.side_effect = ConnectionError("Failed to connect")

        original_sleep = asyncio.sleep

        async def sleep_side_effect(delay: float) -> Any:
            sleep_started.set()
            await original_sleep(delay=delay)

        mocker.patch("asyncio.sleep", side_effect=sleep_side_effect)
        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)

        async with client:
            async with asyncio.timeout(delay=1):
                await sleep_started.wait()

            assert client._reconnect_task is not None
            client._reconnect_task.cancel()
            try:
                await client._reconnect_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_reconnect_loop_cleanup_edge_cases(
        self, mock_underlying_client: Any, mock_session: Any, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG)

        mock_underlying_client.connect.return_value = mock_session

        async def wait_forever(*args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(10)

        mock_session.events.wait_for.side_effect = wait_forever

        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)
        async with client:
            await asyncio.sleep(0.01)
            assert client._reconnect_task
            client._reconnect_task.cancel()
            try:
                await client._reconnect_task
            except asyncio.CancelledError:
                pass

        assert client._session is None

    @pytest.mark.asyncio
    async def test_reconnect_loop_cleanup_error(
        self, mock_underlying_client: Any, mock_session: Any, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG)

        mock_underlying_client.connect.return_value = mock_session

        async def wait_forever(*args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(10)

        mock_session.events.wait_for.side_effect = wait_forever

        mock_session.close.side_effect = RuntimeError("Cleanup failed")

        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)

        async with client:
            await asyncio.sleep(0.01)

            assert client._reconnect_task is not None
            client._reconnect_task.cancel()
            try:
                await client._reconnect_task
            except asyncio.CancelledError:
                pass

        assert "Error closing old session during reconnect: Cleanup failed" in caplog.text

    @pytest.mark.asyncio
    async def test_reconnect_loop_crash_after_connect(
        self, client: ReconnectingClient, mock_underlying_client: Any, mock_session: Any, mocker: MockerFixture
    ) -> None:
        mock_underlying_client.connect.return_value = mock_session

        mock_emit = mocker.patch.object(client, "emit", new_callable=mocker.AsyncMock)
        mock_emit.side_effect = RuntimeError("Crash after connect")

        async with client:
            try:
                assert client._reconnect_task
                await client._reconnect_task
            except RuntimeError:
                pass

        assert isinstance(client._crashed_exception, RuntimeError)

    @pytest.mark.asyncio
    async def test_reconnect_loop_crash_during_session_wait(
        self, client: ReconnectingClient, mock_underlying_client: Any, mock_session: Any
    ) -> None:
        mock_underlying_client.connect.return_value = mock_session

        mock_session.events.wait_for.side_effect = ValueError("Unexpected crash in wait")

        async with client:
            try:
                assert client._reconnect_task
                await client._reconnect_task
            except ValueError:
                pass

        assert isinstance(client._crashed_exception, ValueError)

    @pytest.mark.asyncio
    async def test_reconnect_loop_crashes_and_sets_event(
        self, client: ReconnectingClient, mock_underlying_client: Any, mocker: MockerFixture
    ) -> None:
        mock_underlying_client.connect.side_effect = ValueError("Fatal crash")
        event_set_spy = mocker.spy(asyncio.Event, "set")

        async with client:
            assert client._reconnect_task is not None
            await client._reconnect_task

        assert client._crashed_exception is not None
        assert isinstance(client._crashed_exception, ValueError)
        assert client._connected_event.is_set()
        event_set_spy.assert_called()

    @pytest.mark.asyncio
    async def test_reconnect_loop_delay_capping(
        self, mock_underlying_client: Any, mock_session: Any, mocker: MockerFixture
    ) -> None:
        connection_established = asyncio.Event()
        mock_sleep = mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
        config = ClientConfig(retry_delay=0.1, retry_backoff=2.0, max_retry_delay=0.15)
        mock_underlying_client.config = config
        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)
        mock_emit = mocker.patch.object(client, "emit", new_callable=mocker.AsyncMock)
        mock_emit.side_effect = lambda *args, **kwargs: connection_established.set()

        mock_session.events.wait_for.side_effect = asyncio.CancelledError

        mock_underlying_client.connect.side_effect = [
            ConnectionError(message="Fail 1"),
            ConnectionError(message="Fail 2"),
            mock_session,
        ]

        async with client:
            async with asyncio.timeout(delay=1):
                await connection_established.wait()

        mock_sleep.assert_has_awaits([mocker.call(delay=0.1), mocker.call(delay=0.15)])

    @pytest.mark.asyncio
    async def test_reconnect_loop_full_cycle(
        self, mock_underlying_client: Any, mock_session: Any, caplog: LogCaptureFixture, mocker: MockerFixture
    ) -> None:
        config = ClientConfig(max_connection_retries=5, retry_delay=0.01)
        mock_underlying_client.config = config
        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)

        connect_attempts = 0
        attempt_event = asyncio.Event()

        async def connect_side_effect(*args: Any, **kwargs: Any) -> Any:
            nonlocal connect_attempts
            connect_attempts += 1
            attempt_event.set()
            if connect_attempts == 1:
                return mock_session
            if connect_attempts == 2:
                raise ConnectionError("Simulated network fail")
            if connect_attempts == 3:
                return mock_session
            await asyncio.Future()
            return mock_session

        mock_underlying_client.connect.side_effect = connect_side_effect

        session1_closed = asyncio.Event()

        async def wait_for_side_effect(*args: Any, **kwargs: Any) -> None:
            if connect_attempts == 1:
                return
            await session1_closed.wait()

        mock_session.events.wait_for.side_effect = wait_for_side_effect

        mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

        async with client:
            try:
                async with asyncio.timeout(2.0):
                    while connect_attempts < 3:
                        await attempt_event.wait()
                        attempt_event.clear()
            except asyncio.TimeoutError:
                pytest.fail(f"Timed out waiting for connection attempts. Current: {connect_attempts}")

            assert connect_attempts >= 3
            assert "Connection to https://example.com lost, attempting to reconnect..." in caplog.text
            assert any("Connection attempt 1 failed" in r.message for r in caplog.records)

            session1_closed.set()
            client._closed = True

    @pytest.mark.asyncio
    async def test_reconnect_loop_infinite_retries(self, mock_underlying_client: Any, mocker: MockerFixture) -> None:
        failed_event = asyncio.Event()
        original_sleep = asyncio.sleep
        mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

        config_mock = MagicMock()
        config_mock.max_connection_retries = -1
        config_mock.retry_delay = 0.01
        config_mock.retry_backoff = 1.0
        config_mock.max_retry_delay = 1.0
        config_mock.max_event_queue_size = 100
        config_mock.max_event_listeners = 100
        config_mock.max_event_history_size = 100
        mock_underlying_client.config = config_mock

        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)

        fail_count = 0

        async def connect_side_effect(*args: Any, **kwargs: Any) -> Any:
            nonlocal fail_count
            fail_count += 1
            if fail_count >= 5:
                failed_event.set()
            await original_sleep(0)
            raise TimeoutError("Fail")

        mock_underlying_client.connect.side_effect = connect_side_effect

        async with client:
            async with asyncio.timeout(delay=2.0):
                await failed_event.wait()

            assert client._reconnect_task is not None
            client._reconnect_task.cancel()
            try:
                await client._reconnect_task
            except asyncio.CancelledError:
                pass

        assert mock_underlying_client.connect.call_count >= 5

    @pytest.mark.asyncio
    async def test_reconnect_loop_max_retries_exceeded(
        self, mock_underlying_client: Any, mocker: MockerFixture
    ) -> None:
        failed_event = asyncio.Event()
        mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
        config = ClientConfig(max_connection_retries=2, retry_delay=0.01)
        mock_underlying_client.config = config
        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)
        mock_emit = mocker.patch.object(client, "emit", new_callable=mocker.AsyncMock)

        async def emit_side_effect(*, event_type: EventType, data: Any) -> None:
            if event_type == EventType.CONNECTION_FAILED:
                failed_event.set()

        mock_emit.side_effect = emit_side_effect
        mock_underlying_client.connect.side_effect = ClientError(message="Failed")

        async with client:
            async with asyncio.timeout(delay=1):
                await failed_event.wait()

        assert mock_underlying_client.connect.call_count == 3
        mock_emit.assert_awaited_with(
            event_type=EventType.CONNECTION_FAILED,
            data={"reason": "max_retries_exceeded", "last_error": str(ClientError(message="Failed"))},
        )

    @pytest.mark.asyncio
    async def test_reconnect_loop_respects_closed_flag(self, client: ReconnectingClient) -> None:
        client._closed = True
        await client._reconnect_loop()

    @pytest.mark.asyncio
    async def test_reconnect_loop_retries_multiple_times(
        self, mock_underlying_client: Any, mock_session: Any, mocker: MockerFixture
    ) -> None:
        connection_established = asyncio.Event()
        mock_sleep = mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
        config = ClientConfig(retry_delay=0.01, max_connection_retries=5)
        mock_underlying_client.config = config
        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)
        mock_emit = mocker.patch.object(client, "emit", new_callable=mocker.AsyncMock)
        mock_emit.side_effect = lambda *args, **kwargs: connection_established.set()
        mock_session.events.wait_for.side_effect = asyncio.CancelledError
        errors = [ConnectionError(message="Fail")] * 5
        mock_underlying_client.connect.side_effect = [*errors, mock_session]

        async with client:
            async with asyncio.timeout(delay=1):
                await connection_established.wait()

        assert mock_underlying_client.connect.call_count == 6
        assert mock_sleep.call_count == 5

    @pytest.mark.asyncio
    async def test_reconnect_loop_skips_wait_if_session_closed(
        self, mock_underlying_client: Any, mock_session: Any, mocker: MockerFixture
    ) -> None:
        type(mock_session).state = mocker.PropertyMock(return_value=SessionState.CLOSED)

        mock_underlying_client.connect.side_effect = [mock_session, asyncio.CancelledError]

        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)

        async with client:
            await asyncio.sleep(0.01)

        mock_session.events.wait_for.assert_not_called()

    @pytest.mark.asyncio
    async def test_reconnect_loop_success_and_disconnect(
        self, mock_underlying_client: Any, mock_session: Any, caplog: LogCaptureFixture, mocker: MockerFixture
    ) -> None:
        first_connection_made = asyncio.Event()
        connection_lost = asyncio.Event()
        closed_future: asyncio.Future[None] = asyncio.Future()

        async def wait_for_side_effect(*args: Any, **kwargs: Any) -> None:
            await closed_future

        mock_session.events.wait_for.side_effect = wait_for_side_effect

        mock_underlying_client.connect.side_effect = [mock_session, ConnectionError(message="Reconnect failed")]
        config = ClientConfig(max_connection_retries=0, retry_delay=0.01)
        mock_underlying_client.config = config
        client = ReconnectingClient(url=self.URL, client=mock_underlying_client)
        mock_emit = mocker.patch.object(client, "emit", new_callable=mocker.AsyncMock)

        async def emit_side_effect(*, event_type: EventType, data: Any) -> None:
            if event_type == EventType.CONNECTION_ESTABLISHED:
                first_connection_made.set()
            elif event_type == EventType.CONNECTION_LOST:
                connection_lost.set()

        mock_emit.side_effect = emit_side_effect

        async with client:
            async with asyncio.timeout(delay=1):
                await first_connection_made.wait()
            mock_session.events.wait_for.assert_called()
            closed_future.set_result(None)
            async with asyncio.timeout(delay=1):
                await connection_lost.wait()

        mock_emit.assert_any_call(
            event_type=EventType.CONNECTION_ESTABLISHED, data={"session": mock_session, "attempt": 1}
        )
        mock_emit.assert_any_call(event_type=EventType.CONNECTION_LOST, data={"url": self.URL})
        assert f"Connection to {self.URL} lost, attempting to reconnect..." in caplog.text
