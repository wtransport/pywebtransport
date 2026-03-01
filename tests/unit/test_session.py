"""Unit tests for the pywebtransport.session module."""

import asyncio
import dataclasses
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport import (
    ClientConfig,
    ConnectionError,
    SessionError,
    StreamError,
    TimeoutError,
    WebTransportReceiveStream,
    WebTransportSendStream,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport._driver.driver import EndpointDriver
from pywebtransport._protocol.events import (
    UserCloseSession,
    UserCreateStream,
    UserGetSessionDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserSendDatagram,
)
from pywebtransport.connection import WebTransportConnection
from pywebtransport.session import SessionDiagnostics
from pywebtransport.types import SessionState


class TestSessionDiagnostics:

    def test_init(self) -> None:
        diag = SessionDiagnostics(
            session_id=1,
            state=SessionState.CONNECTED,
            path="/",
            headers={"Host": "example.com"},
            created_at=100.0,
            local_max_data=1000,
            local_data_sent=50,
            local_data_consumed=40,
            peer_max_data=2000,
            peer_data_sent=100,
            local_max_streams_bidi=10,
            local_streams_bidi_opened=1,
            peer_max_streams_bidi=10,
            peer_streams_bidi_opened=2,
            peer_streams_bidi_closed=1,
            local_max_streams_uni=5,
            local_streams_uni_opened=0,
            peer_max_streams_uni=5,
            peer_streams_uni_opened=0,
            peer_streams_uni_closed=0,
            pending_bidi_stream_requests=[],
            pending_uni_stream_requests=[],
            datagrams_sent=5,
            datagram_bytes_sent=500,
            datagrams_received=3,
            datagram_bytes_received=300,
            active_streams=[],
            blocked_streams=[],
            close_code=None,
            close_reason=None,
            closed_at=None,
            ready_at=101.0,
        )

        assert diag.session_id == 1
        assert diag.state == SessionState.CONNECTED
        assert diag.local_data_consumed == 40
        assert diag.peer_streams_bidi_closed == 1

        with pytest.raises(expected_exception=dataclasses.FrozenInstanceError):
            cast(Any, diag).state = SessionState.CLOSED

        assert not hasattr(diag, "__dict__")


class TestWebTransportSession:

    @pytest.fixture
    def mock_config(self, mocker: MockerFixture) -> MagicMock:
        conf = mocker.Mock(spec=ClientConfig)
        conf.max_event_queue_size = 100
        conf.max_event_listeners = 100
        conf.max_event_history_size = 100
        conf.stream_creation_timeout = 0.1

        return cast(MagicMock, conf)

    @pytest.fixture
    def mock_connection(self, mock_driver: MagicMock, mock_config: MagicMock, mocker: MockerFixture) -> MagicMock:
        conn = mocker.Mock(spec=WebTransportConnection)
        conn.config = mock_config
        conn._driver = mock_driver
        conn._handle = 42
        conn.remote_address = ("127.0.0.1", 443)
        conn._stream_handles = {}

        return cast(MagicMock, conn)

    @pytest.fixture
    def mock_driver(self, mocker: MockerFixture) -> MagicMock:
        driver = mocker.Mock(spec=EndpointDriver)
        driver.create_request.side_effect = lambda: (1, asyncio.Future())

        return cast(MagicMock, driver)

    @pytest.fixture
    def session(self, mock_connection: MagicMock) -> WebTransportSession:
        return WebTransportSession(
            connection=mock_connection, session_id=1, path="/chat", headers={"User-Agent": "TestClient"}
        )

    @pytest.mark.asyncio
    async def test_close_already_closed(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        session._cached_state = SessionState.CLOSED

        await session.close()

        mock_driver.create_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_connection_gone(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        await session.close()

        mock_driver.create_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_error_logging(
        self, session: WebTransportSession, mock_driver: MagicMock, mocker: MockerFixture
    ) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        fut.set_exception(ConnectionError("Gone"))
        spy_logger = mocker.patch(target="pywebtransport.session._logger")

        await session.close()

        spy_logger.warning.assert_called_with("Error initiating session close for %s: %s", 1, mocker.ANY, exc_info=True)

    @pytest.mark.asyncio
    async def test_close_success(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        fut.set_result(None)

        await session.close(error_code=100, reason="Done")

        mock_driver.create_request.assert_called_once()
        mock_driver.send_user_event.assert_called_once()
        kwargs = mock_driver.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserCloseSession)
        assert event.session_id == 1
        assert event.error_code == 100
        assert event.reason == "Done"

    @pytest.mark.asyncio
    async def test_context_manager(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportSession, attribute="close", new_callable=mocker.AsyncMock)

        async with session as s:
            assert s is session

        spy_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_stream_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="Connection is gone"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_generic_error(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        fut.set_exception(ValueError("Fail"))

        with pytest.raises(expected_exception=ValueError, match="Fail"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_handle_missing(
        self, session: WebTransportSession, mock_driver: MagicMock, mock_connection: MagicMock
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        mock_connection._stream_handles = {}
        fut.set_result(103)

        with pytest.raises(expected_exception=StreamError, match="Internal error creating stream handle"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_invalid_handle_type(
        self, session: WebTransportSession, mock_driver: MagicMock, mock_connection: MagicMock, mocker: MockerFixture
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        mock_recv = mocker.Mock(spec=WebTransportReceiveStream)
        mock_connection._stream_handles = {104: mock_recv}
        fut.set_result(104)

        with pytest.raises(expected_exception=StreamError, match="Invalid stream handle type"):
            await session.create_bidirectional_stream()

    @pytest.mark.parametrize(
        argnames="method, wrong_type, error_msg",
        argvalues=[
            ("create_bidirectional_stream", WebTransportSendStream, "Expected bidirectional stream"),
            ("create_unidirectional_stream", WebTransportStream, "Expected unidirectional send stream"),
        ],
    )
    @pytest.mark.asyncio
    async def test_create_stream_mismatch_type(
        self,
        session: WebTransportSession,
        mock_driver: MagicMock,
        mock_connection: MagicMock,
        mocker: MockerFixture,
        method: str,
        wrong_type: type,
        error_msg: str,
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        mock_wrong = mocker.Mock(spec=wrong_type)
        mock_connection._stream_handles = {105: mock_wrong}
        fut.set_result(105)
        create_method = getattr(session, method)

        with pytest.raises(expected_exception=StreamError, match=error_msg):
            await create_method()

    @pytest.mark.parametrize(
        argnames="method, stream_type, is_uni, req_id",
        argvalues=[
            ("create_bidirectional_stream", WebTransportStream, False, 101),
            ("create_unidirectional_stream", WebTransportSendStream, True, 102),
        ],
    )
    @pytest.mark.asyncio
    async def test_create_stream_success(
        self,
        session: WebTransportSession,
        mock_driver: MagicMock,
        mock_connection: MagicMock,
        mocker: MockerFixture,
        method: str,
        stream_type: type,
        is_uni: bool,
        req_id: int,
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        mock_stream = mocker.Mock(spec=stream_type)
        mock_connection._stream_handles = {req_id: mock_stream}
        fut.set_result(req_id)
        create_method = getattr(session, method)

        stream = await create_method()

        assert stream is mock_stream
        kwargs = mock_driver.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserCreateStream)
        assert event.is_unidirectional is is_uni

    @pytest.mark.asyncio
    async def test_create_stream_timeout(
        self, session: WebTransportSession, mock_driver: MagicMock, mocker: MockerFixture
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        mocker.patch(target="asyncio.timeout", side_effect=asyncio.TimeoutError)
        spy_logger = mocker.patch(target="pywebtransport.session._logger")

        with pytest.raises(expected_exception=TimeoutError, match="timed out creating stream"):
            await session.create_bidirectional_stream()

        spy_logger.warning.assert_called_with("Timeout creating stream on session %s", 1)

    @pytest.mark.asyncio
    async def test_diagnostics_connection_error(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        fut: asyncio.Future[dict[str, Any]] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        fut.set_exception(ConnectionError("Closed"))

        with pytest.raises(expected_exception=SessionError, match="Connection is closed"):
            await session.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="Connection is gone"):
            await session.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_success(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        fut: asyncio.Future[dict[str, Any]] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        data = {
            "session_id": 1,
            "state": SessionState.CONNECTED,
            "path": "/",
            "headers": {},
            "created_at": 0.0,
            "local_max_data": 0,
            "local_data_sent": 0,
            "local_data_consumed": 0,
            "peer_max_data": 0,
            "peer_data_sent": 0,
            "local_max_streams_bidi": 0,
            "local_streams_bidi_opened": 0,
            "peer_max_streams_bidi": 0,
            "peer_streams_bidi_opened": 0,
            "peer_streams_bidi_closed": 0,
            "local_max_streams_uni": 0,
            "local_streams_uni_opened": 0,
            "peer_max_streams_uni": 0,
            "peer_streams_uni_opened": 0,
            "peer_streams_uni_closed": 0,
            "pending_bidi_stream_requests": [],
            "pending_uni_stream_requests": [],
            "datagrams_sent": 0,
            "datagram_bytes_sent": 0,
            "datagrams_received": 0,
            "datagram_bytes_received": 0,
            "active_streams": [],
            "blocked_streams": [],
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
            "ready_at": None,
        }
        fut.set_result(data)

        diag = await session.diagnostics()

        assert isinstance(diag, SessionDiagnostics)
        assert diag.session_id == 1
        kwargs = mock_driver.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserGetSessionDiagnostics)

    @pytest.mark.asyncio
    async def test_grant_data_credit(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        fut.set_result(None)

        await session.grant_data_credit(max_data=1000)

        kwargs = mock_driver.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserGrantDataCredit)
        assert event.max_data == 1000

    @pytest.mark.asyncio
    async def test_grant_streams_credit(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        fut.set_result(None)

        await session.grant_streams_credit(is_unidirectional=True, max_streams=5)

        kwargs = mock_driver.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserGrantStreamsCredit)
        assert event.max_streams == 5
        assert event.is_unidirectional is True

    def test_headers_copy(self, session: WebTransportSession) -> None:
        h = cast(dict[str, str], session.headers)
        h["New"] = "Value"
        internal_headers = cast(dict[str, str], session._headers)

        assert "New" not in internal_headers

    def test_init(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        assert session._connection() is mock_connection
        assert session._session_id == 1
        assert session._path == "/chat"
        assert session._headers == {"User-Agent": "TestClient"}
        assert session._cached_state == SessionState.CONNECTING
        assert session.events is not None

        assert not hasattr(session, "__dict__")

    @pytest.mark.asyncio
    async def test_methods_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError):
            await session.grant_data_credit(max_data=1)

        with pytest.raises(expected_exception=ConnectionError):
            await session.grant_streams_credit(is_unidirectional=True, max_streams=1)

        with pytest.raises(expected_exception=ConnectionError):
            await session.send_datagram(data=b"")

    def test_on_session_closed(self, session: WebTransportSession) -> None:
        session._on_session_closed(event=MagicMock())

        assert session.state == SessionState.CLOSED

    def test_on_session_ready(self, session: WebTransportSession) -> None:
        session._on_session_ready(event=MagicMock())

        assert session.state == SessionState.CONNECTED

    def test_properties(self, session: WebTransportSession) -> None:
        assert session.path == "/chat"
        assert session.is_closed is False
        assert session.session_id == 1
        assert session.state == SessionState.CONNECTING

    def test_remote_address(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        assert session.remote_address == ("127.0.0.1", 443)

    def test_remote_address_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        assert session.remote_address is None

    def test_remote_address_none(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection.remote_address = None

        assert session.remote_address is None

    def test_repr(self, session: WebTransportSession) -> None:
        assert "id=1" in repr(session)
        assert "state=" in repr(session)

    @pytest.mark.asyncio
    async def test_send_datagram(self, session: WebTransportSession, mock_driver: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_driver.create_request.side_effect = None
        mock_driver.create_request.return_value = (1, fut)
        fut.set_result(None)

        await session.send_datagram(data=b"test")

        kwargs = mock_driver.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserSendDatagram)
        assert event.data == b"test"
