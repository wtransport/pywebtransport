"""Unit tests for the pywebtransport.session module."""

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pywebtransport import (
    ClientConfig,
    ConnectionError,
    ErrorCodes,
    Event,
    SessionClosedError,
    SessionError,
    StreamError,
    TimeoutError,
    WebTransportReceiveStream,
    WebTransportSendStream,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport._controller.controller import EndpointController
from pywebtransport._protocol.events import (
    UserAcceptSession,
    UserCloseSession,
    UserCreateStream,
    UserExportKeyingMaterial,
    UserGetSessionDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserRejectSession,
    UserSendDatagram,
)
from pywebtransport.connection import WebTransportConnection
from pywebtransport.session import SessionDiagnostics
from pywebtransport.types import EventType, SessionState


class TestSessionDiagnostics:

    def test_init(self) -> None:
        diag = SessionDiagnostics(
            active_streams=[],
            blocked_streams=[],
            close_code=None,
            close_reason=None,
            closed_at=None,
            created_at=100.0,
            datagram_bytes_received=300,
            datagram_bytes_sent=500,
            datagrams_received=3,
            datagrams_sent=5,
            flow_control_negotiated=True,
            headers={"Host": "example.com"},
            is_client=True,
            local_data_consumed=40,
            local_data_received=80,
            local_data_sent=50,
            local_max_data=1000,
            local_max_streams_bidi=10,
            local_max_streams_uni=5,
            local_streams_bidi_opened=1,
            local_streams_uni_opened=0,
            path="/",
            peer_max_data=2000,
            peer_max_streams_bidi=10,
            peer_max_streams_uni=5,
            peer_streams_bidi_closed=1,
            peer_streams_bidi_opened=2,
            peer_streams_uni_closed=0,
            peer_streams_uni_opened=0,
            pending_bidi_stream_requests=[],
            pending_uni_stream_requests=[],
            ready_at=101.0,
            session_id=1,
            state=SessionState.CONNECTED,
            wt_protocol="p1",
            wt_available_protocols=["p1"],
        )

        assert diag.session_id == 1
        assert diag.state == SessionState.CONNECTED
        assert diag.local_data_consumed == 40
        assert diag.local_data_received == 80
        assert diag.peer_streams_bidi_closed == 1
        assert diag.wt_available_protocols == ["p1"]
        assert diag.wt_protocol == "p1"
        assert diag.is_client is True
        assert diag.flow_control_negotiated is True

        with pytest.raises(expected_exception=dataclasses.FrozenInstanceError):
            cast(Any, diag).state = SessionState.CLOSED

        assert not hasattr(diag, "__dict__")


class TestWebTransportSession:

    @pytest.fixture
    def mock_config(self, mocker: MockerFixture) -> MagicMock:
        conf = mocker.Mock(spec=ClientConfig)
        conf.event_queue_capacity = 100
        conf.max_event_listeners = 100
        conf.event_history_capacity = 100
        conf.stream_creation_timeout = 0.1

        return cast(MagicMock, conf)

    @pytest.fixture
    def mock_connection(self, mock_controller: MagicMock, mock_config: MagicMock, mocker: MockerFixture) -> MagicMock:
        conn = mocker.Mock(spec=WebTransportConnection)
        conn.config = mock_config
        conn._controller = mock_controller
        conn.handle = 42
        conn.remote_address = ("127.0.0.1", 443)
        conn._stream_handles = {}

        return cast(MagicMock, conn)

    @pytest.fixture
    def mock_controller(self, mocker: MockerFixture) -> MagicMock:
        ctrl = mocker.Mock(spec=EndpointController)
        ctrl._pending_manager = mocker.Mock()
        ctrl._pending_manager.create_request.side_effect = lambda: (1, asyncio.Future())

        return cast(MagicMock, ctrl)

    @pytest.fixture
    def session(self, mock_connection: MagicMock) -> WebTransportSession:
        return WebTransportSession(
            connection=mock_connection, session_id=1, path="/chat", headers={"User-Agent": "TestClient"}
        )

    @pytest.mark.asyncio
    async def test_accept_bidirectional_stream_closed(self, session: WebTransportSession) -> None:
        session._incoming_bidi_streams.put_nowait(None)

        with pytest.raises(expected_exception=SessionClosedError):
            await session.accept_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_accept_bidirectional_stream_success(self, session: WebTransportSession) -> None:
        stream = MagicMock(spec=WebTransportStream)
        session._incoming_bidi_streams.put_nowait(stream)

        result = await session.accept_bidirectional_stream()

        assert result is stream

    @pytest.mark.asyncio
    async def test_accept_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.accept()

    @pytest.mark.asyncio
    async def test_accept_success(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_result(None)

        session.wt_protocol = "h3"
        await session.accept()

        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserAcceptSession)
        assert event.session_id == 1
        assert event.wt_protocol == "h3"
        assert session.state == SessionState.CONNECTED

    @pytest.mark.asyncio
    async def test_accept_unidirectional_stream_closed(self, session: WebTransportSession) -> None:
        session._incoming_uni_streams.put_nowait(None)

        with pytest.raises(expected_exception=SessionClosedError):
            await session.accept_unidirectional_stream()

    @pytest.mark.asyncio
    async def test_accept_unidirectional_stream_success(self, session: WebTransportSession) -> None:
        stream = MagicMock(spec=WebTransportReceiveStream)
        session._incoming_uni_streams.put_nowait(stream)

        result = await session.accept_unidirectional_stream()

        assert result is stream

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="test_error_code, test_reason, expected_code, expected_reason",
        argvalues=[
            (100, "Done", 100, "Done"),
            (None, None, ErrorCodes.APP_NO_ERROR, None),
        ],
    )
    async def test_close(
        self,
        session: WebTransportSession,
        mock_controller: MagicMock,
        test_error_code: int | None,
        test_reason: str | None,
        expected_code: int,
        expected_reason: str | None,
    ) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_result(None)

        if test_error_code is not None:
            await session.close(error_code=test_error_code, reason=test_reason)
        else:
            await session.close()

        mock_controller._pending_manager.create_request.assert_called_once()
        mock_controller.send_user_event.assert_called_once()
        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserCloseSession)
        assert event.session_id == 1
        assert event.error_code == expected_code
        assert event.reason == expected_reason

    @pytest.mark.asyncio
    async def test_close_already_closed(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        session._cached_state = SessionState.CLOSED

        await session.close()

        mock_controller._pending_manager.create_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_connection_gone(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        await session.close()

        mock_controller._pending_manager.create_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_error_logging(
        self, session: WebTransportSession, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_exception(ConnectionError(message="Gone"))
        spy_logger = mocker.patch(target="pywebtransport.session._logger")

        await session.close()

        spy_logger.warning.assert_called_with(
            "wt_session close failed session_id=%d err=%s", 1, mocker.ANY, exc_info=True
        )

    @pytest.mark.asyncio
    async def test_context_manager(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportSession, attribute="close", new_callable=mocker.AsyncMock)

        async with session as s:
            assert s is session

        spy_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_stream_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_generic_error(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_exception(ValueError("Fail"))

        with pytest.raises(expected_exception=ValueError, match="Fail"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_handle_missing(
        self, session: WebTransportSession, mock_controller: MagicMock, mock_connection: MagicMock
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        mock_connection._stream_handles = {}
        fut.set_result(103)

        with pytest.raises(expected_exception=StreamError, match="wt_stream resolve failed stream_id=103"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_invalid_handle_type(
        self,
        session: WebTransportSession,
        mock_controller: MagicMock,
        mock_connection: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        mock_recv = mocker.Mock(spec=WebTransportReceiveStream)
        mock_connection._stream_handles = {104: mock_recv}
        fut.set_result(104)

        with pytest.raises(expected_exception=StreamError, match=r"wt_stream validate invalid actual=.* stream_id=104"):
            await session.create_bidirectional_stream()

    @pytest.mark.parametrize(
        argnames="method, wrong_type, error_msg",
        argvalues=[
            (
                "create_bidirectional_stream",
                WebTransportSendStream,
                r"wt_stream validate invalid actual=.* expected=bidirectional",
            ),
            (
                "create_unidirectional_stream",
                WebTransportStream,
                r"wt_stream validate invalid actual=.* expected=send_only",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_create_stream_mismatch_type(
        self,
        session: WebTransportSession,
        mock_controller: MagicMock,
        mock_connection: MagicMock,
        mocker: MockerFixture,
        method: str,
        wrong_type: type,
        error_msg: str,
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
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
        mock_controller: MagicMock,
        mock_connection: MagicMock,
        mocker: MockerFixture,
        method: str,
        stream_type: type,
        is_uni: bool,
        req_id: int,
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        mock_stream = mocker.Mock(spec=stream_type)
        mock_connection._stream_handles = {req_id: mock_stream}
        fut.set_result(req_id)
        create_method = getattr(session, method)

        stream = await create_method()

        assert stream is mock_stream
        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserCreateStream)
        assert event.is_unidirectional is is_uni

    @pytest.mark.asyncio
    async def test_create_stream_timeout(
        self, session: WebTransportSession, mock_controller: MagicMock, mocker: MockerFixture
    ) -> None:
        fut: asyncio.Future[int] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        mocker.patch(target="asyncio.timeout", side_effect=asyncio.TimeoutError)

        with pytest.raises(
            expected_exception=TimeoutError,
            match="wt_stream create failed session_id=1",
        ):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_diagnostics_connection_error(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        fut: asyncio.Future[dict[str, Any]] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_exception(ConnectionError(message="Closed"))

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_success(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        fut: asyncio.Future[dict[str, Any]] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        data: dict[str, Any] = {
            "active_streams": [],
            "blocked_streams": [],
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
            "created_at": 0.0,
            "datagram_bytes_received": 0,
            "datagram_bytes_sent": 0,
            "datagrams_received": 0,
            "datagrams_sent": 0,
            "flow_control_negotiated": True,
            "headers": {},
            "is_client": False,
            "local_data_consumed": 0,
            "local_data_received": 0,
            "local_data_sent": 0,
            "local_max_data": 0,
            "local_max_streams_bidi": 0,
            "local_max_streams_uni": 0,
            "local_streams_bidi_opened": 0,
            "local_streams_uni_opened": 0,
            "path": "/",
            "peer_max_data": 0,
            "peer_max_streams_bidi": 0,
            "peer_max_streams_uni": 0,
            "peer_streams_bidi_closed": 0,
            "peer_streams_bidi_opened": 0,
            "peer_streams_uni_closed": 0,
            "peer_streams_uni_opened": 0,
            "pending_bidi_stream_requests": [],
            "pending_uni_stream_requests": [],
            "ready_at": None,
            "session_id": 1,
            "state": SessionState.CONNECTED,
            "wt_protocol": "p1",
        }
        fut.set_result(data)
        session._wt_available_protocols = ["p1"]

        diag = await session.diagnostics()

        assert isinstance(diag, SessionDiagnostics)
        assert diag.session_id == 1
        assert diag.wt_available_protocols == ["p1"]
        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserGetSessionDiagnostics)

    def test_enqueue_stream_bidi(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream = mocker.Mock(spec=WebTransportStream)
        event = Event(type=EventType.STREAM_OPENED, data={"stream": stream})

        session._enqueue_stream(event=event)

        assert session._incoming_bidi_streams.qsize() == 1
        assert session._incoming_bidi_streams.get_nowait() is stream

    def test_enqueue_stream_invalid_data_type(self, session: WebTransportSession) -> None:
        event = Event(type=EventType.STREAM_OPENED, data="not_a_dict")

        session._enqueue_stream(event=event)

        assert session._incoming_bidi_streams.empty()

    def test_enqueue_stream_missing_stream(self, session: WebTransportSession) -> None:
        event = Event(type=EventType.STREAM_OPENED, data={"no_stream_key": True})

        session._enqueue_stream(event=event)

        assert session._incoming_bidi_streams.empty()

    def test_enqueue_stream_uni(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream = mocker.Mock(spec=WebTransportReceiveStream)
        event = Event(type=EventType.STREAM_OPENED, data={"stream": stream})

        session._enqueue_stream(event=event)

        assert session._incoming_uni_streams.qsize() == 1
        assert session._incoming_uni_streams.get_nowait() is stream

    def test_enqueue_stream_unsupported_type(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream = mocker.Mock(spec=WebTransportSendStream)
        event = Event(type=EventType.STREAM_OPENED, data={"stream": stream})

        session._enqueue_stream(event=event)

        assert session._incoming_bidi_streams.empty()
        assert session._incoming_uni_streams.empty()
        assert True

    @pytest.mark.asyncio
    async def test_export_keying_material_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError):
            await session.export_keying_material(label="test", context=b"", length=32)

    @pytest.mark.asyncio
    async def test_export_keying_material_success(
        self, session: WebTransportSession, mock_controller: MagicMock
    ) -> None:
        fut: asyncio.Future[bytes] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_result(b"key_material_bytes")

        result = await session.export_keying_material(label="exporter", context=b"ctx", length=18)

        assert result == b"key_material_bytes"
        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserExportKeyingMaterial)
        assert event.session_id == 1
        assert event.label == "exporter"
        assert event.context == b"ctx"
        assert event.length == 18

    @pytest.mark.asyncio
    async def test_grant_data_credit(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_result(None)

        await session.grant_data_credit(max_data=1000)

        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserGrantDataCredit)
        assert event.max_data == 1000

    @pytest.mark.asyncio
    async def test_grant_streams_credit(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_result(None)

        await session.grant_streams_credit(is_unidirectional=True, max_streams=5)

        kwargs = mock_controller.send_user_event.call_args[1]

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

    @pytest.mark.asyncio
    async def test_incoming_bidirectional_streams(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream1 = mocker.Mock(spec=WebTransportStream)
        stream2 = mocker.Mock(spec=WebTransportStream)
        session._incoming_bidi_streams.put_nowait(stream1)
        session._incoming_bidi_streams.put_nowait(stream2)
        session._incoming_bidi_streams.put_nowait(None)

        generator = session.incoming_bidirectional_streams()
        results: list[WebTransportStream] = []
        async for s in generator:
            results.append(s)

        assert results == [stream1, stream2]
        assert isinstance(generator, AsyncIterator)

    @pytest.mark.asyncio
    async def test_incoming_unidirectional_streams(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream1 = mocker.Mock(spec=WebTransportReceiveStream)
        stream2 = mocker.Mock(spec=WebTransportReceiveStream)
        session._incoming_uni_streams.put_nowait(stream1)
        session._incoming_uni_streams.put_nowait(stream2)
        session._incoming_uni_streams.put_nowait(None)

        generator = session.incoming_unidirectional_streams()
        results: list[WebTransportReceiveStream] = []
        async for s in generator:
            results.append(s)

        assert results == [stream1, stream2]
        assert isinstance(generator, AsyncIterator)

    def test_init(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        assert session._connection() is mock_connection
        assert session._session_id == 1
        assert session._path == "/chat"
        assert session._headers == {"User-Agent": "TestClient"}
        assert session._cached_state == SessionState.CONNECTING
        assert session._wt_available_protocols is None
        assert session._wt_protocol is None
        assert session.events is not None
        assert isinstance(session._incoming_bidi_streams, asyncio.Queue)
        assert isinstance(session._incoming_uni_streams, asyncio.Queue)

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

    def test_on_session_closed_poisons_queues(self, session: WebTransportSession) -> None:
        session._on_session_closed(event=MagicMock())

        assert session.state == SessionState.CLOSED
        assert session._incoming_bidi_streams.qsize() == 1
        assert session._incoming_bidi_streams.get_nowait() is None
        assert session._incoming_uni_streams.qsize() == 1
        assert session._incoming_uni_streams.get_nowait() is None

    def test_on_session_ready_basic(self, session: WebTransportSession) -> None:
        session._on_session_ready(event=Event(type=EventType.SESSION_READY, data={}))

        assert session.state == SessionState.CONNECTED
        assert session.wt_protocol is None

    def test_on_session_ready_invalid_data_type(self, session: WebTransportSession) -> None:
        session._on_session_ready(event=Event(type=EventType.SESSION_READY, data="not_a_dict"))

        assert session.state == SessionState.CONNECTED
        assert session.wt_protocol is None

    def test_on_session_ready_with_wt_protocol(self, session: WebTransportSession) -> None:
        session._on_session_ready(event=Event(type=EventType.SESSION_READY, data={"wt_protocol": "custom-h3"}))

        assert session.state == SessionState.CONNECTED
        assert session.wt_protocol == "custom-h3"

    def test_properties(self, session: WebTransportSession) -> None:
        assert session.path == "/chat"
        assert session.is_closed is False
        assert session.session_id == 1
        assert session.state == SessionState.CONNECTING
        assert session.wt_protocol is None
        assert session.wt_available_protocols is None

        session.wt_protocol = "test-proto"

        assert session.wt_protocol == "test-proto"

    @pytest.mark.asyncio
    async def test_reject_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError):
            await session.reject()

    @pytest.mark.asyncio
    async def test_reject_success(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_result(None)

        await session.reject(status_code=404)

        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserRejectSession)
        assert event.session_id == 1
        assert event.status_code == 404
        assert session.state == SessionState.CLOSED
        assert session._incoming_bidi_streams.qsize() == 1
        assert session._incoming_bidi_streams.get_nowait() is None
        assert session._incoming_uni_streams.qsize() == 1
        assert session._incoming_uni_streams.get_nowait() is None

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
    async def test_send_datagram(self, session: WebTransportSession, mock_controller: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_controller._pending_manager.create_request.side_effect = None
        mock_controller._pending_manager.create_request.return_value = (1, fut)
        fut.set_result(None)

        await session.send_datagram(data=b"test")

        kwargs = mock_controller.send_user_event.call_args[1]

        assert kwargs["handle"] == 42
        event = kwargs["event"]
        assert isinstance(event, UserSendDatagram)
        assert event.data == b"test"
