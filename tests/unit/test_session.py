"""Unit tests for the pywebtransport.session module."""

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from pytest_mock import MockerFixture

from pywebtransport import (
    ClientConfig,
    ConnectionError,
    DatagramError,
    ErrorCodes,
    SessionError,
    StreamError,
    TimeoutError,
    WebTransportBidirectionalStream,
    WebTransportConnection,
    WebTransportReceiveStream,
    WebTransportSendStream,
    WebTransportSession,
)
from pywebtransport._controller.events import (
    UserAcceptSession,
    UserCloseSession,
    UserCreateStream,
    UserDiagnoseSession,
    UserExportKeyingMaterial,
    UserReadDatagram,
    UserRejectSession,
    UserSendDatagram,
)
from pywebtransport.session import SessionDiagnostics
from pywebtransport.types import SessionState


class TestSessionDiagnostics:

    def test_init(self) -> None:
        diagnostics = SessionDiagnostics(
            close_code=None,
            close_reason=None,
            closed_at=None,
            created_at=100.0,
            data_consumed=40,
            data_received=80,
            data_sent=50,
            flow_control_negotiated=True,
            headers={"Host": "example.com"},
            is_client=True,
            local_max_data=4 * 1024 * 1024,
            local_max_streams_bidi=10,
            local_max_streams_uni=10,
            local_streams_bidi_opened=1,
            local_streams_uni_opened=0,
            path="/",
            peer_max_data=4 * 1024 * 1024,
            peer_max_streams_bidi=10,
            peer_max_streams_uni=10,
            peer_streams_bidi_closed=1,
            peer_streams_bidi_opened=2,
            peer_streams_uni_closed=0,
            peer_streams_uni_opened=0,
            ready_at=101.0,
            session_id=1,
            state=SessionState.CONNECTED,
            wt_available_protocols=["p1", "p2"],
            wt_protocol="p1",
        )

        assert diagnostics.flow_control_negotiated is True
        assert diagnostics.is_client is True
        assert diagnostics.data_consumed == 40
        assert diagnostics.data_received == 80
        assert diagnostics.peer_streams_bidi_closed == 1
        assert diagnostics.session_id == 1
        assert diagnostics.state == SessionState.CONNECTED
        assert diagnostics.wt_available_protocols == ["p1", "p2"]
        assert diagnostics.wt_protocol == "p1"

        with pytest.raises(expected_exception=dataclasses.FrozenInstanceError):
            cast(Any, diagnostics).state = SessionState.CLOSED

        assert not hasattr(diagnostics, "__dict__")


class TestWebTransportSession:

    @pytest.fixture
    def mock_config(self, mocker: MockerFixture) -> MagicMock:
        config = mocker.Mock(spec=ClientConfig)
        config.stream_create_timeout = 0.1

        return cast(MagicMock, config)

    @pytest.fixture
    def mock_connection(self, mock_config: MagicMock, mocker: MockerFixture) -> MagicMock:
        connection = mocker.Mock(spec=WebTransportConnection)
        connection.config = mock_config
        connection.handle = 42
        connection.remote_address = ("127.0.0.1", 443)
        connection._execute_request = mocker.AsyncMock()

        return cast(MagicMock, connection)

    @pytest_asyncio.fixture
    async def session(self, mock_connection: MagicMock) -> WebTransportSession:
        return WebTransportSession(
            connection=mock_connection, session_id=1, path="/chat", headers={"User-Agent": "TestClient"}
        )

    @pytest.mark.asyncio
    async def test_accept_cancelled(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await session.accept()

    @pytest.mark.asyncio
    async def test_accept_closed(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        session._handle_closed(error_code=None)

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.accept()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_accept_connection_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await session.accept()

    @pytest.mark.asyncio
    async def test_accept_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.accept()

    @pytest.mark.asyncio
    async def test_accept_generic_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=SessionError, match="wt_session open failed session_id=1"):
            await session.accept()

    @pytest.mark.asyncio
    async def test_accept_session_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        error = SessionError(message="wt_protocol validate invalid", session_id=1)
        mock_connection._execute_request.side_effect = error

        with pytest.raises(expected_exception=SessionError) as exception_info:
            await session.accept(wt_protocol="h2")

        assert exception_info.value is error
        assert session.is_closed is False
        assert session.wt_protocol is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="kwargs, expected_headers, expected_wt_protocol",
        argvalues=[
            ({"headers": {b"server": b"pywebtransport"}, "wt_protocol": "h3"}, {b"server": b"pywebtransport"}, "h3"),
            ({}, {}, None),
        ],
    )
    async def test_accept_success(
        self,
        session: WebTransportSession,
        mock_connection: MagicMock,
        kwargs: dict[str, Any],
        expected_headers: dict[bytes, bytes],
        expected_wt_protocol: str | None,
    ) -> None:
        mock_connection._execute_request.return_value = None

        await session.accept(**kwargs)

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)

        assert isinstance(event, UserAcceptSession)
        assert event.session_id == 1
        assert event.headers == expected_headers
        assert event.wt_protocol == expected_wt_protocol
        assert session.is_closed is False
        assert session._ready.done() is False

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
        mock_connection: MagicMock,
        test_error_code: int | None,
        test_reason: str | None,
        expected_code: int,
        expected_reason: str | None,
    ) -> None:
        mock_connection._execute_request.return_value = None

        if test_error_code is not None:
            await session.close(error_code=test_error_code, reason=test_reason)
        else:
            await session.close()

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)

        assert isinstance(event, UserCloseSession)
        assert event.session_id == 1
        assert event.error_code == expected_code
        assert event.reason == expected_reason

    @pytest.mark.asyncio
    async def test_close_already_closed(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        session._handle_closed(error_code=None)

        await session.close()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_cancelled(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await session.close()

    @pytest.mark.asyncio
    async def test_close_connection_error_channel_closed(
        self, session: WebTransportSession, mock_connection: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="channel closed")
        spy_logger = mocker.patch(target="pywebtransport.session._logger")

        await session.close()

        spy_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_connection_error_warning(
        self, session: WebTransportSession, mock_connection: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")
        spy_logger = mocker.patch(target="pywebtransport.session._logger")

        await session.close()

        spy_logger.warning.assert_called_with("wt_session close failed session_id=%d err=%s", 1, mocker.ANY)

    @pytest.mark.asyncio
    async def test_close_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        await session.close()

    @pytest.mark.asyncio
    async def test_close_generic_error(
        self, session: WebTransportSession, mock_connection: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")
        spy_logger = mocker.patch(target="pywebtransport.session._logger")

        await session.close()

        spy_logger.warning.assert_called_with("wt_session close failed session_id=%d err=%s", 1, mocker.ANY)

    @pytest.mark.asyncio
    async def test_close_session_error_warning(
        self, session: WebTransportSession, mock_connection: MagicMock, mocker: MockerFixture
    ) -> None:
        mock_connection._execute_request.side_effect = SessionError(message="fail", session_id=1)
        spy_logger = mocker.patch(target="pywebtransport.session._logger")

        await session.close()

        spy_logger.warning.assert_called_with("wt_session close failed session_id=%d err=%s", 1, mocker.ANY)

    @pytest.mark.asyncio
    async def test_context_manager(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportSession, attribute="close", new_callable=mocker.AsyncMock)

        async with session as entered_session:
            assert entered_session is session

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_NO_ERROR)

    @pytest.mark.asyncio
    async def test_context_manager_cancelled(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportSession, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=asyncio.CancelledError):
            async with session:
                raise asyncio.CancelledError

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_CANCELLED)

    @pytest.mark.asyncio
    async def test_context_manager_exception_with_error_code(
        self, session: WebTransportSession, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(target=WebTransportSession, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=SessionError):
            async with session:
                raise SessionError(message="fail", error_code=123)

        spy_close.assert_awaited_once_with(error_code=123)

    @pytest.mark.asyncio
    async def test_create_stream_cancelled(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_closed(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        session._handle_closed(error_code=None)

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.create_bidirectional_stream()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_stream_connection_error(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_generic_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream create failed session_id=1"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_create_stream_invalid_handle_type(
        self,
        session: WebTransportSession,
        mock_connection: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        mock_receive_stream = mocker.Mock(spec=WebTransportReceiveStream)
        mock_receive_stream.stream_id = 104
        mock_connection._execute_request.return_value = mock_receive_stream

        with pytest.raises(expected_exception=StreamError, match=r"wt_stream validate invalid actual=.* stream_id=104"):
            await session.create_bidirectional_stream()

    @pytest.mark.parametrize(
        argnames="method, wrong_type, error_message",
        argvalues=[
            (
                "create_bidirectional_stream",
                WebTransportSendStream,
                r"wt_stream validate invalid actual=.* expected=bidirectional",
            ),
            (
                "create_unidirectional_stream",
                WebTransportBidirectionalStream,
                r"wt_stream validate invalid actual=.* expected=send_only",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_create_stream_mismatch_type(
        self,
        session: WebTransportSession,
        mock_connection: MagicMock,
        mocker: MockerFixture,
        method: str,
        wrong_type: type,
        error_message: str,
    ) -> None:
        mock_wrong = mocker.Mock(spec=wrong_type)
        mock_connection._execute_request.return_value = mock_wrong
        create_method = getattr(session, method)

        with pytest.raises(expected_exception=StreamError, match=error_message):
            await create_method()

    @pytest.mark.asyncio
    async def test_create_stream_session_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        error = SessionError(message="fail", session_id=1)
        mock_connection._execute_request.side_effect = error

        with pytest.raises(expected_exception=SessionError) as exception_info:
            await session.create_unidirectional_stream()

        assert exception_info.value is error

    @pytest.mark.parametrize(
        argnames="method, stream_type, is_unidirectional",
        argvalues=[
            ("create_bidirectional_stream", WebTransportBidirectionalStream, False),
            ("create_unidirectional_stream", WebTransportSendStream, True),
        ],
    )
    @pytest.mark.asyncio
    async def test_create_stream_success(
        self,
        session: WebTransportSession,
        mock_connection: MagicMock,
        mocker: MockerFixture,
        method: str,
        stream_type: type,
        is_unidirectional: bool,
    ) -> None:
        mock_stream = mocker.Mock(spec=stream_type)
        mock_connection._execute_request.return_value = mock_stream
        create_method = getattr(session, method)

        stream = await create_method()

        assert stream is mock_stream
        mock_connection._execute_request.assert_awaited_once()
        kwargs = mock_connection._execute_request.call_args.kwargs
        assert kwargs["resolver"] is mock_connection._resolve_stream_handle
        event = kwargs["event_factory"](123)

        assert isinstance(event, UserCreateStream)
        assert event.session_id == 1
        assert event.is_unidirectional is is_unidirectional

    @pytest.mark.asyncio
    async def test_create_stream_timeout(
        self, session: WebTransportSession, mock_config: MagicMock, mock_connection: MagicMock
    ) -> None:
        async def execute_request(**kwargs: Any) -> Any:
            await asyncio.Event().wait()

        mock_config.stream_create_timeout = 0.01
        mock_connection._execute_request.side_effect = execute_request

        with pytest.raises(expected_exception=TimeoutError, match="wt_stream create failed session_id=1"):
            await session.create_bidirectional_stream()

    @pytest.mark.asyncio
    async def test_diagnose_cancelled(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await session.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_closed(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        session._handle_closed(error_code=None)

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.diagnose()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_diagnose_connection_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="closed")

        with pytest.raises(expected_exception=ConnectionError):
            await session.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_generic_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_session_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = SessionError(message="fail", session_id=1)

        with pytest.raises(expected_exception=SessionError):
            await session.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_success(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        data: dict[str, Any] = {
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
            "created_at": 0.0,
            "data_consumed": 0,
            "data_received": 0,
            "data_sent": 0,
            "flow_control_negotiated": True,
            "headers": {},
            "is_client": False,
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
            "ready_at": None,
            "session_id": 1,
            "state": SessionState.CONNECTED,
            "wt_available_protocols": ["p1", "p2"],
            "wt_protocol": "p1",
        }
        mock_connection._execute_request.return_value = data

        diagnostics = await session.diagnose()

        assert diagnostics == SessionDiagnostics(**data)
        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)

        assert isinstance(event, UserDiagnoseSession)
        assert event.session_id == 1

    @pytest.mark.asyncio
    async def test_ensure_ready_cancelled_waiter_keeps_readiness(self, session: WebTransportSession) -> None:
        waiter = asyncio.create_task(session.ensure_ready())
        await asyncio.sleep(0)

        waiter.cancel()
        with pytest.raises(expected_exception=asyncio.CancelledError):
            await waiter
        session._handle_ready(wt_protocol=None)

        await session.ensure_ready()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="error_code, expected_code",
        argvalues=[
            (123, 123),
            (None, ErrorCodes.LIB_SESSION_STATE_ERROR),
        ],
    )
    async def test_ensure_ready_closed(
        self, session: WebTransportSession, error_code: int | None, expected_code: int
    ) -> None:
        session._handle_closed(error_code=error_code)

        with pytest.raises(
            expected_exception=SessionError, match="wt_session open failed session_id=1"
        ) as exception_info:
            await session.ensure_ready()

        assert exception_info.value.error_code == expected_code

    @pytest.mark.asyncio
    async def test_ensure_ready_waits_and_fails(self, session: WebTransportSession) -> None:
        waiter = asyncio.create_task(session.ensure_ready())
        await asyncio.sleep(0)

        assert not waiter.done()

        session._handle_closed(error_code=123)

        with pytest.raises(expected_exception=SessionError) as exception_info:
            await waiter

        assert exception_info.value.error_code == 123

    @pytest.mark.asyncio
    async def test_ensure_ready_waits_and_succeeds(self, session: WebTransportSession) -> None:
        waiter = asyncio.create_task(session.ensure_ready())
        await asyncio.sleep(0)

        assert not waiter.done()

        session._handle_ready(wt_protocol="h3")
        await waiter

        assert session.wt_protocol == "h3"

    @pytest.mark.asyncio
    async def test_export_keying_material_cancelled(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await session.export_keying_material(label="test", context=b"", length=32)

    @pytest.mark.asyncio
    async def test_export_keying_material_closed(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        session._handle_closed(error_code=None)

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.export_keying_material(label="test", context=b"", length=32)

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_export_keying_material_connection_error(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await session.export_keying_material(label="test", context=b"", length=32)

    @pytest.mark.asyncio
    async def test_export_keying_material_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.export_keying_material(label="test", context=b"", length=32)

    @pytest.mark.asyncio
    async def test_export_keying_material_generic_error(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.export_keying_material(label="test", context=b"", length=32)

    @pytest.mark.asyncio
    async def test_export_keying_material_session_error(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = SessionError(message="fail", session_id=1)

        with pytest.raises(expected_exception=SessionError):
            await session.export_keying_material(label="test", context=b"", length=32)

    @pytest.mark.asyncio
    async def test_export_keying_material_success(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.return_value = b"key_material_bytes"

        result = await session.export_keying_material(label="exporter", context=b"context", length=18)

        assert result == b"key_material_bytes"
        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)

        assert isinstance(event, UserExportKeyingMaterial)
        assert event.session_id == 1
        assert event.label == "exporter"
        assert event.context == b"context"
        assert event.length == 18

    def test_handle_closed(self, session: WebTransportSession) -> None:
        session._handle_closed(error_code=123)

        assert session.is_closed is True
        assert session._incoming_streams_bidi.qsize() == 1
        assert session._incoming_streams_bidi.get_nowait() is None
        assert session._incoming_streams_uni.qsize() == 1
        assert session._incoming_streams_uni.get_nowait() is None
        error = session._ready.result()
        assert isinstance(error, SessionError)
        assert error.error_code == 123

    def test_handle_closed_after_ready(self, session: WebTransportSession) -> None:
        session._handle_ready(wt_protocol=None)

        session._handle_closed(error_code=123)

        assert session.is_closed is True
        assert session._ready.result() is None

    @pytest.mark.parametrize(argnames="wt_protocol", argvalues=["custom-h3", None])
    def test_handle_ready(self, session: WebTransportSession, wt_protocol: str | None) -> None:
        session._handle_ready(wt_protocol=wt_protocol)

        assert session.wt_protocol == wt_protocol
        assert session._ready.result() is None

    def test_handle_stream_opened_bidirectional(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream = mocker.Mock(spec=WebTransportBidirectionalStream)
        stream.is_peer_initiated = True

        session._handle_stream_opened(stream=stream)

        assert session._incoming_streams_bidi.qsize() == 1
        assert session._incoming_streams_bidi.get_nowait() is stream
        assert session._incoming_streams_uni.empty()

    def test_handle_stream_opened_local_stream_ignored(
        self, session: WebTransportSession, mocker: MockerFixture
    ) -> None:
        stream = mocker.Mock(spec=WebTransportBidirectionalStream)
        stream.is_peer_initiated = False

        session._handle_stream_opened(stream=stream)

        assert session._incoming_streams_bidi.empty()
        assert session._incoming_streams_uni.empty()

    def test_handle_stream_opened_unidirectional(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream = mocker.Mock(spec=WebTransportReceiveStream)
        stream.is_peer_initiated = True

        session._handle_stream_opened(stream=stream)

        assert session._incoming_streams_uni.qsize() == 1
        assert session._incoming_streams_uni.get_nowait() is stream
        assert session._incoming_streams_bidi.empty()

    def test_handle_stream_opened_unsupported_type(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream = mocker.Mock(spec=WebTransportSendStream)
        stream.is_peer_initiated = True

        session._handle_stream_opened(stream=stream)

        assert session._incoming_streams_bidi.empty()
        assert session._incoming_streams_uni.empty()

    def test_headers_copy(self, session: WebTransportSession) -> None:
        headers = cast(dict[str, str], session.headers)
        headers["New"] = "Value"
        internal_headers = cast(dict[str, str], session._headers)

        assert "New" not in internal_headers

    @pytest.mark.asyncio
    async def test_incoming_bidirectional_streams(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream1 = mocker.Mock(spec=WebTransportBidirectionalStream)
        stream2 = mocker.Mock(spec=WebTransportBidirectionalStream)
        session._incoming_streams_bidi.put_nowait(stream1)
        session._incoming_streams_bidi.put_nowait(stream2)
        session._incoming_streams_bidi.put_nowait(None)

        generator = session.incoming_bidirectional_streams()
        results: list[WebTransportBidirectionalStream] = []
        async for stream in generator:
            results.append(stream)

        assert results == [stream1, stream2]
        assert isinstance(generator, AsyncIterator)
        assert session._incoming_streams_bidi.get_nowait() is None

    @pytest.mark.asyncio
    async def test_incoming_unidirectional_streams(self, session: WebTransportSession, mocker: MockerFixture) -> None:
        stream1 = mocker.Mock(spec=WebTransportReceiveStream)
        stream2 = mocker.Mock(spec=WebTransportReceiveStream)
        session._incoming_streams_uni.put_nowait(stream1)
        session._incoming_streams_uni.put_nowait(stream2)
        session._incoming_streams_uni.put_nowait(None)

        generator = session.incoming_unidirectional_streams()
        results: list[WebTransportReceiveStream] = []
        async for stream in generator:
            results.append(stream)

        assert results == [stream1, stream2]
        assert isinstance(generator, AsyncIterator)
        assert session._incoming_streams_uni.get_nowait() is None

    def test_init(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        assert session._connection() is mock_connection
        assert session._session_id == 1
        assert session._path == "/chat"
        assert session._headers == {"User-Agent": "TestClient"}
        assert session._wt_available_protocols is None
        assert session._wt_protocol is None
        assert session._datagram_read_task is None
        assert session._is_closed is False
        assert session._ready.done() is False
        assert session.events is not None
        assert isinstance(session._incoming_streams_bidi, asyncio.Queue)
        assert isinstance(session._incoming_streams_uni, asyncio.Queue)

        assert not hasattr(session, "__dict__")

    def test_properties(self, session: WebTransportSession) -> None:
        assert session.headers == {"User-Agent": "TestClient"}
        assert session.is_closed is False
        assert session.path == "/chat"
        assert session.session_id == 1
        assert session.wt_available_protocols is None
        assert session.wt_protocol is None

    @pytest.mark.asyncio
    async def test_read_datagram_cancelled_reader_keeps_request(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        response: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        mock_connection._execute_request = MagicMock(return_value=response)
        reader = asyncio.create_task(session.read_datagram())
        await asyncio.sleep(0)

        reader.cancel()
        with pytest.raises(expected_exception=asyncio.CancelledError):
            await reader
        response.set_result(b"datagram")

        assert await session.read_datagram() == b"datagram"
        mock_connection._execute_request.assert_called_once()
        assert session._datagram_read_task is None

    @pytest.mark.asyncio
    async def test_read_datagram_closed(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        session._handle_closed(error_code=None)

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.read_datagram()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_datagram_concurrent_readers_request_in_turn(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        responses: list[asyncio.Future[bytes]] = []

        def execute_request(**kwargs: Any) -> asyncio.Future[bytes]:
            response: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
            responses.append(response)
            return response

        mock_connection._execute_request = MagicMock(side_effect=execute_request)
        first_reader = asyncio.create_task(session.read_datagram())
        second_reader = asyncio.create_task(session.read_datagram())
        await asyncio.sleep(0)

        assert len(responses) == 1

        responses[0].set_result(b"first")
        assert await first_reader == b"first"
        await asyncio.sleep(0)

        assert len(responses) == 2

        responses[1].set_result(b"second")
        assert await second_reader == b"second"

    @pytest.mark.asyncio
    async def test_read_datagram_connection_error(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await session.read_datagram()

        assert session._datagram_read_task is None

    @pytest.mark.asyncio
    async def test_read_datagram_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.read_datagram()

    @pytest.mark.asyncio
    async def test_read_datagram_generic_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=SessionError, match="wt_session receive failed session_id=1"):
            await session.read_datagram()

    @pytest.mark.asyncio
    async def test_read_datagram_session_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = SessionError(message="fail", session_id=1)

        with pytest.raises(expected_exception=SessionError):
            await session.read_datagram()

    @pytest.mark.asyncio
    async def test_read_datagram_success(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.return_value = b"datagram"

        result = await session.read_datagram()

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)

        assert isinstance(event, UserReadDatagram)
        assert event.request_id == 123
        assert event.session_id == session.session_id
        assert result == b"datagram"
        assert session._datagram_read_task is None

    @pytest.mark.asyncio
    async def test_reject_cancelled(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await session.reject(status_code=404)

    @pytest.mark.asyncio
    async def test_reject_closed(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        session._handle_closed(error_code=None)

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.reject()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_reject_connection_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="channel closed")

        with pytest.raises(expected_exception=ConnectionError):
            await session.reject(status_code=404)

    @pytest.mark.asyncio
    async def test_reject_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.reject()

    @pytest.mark.asyncio
    async def test_reject_generic_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=SessionError, match="wt_session reject failed session_id=1"):
            await session.reject(status_code=404)

    @pytest.mark.asyncio
    async def test_reject_session_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        error = SessionError(message="wt_session validate failed", session_id=1)
        mock_connection._execute_request.side_effect = error

        with pytest.raises(expected_exception=SessionError) as exception_info:
            await session.reject(status_code=404)

        assert exception_info.value is error

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="kwargs, expected_headers, expected_status_code",
        argvalues=[
            ({"headers": {b"retry-after": b"120"}, "status_code": 429}, {b"retry-after": b"120"}, 429),
            ({}, {}, 403),
        ],
    )
    async def test_reject_success(
        self,
        session: WebTransportSession,
        mock_connection: MagicMock,
        kwargs: dict[str, Any],
        expected_headers: dict[bytes, bytes],
        expected_status_code: int,
    ) -> None:
        mock_connection._execute_request.return_value = None

        await session.reject(**kwargs)

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)

        assert isinstance(event, UserRejectSession)
        assert event.session_id == 1
        assert event.headers == expected_headers
        assert event.status_code == expected_status_code
        assert session.is_closed is False
        assert session._incoming_streams_bidi.empty()
        assert session._incoming_streams_uni.empty()

    def test_remote_address(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        assert session.remote_address == ("127.0.0.1", 443)

    def test_remote_address_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        assert session.remote_address is None

    def test_remote_address_none(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection.remote_address = None

        assert session.remote_address is None

    @pytest.mark.asyncio
    async def test_send_datagram_cancelled(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await session.send_datagram(data=b"test")

    @pytest.mark.asyncio
    async def test_send_datagram_closed(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        session._handle_closed(error_code=None)

        with pytest.raises(expected_exception=SessionError, match="wt_session resolve failed session_id=1"):
            await session.send_datagram(data=b"test")

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_datagram_connection_error(
        self, session: WebTransportSession, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await session.send_datagram(data=b"test")

    @pytest.mark.asyncio
    async def test_send_datagram_connection_gone(self, session: WebTransportSession) -> None:
        session._connection = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await session.send_datagram(data=b"test")

    @pytest.mark.asyncio
    async def test_send_datagram_datagram_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = DatagramError(message="fail")

        with pytest.raises(expected_exception=DatagramError):
            await session.send_datagram(data=b"test")

    @pytest.mark.asyncio
    async def test_send_datagram_generic_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=SessionError, match="wt_session send failed session_id=1"):
            await session.send_datagram(data=b"test")

    @pytest.mark.asyncio
    async def test_send_datagram_session_error(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = SessionError(message="fail", session_id=1)

        with pytest.raises(expected_exception=SessionError):
            await session.send_datagram(data=b"test")

    @pytest.mark.asyncio
    async def test_send_datagram_success(self, session: WebTransportSession, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.return_value = None

        await session.send_datagram(data=b"test")

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)

        assert isinstance(event, UserSendDatagram)
        assert event.session_id == 1
        assert event.data == b"test"
