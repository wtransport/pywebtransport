"""Unit tests for the pywebtransport.stream module."""

import asyncio
import dataclasses
from typing import Any, cast
from unittest.mock import MagicMock, call

import pytest
from pytest_mock import MockerFixture

from pywebtransport import (
    ClientConfig,
    ConnectionError,
    ErrorCodes,
    StreamError,
    TimeoutError,
    WebTransportReceiveStream,
    WebTransportSendStream,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport._protocol.events import (
    UserGetStreamDiagnostics,
    UserReadStream,
    UserResetStream,
    UserSendStreamData,
    UserStopSending,
)
from pywebtransport.connection import WebTransportConnection
from pywebtransport.stream import StreamDiagnostics, _BaseStream, _ensure_buffer
from pywebtransport.types import StreamDirection, StreamState


class TestBaseStream:

    @pytest.fixture
    def mock_connection(self, mocker: MockerFixture) -> MagicMock:
        conn = mocker.Mock(spec=WebTransportConnection)
        conn.config = mocker.Mock(spec=ClientConfig)
        conn.config.read_timeout = 0.1
        conn.config.write_timeout = 0.1
        conn.config.max_stream_read_buffer_size = 1024
        conn.handle = 42
        conn.execute_request = mocker.AsyncMock()

        return cast(MagicMock, conn)

    @pytest.fixture
    def mock_session(self, mock_connection: MagicMock, mocker: MockerFixture) -> MagicMock:
        session = mocker.Mock(spec=WebTransportSession)
        session._connection = mocker.Mock(return_value=mock_connection)

        return cast(MagicMock, session)

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> _BaseStream:
        return _BaseStream(session=mock_session, stream_id=1, is_remote=False)

    @pytest.mark.asyncio
    async def test_diagnostics_cancelled(self, stream: _BaseStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_connection_closed_error(self, stream: _BaseStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = ConnectionError(message="Closed")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_connection_gone(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await stream.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_generic_error(self, stream: _BaseStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = ValueError("Fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream resolve failed"):
            await stream.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_success_deque(self, stream: _BaseStream, mock_connection: MagicMock) -> None:
        data = {
            "bytes_received": 0,
            "bytes_sent": 0,
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
            "created_at": 0.0,
            "direction": StreamDirection.BIDIRECTIONAL,
            "is_peer_initiated": False,
            "read_buffer_size": 0,
            "session_id": 100,
            "state": StreamState.OPEN,
            "stream_id": stream.stream_id,
            "write_buffer_size": 4,
        }
        mock_connection.execute_request.return_value = data

        diag = await stream.diagnostics()

        mock_connection.execute_request.assert_awaited_once()
        kwargs = mock_connection.execute_request.call_args.kwargs
        event = kwargs["event_factory"](123)
        assert isinstance(event, UserGetStreamDiagnostics)
        assert event.stream_id == stream.stream_id
        assert diag.is_peer_initiated is False

    @pytest.mark.asyncio
    async def test_diagnostics_success_no_conversion_needed(
        self, stream: _BaseStream, mock_connection: MagicMock
    ) -> None:
        data = {
            "bytes_received": 0,
            "bytes_sent": 0,
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
            "created_at": 0.0,
            "direction": StreamDirection.BIDIRECTIONAL,
            "is_peer_initiated": True,
            "read_buffer_size": 0,
            "session_id": 100,
            "state": StreamState.OPEN,
            "stream_id": stream.stream_id,
            "write_buffer_size": 0,
        }
        mock_connection.execute_request.return_value = data

        diag = await stream.diagnostics()

        assert diag.is_peer_initiated is True

    def test_init(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        assert stream._session() is mock_session
        assert stream._stream_id == stream.stream_id
        assert stream._is_remote is False
        assert stream._cached_state == StreamState.OPEN
        assert stream.events is not None

        assert not hasattr(stream, "__dict__")

    def test_is_closed(self, stream: _BaseStream) -> None:
        stream._cached_state = StreamState.OPEN

        assert not stream.is_closed

        stream._cached_state = StreamState.CLOSED

        assert stream.is_closed

    def test_is_remote_property(self, mock_session: MagicMock) -> None:
        s1 = _BaseStream(session=mock_session, stream_id=1, is_remote=True)

        assert s1.is_remote is True

        s2 = _BaseStream(session=mock_session, stream_id=2, is_remote=False)

        assert s2.is_remote is False

    @pytest.mark.asyncio
    async def test_on_closed_handler(self, stream: _BaseStream) -> None:
        stream.events.emit_nowait(event_type="stream_closed", data={})
        await asyncio.sleep(delay=0)

        assert stream.state == StreamState.CLOSED

    def test_properties(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        assert stream.stream_id == stream.stream_id
        assert stream.is_remote is False
        assert stream.state == StreamState.OPEN
        assert stream.session is mock_session
        assert stream.is_closed is False

    def test_repr(self, stream: _BaseStream) -> None:
        assert "_BaseStream" in repr(stream)
        assert f"id={stream.stream_id}" in repr(stream)

    def test_session_property_gone(self, stream: _BaseStream) -> None:
        stream._session = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_session resolve failed"):
            _ = stream.session


class TestStreamDiagnostics:

    def test_init(self) -> None:
        diag = StreamDiagnostics(
            bytes_received=20,
            bytes_sent=10,
            close_code=None,
            close_reason=None,
            closed_at=None,
            created_at=100.0,
            direction=StreamDirection.BIDIRECTIONAL,
            is_peer_initiated=True,
            read_buffer_size=0,
            session_id=100,
            state=StreamState.OPEN,
            stream_id=1,
            write_buffer_size=0,
        )

        assert diag.stream_id == 1
        assert diag.state == StreamState.OPEN
        assert diag.is_peer_initiated is True

        with pytest.raises(expected_exception=dataclasses.FrozenInstanceError):
            cast(Any, diag).state = StreamState.CLOSED

        assert not hasattr(diag, "__dict__")


class TestWebTransportReceiveStream(TestBaseStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportReceiveStream:
        return WebTransportReceiveStream(session=mock_session, stream_id=2, is_remote=False)

    @pytest.mark.asyncio
    async def test_aiter(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"1", b"2", b""])

        res = [chunk async for chunk in stream]

        assert res == [b"1", b"2"]

    @pytest.mark.asyncio
    async def test_close(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_stop = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="stop_receiving", new_callable=mocker.AsyncMock
        )

        await stream.close()

        spy_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_stop = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="stop_receiving", new_callable=mocker.AsyncMock
        )

        async with stream as s:
            assert s is stream

        spy_stop.assert_awaited_once()

    def test_init(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportReceiveStream)
        assert stream._session() is mock_session
        assert stream._stream_id == 2
        assert stream._is_remote is False
        assert stream._cached_state == StreamState.OPEN
        assert stream.events is not None
        assert stream._read_eof is False

        assert not hasattr(stream, "__dict__")

    def test_properties(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportReceiveStream)
        assert stream.stream_id == 2
        assert stream.is_remote is False
        assert stream.state == StreamState.OPEN
        assert stream.session is mock_session
        assert stream.is_closed is False
        assert stream.direction == StreamDirection.RECEIVE_ONLY
        assert stream.can_read is True

        stream._cached_state = StreamState.RESET_RECEIVED

        assert stream.can_read is False

    @pytest.mark.asyncio
    async def test_read_all(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"a", b"b", b""])

        assert await stream.read_all() == b"ab"

    @pytest.mark.asyncio
    async def test_read_cancelled(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.read()

    @pytest.mark.asyncio
    async def test_read_closed(self, stream: WebTransportReceiveStream) -> None:
        stream._cached_state = StreamState.CLOSED

        assert await stream.read() == b""

    @pytest.mark.asyncio
    async def test_read_connection_error(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = ConnectionError(message="Fail")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.read()

    @pytest.mark.asyncio
    async def test_read_eof(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.return_value = b""

        data = await stream.read()

        assert data == b""
        assert stream._read_eof is True

        mock_connection.execute_request.reset_mock()

        assert await stream.read() == b""
        mock_connection.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_generic_error(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = ValueError("Fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream receive failed"):
            await stream.read()

    @pytest.mark.asyncio
    async def test_read_no_connection(self, stream: WebTransportReceiveStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError):
            await stream.read()

    @pytest.mark.asyncio
    async def test_read_stream_error(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = StreamError(
            message="State", error_code=ErrorCodes.LIB_STREAM_STATE_ERROR
        )

        assert await stream.read() == b""

        mock_connection.execute_request.reset_mock()

        assert await stream.read() == b""
        mock_connection.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_stream_error_reraise(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute_request.side_effect = StreamError(message="Other", error_code=ErrorCodes.H3_FRAME_ERROR)

        with pytest.raises(expected_exception=StreamError, match="Other"):
            await stream.read()

    @pytest.mark.asyncio
    async def test_read_success(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.return_value = b"data"

        data = await stream.read(max_bytes=10)

        assert data == b"data"
        mock_connection.execute_request.assert_awaited_once()
        event = mock_connection.execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserReadStream)
        assert event.stream_id == 2
        assert event.max_bytes == 10

    @pytest.mark.asyncio
    async def test_readexactly(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"12", b"34"])

        assert await stream.readexactly(n=4) == b"1234"

    @pytest.mark.asyncio
    async def test_readexactly_incomplete(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", return_value=b"")

        with pytest.raises(expected_exception=asyncio.IncompleteReadError):
            await stream.readexactly(n=5)

    @pytest.mark.asyncio
    async def test_readexactly_no_connection(self, stream: WebTransportReceiveStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await stream.readexactly(n=1)

    @pytest.mark.asyncio
    async def test_readexactly_params(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(expected_exception=ValueError):
            await stream.readexactly(n=-1)

        assert await stream.readexactly(n=0) == b""

    @pytest.mark.asyncio
    async def test_readexactly_timeout(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch(target="asyncio.timeout", side_effect=asyncio.TimeoutError)

        with pytest.raises(expected_exception=TimeoutError, match="wt_stream receive failed"):
            await stream.readexactly(n=1)

    @pytest.mark.asyncio
    async def test_readline(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="readuntil", return_value=b"line\n")

        assert await stream.readline() == b"line\n"

    @pytest.mark.asyncio
    async def test_readuntil_empty_separator(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(expected_exception=ValueError):
            await stream.readuntil(separator=b"")

    @pytest.mark.asyncio
    async def test_readuntil_incomplete(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", return_value=b"")

        with pytest.raises(expected_exception=asyncio.IncompleteReadError):
            await stream.readuntil(separator=b"\n")

    @pytest.mark.asyncio
    async def test_readuntil_limit(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"1", b"2", b"3"])

        with pytest.raises(expected_exception=StreamError, match="wt_stream receive exceeded"):
            await stream.readuntil(separator=b"\n", limit=2)

    @pytest.mark.asyncio
    async def test_readuntil_no_connection(self, stream: WebTransportReceiveStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await stream.readuntil(separator=b"\n")

    @pytest.mark.asyncio
    async def test_readuntil_params(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(expected_exception=ValueError):
            await stream.readuntil(separator=b"")

    @pytest.mark.asyncio
    async def test_readuntil_success(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"a", b"b", b"\n"])

        assert await stream.readuntil(separator=b"\n") == b"ab\n"

    @pytest.mark.asyncio
    async def test_readuntil_timeout(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch(target="asyncio.timeout", side_effect=asyncio.TimeoutError)

        with pytest.raises(expected_exception=TimeoutError, match="wt_stream receive failed"):
            await stream.readuntil(separator=b"\n")

    def test_repr(self, stream: _BaseStream) -> None:
        assert isinstance(stream, WebTransportReceiveStream)
        assert "WebTransportReceiveStream" in repr(stream)
        assert "id=2" in repr(stream)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="test_error_code, expected_code",
        argvalues=[
            (123, 123),
            (None, ErrorCodes.APP_NO_ERROR),
        ],
    )
    async def test_stop_receiving(
        self,
        stream: WebTransportReceiveStream,
        mock_connection: MagicMock,
        test_error_code: int | None,
        expected_code: int,
    ) -> None:
        mock_connection.execute_request.return_value = None

        if test_error_code is not None:
            await stream.stop_receiving(error_code=test_error_code)
        else:
            await stream.stop_receiving()

        mock_connection.execute_request.assert_awaited_once()
        event = mock_connection.execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserStopSending)
        assert event.stream_id == 2
        assert event.error_code == expected_code

    @pytest.mark.asyncio
    async def test_stop_receiving_cancelled(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_connection_error(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute_request.side_effect = ConnectionError(message="Fail")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_eof(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        stream._read_eof = True

        await stream.stop_receiving(error_code=123)

        mock_connection.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_receiving_generic_error(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute_request.side_effect = ValueError("Fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream abort failed"):
            await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_no_connection(
        self, stream: WebTransportReceiveStream, mock_session: MagicMock
    ) -> None:
        mock_session._connection.return_value = None

        await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_stream_error_ignored(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute_request.side_effect = StreamError(
            message="State", error_code=ErrorCodes.LIB_STREAM_STATE_ERROR
        )

        await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_stream_error_reraised(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute_request.side_effect = StreamError(message="Other", error_code=ErrorCodes.H3_FRAME_ERROR)

        with pytest.raises(expected_exception=StreamError):
            await stream.stop_receiving()


class TestWebTransportSendStream(TestBaseStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportSendStream:
        return WebTransportSendStream(session=mock_session, stream_id=3, is_remote=False)

    @pytest.mark.asyncio
    async def test_close(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)

        await stream.close()

        spy_write.assert_awaited_once_with(data=b"", end_stream=True)

    @pytest.mark.asyncio
    async def test_close_generic_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportSendStream, attribute="write", side_effect=ValueError("Boom"))

        with pytest.raises(expected_exception=StreamError, match="wt_stream close failed"):
            await stream.close()

    @pytest.mark.asyncio
    async def test_close_stream_error_ignored(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        mocker.patch.object(
            target=WebTransportSendStream, attribute="write", side_effect=StreamError(message="Expected")
        )

        await stream.close()

    @pytest.mark.asyncio
    async def test_close_with_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_reset = mocker.patch.object(target=WebTransportSendStream, attribute="reset", new_callable=mocker.AsyncMock)

        await stream.close(error_code=1)

        spy_reset.assert_awaited_once_with(error_code=1)

    @pytest.mark.asyncio
    async def test_context_manager_exit_cancelled(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=asyncio.CancelledError):
            async with stream:
                raise asyncio.CancelledError()

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_CANCELLED)

    @pytest.mark.asyncio
    async def test_context_manager_exit_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock)

        class MyErr(Exception):
            error_code = 999

        with pytest.raises(expected_exception=MyErr):
            async with stream:
                raise MyErr()

        spy_close.assert_awaited_once_with(error_code=999)

    @pytest.mark.asyncio
    async def test_context_manager_exit_generic_error(
        self, stream: WebTransportSendStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=RuntimeError):
            async with stream:
                raise RuntimeError("Generic")

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_GENERIC_ERROR)

    @pytest.mark.asyncio
    async def test_context_manager_exit_success(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock)

        async with stream:
            pass

        spy_close.assert_awaited_once_with(error_code=None)

    def test_init(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportSendStream)
        assert stream._session() is mock_session
        assert stream._stream_id == 3
        assert stream._is_remote is False
        assert stream._cached_state == StreamState.OPEN
        assert stream.events is not None

        assert not hasattr(stream, "__dict__")

    def test_properties(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportSendStream)
        assert stream.stream_id == 3
        assert stream.is_remote is False
        assert stream.state == StreamState.OPEN
        assert stream.session is mock_session
        assert stream.is_closed is False
        assert stream.direction == StreamDirection.SEND_ONLY
        assert stream.can_write is True

        stream._cached_state = StreamState.RESET_SENT

        assert stream.can_write is False

    def test_repr(self, stream: _BaseStream) -> None:
        assert isinstance(stream, WebTransportSendStream)
        assert "WebTransportSendStream" in repr(stream)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="test_error_code, expected_code",
        argvalues=[
            (99, 99),
            (None, ErrorCodes.APP_NO_ERROR),
        ],
    )
    async def test_reset(
        self,
        stream: WebTransportSendStream,
        mock_connection: MagicMock,
        test_error_code: int | None,
        expected_code: int,
    ) -> None:
        mock_connection.execute_request.return_value = None

        if test_error_code is not None:
            await stream.reset(error_code=test_error_code)
        else:
            await stream.reset()

        mock_connection.execute_request.assert_awaited_once()
        event = mock_connection.execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserResetStream)
        assert event.stream_id == 3
        assert event.error_code == expected_code

    @pytest.mark.asyncio
    async def test_reset_cancelled(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_connection_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = ConnectionError(message="Fail")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_generic_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = ValueError("Fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream abort failed"):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_no_connection(self, stream: WebTransportSendStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_stream_error_ignored(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = StreamError(
            message="State", error_code=ErrorCodes.LIB_STREAM_STATE_ERROR
        )

        await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_stream_error_reraised(
        self, stream: WebTransportSendStream, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute_request.side_effect = StreamError(message="Other", error_code=ErrorCodes.H3_FRAME_ERROR)

        with pytest.raises(expected_exception=StreamError):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_write(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.return_value = None

        await stream.write(data=b"test", end_stream=True)

        mock_connection.execute_request.assert_awaited_once()
        event = mock_connection.execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserSendStreamData)
        assert event.stream_id == 3
        assert event.data == b"test"
        assert event.end_stream is True

    @pytest.mark.asyncio
    async def test_write_all(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)

        await stream.write_all(data=b"1234", chunk_size=2, end_stream=True)

        assert spy_write.await_count == 2
        spy_write.assert_has_awaits(calls=[call(data=b"12", end_stream=False), call(data=b"34", end_stream=True)])

    @pytest.mark.asyncio
    async def test_write_all_empty_end(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)

        await stream.write_all(data=b"", end_stream=True)

        spy_write.assert_awaited_once_with(data=b"", end_stream=True)

    @pytest.mark.asyncio
    async def test_write_all_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        mocker.patch.object(
            target=WebTransportSendStream, attribute="write", side_effect=StreamError(message="Fail", stream_id=3)
        )

        with pytest.raises(expected_exception=StreamError):
            await stream.write_all(data=b"data")

    @pytest.mark.asyncio
    async def test_write_all_type_error(self, stream: WebTransportSendStream) -> None:
        with pytest.raises(expected_exception=StreamError, match="wt_stream validate invalid"):
            await stream.write_all(data=123)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_write_cancelled(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.write(data=b"test")

    @pytest.mark.asyncio
    async def test_write_early_return(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        await stream.write(data=b"", end_stream=False)

        mock_connection.execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_generic_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = ValueError("Fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream send failed"):
            await stream.write(data=b"payload")

    @pytest.mark.asyncio
    async def test_write_no_connection(self, stream: WebTransportSendStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError):
            await stream.write(data=b"a")

    @pytest.mark.asyncio
    async def test_write_stream_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection.execute_request.side_effect = StreamError(message="Fail")

        with pytest.raises(expected_exception=StreamError):
            await stream.write(data=b"payload")

    @pytest.mark.asyncio
    async def test_write_type_error(self, stream: WebTransportSendStream) -> None:
        with pytest.raises(expected_exception=StreamError, match="wt_stream validate invalid"):
            await stream.write(data=123)  # type: ignore[arg-type]


class TestWebTransportStream(TestBaseStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportStream:
        return WebTransportStream(session=mock_session, stream_id=4, is_remote=False)

    @pytest.mark.asyncio
    async def test_aiter(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(
            target=WebTransportReceiveStream, attribute="__anext__", side_effect=[b"1", StopAsyncIteration]
        )

        chunks = [c async for c in stream]

        assert chunks == [b"1"]

    @pytest.mark.asyncio
    async def test_close(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_send_close = mocker.patch.object(
            target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock
        )
        spy_recv_stop = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="stop_receiving", new_callable=mocker.AsyncMock
        )

        await stream.close(error_code=10)

        spy_send_close.assert_awaited_once_with(error_code=10)
        spy_recv_stop.assert_awaited_once_with(error_code=10)

    @pytest.mark.asyncio
    async def test_close_no_args(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_send_close = mocker.patch.object(
            target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock
        )
        spy_recv_stop = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="stop_receiving", new_callable=mocker.AsyncMock
        )

        await stream.close()

        spy_send_close.assert_awaited_once_with(error_code=None)
        spy_recv_stop.assert_awaited_once_with(error_code=ErrorCodes.APP_NO_ERROR)

    def test_composition(self, stream: WebTransportStream) -> None:
        assert isinstance(stream._reader, WebTransportReceiveStream)
        assert isinstance(stream._writer, WebTransportSendStream)
        assert stream.direction == StreamDirection.BIDIRECTIONAL

    @pytest.mark.asyncio
    async def test_context_manager(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportStream, attribute="close", new_callable=mocker.AsyncMock)

        async with stream:
            pass

        spy_close.assert_awaited_once_with(error_code=None)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames="exception_type, expected_code",
        argvalues=[
            (ValueError, ErrorCodes.APP_GENERIC_ERROR),
            (asyncio.CancelledError, ErrorCodes.APP_CANCELLED),
        ],
    )
    async def test_context_manager_error(
        self, stream: WebTransportStream, mocker: MockerFixture, exception_type: type[BaseException], expected_code: int
    ) -> None:
        spy_close = mocker.patch.object(target=WebTransportStream, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=exception_type):
            async with stream:
                raise exception_type()

        spy_close.assert_awaited_once_with(error_code=expected_code)

    @pytest.mark.asyncio
    async def test_context_manager_exit_error_with_code(
        self, stream: WebTransportStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(target=WebTransportStream, attribute="close", new_callable=mocker.AsyncMock)

        class MyErr(Exception):
            error_code = 12345

        with pytest.raises(expected_exception=MyErr):
            async with stream:
                raise MyErr()

        spy_close.assert_awaited_once_with(error_code=12345)

    @pytest.mark.asyncio
    async def test_delegated_read(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", return_value=b"rd")

        assert await stream.read() == b"rd"

    @pytest.mark.asyncio
    async def test_delegated_read_all(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read_all", return_value=b"all")

        assert await stream.read_all() == b"all"

    @pytest.mark.asyncio
    async def test_delegated_readexactly(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="readexactly", return_value=b"ex")

        assert await stream.readexactly(n=2) == b"ex"

    @pytest.mark.asyncio
    async def test_delegated_readline(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="readline", return_value=b"ln")

        assert await stream.readline() == b"ln"

    @pytest.mark.asyncio
    async def test_delegated_readuntil(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="readuntil", return_value=b"ut")

        assert await stream.readuntil(separator=b"t") == b"ut"

    @pytest.mark.asyncio
    async def test_delegated_reset(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_reset = mocker.patch.object(target=WebTransportSendStream, attribute="reset", new_callable=mocker.AsyncMock)

        await stream.reset(error_code=2)

        spy_reset.assert_awaited_once_with(error_code=2)

    @pytest.mark.asyncio
    async def test_delegated_stop_receiving(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_stop = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="stop_receiving", new_callable=mocker.AsyncMock
        )

        await stream.stop_receiving(error_code=1)

        spy_stop.assert_awaited_once_with(error_code=1)

    @pytest.mark.asyncio
    async def test_delegated_write(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)

        await stream.write(data=b"wr")

        spy_write.assert_awaited_once_with(data=b"wr", end_stream=False)

    @pytest.mark.asyncio
    async def test_delegated_write_all(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_write_all = mocker.patch.object(
            target=WebTransportSendStream, attribute="write_all", new_callable=mocker.AsyncMock
        )

        await stream.write_all(data=b"wall")

        spy_write_all.assert_awaited_once_with(data=b"wall", chunk_size=65536, end_stream=False)

    def test_init(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportStream)
        assert stream._session() is mock_session
        assert stream._stream_id == 4
        assert stream._is_remote is False
        assert stream._cached_state == StreamState.OPEN
        assert stream.events is not None
        assert isinstance(stream._reader, WebTransportReceiveStream)
        assert isinstance(stream._writer, WebTransportSendStream)

        assert not hasattr(stream, "__dict__")

    @pytest.mark.asyncio
    async def test_on_closed_propagates(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_reader_on_closed = mocker.patch.object(target=WebTransportReceiveStream, attribute="_on_closed")
        spy_writer_on_closed = mocker.patch.object(target=WebTransportSendStream, attribute="_on_closed")

        stream.events.emit_nowait(event_type="stream_closed", data={})
        await asyncio.sleep(delay=0)

        assert stream.state == StreamState.CLOSED
        spy_reader_on_closed.assert_called_once()
        spy_writer_on_closed.assert_called_once()

    def test_properties(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportStream)
        assert stream.stream_id == 4
        assert stream.is_remote is False
        assert stream.state == StreamState.OPEN
        assert stream.session is mock_session
        assert stream.is_closed is False
        assert stream.direction == StreamDirection.BIDIRECTIONAL
        assert stream.can_read is True
        assert stream.can_write is True

    def test_repr(self, stream: _BaseStream) -> None:
        assert isinstance(stream, WebTransportStream)
        assert "WebTransportStream" in repr(stream)


@pytest.mark.parametrize(
    argnames="data, expected_type, expected_content",
    argvalues=[
        ("hello", bytes, b"hello"),
        (b"world", bytes, b"world"),
        (bytearray(b"array"), bytearray, bytearray(b"array")),
        (memoryview(b"view"), memoryview, b"view"),
    ],
)
def test_ensure_buffer(data: Any, expected_type: type, expected_content: Any) -> None:
    result = _ensure_buffer(data=data)

    assert isinstance(result, expected_type)
    if isinstance(result, memoryview):
        assert result.tobytes() == expected_content
    else:
        assert result == expected_content


def test_ensure_buffer_invalid_type() -> None:
    with pytest.raises(expected_exception=TypeError, match="mem_buffer validate invalid actual=int expected=buffer"):
        _ensure_buffer(data=cast(Any, 123))
