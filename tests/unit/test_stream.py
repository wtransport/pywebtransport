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
    WebTransportBidirectionalStream,
    WebTransportConnection,
    WebTransportReceiveStream,
    WebTransportSendStream,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport._controller.events import (
    UserDiagnoseStream,
    UserReadStream,
    UserResetStream,
    UserStopSending,
    UserWriteStream,
)
from pywebtransport.stream import StreamDiagnostics, _ensure_buffer
from pywebtransport.types import StreamDirection, StreamReceiveState, StreamSendState


class TestStreamDiagnostics:

    def test_init(self) -> None:
        diagnostics = StreamDiagnostics(
            created_at=100.0,
            direction=StreamDirection.BIDIRECTIONAL,
            is_peer_initiated=True,
            receive_error_code=None,
            receive_state=StreamReceiveState.RECEIVE,
            send_error_code=None,
            send_state=StreamSendState.READY,
            session_id=100,
            stream_id=1,
        )

        assert diagnostics.stream_id == 1
        assert diagnostics.receive_state == StreamReceiveState.RECEIVE
        assert diagnostics.send_state == StreamSendState.READY
        assert diagnostics.is_peer_initiated is True

        with pytest.raises(expected_exception=dataclasses.FrozenInstanceError):
            cast(Any, diagnostics).send_state = StreamSendState.RESET_SENT

        assert not hasattr(diagnostics, "__dict__")


class TestWebTransportStream:

    @pytest.fixture
    def mock_connection(self, mocker: MockerFixture) -> MagicMock:
        connection = mocker.Mock(spec=WebTransportConnection)
        connection.config = mocker.Mock(spec=ClientConfig)
        connection.config.read_timeout = 0.1
        connection.handle = 42
        connection._execute_request = mocker.AsyncMock()

        return cast(MagicMock, connection)

    @pytest.fixture
    def mock_session(self, mock_connection: MagicMock, mocker: MockerFixture) -> MagicMock:
        session = mocker.Mock(spec=WebTransportSession)
        session._connection = mocker.Mock(return_value=mock_connection)

        return cast(MagicMock, session)

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportStream:
        return WebTransportStream(stream_id=1, session=mock_session, is_peer_initiated=False)

    @pytest.mark.asyncio
    async def test_diagnose_cancelled(self, stream: WebTransportStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_closed(self, stream: WebTransportStream, mock_connection: MagicMock) -> None:
        stream._handle_closed()

        with pytest.raises(expected_exception=StreamError, match="wt_stream resolve failed"):
            await stream.diagnose()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_diagnose_connection_error(self, stream: WebTransportStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="closed")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_connection_gone(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await stream.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_generic_error(self, stream: WebTransportStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream resolve failed"):
            await stream.diagnose()

    @pytest.mark.asyncio
    async def test_diagnose_stream_error(self, stream: WebTransportStream, mock_connection: MagicMock) -> None:
        error = StreamError(message="wt_stream resolve failed", stream_id=stream.stream_id)
        mock_connection._execute_request.side_effect = error

        with pytest.raises(expected_exception=StreamError) as exception_info:
            await stream.diagnose()

        assert exception_info.value is error

    @pytest.mark.asyncio
    async def test_diagnose_success(self, stream: WebTransportStream, mock_connection: MagicMock) -> None:
        data: dict[str, Any] = {
            "created_at": 0.0,
            "direction": StreamDirection.BIDIRECTIONAL,
            "is_peer_initiated": False,
            "receive_error_code": None,
            "receive_state": StreamReceiveState.RECEIVE,
            "send_error_code": None,
            "send_state": StreamSendState.READY,
            "session_id": 100,
            "stream_id": stream.stream_id,
        }
        mock_connection._execute_request.return_value = data

        diagnostics = await stream.diagnose()

        mock_connection._execute_request.assert_awaited_once()
        kwargs = mock_connection._execute_request.call_args.kwargs
        event = kwargs["event_factory"](123)
        assert isinstance(event, UserDiagnoseStream)
        assert event.stream_id == stream.stream_id
        assert diagnostics == StreamDiagnostics(**data)

    def test_handle_closed(self, stream: WebTransportStream) -> None:
        stream._handle_closed()

        assert stream.is_closed is True

    def test_init(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        assert stream._stream_id == stream.stream_id
        assert stream._session() is mock_session
        assert stream._is_peer_initiated is False
        assert stream._is_closed is False
        assert stream.events is not None

        assert not hasattr(stream, "__dict__")

    def test_is_peer_initiated_property(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        peer_stream = type(stream)(stream_id=stream.stream_id, session=mock_session, is_peer_initiated=True)

        assert peer_stream.is_peer_initiated is True
        assert stream.is_peer_initiated is False

    def test_properties(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        assert stream.is_closed is False
        assert stream.is_peer_initiated is False
        assert stream.session is mock_session
        assert stream.stream_id == 1

    def test_session_property_gone(self, stream: WebTransportStream) -> None:
        stream._session = lambda: None  # type: ignore[assignment]

        with pytest.raises(expected_exception=ConnectionError, match="wt_session resolve failed"):
            _ = stream.session


class TestWebTransportBidirectionalStream(TestWebTransportStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportBidirectionalStream:
        return WebTransportBidirectionalStream(stream_id=4, session=mock_session, is_peer_initiated=False)

    @pytest.mark.asyncio
    async def test_aiter(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        mocker.patch.object(
            target=WebTransportReceiveStream, attribute="__anext__", side_effect=[b"1", StopAsyncIteration]
        )

        chunks = [chunk async for chunk in stream]

        assert chunks == [b"1"]

    @pytest.mark.asyncio
    async def test_close(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        manager = mocker.MagicMock()
        spy_receiver_close = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="close", new_callable=mocker.AsyncMock
        )
        spy_sender_close = mocker.patch.object(
            target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock
        )
        manager.attach_mock(spy_receiver_close, "receiver_close")
        manager.attach_mock(spy_sender_close, "sender_close")

        await stream.close(error_code=10)

        spy_receiver_close.assert_awaited_once_with(error_code=10)
        spy_sender_close.assert_awaited_once_with(error_code=10)
        assert [mock_call[0] for mock_call in manager.mock_calls] == ["receiver_close", "sender_close"]

    @pytest.mark.asyncio
    async def test_close_no_args(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_receiver_close = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="close", new_callable=mocker.AsyncMock
        )
        spy_sender_close = mocker.patch.object(
            target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock
        )

        await stream.close()

        spy_receiver_close.assert_awaited_once_with()
        spy_sender_close.assert_awaited_once_with(error_code=None)

    @pytest.mark.asyncio
    async def test_context_manager(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(
            target=WebTransportBidirectionalStream, attribute="close", new_callable=mocker.AsyncMock
        )

        async with stream as entered:
            assert entered is stream

        spy_close.assert_awaited_once_with(error_code=None)

    @pytest.mark.asyncio
    async def test_context_manager_exit_cancelled(
        self, stream: WebTransportBidirectionalStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(
            target=WebTransportBidirectionalStream, attribute="close", new_callable=mocker.AsyncMock
        )

        with pytest.raises(expected_exception=asyncio.CancelledError):
            async with stream:
                raise asyncio.CancelledError()

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_CANCELLED)

    @pytest.mark.asyncio
    async def test_context_manager_exit_error(
        self, stream: WebTransportBidirectionalStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(
            target=WebTransportBidirectionalStream, attribute="close", new_callable=mocker.AsyncMock
        )

        class CustomError(Exception):
            error_code = 999

        with pytest.raises(expected_exception=CustomError):
            async with stream:
                raise CustomError()

        spy_close.assert_awaited_once_with(error_code=999)

    @pytest.mark.asyncio
    async def test_context_manager_exit_generic_error(
        self, stream: WebTransportBidirectionalStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(
            target=WebTransportBidirectionalStream, attribute="close", new_callable=mocker.AsyncMock
        )

        with pytest.raises(expected_exception=RuntimeError):
            async with stream:
                raise RuntimeError("generic")

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_GENERIC_ERROR)

    @pytest.mark.asyncio
    async def test_delegated_read(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_read = mocker.patch.object(target=WebTransportReceiveStream, attribute="read", return_value=b"read")

        assert await stream.read(max_size=4) == b"read"
        spy_read.assert_awaited_once_with(max_size=4)

    @pytest.mark.asyncio
    async def test_delegated_read_all(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_read_all = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="read_all", return_value=b"read_all"
        )

        assert await stream.read_all() == b"read_all"
        spy_read_all.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_delegated_readexactly(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_readexactly = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="readexactly", return_value=b"readexactly"
        )

        assert await stream.readexactly(size=11) == b"readexactly"
        spy_readexactly.assert_awaited_once_with(size=11)

    @pytest.mark.asyncio
    async def test_delegated_readline(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_readline = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="readline", return_value=b"readline"
        )

        assert await stream.readline(max_size=8) == b"readline"
        spy_readline.assert_awaited_once_with(max_size=8)

    @pytest.mark.asyncio
    async def test_delegated_readuntil(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_readuntil = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="readuntil", return_value=b"readuntil"
        )

        assert await stream.readuntil(separator=b"l", max_size=9) == b"readuntil"
        spy_readuntil.assert_awaited_once_with(separator=b"l", max_size=9)

    @pytest.mark.asyncio
    async def test_delegated_reset(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_reset = mocker.patch.object(target=WebTransportSendStream, attribute="reset", new_callable=mocker.AsyncMock)

        await stream.reset(error_code=2)

        spy_reset.assert_awaited_once_with(error_code=2)

    @pytest.mark.asyncio
    async def test_delegated_stop_receiving(
        self, stream: WebTransportBidirectionalStream, mocker: MockerFixture
    ) -> None:
        spy_stop = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="stop_receiving", new_callable=mocker.AsyncMock
        )

        await stream.stop_receiving(error_code=1)

        spy_stop.assert_awaited_once_with(error_code=1)

    @pytest.mark.asyncio
    async def test_delegated_write(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)

        await stream.write(data=b"write")

        spy_write.assert_awaited_once_with(data=b"write", end_stream=False)

    @pytest.mark.asyncio
    async def test_delegated_write_all(self, stream: WebTransportBidirectionalStream, mocker: MockerFixture) -> None:
        spy_write_all = mocker.patch.object(
            target=WebTransportSendStream, attribute="write_all", new_callable=mocker.AsyncMock
        )

        await stream.write_all(data=b"write_all")

        spy_write_all.assert_awaited_once_with(data=b"write_all", end_stream=False)

    def test_handle_closed(self, stream: WebTransportStream) -> None:
        assert isinstance(stream, WebTransportBidirectionalStream)

        stream._handle_closed()

        assert stream.is_closed is True
        assert stream._receiver.is_closed is True
        assert stream._sender.is_closed is True

    def test_init(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportBidirectionalStream)
        assert stream._stream_id == 4
        assert stream._session() is mock_session
        assert stream._is_peer_initiated is False
        assert stream._is_closed is False
        assert stream.events is not None
        assert isinstance(stream._receiver, WebTransportReceiveStream)
        assert stream._receiver.stream_id == 4
        assert isinstance(stream._sender, WebTransportSendStream)
        assert stream._sender.stream_id == 4

        assert not hasattr(stream, "__dict__")

    def test_properties(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportBidirectionalStream)
        assert stream.direction == StreamDirection.BIDIRECTIONAL
        assert stream.is_closed is False
        assert stream.is_peer_initiated is False
        assert stream.session is mock_session
        assert stream.stream_id == 4


class TestWebTransportReceiveStream(TestWebTransportStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportReceiveStream:
        return WebTransportReceiveStream(stream_id=2, session=mock_session, is_peer_initiated=False)

    @pytest.mark.asyncio
    async def test_aiter(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"1", b"2", b""])

        chunks = [chunk async for chunk in stream]

        assert chunks == [b"1", b"2"]

    @pytest.mark.asyncio
    async def test_close(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_stop = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="stop_receiving", new_callable=mocker.AsyncMock
        )

        await stream.close(error_code=7)

        spy_stop.assert_awaited_once_with(error_code=7)

    @pytest.mark.asyncio
    async def test_close_no_args(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_stop = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="stop_receiving", new_callable=mocker.AsyncMock
        )

        await stream.close()

        spy_stop.assert_awaited_once_with(error_code=ErrorCodes.APP_NO_ERROR)

    @pytest.mark.asyncio
    async def test_context_manager(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="close", new_callable=mocker.AsyncMock
        )

        async with stream as entered:
            assert entered is stream

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_NO_ERROR)

    @pytest.mark.asyncio
    async def test_context_manager_exit_cancelled(
        self, stream: WebTransportReceiveStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="close", new_callable=mocker.AsyncMock
        )

        with pytest.raises(expected_exception=asyncio.CancelledError):
            async with stream:
                raise asyncio.CancelledError()

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_CANCELLED)

    @pytest.mark.asyncio
    async def test_context_manager_exit_error(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="close", new_callable=mocker.AsyncMock
        )

        class CustomError(Exception):
            error_code = 999

        with pytest.raises(expected_exception=CustomError):
            async with stream:
                raise CustomError()

        spy_close.assert_awaited_once_with(error_code=999)

    @pytest.mark.asyncio
    async def test_context_manager_exit_generic_error(
        self, stream: WebTransportReceiveStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="close", new_callable=mocker.AsyncMock
        )

        with pytest.raises(expected_exception=RuntimeError):
            async with stream:
                raise RuntimeError("generic")

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_GENERIC_ERROR)

    def test_init(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportReceiveStream)
        assert stream._stream_id == 2
        assert stream._session() is mock_session
        assert stream._is_peer_initiated is False
        assert stream._is_closed is False
        assert stream.events is not None
        assert stream._buffer == bytearray()
        assert stream._eof is False
        assert stream._read_task is None

        assert not hasattr(stream, "__dict__")

    def test_properties(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportReceiveStream)
        assert stream.direction == StreamDirection.RECEIVE_ONLY
        assert stream.is_closed is False
        assert stream.is_peer_initiated is False
        assert stream.session is mock_session
        assert stream.stream_id == 2

    @pytest.mark.asyncio
    async def test_read_all(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"a", b"b", b""])

        assert await stream.read_all() == b"ab"

    @pytest.mark.asyncio
    async def test_read_buffer_partial(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        stream._buffer = bytearray(b"abcdef")

        data = await stream.read(max_size=2)

        assert data == b"ab"
        assert stream._buffer == bytearray(b"cdef")
        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_buffer_unlimited(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        stream._buffer = bytearray(b"abcdef")

        data = await stream.read()

        assert data == b"abcdef"
        assert stream._buffer == bytearray()
        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_cancelled_reader_keeps_request(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        response: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        mock_connection._execute_request = MagicMock(return_value=response)
        reader = asyncio.create_task(stream.read())
        await asyncio.sleep(0)

        reader.cancel()
        with pytest.raises(expected_exception=asyncio.CancelledError):
            await reader
        response.set_result(b"data")

        assert await stream.read() == b"data"
        mock_connection._execute_request.assert_called_once()
        assert stream._read_task is None

    @pytest.mark.asyncio
    async def test_read_closed(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        stream._handle_closed()

        with pytest.raises(expected_exception=StreamError, match="wt_stream resolve failed stream_id=2"):
            await stream.read()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_concurrent_readers_request_in_turn(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        responses: list[asyncio.Future[bytes]] = []

        def execute_request(**kwargs: Any) -> asyncio.Future[bytes]:
            response: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
            responses.append(response)
            return response

        mock_connection._execute_request = MagicMock(side_effect=execute_request)
        first_reader = asyncio.create_task(stream.read())
        second_reader = asyncio.create_task(stream.read())
        await asyncio.sleep(0)

        assert len(responses) == 1

        responses[0].set_result(b"first")
        assert await first_reader == b"first"
        await asyncio.sleep(0)

        assert len(responses) == 2

        responses[1].set_result(b"second")
        assert await second_reader == b"second"

    @pytest.mark.asyncio
    async def test_read_connection_error(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.read()

        assert stream._read_task is None

    @pytest.mark.asyncio
    async def test_read_connection_gone(self, stream: WebTransportReceiveStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await stream.read()

    @pytest.mark.asyncio
    async def test_read_eof(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.return_value = b""

        data = await stream.read()

        assert data == b""
        assert stream._eof is True

        mock_connection._execute_request.reset_mock()

        assert await stream.read() == b""
        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_generic_error(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream receive failed"):
            await stream.read()

    @pytest.mark.asyncio
    async def test_read_negative_max_size(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(expected_exception=ValueError):
            await stream.read(max_size=-2)

    @pytest.mark.asyncio
    async def test_read_shared_request_buffers_excess(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        response: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        mock_connection._execute_request = MagicMock(return_value=response)
        unbounded_reader = asyncio.create_task(stream.read())
        await asyncio.sleep(0)
        bounded_reader = asyncio.create_task(stream.read(max_size=2))
        await asyncio.sleep(0)

        unbounded_reader.cancel()
        with pytest.raises(expected_exception=asyncio.CancelledError):
            await unbounded_reader
        response.set_result(b"abcdef")

        assert await bounded_reader == b"ab"
        assert stream._buffer == bytearray(b"cdef")
        mock_connection._execute_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_stream_error(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        error = StreamError(message="state", error_code=ErrorCodes.LIB_STREAM_STATE_ERROR)
        mock_connection._execute_request.side_effect = error

        with pytest.raises(expected_exception=StreamError) as exception_info:
            await stream.read()

        assert exception_info.value is error
        assert stream._eof is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(argnames="max_size, expected_size", argvalues=[(-1, None), (10, 10)])
    async def test_read_success(
        self,
        stream: WebTransportReceiveStream,
        mock_connection: MagicMock,
        max_size: int,
        expected_size: int | None,
    ) -> None:
        mock_connection._execute_request.return_value = b"data"

        data = await stream.read(max_size=max_size)

        assert data == b"data"
        assert stream._read_task is None
        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserReadStream)
        assert event.stream_id == 2
        assert event.max_size == expected_size

    @pytest.mark.asyncio
    async def test_read_zero_max_size(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.return_value = b""

        data = await stream.read(max_size=0)

        assert data == b""
        assert stream._eof is False
        assert stream._read_task is None
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserReadStream)
        assert event.max_size == 0

    @pytest.mark.asyncio
    async def test_read_zero_max_size_connection_error(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.read(max_size=0)

    @pytest.mark.asyncio
    async def test_read_zero_max_size_generic_error(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream receive failed stream_id=2"):
            await stream.read(max_size=0)

    @pytest.mark.asyncio
    async def test_readexactly(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_read = mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"12", b"34"])

        assert await stream.readexactly(size=4) == b"1234"
        spy_read.assert_has_awaits(calls=[call(max_size=4), call(max_size=2)])

    @pytest.mark.asyncio
    async def test_readexactly_connection_gone(
        self, stream: WebTransportReceiveStream, mock_session: MagicMock
    ) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await stream.readexactly(size=1)

    @pytest.mark.asyncio
    async def test_readexactly_incomplete(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"12", b""])

        with pytest.raises(expected_exception=asyncio.IncompleteReadError) as exception_info:
            await stream.readexactly(size=5)

        assert exception_info.value.partial == b"12"

    @pytest.mark.asyncio
    async def test_readexactly_negative_size(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        with pytest.raises(expected_exception=ValueError):
            await stream.readexactly(size=-1)

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_readexactly_timeout(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        async def execute_request(**kwargs: Any) -> bytes:
            await asyncio.Event().wait()
            return b""

        mock_connection.config.read_timeout = 0.01
        mock_connection._execute_request.side_effect = execute_request

        with pytest.raises(expected_exception=TimeoutError, match="wt_stream receive failed stream_id=2"):
            await stream.readexactly(size=1)

        assert stream._read_task is not None
        stream._read_task.cancel()

    @pytest.mark.asyncio
    async def test_readexactly_zero_size(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.return_value = b""

        assert await stream.readexactly(size=0) == b""

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserReadStream)
        assert event.max_size == 0

    @pytest.mark.asyncio
    async def test_readline(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_readuntil = mocker.patch.object(
            target=WebTransportReceiveStream, attribute="readuntil", return_value=b"line\n"
        )

        assert await stream.readline(max_size=10) == b"line\n"
        spy_readuntil.assert_awaited_once_with(separator=b"\n", max_size=10)

    @pytest.mark.asyncio
    async def test_readuntil_buffer_pushback(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"a\nb"])

        assert await stream.readuntil(separator=b"\n") == b"a\n"
        assert stream._buffer == bytearray(b"b")

    @pytest.mark.asyncio
    async def test_readuntil_connection_gone(self, stream: WebTransportReceiveStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError, match="wt_connection resolve failed"):
            await stream.readuntil(separator=b"\n")

    @pytest.mark.asyncio
    async def test_readuntil_empty_separator(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(expected_exception=ValueError):
            await stream.readuntil(separator=b"")

    @pytest.mark.asyncio
    async def test_readuntil_incomplete(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"ab", b""])

        with pytest.raises(expected_exception=asyncio.IncompleteReadError) as exception_info:
            await stream.readuntil(separator=b"\n")

        assert exception_info.value.partial == b"ab"

    @pytest.mark.asyncio
    async def test_readuntil_limit_at_separator(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"1234\n"])

        with pytest.raises(expected_exception=StreamError, match="wt_stream receive exceeded actual=5 limit=2"):
            await stream.readuntil(separator=b"\n", max_size=2)

    @pytest.mark.asyncio
    async def test_readuntil_limit_not_found(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"1", b"2", b"3"])

        with pytest.raises(expected_exception=StreamError, match="wt_stream receive exceeded actual=3 limit=2"):
            await stream.readuntil(separator=b"\n", max_size=2)

    @pytest.mark.asyncio
    async def test_readuntil_negative_max_size(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(expected_exception=ValueError):
            await stream.readuntil(separator=b"\n", max_size=-2)

    @pytest.mark.asyncio
    async def test_readuntil_success(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(target=WebTransportReceiveStream, attribute="read", side_effect=[b"a", b"b\n"])

        assert await stream.readuntil(separator=b"\n") == b"ab\n"
        assert stream._buffer == bytearray()

    @pytest.mark.asyncio
    async def test_readuntil_timeout(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        async def execute_request(**kwargs: Any) -> bytes:
            await asyncio.Event().wait()
            return b""

        mock_connection.config.read_timeout = 0.01
        mock_connection._execute_request.side_effect = execute_request

        with pytest.raises(expected_exception=TimeoutError, match="wt_stream receive failed stream_id=2"):
            await stream.readuntil(separator=b"\n")

        assert stream._read_task is not None
        stream._read_task.cancel()

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
        mock_connection._execute_request.return_value = None

        if test_error_code is not None:
            await stream.stop_receiving(error_code=test_error_code)
        else:
            await stream.stop_receiving()

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserStopSending)
        assert event.stream_id == 2
        assert event.error_code == expected_code

    @pytest.mark.asyncio
    async def test_stop_receiving_cancelled(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_closed(self, stream: WebTransportReceiveStream, mock_connection: MagicMock) -> None:
        stream._handle_closed()

        await stream.stop_receiving()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_receiving_connection_error(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_connection_gone(
        self, stream: WebTransportReceiveStream, mock_session: MagicMock
    ) -> None:
        mock_session._connection.return_value = None

        await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_generic_error(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream abort failed"):
            await stream.stop_receiving()

    @pytest.mark.asyncio
    async def test_stop_receiving_stream_error(
        self, stream: WebTransportReceiveStream, mock_connection: MagicMock
    ) -> None:
        mock_connection._execute_request.side_effect = StreamError(
            message="state", error_code=ErrorCodes.LIB_STREAM_STATE_ERROR
        )

        with pytest.raises(expected_exception=StreamError):
            await stream.stop_receiving()


class TestWebTransportSendStream(TestWebTransportStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportSendStream:
        return WebTransportSendStream(stream_id=3, session=mock_session, is_peer_initiated=False)

    @pytest.mark.asyncio
    async def test_close(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)

        await stream.close()

        spy_write.assert_awaited_once_with(data=b"", end_stream=True)

    @pytest.mark.asyncio
    async def test_close_closed(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)
        spy_reset = mocker.patch.object(target=WebTransportSendStream, attribute="reset", new_callable=mocker.AsyncMock)
        stream._handle_closed()

        await stream.close()
        await stream.close(error_code=1)

        spy_write.assert_not_awaited()
        spy_reset.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_with_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_reset = mocker.patch.object(target=WebTransportSendStream, attribute="reset", new_callable=mocker.AsyncMock)

        await stream.close(error_code=1)

        spy_reset.assert_awaited_once_with(error_code=1)

    @pytest.mark.asyncio
    async def test_context_manager(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock)

        async with stream as entered:
            assert entered is stream

        spy_close.assert_awaited_once_with(error_code=None)

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

        class CustomError(Exception):
            error_code = 999

        with pytest.raises(expected_exception=CustomError):
            async with stream:
                raise CustomError()

        spy_close.assert_awaited_once_with(error_code=999)

    @pytest.mark.asyncio
    async def test_context_manager_exit_generic_error(
        self, stream: WebTransportSendStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(target=WebTransportSendStream, attribute="close", new_callable=mocker.AsyncMock)

        with pytest.raises(expected_exception=RuntimeError):
            async with stream:
                raise RuntimeError("generic")

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APP_GENERIC_ERROR)

    def test_init(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportSendStream)
        assert stream._stream_id == 3
        assert stream._session() is mock_session
        assert stream._is_peer_initiated is False
        assert stream._is_closed is False
        assert stream.events is not None

        assert not hasattr(stream, "__dict__")

    def test_properties(self, stream: WebTransportStream, mock_session: MagicMock) -> None:
        assert isinstance(stream, WebTransportSendStream)
        assert stream.direction == StreamDirection.SEND_ONLY
        assert stream.is_closed is False
        assert stream.is_peer_initiated is False
        assert stream.session is mock_session
        assert stream.stream_id == 3

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
        mock_connection._execute_request.return_value = None

        if test_error_code is not None:
            await stream.reset(error_code=test_error_code)
        else:
            await stream.reset()

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserResetStream)
        assert event.stream_id == 3
        assert event.error_code == expected_code

    @pytest.mark.asyncio
    async def test_reset_cancelled(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_closed(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        stream._handle_closed()

        await stream.reset()

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_reset_connection_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ConnectionError(message="fail")

        with pytest.raises(expected_exception=ConnectionError):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_connection_gone(self, stream: WebTransportSendStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_generic_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream abort failed"):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_reset_stream_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = StreamError(
            message="state", error_code=ErrorCodes.LIB_STREAM_STATE_ERROR
        )

        with pytest.raises(expected_exception=StreamError):
            await stream.reset()

    @pytest.mark.asyncio
    async def test_write(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.return_value = None

        await stream.write(data=b"test", end_stream=True)

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserWriteStream)
        assert event.stream_id == 3
        assert event.data == b"test"
        assert event.end_stream is True

    @pytest.mark.asyncio
    async def test_write_all(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        mocker.patch(target="pywebtransport.stream._WRITE_CHUNK_SIZE", new=2)
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)

        await stream.write_all(data=b"1234", end_stream=True)

        assert spy_write.await_count == 2
        spy_write.assert_has_awaits(calls=[call(data=b"12", end_stream=False), call(data=b"34", end_stream=True)])

    @pytest.mark.asyncio
    @pytest.mark.parametrize(argnames="end_stream", argvalues=[False, True])
    async def test_write_all_empty(
        self, stream: WebTransportSendStream, mocker: MockerFixture, end_stream: bool
    ) -> None:
        spy_write = mocker.patch.object(target=WebTransportSendStream, attribute="write", new_callable=mocker.AsyncMock)

        await stream.write_all(data=b"", end_stream=end_stream)

        spy_write.assert_awaited_once_with(data=b"", end_stream=end_stream)

    @pytest.mark.asyncio
    async def test_write_all_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        mocker.patch.object(
            target=WebTransportSendStream, attribute="write", side_effect=StreamError(message="fail", stream_id=3)
        )

        with pytest.raises(expected_exception=StreamError):
            await stream.write_all(data=b"data")

    @pytest.mark.asyncio
    async def test_write_all_type_error(self, stream: WebTransportSendStream) -> None:
        with pytest.raises(expected_exception=StreamError, match="wt_stream validate invalid"):
            await stream.write_all(data=123)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_write_cancelled(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = asyncio.CancelledError()

        with pytest.raises(expected_exception=asyncio.CancelledError):
            await stream.write(data=b"test")

    @pytest.mark.asyncio
    async def test_write_closed(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        stream._handle_closed()

        with pytest.raises(expected_exception=StreamError, match="wt_stream resolve failed stream_id=3"):
            await stream.write(data=b"test")

        mock_connection._execute_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_connection_gone(self, stream: WebTransportSendStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(expected_exception=ConnectionError):
            await stream.write(data=b"a")

    @pytest.mark.asyncio
    async def test_write_empty_not_end_stream(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.return_value = None

        await stream.write(data=b"", end_stream=False)

        mock_connection._execute_request.assert_awaited_once()
        event = mock_connection._execute_request.call_args.kwargs["event_factory"](123)
        assert isinstance(event, UserWriteStream)
        assert event.data == b""
        assert event.end_stream is False

    @pytest.mark.asyncio
    async def test_write_generic_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = ValueError("fail")

        with pytest.raises(expected_exception=StreamError, match="wt_stream send failed"):
            await stream.write(data=b"payload")

    @pytest.mark.asyncio
    async def test_write_stream_error(self, stream: WebTransportSendStream, mock_connection: MagicMock) -> None:
        mock_connection._execute_request.side_effect = StreamError(message="fail")

        with pytest.raises(expected_exception=StreamError):
            await stream.write(data=b"payload")

    @pytest.mark.asyncio
    async def test_write_type_error(self, stream: WebTransportSendStream) -> None:
        with pytest.raises(expected_exception=StreamError, match="wt_stream validate invalid"):
            await stream.write(data=123)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    argnames="data, expected_type, expected_content",
    argvalues=[
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
