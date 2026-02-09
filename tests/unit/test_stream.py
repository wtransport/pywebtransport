"""Unit tests for the pywebtransport.stream.stream module."""

import asyncio
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
from pywebtransport._adapter.client import WebTransportClientProtocol
from pywebtransport._protocol.events import (
    UserGetStreamDiagnostics,
    UserResetStream,
    UserSendStreamData,
    UserStopSending,
    UserStreamRead,
)
from pywebtransport.connection import WebTransportConnection
from pywebtransport.stream import StreamDiagnostics, _BaseStream
from pywebtransport.types import StreamDirection, StreamState


class TestBaseStream:

    @pytest.fixture
    def mock_connection(self, mock_protocol: MagicMock, mocker: MockerFixture) -> MagicMock:
        conn = mocker.Mock(spec=WebTransportConnection)
        conn.config = mocker.Mock(spec=ClientConfig)
        conn.config.read_timeout = 0.1
        conn.config.write_timeout = 0.1
        conn.config.max_stream_read_buffer = 1024
        conn._protocol = mock_protocol
        return cast(MagicMock, conn)

    @pytest.fixture
    def mock_protocol(self, mocker: MockerFixture) -> MagicMock:
        proto = mocker.Mock(spec=WebTransportClientProtocol)
        proto.create_request.side_effect = lambda: (1, asyncio.Future())
        return cast(MagicMock, proto)

    @pytest.fixture
    def mock_session(self, mock_connection: MagicMock, mocker: MockerFixture) -> MagicMock:
        session = mocker.Mock(spec=WebTransportSession)
        session._connection = mocker.Mock(return_value=mock_connection)
        return cast(MagicMock, session)

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> _BaseStream:
        return _BaseStream(session=mock_session, stream_id=1, is_remote=False)

    @pytest.mark.asyncio
    async def test_diagnostics_connection_closed_error(self, stream: _BaseStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[Any] = asyncio.Future()
        fut.set_exception(ConnectionError("Closed"))
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)

        with pytest.raises(StreamError, match="Connection is closed"):
            await stream.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_connection_gone(self, stream: _BaseStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(ConnectionError, match="Connection is gone"):
            await stream.diagnostics()

    @pytest.mark.asyncio
    async def test_diagnostics_success_deque(self, stream: _BaseStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[dict[str, Any]] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)

        data = {
            "stream_id": 1,
            "session_id": 100,
            "direction": StreamDirection.BIDIRECTIONAL,
            "state": StreamState.OPEN,
            "is_peer_initiated": False,
            "created_at": 0.0,
            "bytes_sent": 0,
            "bytes_received": 0,
            "read_buffer_size": 0,
            "write_buffer_size": 4,
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
        }
        fut.set_result(data)

        diag = await stream.diagnostics()

        assert isinstance(mock_protocol.send_event.call_args.kwargs["event"], UserGetStreamDiagnostics)
        assert diag.is_peer_initiated is False

    @pytest.mark.asyncio
    async def test_diagnostics_success_no_conversion_needed(
        self, stream: _BaseStream, mock_protocol: MagicMock
    ) -> None:
        fut: asyncio.Future[dict[str, Any]] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)

        data = {
            "stream_id": 1,
            "session_id": 100,
            "direction": StreamDirection.BIDIRECTIONAL,
            "state": StreamState.OPEN,
            "is_peer_initiated": True,
            "created_at": 0.0,
            "bytes_sent": 0,
            "bytes_received": 0,
            "read_buffer_size": 0,
            "write_buffer_size": 0,
            "close_code": None,
            "close_reason": None,
            "closed_at": None,
        }
        fut.set_result(data)

        diag = await stream.diagnostics()
        assert diag.is_peer_initiated is True

    @pytest.mark.asyncio
    async def test_is_closed(self, stream: _BaseStream) -> None:
        stream._cached_state = StreamState.OPEN

        assert not stream.is_closed

        stream._cached_state = StreamState.CLOSED

        assert stream.is_closed

    @pytest.mark.asyncio
    async def test_is_remote_property(self, mock_session: MagicMock) -> None:
        s1 = _BaseStream(session=mock_session, stream_id=1, is_remote=True)
        assert s1.is_remote is True

        s2 = _BaseStream(session=mock_session, stream_id=2, is_remote=False)
        assert s2.is_remote is False

    @pytest.mark.asyncio
    async def test_on_closed_handler(self, stream: _BaseStream) -> None:
        stream.events.emit_nowait(event_type="stream_closed", data={})
        await asyncio.sleep(0)

        assert stream.state == StreamState.CLOSED

    @pytest.mark.asyncio
    async def test_repr(self, stream: _BaseStream) -> None:
        assert "_BaseStream" in repr(stream)
        assert "id=1" in repr(stream)

    @pytest.mark.asyncio
    async def test_session_property_gone(self, stream: _BaseStream) -> None:
        stream._session = lambda: None  # type: ignore

        with pytest.raises(ConnectionError, match="Session is gone"):
            _ = stream.session


class TestStreamDiagnostics:

    def test_init(self) -> None:
        diag = StreamDiagnostics(
            stream_id=1,
            session_id=100,
            direction=StreamDirection.BIDIRECTIONAL,
            state=StreamState.OPEN,
            is_peer_initiated=True,
            created_at=100.0,
            bytes_sent=10,
            bytes_received=20,
            read_buffer_size=0,
            write_buffer_size=0,
            close_code=None,
            close_reason=None,
            closed_at=None,
        )

        assert diag.stream_id == 1
        assert diag.state == StreamState.OPEN
        assert diag.is_peer_initiated is True


@pytest.mark.asyncio
class TestWebTransportReceiveStream(TestBaseStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportReceiveStream:
        return WebTransportReceiveStream(session=mock_session, stream_id=2, is_remote=False)

    async def test_aiter(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "read", side_effect=[b"1", b"2", b""])

        res = [chunk async for chunk in stream]

        assert res == [b"1", b"2"]

    async def test_close(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_stop = mocker.patch.object(stream, "stop_receiving", new_callable=mocker.AsyncMock)

        await stream.close()

        spy_stop.assert_awaited_once()

    async def test_context_manager(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        spy_stop = mocker.patch.object(stream, "stop_receiving", new_callable=mocker.AsyncMock)

        async with stream as s:
            assert s is stream

        spy_stop.assert_awaited_once()

    async def test_properties(self, stream: WebTransportReceiveStream) -> None:
        assert stream.direction == StreamDirection.RECEIVE_ONLY
        assert stream.can_read is True

        stream._cached_state = StreamState.RESET_RECEIVED

        assert stream.can_read is False

    async def test_read_all(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "read", side_effect=[b"a", b"b", b""])

        assert await stream.read_all() == b"ab"

    async def test_read_closed(self, stream: WebTransportReceiveStream) -> None:
        stream._cached_state = StreamState.CLOSED

        assert await stream.read() == b""

    async def test_read_eof(self, stream: WebTransportReceiveStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[bytes] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_result(b"")

        data = await stream.read()

        assert data == b""
        assert stream._read_eof is True

        mock_protocol.create_request.reset_mock()

        assert await stream.read() == b""
        mock_protocol.create_request.assert_not_called()

    async def test_read_generic_error(self, stream: WebTransportReceiveStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[bytes] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_exception(ValueError("Fail"))

        with pytest.raises(ValueError):
            await stream.read()

    async def test_read_no_connection(self, stream: WebTransportReceiveStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(ConnectionError):
            await stream.read()

    async def test_read_stream_error(self, stream: WebTransportReceiveStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[bytes] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_exception(StreamError("State", error_code=ErrorCodes.STREAM_STATE_ERROR))

        assert await stream.read() == b""

        mock_protocol.create_request.reset_mock()

        assert await stream.read() == b""
        mock_protocol.create_request.assert_not_called()

    async def test_read_stream_error_reraise(self, stream: WebTransportReceiveStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[bytes] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_exception(StreamError("Other", error_code=ErrorCodes.H3_FRAME_ERROR))

        with pytest.raises(StreamError, match="Other"):
            await stream.read()

    async def test_read_success(self, stream: WebTransportReceiveStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[bytes] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_result(b"data")

        data = await stream.read(max_bytes=10)

        assert data == b"data"
        event = mock_protocol.send_event.call_args.kwargs["event"]
        assert isinstance(event, UserStreamRead)
        assert event.max_bytes == 10

    async def test_readexactly(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "read", side_effect=[b"12", b"34"])

        assert await stream.readexactly(n=4) == b"1234"

    async def test_readexactly_incomplete(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "read", return_value=b"")

        with pytest.raises(asyncio.IncompleteReadError):
            await stream.readexactly(n=5)

    async def test_readexactly_no_connection(self, stream: WebTransportReceiveStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(ConnectionError, match="Connection is gone"):
            await stream.readexactly(n=1)

    async def test_readexactly_params(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(ValueError):
            await stream.readexactly(n=-1)

        assert await stream.readexactly(n=0) == b""

    async def test_readexactly_timeout(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch("asyncio.timeout", side_effect=asyncio.TimeoutError)

        with pytest.raises(TimeoutError, match="readexactly timed out"):
            await stream.readexactly(n=1)

    async def test_readline(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "readuntil", return_value=b"line\n")

        assert await stream.readline() == b"line\n"

    async def test_readuntil_empty_separator(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(ValueError):
            await stream.readuntil(separator=b"")

    async def test_readuntil_incomplete(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "read", return_value=b"")

        with pytest.raises(asyncio.IncompleteReadError):
            await stream.readuntil(separator=b"\n")

    async def test_readuntil_limit(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "read", side_effect=[b"1", b"2", b"3"])

        with pytest.raises(StreamError, match="Separator not found within limit"):
            await stream.readuntil(separator=b"\n", limit=2)

    async def test_readuntil_no_connection(self, stream: WebTransportReceiveStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(ConnectionError, match="Connection is gone"):
            await stream.readuntil(separator=b"\n")

    async def test_readuntil_params(self, stream: WebTransportReceiveStream) -> None:
        with pytest.raises(ValueError):
            await stream.readuntil(separator=b"")

    async def test_readuntil_success(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "read", side_effect=[b"a", b"b", b"\n"])

        assert await stream.readuntil(separator=b"\n") == b"ab\n"

    async def test_readuntil_timeout(self, stream: WebTransportReceiveStream, mocker: MockerFixture) -> None:
        mocker.patch("asyncio.timeout", side_effect=asyncio.TimeoutError)

        with pytest.raises(TimeoutError, match="readuntil timed out"):
            await stream.readuntil(separator=b"\n")

    async def test_repr(self, stream: WebTransportReceiveStream) -> None:  # type: ignore[override]
        assert "WebTransportReceiveStream" in repr(stream)
        assert "id=2" in repr(stream)

    async def test_stop_receiving(self, stream: WebTransportReceiveStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_result(None)

        await stream.stop_receiving(error_code=123)

        assert stream.state == StreamState.RESET_RECEIVED
        event = mock_protocol.send_event.call_args.kwargs["event"]
        assert isinstance(event, UserStopSending)
        assert event.error_code == 123

    async def test_stop_receiving_no_connection(
        self, stream: WebTransportReceiveStream, mock_session: MagicMock
    ) -> None:
        mock_session._connection.return_value = None

        await stream.stop_receiving()


@pytest.mark.asyncio
class TestWebTransportSendStream(TestBaseStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportSendStream:
        return WebTransportSendStream(session=mock_session, stream_id=3, is_remote=False)

    async def test_close(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(stream, "write", new_callable=mocker.AsyncMock)

        await stream.close()

        spy_write.assert_awaited_once_with(data=b"", end_stream=True)
        assert stream.state == StreamState.HALF_CLOSED_LOCAL

    async def test_close_generic_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "write", side_effect=ValueError("Boom"))

        with pytest.raises(ValueError):
            await stream.close()

    async def test_close_stream_error_ignored(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "write", side_effect=StreamError("Expected"))

        await stream.close()

    async def test_close_with_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_reset = mocker.patch.object(stream, "reset", new_callable=mocker.AsyncMock)

        await stream.close(error_code=1)

        spy_reset.assert_awaited_once_with(error_code=1)

    async def test_context_manager_exit_cancelled(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(stream, "close", new_callable=mocker.AsyncMock)

        with pytest.raises(asyncio.CancelledError):
            async with stream:
                raise asyncio.CancelledError()

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APPLICATION_ERROR)

    async def test_context_manager_exit_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(stream, "close", new_callable=mocker.AsyncMock)

        class MyErr(Exception):
            error_code = 999

        with pytest.raises(MyErr):
            async with stream:
                raise MyErr()

        spy_close.assert_awaited_once_with(error_code=999)

    async def test_context_manager_exit_generic_error(
        self, stream: WebTransportSendStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(stream, "close", new_callable=mocker.AsyncMock)

        with pytest.raises(RuntimeError):
            async with stream:
                raise RuntimeError("Generic")

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APPLICATION_ERROR)

    async def test_context_manager_exit_success(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(stream, "close", new_callable=mocker.AsyncMock)

        async with stream:
            pass

        spy_close.assert_awaited_once_with(error_code=None)

    async def test_properties(self, stream: WebTransportSendStream) -> None:
        assert stream.direction == StreamDirection.SEND_ONLY
        assert stream.can_write is True

        stream._cached_state = StreamState.RESET_SENT

        assert stream.can_write is False

    async def test_repr(self, stream: WebTransportSendStream) -> None:  # type: ignore[override]
        assert "WebTransportSendStream" in repr(stream)

    async def test_reset(self, stream: WebTransportSendStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_result(None)

        await stream.reset(error_code=99)

        assert stream.state == StreamState.RESET_SENT
        event = mock_protocol.send_event.call_args.kwargs["event"]
        assert isinstance(event, UserResetStream)
        assert event.error_code == 99

    async def test_reset_no_connection(self, stream: WebTransportSendStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(ConnectionError):
            await stream.reset()

    async def test_write(self, stream: WebTransportSendStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_result(None)

        await stream.write(data=b"test", end_stream=True)

        event = mock_protocol.send_event.call_args.kwargs["event"]
        assert isinstance(event, UserSendStreamData)
        assert event.data == b"test"
        assert event.end_stream is True

    async def test_write_all(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(stream, "write", new_callable=mocker.AsyncMock)

        await stream.write_all(data=b"1234", chunk_size=2, end_stream=True)

        assert spy_write.await_count == 2
        spy_write.assert_has_awaits([call(data=b"12", end_stream=False), call(data=b"34", end_stream=True)])

    async def test_write_all_empty_end(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        spy_write = mocker.patch.object(stream, "write", new_callable=mocker.AsyncMock)

        await stream.write_all(data=b"", end_stream=True)

        spy_write.assert_awaited_once_with(data=b"", end_stream=True)

    async def test_write_all_error(self, stream: WebTransportSendStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream, "write", side_effect=StreamError("Fail", stream_id=3))

        with pytest.raises(StreamError):
            await stream.write_all(data=b"data")

    async def test_write_early_return(self, stream: WebTransportSendStream, mock_protocol: MagicMock) -> None:
        await stream.write(data=b"", end_stream=False)

        mock_protocol.create_request.assert_not_called()

    async def test_write_no_connection(self, stream: WebTransportSendStream, mock_session: MagicMock) -> None:
        mock_session._connection.return_value = None

        with pytest.raises(ConnectionError):
            await stream.write(data=b"a")

    async def test_write_timeout_propagation(self, stream: WebTransportSendStream, mock_protocol: MagicMock) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        mock_protocol.create_request.side_effect = None
        mock_protocol.create_request.return_value = (1, fut)
        fut.set_exception(TimeoutError("Timeout"))

        with pytest.raises(TimeoutError):
            await stream.write(data=b"payload")

    async def test_write_type_error(self, stream: WebTransportSendStream) -> None:
        with pytest.raises(TypeError):
            await stream.write(data=123)  # type: ignore[arg-type]


@pytest.mark.asyncio
class TestWebTransportStream(TestBaseStream):

    @pytest.fixture
    def stream(self, mock_session: MagicMock) -> WebTransportStream:
        return WebTransportStream(session=mock_session, stream_id=4, is_remote=False)

    async def test_aiter(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._reader, "__anext__", side_effect=[b"1", StopAsyncIteration])

        chunks = [c async for c in stream]

        assert chunks == [b"1"]

    async def test_close(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_send_close = mocker.patch.object(stream._writer, "close", new_callable=mocker.AsyncMock)
        spy_recv_stop = mocker.patch.object(stream._reader, "stop_receiving", new_callable=mocker.AsyncMock)

        await stream.close(error_code=10)

        spy_send_close.assert_awaited_once_with(error_code=10)
        spy_recv_stop.assert_awaited_once_with(error_code=10)

    async def test_close_no_args(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_send_close = mocker.patch.object(stream._writer, "close", new_callable=mocker.AsyncMock)
        spy_recv_stop = mocker.patch.object(stream._reader, "stop_receiving", new_callable=mocker.AsyncMock)

        await stream.close()

        spy_send_close.assert_awaited_once_with(error_code=None)
        spy_recv_stop.assert_awaited_once_with(error_code=ErrorCodes.NO_ERROR)

    async def test_composition(self, stream: WebTransportStream) -> None:
        assert isinstance(stream._reader, WebTransportReceiveStream)
        assert isinstance(stream._writer, WebTransportSendStream)
        assert stream.direction == StreamDirection.BIDIRECTIONAL

    async def test_composition_properties(self, stream: WebTransportStream) -> None:
        assert stream.can_read is True
        assert stream.can_write is True

    async def test_context_manager(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        spy_close = mocker.patch.object(stream, "close", new_callable=mocker.AsyncMock)

        async with stream:
            pass

        spy_close.assert_awaited_once_with(error_code=None)

    @pytest.mark.parametrize("exception_type", [ValueError, asyncio.CancelledError])
    async def test_context_manager_error(
        self, stream: WebTransportStream, mocker: MockerFixture, exception_type: type[BaseException]
    ) -> None:
        spy_close = mocker.patch.object(stream, "close", new_callable=mocker.AsyncMock)

        with pytest.raises(exception_type):
            async with stream:
                raise exception_type()

        spy_close.assert_awaited_once_with(error_code=ErrorCodes.APPLICATION_ERROR)

    async def test_context_manager_exit_error_with_code(
        self, stream: WebTransportStream, mocker: MockerFixture
    ) -> None:
        spy_close = mocker.patch.object(stream, "close", new_callable=mocker.AsyncMock)

        class MyErr(Exception):
            error_code = 12345

        with pytest.raises(MyErr):
            async with stream:
                raise MyErr()

        spy_close.assert_awaited_once_with(error_code=12345)

    async def test_delegated_read(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._reader, "read", return_value=b"rd")

        assert await stream.read() == b"rd"

    async def test_delegated_read_all(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._reader, "read_all", return_value=b"all")

        assert await stream.read_all() == b"all"

    async def test_delegated_readexactly(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._reader, "readexactly", return_value=b"ex")

        assert await stream.readexactly(n=2) == b"ex"

    async def test_delegated_readline(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._reader, "readline", return_value=b"ln")

        assert await stream.readline() == b"ln"

    async def test_delegated_readuntil(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._reader, "readuntil", return_value=b"ut")

        assert await stream.readuntil(separator=b"t") == b"ut"

    async def test_delegated_reset(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._writer, "reset", new_callable=mocker.AsyncMock)

        await stream.reset(error_code=2)

        cast(MagicMock, stream._writer.reset).assert_awaited_once_with(error_code=2)

    async def test_delegated_stop_receiving(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._reader, "stop_receiving", new_callable=mocker.AsyncMock)

        await stream.stop_receiving(error_code=1)

        cast(MagicMock, stream._reader.stop_receiving).assert_awaited_once_with(error_code=1)

    async def test_delegated_write(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._writer, "write", new_callable=mocker.AsyncMock)

        await stream.write(data=b"wr")

        cast(MagicMock, stream._writer.write).assert_awaited_once_with(data=b"wr", end_stream=False)

    async def test_delegated_write_all(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        mocker.patch.object(stream._writer, "write_all", new_callable=mocker.AsyncMock)

        await stream.write_all(data=b"wall")

        cast(MagicMock, stream._writer.write_all).assert_awaited_once_with(
            data=b"wall", chunk_size=65536, end_stream=False
        )

    async def test_on_closed_propagates(self, stream: WebTransportStream, mocker: MockerFixture) -> None:
        stream._reader = mocker.Mock()
        stream._writer = mocker.Mock()
        stream.events.emit_nowait(event_type="stream_closed", data={})
        await asyncio.sleep(0)

        assert stream.state == StreamState.CLOSED
        cast(MagicMock, stream._reader)._on_closed.assert_called_once()
        cast(MagicMock, stream._writer)._on_closed.assert_called_once()

    async def test_repr(self, stream: WebTransportStream) -> None:  # type: ignore[override]
        assert "WebTransportStream" in repr(stream)
