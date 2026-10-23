"""WebTransport stream."""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, Self

from pywebtransport._controller.events import (
    UserDiagnoseStream,
    UserReadStream,
    UserResetStream,
    UserStopSending,
    UserWriteStream,
)
from pywebtransport.constants import ErrorCodes
from pywebtransport.events import EventEmitter
from pywebtransport.exceptions import ConnectionError, StreamError, TimeoutError
from pywebtransport.types import (
    Buffer,
    ErrorCode,
    SessionId,
    StreamDirection,
    StreamId,
    StreamReceiveState,
    StreamSendState,
)

if TYPE_CHECKING:
    from pywebtransport.session import WebTransportSession

__all__: list[str] = [
    "StreamDiagnostics",
    "WebTransportBidirectionalStream",
    "WebTransportReceiveStream",
    "WebTransportSendStream",
    "WebTransportStream",
]

_READ_CHUNK_SIZE: Final[int] = 65536
_WRITE_CHUNK_SIZE: Final[int] = 65536


@dataclass(frozen=True, kw_only=True, slots=True)
class StreamDiagnostics:
    """Encapsulate stream diagnostic data."""

    created_at: float
    direction: StreamDirection
    is_peer_initiated: bool
    receive_error_code: ErrorCode | None
    receive_state: StreamReceiveState | None
    send_error_code: ErrorCode | None
    send_state: StreamSendState | None
    session_id: SessionId
    stream_id: StreamId


class WebTransportStream:
    """Manage the high-level WebTransport stream."""

    __slots__ = ("_stream_id", "_session", "_is_peer_initiated", "_is_closed", "events")

    def __init__(self, *, stream_id: StreamId, session: WebTransportSession, is_peer_initiated: bool) -> None:
        """Initialize the instance."""
        self._stream_id = stream_id
        self._session = weakref.ref(session)
        self._is_peer_initiated = is_peer_initiated

        self._is_closed = False
        self.events = EventEmitter()

    @property
    def is_closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._is_closed

    @property
    def is_peer_initiated(self) -> bool:
        """Return True if the stream was initiated by the peer."""
        return self._is_peer_initiated

    @property
    def session(self) -> WebTransportSession:
        """Return the session handle."""
        session = self._session()
        if session is None:
            raise ConnectionError(message="wt_session resolve failed")
        return session

    @property
    def stream_id(self) -> StreamId:
        """Return the stream ID."""
        return self._stream_id

    async def diagnose(self) -> StreamDiagnostics:
        """Diagnose the stream."""
        if self._is_closed:
            raise StreamError(message=f"wt_stream resolve failed stream_id={self.stream_id}", stream_id=self.stream_id)

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            diagnostic_data: dict[str, Any] = await connection._execute_request(
                event_factory=lambda request_id: UserDiagnoseStream(request_id=request_id, stream_id=self.stream_id)
            )
        except (ConnectionError, StreamError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream resolve failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

        return StreamDiagnostics(**diagnostic_data)

    def _handle_closed(self) -> None:
        self._is_closed = True


class WebTransportBidirectionalStream(WebTransportStream):
    """Manage the sending and receiving parts of a WebTransport stream."""

    __slots__ = ("_receiver", "_sender")

    def __init__(self, *, stream_id: StreamId, session: WebTransportSession, is_peer_initiated: bool) -> None:
        """Initialize the instance."""
        super().__init__(stream_id=stream_id, session=session, is_peer_initiated=is_peer_initiated)

        self._receiver = WebTransportReceiveStream(
            stream_id=stream_id, session=session, is_peer_initiated=is_peer_initiated
        )
        self._sender = WebTransportSendStream(stream_id=stream_id, session=session, is_peer_initiated=is_peer_initiated)

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        exit_error_code: ErrorCode | None = None

        if isinstance(exc_value, asyncio.CancelledError):
            exit_error_code = ErrorCodes.APP_CANCELLED
        elif isinstance(exc_value, BaseException):
            exit_error_code = getattr(exc_value, "error_code", ErrorCodes.APP_GENERIC_ERROR)

        await self.close(error_code=exit_error_code)

    @property
    def direction(self) -> StreamDirection:
        """Return the stream direction."""
        return StreamDirection.BIDIRECTIONAL

    async def close(self, *, error_code: ErrorCode | None = None) -> None:
        """Terminate the sending and receiving parts of the stream."""
        if error_code is None:
            await self._receiver.close()
        else:
            await self._receiver.close(error_code=error_code)
        await self._sender.close(error_code=error_code)

    async def read(self, *, max_size: int = -1) -> bytes:
        """Read data from the stream."""
        return await self._receiver.read(max_size=max_size)

    async def read_all(self) -> bytes:
        """Read all data from the stream until EOF."""
        return await self._receiver.read_all()

    async def readexactly(self, *, size: int) -> bytes:
        """Read the exact size from the stream."""
        return await self._receiver.readexactly(size=size)

    async def readline(self, *, max_size: int = -1) -> bytes:
        """Read a line from the stream."""
        return await self._receiver.readline(max_size=max_size)

    async def readuntil(self, *, separator: bytes, max_size: int = -1) -> bytes:
        """Read data from the stream until a separator is found."""
        return await self._receiver.readuntil(separator=separator, max_size=max_size)

    async def reset(self, *, error_code: ErrorCode = ErrorCodes.APP_NO_ERROR) -> None:
        """Abandon stream transmission."""
        await self._sender.reset(error_code=error_code)

    async def stop_receiving(self, *, error_code: ErrorCode = ErrorCodes.APP_NO_ERROR) -> None:
        """Signal the peer to stop sending data."""
        await self._receiver.stop_receiving(error_code=error_code)

    async def write(self, *, data: Buffer, end_stream: bool = False) -> None:
        """Write data to the stream."""
        await self._sender.write(data=data, end_stream=end_stream)

    async def write_all(self, *, data: Buffer, end_stream: bool = False) -> None:
        """Write data to the stream in chunks."""
        await self._sender.write_all(data=data, end_stream=end_stream)

    def _handle_closed(self) -> None:
        super()._handle_closed()
        self._receiver._handle_closed()
        self._sender._handle_closed()

    def __aiter__(self) -> AsyncIterator[bytes]:
        """Iterate over the stream chunks."""
        return self

    async def __anext__(self) -> bytes:
        """Get the next chunk of data."""
        return await self._receiver.__anext__()


class WebTransportReceiveStream(WebTransportStream):
    """Manage the receiving part of a WebTransport stream."""

    __slots__ = ("_buffer", "_eof", "_read_task")

    def __init__(self, *, stream_id: StreamId, session: WebTransportSession, is_peer_initiated: bool) -> None:
        """Initialize the instance."""
        super().__init__(stream_id=stream_id, session=session, is_peer_initiated=is_peer_initiated)

        self._buffer = bytearray()
        self._eof = False
        self._read_task: asyncio.Task[bytes | Exception] | None = None

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        error_code: ErrorCode = ErrorCodes.APP_NO_ERROR

        if isinstance(exc_value, asyncio.CancelledError):
            error_code = ErrorCodes.APP_CANCELLED
        elif isinstance(exc_value, BaseException):
            error_code = getattr(exc_value, "error_code", ErrorCodes.APP_GENERIC_ERROR)

        await self.close(error_code=error_code)

    @property
    def direction(self) -> StreamDirection:
        """Return the stream direction."""
        return StreamDirection.RECEIVE_ONLY

    async def close(self, *, error_code: ErrorCode = ErrorCodes.APP_NO_ERROR) -> None:
        """Terminate the receiving part of the stream."""
        await self.stop_receiving(error_code=error_code)

    async def read(self, *, max_size: int = -1) -> bytes:
        """Read data from the stream."""
        if max_size < -1:
            raise ValueError(f"wt_stream validate invalid actual={max_size}")

        while True:
            if self._buffer:
                if max_size < 0:
                    data = bytes(self._buffer)
                    self._buffer.clear()
                    return data
                data = bytes(self._buffer[:max_size])
                del self._buffer[:max_size]
                return data

            if self._eof:
                return b""

            read_task = self._read_task
            if read_task is None or max_size == 0:
                if self._is_closed:
                    raise StreamError(
                        message=f"wt_stream resolve failed stream_id={self.stream_id}", stream_id=self.stream_id
                    )

                connection = self.session._connection()
                if connection is None:
                    raise ConnectionError(message="wt_connection resolve failed")

                size = max_size if max_size >= 0 else None

                if max_size == 0:
                    try:
                        await connection._execute_request(
                            event_factory=lambda request_id: UserReadStream(
                                request_id=request_id, stream_id=self.stream_id, max_size=size
                            )
                        )
                    except (ConnectionError, StreamError, asyncio.CancelledError):
                        raise
                    except Exception as e:
                        raise StreamError.from_cause(
                            message=f"wt_stream receive failed stream_id={self.stream_id}",
                            cause=e,
                            stream_id=self.stream_id,
                        ) from e
                    return b""

                async def receive(*, request: Awaitable[bytes]) -> bytes | Exception:
                    try:
                        return await request
                    except Exception as e:
                        return e

                read_task = asyncio.create_task(
                    coro=receive(
                        request=connection._execute_request(
                            event_factory=lambda request_id: UserReadStream(
                                request_id=request_id, stream_id=self.stream_id, max_size=size
                            )
                        )
                    )
                )
                self._read_task = read_task

            outcome = await asyncio.shield(read_task)
            if self._read_task is not read_task:
                continue
            self._read_task = None

            if isinstance(outcome, (ConnectionError, StreamError)):
                raise outcome
            if isinstance(outcome, Exception):
                raise StreamError.from_cause(
                    message=f"wt_stream receive failed stream_id={self.stream_id}",
                    cause=outcome,
                    stream_id=self.stream_id,
                ) from outcome

            if not outcome:
                self._eof = True
                return b""

            if max_size >= 0 and len(outcome) > max_size:
                self._buffer.extend(outcome[max_size:])
                return bytes(outcome[:max_size])
            return bytes(outcome)

    async def read_all(self) -> bytes:
        """Read all data from the stream until EOF."""
        chunks: list[bytes] = []
        async for chunk in self:
            chunks.append(chunk)
        return b"".join(chunks)

    async def readexactly(self, *, size: int) -> bytes:
        """Read the exact size from the stream."""
        if size < 0:
            raise ValueError(f"wt_stream validate invalid actual={size}")
        if size == 0:
            await self.read(max_size=0)
            return b""

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")
        read_timeout = connection.config.read_timeout

        chunks: list[bytes] = []
        bytes_read = 0

        try:
            async with asyncio.timeout(read_timeout):
                while bytes_read < size:
                    needed = size - bytes_read
                    chunk = await self.read(max_size=needed)
                    if not chunk:
                        partial = b"".join(chunks)
                        raise asyncio.IncompleteReadError(partial, size)

                    chunks.append(chunk)
                    bytes_read += len(chunk)
        except asyncio.TimeoutError:
            raise TimeoutError(message=f"wt_stream receive failed stream_id={self.stream_id}") from None

        return b"".join(chunks)

    async def readline(self, *, max_size: int = -1) -> bytes:
        """Read a line from the stream."""
        return await self.readuntil(separator=b"\n", max_size=max_size)

    async def readuntil(self, *, separator: bytes, max_size: int = -1) -> bytes:
        """Read data from the stream until a separator is found."""
        if not separator:
            raise ValueError(f"wt_stream validate invalid actual={separator}")
        if max_size < -1:
            raise ValueError(f"wt_stream validate invalid actual={max_size}")

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")
        read_timeout = connection.config.read_timeout

        data = bytearray()
        try:
            async with asyncio.timeout(read_timeout):
                while True:
                    chunk = await self.read(max_size=_READ_CHUNK_SIZE)
                    if not chunk:
                        raise asyncio.IncompleteReadError(bytes(data), None)

                    data.extend(chunk)
                    index = data.find(separator)
                    if index != -1:
                        end = index + len(separator)
                        if max_size >= 0 and end > max_size:
                            raise StreamError(
                                message=f"wt_stream receive exceeded actual={end} limit={max_size} "
                                f"stream_id={self.stream_id}",
                                stream_id=self.stream_id,
                            )
                        self._buffer.extend(data[end:])
                        return bytes(data[:end])

                    if max_size >= 0 and len(data) > max_size:
                        raise StreamError(
                            message=f"wt_stream receive exceeded actual={len(data)} limit={max_size} "
                            f"stream_id={self.stream_id}",
                            stream_id=self.stream_id,
                        )
        except asyncio.TimeoutError:
            raise TimeoutError(message=f"wt_stream receive failed stream_id={self.stream_id}") from None

    async def stop_receiving(self, *, error_code: ErrorCode = ErrorCodes.APP_NO_ERROR) -> None:
        """Signal the peer to stop sending data."""
        if self._is_closed:
            return

        connection = self.session._connection()
        if connection is None:
            return

        try:
            await connection._execute_request(
                event_factory=lambda request_id: UserStopSending(
                    request_id=request_id, stream_id=self.stream_id, error_code=error_code
                )
            )
        except (ConnectionError, StreamError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream abort failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

    def __aiter__(self) -> AsyncIterator[bytes]:
        """Iterate over the stream chunks."""
        return self

    async def __anext__(self) -> bytes:
        """Get the next chunk of data."""
        data = await self.read()
        if not data:
            raise StopAsyncIteration
        return data


class WebTransportSendStream(WebTransportStream):
    """Manage the sending part of a WebTransport stream."""

    __slots__ = ()

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        exit_error_code: ErrorCode | None = None

        if isinstance(exc_value, asyncio.CancelledError):
            exit_error_code = ErrorCodes.APP_CANCELLED
        elif isinstance(exc_value, BaseException):
            exit_error_code = getattr(exc_value, "error_code", ErrorCodes.APP_GENERIC_ERROR)

        await self.close(error_code=exit_error_code)

    @property
    def direction(self) -> StreamDirection:
        """Return the stream direction."""
        return StreamDirection.SEND_ONLY

    async def close(self, *, error_code: ErrorCode | None = None) -> None:
        """Terminate the sending part of the stream."""
        if self._is_closed:
            return

        if error_code is not None:
            await self.reset(error_code=error_code)
            return

        await self.write(data=b"", end_stream=True)

    async def reset(self, *, error_code: ErrorCode = ErrorCodes.APP_NO_ERROR) -> None:
        """Abandon stream transmission."""
        if self._is_closed:
            return

        connection = self.session._connection()
        if connection is None:
            return

        try:
            await connection._execute_request(
                event_factory=lambda request_id: UserResetStream(
                    request_id=request_id, stream_id=self.stream_id, error_code=error_code
                )
            )
        except (ConnectionError, StreamError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream abort failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

    async def write(self, *, data: Buffer, end_stream: bool = False) -> None:
        """Write data to the stream."""
        try:
            data = _ensure_buffer(data=data)
        except TypeError as e:
            raise StreamError(
                message=f"wt_stream validate invalid stream_id={self.stream_id}", stream_id=self.stream_id
            ) from e

        if self._is_closed:
            raise StreamError(message=f"wt_stream resolve failed stream_id={self.stream_id}", stream_id=self.stream_id)

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection._execute_request(
                event_factory=lambda request_id: UserWriteStream(
                    request_id=request_id, stream_id=self.stream_id, data=data, end_stream=end_stream
                )
            )
        except (ConnectionError, StreamError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream send failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

    async def write_all(self, *, data: Buffer, end_stream: bool = False) -> None:
        """Write data to the stream in chunks."""
        try:
            data = _ensure_buffer(data=data)
        except TypeError as e:
            raise StreamError(
                message=f"wt_stream validate invalid stream_id={self.stream_id}", stream_id=self.stream_id
            ) from e

        offset = 0
        data_len = len(data)

        if not data:
            await self.write(data=b"", end_stream=end_stream)
            return

        while offset < data_len:
            chunk = data[offset : offset + _WRITE_CHUNK_SIZE]
            offset += len(chunk)
            is_last_chunk = offset >= data_len
            await self.write(data=chunk, end_stream=end_stream if is_last_chunk else False)


def _ensure_buffer(*, data: Buffer) -> Buffer:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError(f"mem_buffer validate invalid actual={type(data).__name__} expected=buffer")
    return data
