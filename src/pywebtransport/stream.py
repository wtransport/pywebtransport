"""High-level abstractions for WebTransport streams."""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, Self

from pywebtransport._protocol.events import (
    UserGetStreamDiagnostics,
    UserReadStream,
    UserResetStream,
    UserSendStreamData,
    UserStopSending,
)
from pywebtransport.constants import ErrorCodes
from pywebtransport.events import Event, EventEmitter
from pywebtransport.exceptions import ConnectionError, StreamError, TimeoutError
from pywebtransport.types import Buffer, EventType, SessionId, StreamDirection, StreamId, StreamState

if TYPE_CHECKING:
    from pywebtransport.session import WebTransportSession

__all__: list[str] = [
    "StreamDiagnostics",
    "StreamType",
    "WebTransportReceiveStream",
    "WebTransportSendStream",
    "WebTransportStream",
]

type StreamType = WebTransportStream | WebTransportReceiveStream | WebTransportSendStream

_EVENT_HISTORY_CAPACITY: Final[int] = 0
_EVENT_LISTENER_CAPACITY: Final[int] = 10
_EVENT_QUEUE_CAPACITY: Final[int] = 10
_WRITE_CHUNK_SIZE: Final[int] = 65536

_logger = logging.getLogger(name=__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class StreamDiagnostics:
    """Encapsulate stream diagnostic data."""

    bytes_received: int
    bytes_sent: int
    close_code: int | None
    close_reason: str | None
    closed_at: float | None
    created_at: float
    direction: StreamDirection
    is_peer_initiated: bool
    read_buffer_size: int
    session_id: SessionId
    state: StreamState
    stream_id: StreamId
    write_buffer_size: int


class _BaseStream:
    """Manage the common state for WebTransport stream handles."""

    __slots__ = ("_cached_state", "_is_remote", "_session", "_stream_id", "events")

    def __init__(self, *, session: WebTransportSession, stream_id: StreamId, is_remote: bool) -> None:
        """Initialize the instance."""
        self._session = weakref.ref(session)
        self._stream_id = stream_id
        self._is_remote = is_remote

        self._cached_state = StreamState.OPEN
        self.events = EventEmitter(
            max_listeners=_EVENT_LISTENER_CAPACITY,
            max_history=_EVENT_HISTORY_CAPACITY,
            max_queue_size=_EVENT_QUEUE_CAPACITY,
        )

        self.events.on(event_type=EventType.STREAM_CLOSED, handler=self._on_closed)

    @property
    def is_closed(self) -> bool:
        """Return True if the stream is fully closed."""
        return self._cached_state == StreamState.CLOSED

    @property
    def is_remote(self) -> bool:
        """Return True if the stream was initiated by the peer."""
        return self._is_remote

    @property
    def session(self) -> WebTransportSession:
        """Return the parent session handle."""
        session = self._session()
        if session is None:
            raise ConnectionError(message="wt_session resolve failed")
        return session

    @property
    def state(self) -> StreamState:
        """Return the current state of the stream."""
        return self._cached_state

    @property
    def stream_id(self) -> StreamId:
        """Return the unique identifier for this stream."""
        return self._stream_id

    async def diagnostics(self) -> StreamDiagnostics:
        """Retrieve diagnostic information about the stream."""
        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            diag_data: dict[str, Any] = await connection.execute_request(
                event_factory=lambda request_id: UserGetStreamDiagnostics(
                    request_id=request_id, stream_id=self.stream_id
                )
            )
        except (ConnectionError, StreamError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream resolve failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

        return StreamDiagnostics(**diag_data)

    def _on_closed(self, event: Event) -> None:
        """Handle the stream closed event."""
        self._cached_state = StreamState.CLOSED

    def __repr__(self) -> str:
        """Return the string representation."""
        return f"<{self.__class__.__name__} id={self.stream_id} state={self.state}>"


class WebTransportReceiveStream(_BaseStream):
    """Manage the readable side of a WebTransport stream."""

    __slots__ = ("_read_eof",)

    def __init__(self, *, session: WebTransportSession, stream_id: StreamId, is_remote: bool) -> None:
        """Initialize the instance."""
        super().__init__(session=session, stream_id=stream_id, is_remote=is_remote)

        self._read_eof = False

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        await self.stop_receiving()

    @property
    def can_read(self) -> bool:
        """Return True if the stream is readable."""
        return self._cached_state not in (StreamState.RESET_RECEIVED, StreamState.CLOSED)

    @property
    def direction(self) -> StreamDirection:
        """Return the directionality of the stream."""
        return StreamDirection.RECEIVE_ONLY

    async def close(self) -> None:
        """Terminate the receiving side of the stream."""
        await self.stop_receiving()

    async def read(self, *, max_bytes: int = -1) -> bytes:
        """Read data from the stream."""
        if self._read_eof:
            return b""

        if self.is_closed:
            self._read_eof = True
            return b""

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        limit = max_bytes if max_bytes >= 0 else None

        try:
            data = await connection.execute_request(
                event_factory=lambda request_id: UserReadStream(
                    request_id=request_id, stream_id=self.stream_id, max_bytes=limit
                )
            )
        except StreamError as e:
            if e.error_code == ErrorCodes.LIB_STREAM_STATE_ERROR:
                self._read_eof = True
                return b""
            raise
        except (ConnectionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream receive failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

        if not data and max_bytes != 0:
            self._read_eof = True

        return bytes(data)

    async def read_all(self) -> bytes:
        """Read all data from the stream until EOF."""
        chunks: list[bytes] = []
        async for chunk in self:
            chunks.append(chunk)
        return b"".join(chunks)

    async def readexactly(self, *, n: int) -> bytes:
        """Read exactly n bytes from the stream."""
        if n < 0:
            raise ValueError(f"wt_stream validate invalid actual={n}")
        if n == 0:
            return b""

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")
        read_timeout = connection.config.read_timeout

        chunks: list[bytes] = []
        bytes_read = 0

        try:
            async with asyncio.timeout(read_timeout):
                while bytes_read < n:
                    needed = n - bytes_read
                    chunk = await self.read(max_bytes=needed)
                    if not chunk:
                        partial = b"".join(chunks)
                        raise asyncio.IncompleteReadError(partial, n)

                    chunks.append(chunk)
                    bytes_read += len(chunk)
        except asyncio.TimeoutError:
            raise TimeoutError(message=f"wt_stream receive failed stream_id={self.stream_id}") from None

        return b"".join(chunks)

    async def readline(self, *, limit: int = -1) -> bytes:
        """Read a line from the stream."""
        return await self.readuntil(separator=b"\n", limit=limit)

    async def readuntil(self, *, separator: bytes, limit: int = -1) -> bytes:
        """Read data from the stream until a separator is found."""
        if not separator:
            raise ValueError(f"wt_stream validate invalid actual={separator}")

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")
        read_timeout = connection.config.read_timeout

        data = bytearray()
        try:
            async with asyncio.timeout(read_timeout):
                while True:
                    chunk = await self.read(max_bytes=1)
                    if not chunk:
                        raise asyncio.IncompleteReadError(bytes(data), None)
                    data.extend(chunk)
                    if data.endswith(separator):
                        return bytes(data)
                    if limit > 0 and len(data) > limit:
                        raise StreamError(
                            message=f"wt_stream receive exceeded actual={len(data)} limit={limit} "
                            f"stream_id={self.stream_id}",
                            stream_id=self.stream_id,
                        )
        except asyncio.TimeoutError:
            raise TimeoutError(message=f"wt_stream receive failed stream_id={self.stream_id}") from None

    async def stop_receiving(self, *, error_code: int = ErrorCodes.APP_NO_ERROR) -> None:
        """Signal the peer to stop sending data."""
        if self._read_eof:
            return

        connection = self.session._connection()
        if connection is None:
            return

        try:
            await connection.execute_request(
                event_factory=lambda request_id: UserStopSending(
                    request_id=request_id, stream_id=self.stream_id, error_code=error_code
                )
            )
        except StreamError as e:
            if e.error_code != ErrorCodes.LIB_STREAM_STATE_ERROR:
                raise
        except (ConnectionError, asyncio.CancelledError):
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


class WebTransportSendStream(_BaseStream):
    """Manage the writable side of a WebTransport stream."""

    __slots__ = ()

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        exit_error_code: int | None = None

        if isinstance(exc_val, asyncio.CancelledError):
            exit_error_code = ErrorCodes.APP_CANCELLED
        elif isinstance(exc_val, BaseException):
            exit_error_code = getattr(exc_val, "error_code", ErrorCodes.APP_GENERIC_ERROR)

        await self.close(error_code=exit_error_code)

    @property
    def can_write(self) -> bool:
        """Return True if the stream is writable."""
        return self._cached_state not in (StreamState.HALF_CLOSED_LOCAL, StreamState.CLOSED, StreamState.RESET_SENT)

    @property
    def direction(self) -> StreamDirection:
        """Return the directionality of the stream."""
        return StreamDirection.SEND_ONLY

    async def close(self, *, error_code: int | None = None) -> None:
        """Terminate the sending side of the stream."""
        if error_code is not None:
            await self.reset(error_code=error_code)
            return

        try:
            await self.write(data=b"", end_stream=True)
        except StreamError as e:
            _logger.debug("wt_stream close failed stream_id=%d err=%s", self.stream_id, e)
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream close failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

    async def reset(self, *, error_code: int = ErrorCodes.APP_NO_ERROR) -> None:
        """Abruptly terminate stream transmission."""
        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection.execute_request(
                event_factory=lambda request_id: UserResetStream(
                    request_id=request_id, stream_id=self.stream_id, error_code=error_code
                )
            )
        except StreamError as e:
            if e.error_code != ErrorCodes.LIB_STREAM_STATE_ERROR:
                raise
        except (ConnectionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream abort failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

    async def write(self, *, data: Buffer, end_stream: bool = False) -> None:
        """Write data to the stream."""
        try:
            buffer_data = _ensure_buffer(data=data)
        except TypeError as e:
            raise StreamError(
                message=f"wt_stream validate invalid stream_id={self.stream_id}", stream_id=self.stream_id
            ) from e

        if not buffer_data and not end_stream:
            return

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection.execute_request(
                event_factory=lambda request_id: UserSendStreamData(
                    request_id=request_id, stream_id=self.stream_id, data=buffer_data, end_stream=end_stream
                )
            )
        except (ConnectionError, StreamError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream send failed stream_id={self.stream_id}", cause=e, stream_id=self.stream_id
            ) from e

    async def write_all(self, *, data: Buffer, chunk_size: int = _WRITE_CHUNK_SIZE, end_stream: bool = False) -> None:
        """Write buffer data to the stream in chunks."""
        try:
            buffer_data = _ensure_buffer(data=data)
        except TypeError as e:
            raise StreamError(
                message=f"wt_stream validate invalid stream_id={self.stream_id}", stream_id=self.stream_id
            ) from e

        offset = 0
        data_len = len(buffer_data)

        if not buffer_data and end_stream:
            await self.write(data=b"", end_stream=True)
            return

        while offset < data_len:
            chunk = buffer_data[offset : offset + chunk_size]
            offset += len(chunk)
            is_last_chunk = offset >= data_len
            await self.write(data=chunk, end_stream=end_stream if is_last_chunk else False)


class WebTransportStream(_BaseStream):
    """Manage the bidirectional WebTransport stream."""

    __slots__ = ("_reader", "_writer")

    def __init__(self, *, session: WebTransportSession, stream_id: StreamId, is_remote: bool) -> None:
        """Initialize the instance."""
        super().__init__(session=session, stream_id=stream_id, is_remote=is_remote)

        self._reader = WebTransportReceiveStream(session=session, stream_id=stream_id, is_remote=is_remote)
        self._writer = WebTransportSendStream(session=session, stream_id=stream_id, is_remote=is_remote)

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the asynchronous context."""
        exit_error_code: int | None = None

        if isinstance(exc_val, asyncio.CancelledError):
            exit_error_code = ErrorCodes.APP_CANCELLED
        elif isinstance(exc_val, BaseException):
            exit_error_code = getattr(exc_val, "error_code", ErrorCodes.APP_GENERIC_ERROR)

        await self.close(error_code=exit_error_code)

    @property
    def can_read(self) -> bool:
        """Return True if the stream is readable."""
        return self._reader.can_read

    @property
    def can_write(self) -> bool:
        """Return True if the stream is writable."""
        return self._writer.can_write

    @property
    def direction(self) -> StreamDirection:
        """Return the directionality of the stream."""
        return StreamDirection.BIDIRECTIONAL

    async def close(self, *, error_code: int | None = None) -> None:
        """Terminate both sides of the stream."""
        await self._writer.close(error_code=error_code)
        stop_code = error_code if error_code is not None else ErrorCodes.APP_NO_ERROR
        await self._reader.stop_receiving(error_code=stop_code)

    async def read(self, *, max_bytes: int = -1) -> bytes:
        """Read data from the stream."""
        return await self._reader.read(max_bytes=max_bytes)

    async def read_all(self) -> bytes:
        """Read all data from the stream until EOF."""
        return await self._reader.read_all()

    async def readexactly(self, *, n: int) -> bytes:
        """Read exactly n bytes from the stream."""
        return await self._reader.readexactly(n=n)

    async def readline(self, *, limit: int = -1) -> bytes:
        """Read a line from the stream."""
        return await self._reader.readline(limit=limit)

    async def readuntil(self, *, separator: bytes, limit: int = -1) -> bytes:
        """Read data from the stream until a separator is found."""
        return await self._reader.readuntil(separator=separator, limit=limit)

    async def reset(self, *, error_code: int = ErrorCodes.APP_NO_ERROR) -> None:
        """Stop sending data and reset the stream."""
        await self._writer.reset(error_code=error_code)

    async def stop_receiving(self, *, error_code: int = ErrorCodes.APP_NO_ERROR) -> None:
        """Signal the peer to stop sending data."""
        await self._reader.stop_receiving(error_code=error_code)

    async def write(self, *, data: Buffer, end_stream: bool = False) -> None:
        """Write data to the stream."""
        await self._writer.write(data=data, end_stream=end_stream)

    async def write_all(self, *, data: Buffer, chunk_size: int = _WRITE_CHUNK_SIZE, end_stream: bool = False) -> None:
        """Write buffer data to the stream in chunks."""
        await self._writer.write_all(data=data, chunk_size=chunk_size, end_stream=end_stream)

    def _on_closed(self, event: Event) -> None:
        """Handle the stream closed event."""
        super()._on_closed(event)
        self._reader._on_closed(event)
        self._writer._on_closed(event)

    def __aiter__(self) -> AsyncIterator[bytes]:
        """Iterate over the stream chunks."""
        return self

    async def __anext__(self) -> bytes:
        """Get the next chunk of data."""
        return await self._reader.__anext__()


def _ensure_buffer(*, data: Buffer | str, encoding: str = "utf-8") -> Buffer:
    """Validate and convert input data to a buffer format."""
    match data:
        case str():
            return data.encode(encoding=encoding)
        case bytes() | bytearray() | memoryview():
            return data
        case _:
            raise TypeError(f"mem_buffer validate invalid actual={type(data).__name__} expected=buffer")
