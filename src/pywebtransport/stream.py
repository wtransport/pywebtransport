"""High-level abstractions for WebTransport streams."""

from __future__ import annotations

import asyncio
import weakref
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from pywebtransport._protocol.events import (
    UserGetStreamDiagnostics,
    UserResetStream,
    UserSendStreamData,
    UserStopStream,
    UserStreamRead,
)
from pywebtransport.constants import ErrorCodes
from pywebtransport.events import Event, EventEmitter
from pywebtransport.exceptions import ConnectionError, StreamError, TimeoutError
from pywebtransport.types import Buffer, SessionId, StreamDirection, StreamId, StreamState
from pywebtransport.utils import ensure_buffer, get_logger

if TYPE_CHECKING:
    from pywebtransport.session import WebTransportSession


__all__: list[str] = [
    "StreamDiagnostics",
    "StreamType",
    "WebTransportReceiveStream",
    "WebTransportSendStream",
    "WebTransportStream",
]

DEFAULT_EVENT_HISTORY_SIZE: int = 0
DEFAULT_EVENT_QUEUE_SIZE: int = 16
DEFAULT_MAX_EVENT_LISTENERS: int = 20

logger = get_logger(name=__name__)


@dataclass(kw_only=True)
class StreamDiagnostics:
    """A snapshot of stream diagnostics."""

    stream_id: StreamId
    session_id: SessionId
    direction: StreamDirection
    state: StreamState
    created_at: float
    bytes_sent: int
    bytes_received: int
    read_buffer: bytes
    read_buffer_size: int
    pending_read_requests: list[Any]
    write_buffer: list[tuple[bytes, Any, bool]]
    write_buffer_size: int
    close_code: int | None
    close_reason: str | None
    closed_at: float | None


class _BaseStream:
    """Base class for WebTransport stream handles."""

    _stream_id: StreamId
    events: EventEmitter

    def __init__(self, *, session: WebTransportSession, stream_id: StreamId) -> None:
        """Initialize the base stream handle."""
        self._session = weakref.ref(session)
        self._stream_id = stream_id
        self._cached_state = StreamState.OPEN
        self.events = EventEmitter(
            max_queue_size=DEFAULT_EVENT_QUEUE_SIZE,
            max_history=DEFAULT_EVENT_HISTORY_SIZE,
            max_listeners=DEFAULT_MAX_EVENT_LISTENERS,
        )
        self.events.on(event_type="stream_closed", handler=self._on_closed)

    @property
    def is_closed(self) -> bool:
        """Return True if the stream is fully closed."""
        return self._cached_state == StreamState.CLOSED

    @property
    def session(self) -> WebTransportSession:
        """Get the parent session handle."""
        session = self._session()
        if session is None:
            raise ConnectionError("Session is gone.")
        return session

    @property
    def state(self) -> StreamState:
        """Get the current state of the stream."""
        return self._cached_state

    @property
    def stream_id(self) -> StreamId:
        """Get the unique identifier for this stream."""
        return self._stream_id

    async def diagnostics(self) -> StreamDiagnostics:
        """Get diagnostic information about the stream."""
        connection = self.session._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserGetStreamDiagnostics(request_id=request_id, stream_id=self.stream_id)
        connection._protocol.send_event(event=event)

        try:
            diag_data = await future
        except ConnectionError as e:
            raise StreamError(f"Connection is closed, cannot get diagnostics: {e}", stream_id=self.stream_id) from e

        if "write_buffer" in diag_data and isinstance(diag_data["write_buffer"], deque):
            diag_data["write_buffer"] = list(diag_data["write_buffer"])

        return StreamDiagnostics(**diag_data)

    def _on_closed(self, event: Event) -> None:
        """Handle stream closed event."""
        self._cached_state = StreamState.CLOSED

    def __repr__(self) -> str:
        """Provide a developer-friendly representation."""
        return f"<{self.__class__.__name__} id={self.stream_id} state={self._cached_state}>"


class WebTransportReceiveStream(_BaseStream):
    """Represents the readable side of a WebTransport stream."""

    def __init__(self, *, session: WebTransportSession, stream_id: StreamId) -> None:
        """Initialize the receive stream handle."""
        super().__init__(session=session, stream_id=stream_id)
        self._read_eof = False

    @property
    def can_read(self) -> bool:
        """Return True if the stream is readable."""
        return self._cached_state not in (StreamState.RESET_RECEIVED, StreamState.CLOSED)

    @property
    def direction(self) -> StreamDirection:
        """Get the directionality of the stream."""
        return StreamDirection.RECEIVE_ONLY

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the async context manager."""
        await self.stop_receiving()

    async def close(self) -> None:
        """Close the receiving side of the stream."""
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
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserStreamRead(request_id=request_id, stream_id=self.stream_id, max_bytes=max_bytes)
        connection._protocol.send_event(event=event)

        try:
            data = await future
        except StreamError as e:
            if e.error_code == ErrorCodes.STREAM_STATE_ERROR:
                self._read_eof = True
                return b""
            raise

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
            raise ValueError("n must be a non-negative integer")
        if n == 0:
            return b""

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")
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
            raise TimeoutError(f"readexactly timed out after {read_timeout}s") from None

        return b"".join(chunks)

    async def readline(self, *, limit: int = -1) -> bytes:
        """Read a line from the stream."""
        return await self.readuntil(separator=b"\n", limit=limit)

    async def readuntil(self, *, separator: bytes, limit: int = -1) -> bytes:
        """Read data from the stream until a separator is found."""
        if not separator:
            raise ValueError("Separator cannot be empty")

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")
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
                        raise StreamError(f"Separator not found within limit {limit}", stream_id=self.stream_id)
        except asyncio.TimeoutError:
            raise TimeoutError(f"readuntil timed out after {read_timeout}s") from None

    async def stop_receiving(self, *, error_code: int = ErrorCodes.NO_ERROR) -> None:
        """Signal the peer to stop sending data."""
        connection = self.session._connection()
        if connection is None:
            return

        request_id, future = connection._protocol.create_request()
        event = UserStopStream(request_id=request_id, stream_id=self.stream_id, error_code=error_code)
        connection._protocol.send_event(event=event)
        await future
        self._cached_state = StreamState.RESET_RECEIVED

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
    """Represents the writable side of a WebTransport stream."""

    @property
    def can_write(self) -> bool:
        """Return True if the stream is writable."""
        return self._cached_state not in (StreamState.HALF_CLOSED_LOCAL, StreamState.CLOSED, StreamState.RESET_SENT)

    @property
    def direction(self) -> StreamDirection:
        """Get the directionality of the stream."""
        return StreamDirection.SEND_ONLY

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the async context manager."""
        exit_error_code: int | None = None
        match exc_val:
            case asyncio.CancelledError():
                exit_error_code = ErrorCodes.APPLICATION_ERROR
            case BaseException():
                exit_error_code = getattr(exc_val, "error_code", ErrorCodes.APPLICATION_ERROR)
            case None:
                pass

        await self.close(error_code=exit_error_code)

    async def close(self, *, error_code: int | None = None) -> None:
        """Close the sending side of the stream."""
        if error_code is not None:
            await self.stop_sending(error_code=error_code)
            return

        try:
            await self.write(data=b"", end_stream=True)
            self._cached_state = StreamState.HALF_CLOSED_LOCAL
        except StreamError as e:
            logger.debug("Ignoring expected StreamError on stream %s close: %s", self.stream_id, e)
        except Exception as e:
            logger.error("Unexpected error during stream %s close: %s", self.stream_id, e, exc_info=True)
            raise

    async def stop_sending(self, *, error_code: int = ErrorCodes.NO_ERROR) -> None:
        """Stop sending data and reset the stream."""
        connection = self.session._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserResetStream(request_id=request_id, stream_id=self.stream_id, error_code=error_code)
        connection._protocol.send_event(event=event)
        await future
        self._cached_state = StreamState.RESET_SENT

    async def write(self, *, data: Buffer, end_stream: bool = False) -> None:
        """Write data to the stream."""
        try:
            buffer_data = ensure_buffer(data=data)
        except TypeError as e:
            logger.debug("Stream %d write failed pre-validation: %s", self.stream_id, e)
            raise

        if not buffer_data and not end_stream:
            return

        connection = self.session._connection()
        if connection is None:
            raise ConnectionError("Connection is gone.")

        request_id, future = connection._protocol.create_request()
        event = UserSendStreamData(
            request_id=request_id, stream_id=self.stream_id, data=buffer_data, end_stream=end_stream
        )
        connection._protocol.send_event(event=event)

        try:
            await future
        except Exception:
            raise

    async def write_all(self, *, data: Buffer, chunk_size: int = 65536, end_stream: bool = False) -> None:
        """Write buffer data to the stream in chunks."""
        try:
            buffer_data = ensure_buffer(data=data)
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
        except StreamError as e:
            logger.debug("Error writing bytes to stream %d: %s", self.stream_id, e)
            raise


class WebTransportStream(_BaseStream):
    """Represents the bidirectional WebTransport stream."""

    def __init__(self, *, session: WebTransportSession, stream_id: StreamId) -> None:
        """Initialize the bidirectional stream handle."""
        super().__init__(session=session, stream_id=stream_id)
        self._reader = WebTransportReceiveStream(session=session, stream_id=stream_id)
        self._writer = WebTransportSendStream(session=session, stream_id=stream_id)

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
        """Get the directionality of the stream."""
        return StreamDirection.BIDIRECTIONAL

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the async context manager."""
        exit_error_code: int | None = None
        match exc_val:
            case asyncio.CancelledError():
                exit_error_code = ErrorCodes.APPLICATION_ERROR
            case BaseException():
                exit_error_code = getattr(exc_val, "error_code", ErrorCodes.APPLICATION_ERROR)
            case None:
                pass
        await self.close(error_code=exit_error_code)

    async def close(self, *, error_code: int | None = None) -> None:
        """Close both sides of the stream."""
        await self._writer.close(error_code=error_code)
        stop_code = error_code if error_code is not None else ErrorCodes.NO_ERROR
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

    async def stop_receiving(self, *, error_code: int = ErrorCodes.NO_ERROR) -> None:
        """Signal the peer to stop sending data."""
        await self._reader.stop_receiving(error_code=error_code)

    async def stop_sending(self, *, error_code: int = ErrorCodes.NO_ERROR) -> None:
        """Stop sending data and reset the stream."""
        await self._writer.stop_sending(error_code=error_code)

    async def write(self, *, data: Buffer, end_stream: bool = False) -> None:
        """Write data to the stream."""
        await self._writer.write(data=data, end_stream=end_stream)

    async def write_all(self, *, data: Buffer, chunk_size: int = 65536, end_stream: bool = False) -> None:
        """Write buffer data to the stream in chunks."""
        await self._writer.write_all(data=data, chunk_size=chunk_size, end_stream=end_stream)

    def _on_closed(self, event: Event) -> None:
        """Handle stream closed event and propagate to children."""
        super()._on_closed(event)
        self._reader._on_closed(event)
        self._writer._on_closed(event)

    def __aiter__(self) -> AsyncIterator[bytes]:
        """Iterate over the stream chunks."""
        return self

    async def __anext__(self) -> bytes:
        """Get the next chunk of data."""
        return await self._reader.__anext__()


type StreamType = WebTransportStream | WebTransportReceiveStream | WebTransportSendStream
