"""WebTransport session."""

from __future__ import annotations

import asyncio
import http
import logging
import weakref
from collections.abc import AsyncGenerator, Awaitable
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, cast

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
from pywebtransport.constants import ErrorCodes
from pywebtransport.events import EventEmitter
from pywebtransport.exceptions import ConnectionError, DatagramError, SessionError, StreamError, TimeoutError
from pywebtransport.stream import (
    WebTransportBidirectionalStream,
    WebTransportReceiveStream,
    WebTransportSendStream,
    WebTransportStream,
)
from pywebtransport.types import Address, Buffer, ErrorCode, Headers, SessionId, SessionState

if TYPE_CHECKING:
    from pywebtransport.connection import WebTransportConnection

__all__: list[str] = ["SessionDiagnostics", "WebTransportSession"]

_logger = logging.getLogger(name=__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class SessionDiagnostics:
    """Encapsulate session diagnostic data."""

    close_code: ErrorCode | None
    close_reason: str | None
    closed_at: float | None
    created_at: float
    data_consumed: int
    data_received: int
    data_sent: int
    flow_control_negotiated: bool
    headers: Headers
    is_client: bool
    local_max_data: int
    local_max_streams_bidi: int
    local_max_streams_uni: int
    local_streams_bidi_opened: int
    local_streams_uni_opened: int
    path: str
    peer_max_data: int
    peer_max_streams_bidi: int
    peer_max_streams_uni: int
    peer_streams_bidi_closed: int
    peer_streams_bidi_opened: int
    peer_streams_uni_closed: int
    peer_streams_uni_opened: int
    ready_at: float | None
    session_id: SessionId
    state: SessionState
    wt_available_protocols: list[str] | None
    wt_protocol: str | None


class WebTransportSession:
    """Manage the high-level WebTransport session."""

    __slots__ = (
        "__weakref__",
        "_session_id",
        "_connection",
        "_path",
        "_headers",
        "_wt_available_protocols",
        "_wt_protocol",
        "_datagram_read_task",
        "_incoming_streams_bidi",
        "_incoming_streams_uni",
        "_is_closed",
        "_ready",
        "events",
    )

    def __init__(
        self,
        *,
        session_id: SessionId,
        connection: WebTransportConnection,
        path: str,
        headers: Headers,
        wt_available_protocols: list[str] | None = None,
        wt_protocol: str | None = None,
    ) -> None:
        """Initialize the instance."""
        self._session_id = session_id
        self._connection = weakref.ref(connection)
        self._path = path
        self._headers = headers
        self._wt_available_protocols = wt_available_protocols
        self._wt_protocol = wt_protocol

        self._datagram_read_task: asyncio.Task[bytes | Exception] | None = None
        self._incoming_streams_bidi: asyncio.Queue[WebTransportBidirectionalStream | None] = asyncio.Queue()
        self._incoming_streams_uni: asyncio.Queue[WebTransportReceiveStream | None] = asyncio.Queue()
        self._is_closed = False
        self._ready: asyncio.Future[SessionError | None] = asyncio.get_running_loop().create_future()
        self.events = EventEmitter()

        _logger.debug("wt_session create session_id=%d", self._session_id)

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
    def headers(self) -> Headers:
        """Return the headers."""
        return self._headers.copy()

    @property
    def is_closed(self) -> bool:
        """Return True if the session is closed."""
        return self._is_closed

    @property
    def path(self) -> str:
        """Return the path."""
        return self._path

    @property
    def remote_address(self) -> Address | None:
        """Return the remote address."""
        connection = self._connection()
        if connection is not None:
            return connection.remote_address
        return None

    @property
    def session_id(self) -> SessionId:
        """Return the session ID."""
        return self._session_id

    @property
    def wt_available_protocols(self) -> list[str] | None:
        """Return the wt_available_protocols."""
        return self._wt_available_protocols

    @property
    def wt_protocol(self) -> str | None:
        """Return the negotiated wt_protocol."""
        return self._wt_protocol

    async def accept(self, *, headers: Headers | None = None, wt_protocol: str | None = None) -> None:
        """Accept the session request."""
        if self._is_closed:
            raise SessionError(
                message=f"wt_session resolve failed session_id={self.session_id}", session_id=self.session_id
            )

        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection._execute_request(
                event_factory=lambda request_id: UserAcceptSession(
                    request_id=request_id,
                    session_id=self.session_id,
                    headers=headers if headers is not None else {},
                    wt_protocol=wt_protocol,
                )
            )
        except (ConnectionError, SessionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session open failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

    async def close(self, *, error_code: ErrorCode = ErrorCodes.APP_NO_ERROR, reason: str | None = None) -> None:
        """Terminate the session."""
        if self._is_closed:
            return

        _logger.debug("wt_session close session_id=%d err=%s", self.session_id, error_code)

        connection = self._connection()
        if connection is None:
            return

        try:
            await connection._execute_request(
                event_factory=lambda request_id: UserCloseSession(
                    request_id=request_id, session_id=self.session_id, error_code=error_code, reason=reason
                )
            )
        except (ConnectionError, SessionError) as e:
            if "channel closed" not in str(e):
                _logger.warning("wt_session close failed session_id=%d err=%s", self.session_id, e)
        except Exception as e:
            _logger.warning("wt_session close failed session_id=%d err=%s", self.session_id, e)

    async def create_bidirectional_stream(self) -> WebTransportBidirectionalStream:
        """Instantiate a bidirectional stream."""
        stream = await self._create_stream(is_unidirectional=False)
        if not isinstance(stream, WebTransportBidirectionalStream):
            raise StreamError(
                message=(
                    f"wt_stream validate invalid actual={type(stream).__name__} expected=bidirectional "
                    f"stream_id={stream.stream_id}"
                ),
                stream_id=stream.stream_id,
            )
        return stream

    async def create_unidirectional_stream(self) -> WebTransportSendStream:
        """Instantiate a unidirectional stream."""
        stream = await self._create_stream(is_unidirectional=True)
        if not isinstance(stream, WebTransportSendStream):
            raise StreamError(
                message=(
                    f"wt_stream validate invalid actual={type(stream).__name__} expected=send_only "
                    f"stream_id={stream.stream_id}"
                ),
                stream_id=stream.stream_id,
            )
        return stream

    async def diagnose(self) -> SessionDiagnostics:
        """Diagnose the session."""
        if self._is_closed:
            raise SessionError(
                message=f"wt_session resolve failed session_id={self.session_id}", session_id=self.session_id
            )

        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            diagnostic_data: dict[str, Any] = await connection._execute_request(
                event_factory=lambda request_id: UserDiagnoseSession(request_id=request_id, session_id=self.session_id)
            )
        except (ConnectionError, SessionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session resolve failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

        return SessionDiagnostics(**diagnostic_data)

    async def ensure_ready(self) -> None:
        """Ensure the session is ready."""
        error = await asyncio.shield(self._ready)
        if error is not None:
            raise error

    async def export_keying_material(self, *, label: str, context: Buffer, length: int) -> bytes:
        """Export TLS keying material."""
        if self._is_closed:
            raise SessionError(
                message=f"wt_session resolve failed session_id={self.session_id}", session_id=self.session_id
            )

        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            return cast(
                bytes,
                await connection._execute_request(
                    event_factory=lambda request_id: UserExportKeyingMaterial(
                        request_id=request_id, session_id=self.session_id, label=label, context=context, length=length
                    )
                ),
            )
        except (ConnectionError, SessionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session resolve failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

    async def incoming_bidirectional_streams(self) -> AsyncGenerator[WebTransportBidirectionalStream, None]:
        """Yield incoming bidirectional streams until the session is closed."""
        while True:
            stream = await self._incoming_streams_bidi.get()
            if stream is None:
                self._incoming_streams_bidi.put_nowait(None)
                return
            yield stream

    async def incoming_unidirectional_streams(self) -> AsyncGenerator[WebTransportReceiveStream, None]:
        """Yield incoming unidirectional streams until the session is closed."""
        while True:
            stream = await self._incoming_streams_uni.get()
            if stream is None:
                self._incoming_streams_uni.put_nowait(None)
                return
            yield stream

    async def read_datagram(self) -> bytes:
        """Read a datagram."""
        while True:
            datagram_read_task = self._datagram_read_task
            if datagram_read_task is None:
                if self._is_closed:
                    raise SessionError(
                        message=f"wt_session resolve failed session_id={self.session_id}", session_id=self.session_id
                    )

                connection = self._connection()
                if connection is None:
                    raise ConnectionError(message="wt_connection resolve failed")

                async def receive(*, request: Awaitable[bytes]) -> bytes | Exception:
                    try:
                        return await request
                    except Exception as e:
                        return e

                datagram_read_task = asyncio.create_task(
                    coro=receive(
                        request=connection._execute_request(
                            event_factory=lambda request_id: UserReadDatagram(
                                request_id=request_id, session_id=self.session_id
                            )
                        )
                    )
                )
                self._datagram_read_task = datagram_read_task

            outcome = await asyncio.shield(datagram_read_task)
            if self._datagram_read_task is not datagram_read_task:
                continue
            self._datagram_read_task = None

            if isinstance(outcome, (ConnectionError, SessionError)):
                raise outcome
            if isinstance(outcome, Exception):
                raise SessionError.from_cause(
                    message=f"wt_session receive failed session_id={self.session_id}",
                    cause=outcome,
                    session_id=self.session_id,
                ) from outcome
            return outcome

    async def reject(self, *, headers: Headers | None = None, status_code: int = http.HTTPStatus.FORBIDDEN) -> None:
        """Reject the session request."""
        if self._is_closed:
            raise SessionError(
                message=f"wt_session resolve failed session_id={self.session_id}", session_id=self.session_id
            )

        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection._execute_request(
                event_factory=lambda request_id: UserRejectSession(
                    request_id=request_id,
                    session_id=self.session_id,
                    headers=headers if headers is not None else {},
                    status_code=status_code,
                )
            )
        except (ConnectionError, SessionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session reject failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

    async def send_datagram(self, *, data: Buffer) -> None:
        """Transmit a datagram."""
        if self._is_closed:
            raise SessionError(
                message=f"wt_session resolve failed session_id={self.session_id}", session_id=self.session_id
            )

        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            await connection._execute_request(
                event_factory=lambda request_id: UserSendDatagram(
                    request_id=request_id, session_id=self.session_id, data=data
                )
            )
        except (ConnectionError, DatagramError, SessionError, asyncio.CancelledError):
            raise
        except Exception as e:
            raise SessionError.from_cause(
                message=f"wt_session send failed session_id={self.session_id}", cause=e, session_id=self.session_id
            ) from e

    async def _create_stream(
        self, *, is_unidirectional: bool
    ) -> WebTransportBidirectionalStream | WebTransportSendStream:
        if self._is_closed:
            raise SessionError(
                message=f"wt_session resolve failed session_id={self.session_id}", session_id=self.session_id
            )

        connection = self._connection()
        if connection is None:
            raise ConnectionError(message="wt_connection resolve failed")

        try:
            timeout = connection.config.stream_create_timeout
            async with asyncio.timeout(delay=timeout):
                stream_handle = await connection._execute_request(
                    event_factory=lambda request_id: UserCreateStream(
                        request_id=request_id, session_id=self.session_id, is_unidirectional=is_unidirectional
                    ),
                    resolver=connection._resolve_stream_handle,
                )
        except (ConnectionError, SessionError, asyncio.CancelledError):
            raise
        except asyncio.TimeoutError:
            raise TimeoutError(message=f"wt_stream create failed session_id={self.session_id}") from None
        except Exception as e:
            raise StreamError.from_cause(
                message=f"wt_stream create failed session_id={self.session_id}", cause=e
            ) from e

        if not isinstance(stream_handle, (WebTransportBidirectionalStream, WebTransportSendStream)):
            stream_id = stream_handle.stream_id
            raise StreamError(
                message=f"wt_stream validate invalid actual={type(stream_handle).__name__} stream_id={stream_id}",
                stream_id=stream_id,
            )

        return stream_handle

    def _handle_closed(self, *, error_code: ErrorCode | None) -> None:
        self._is_closed = True
        self._incoming_streams_bidi.put_nowait(None)
        self._incoming_streams_uni.put_nowait(None)
        if not self._ready.done():
            self._ready.set_result(
                SessionError(
                    message=f"wt_session open failed session_id={self.session_id}",
                    session_id=self.session_id,
                    error_code=error_code,
                )
            )
        _logger.info("wt_session close session_id=%d", self.session_id)

    def _handle_ready(self, *, wt_protocol: str | None) -> None:
        self._wt_protocol = wt_protocol
        self._ready.set_result(None)

    def _handle_stream_opened(self, *, stream: WebTransportStream) -> None:
        if not stream.is_peer_initiated:
            return

        if isinstance(stream, WebTransportBidirectionalStream):
            self._incoming_streams_bidi.put_nowait(stream)
        elif isinstance(stream, WebTransportReceiveStream):
            self._incoming_streams_uni.put_nowait(stream)
