"""High-level wrapper for structured data over datagrams."""

from __future__ import annotations

import asyncio
import struct
import weakref
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, Self

from pywebtransport.events import Event
from pywebtransport.exceptions import ConfigurationError, SerializationError, SessionError, TimeoutError
from pywebtransport.types import EventType, Serializer
from pywebtransport.utils import get_logger

if TYPE_CHECKING:
    from pywebtransport.session import WebTransportSession


__all__: list[str] = ["StructuredDatagramTransport"]

_DEFAULT_QUEUE_SIZE: Final[int] = 100

_logger = get_logger(name=__name__)


class StructuredDatagramTransport:
    """Send and receive structured objects over datagrams."""

    __slots__ = (
        "_session",
        "_registry",
        "_serializer",
        "_class_to_id",
        "_closed",
        "_handler_ref",
        "_incoming_obj_queue",
        "_is_initialized",
        "_sentinel",
        "__weakref__",
    )

    _HEADER_FORMAT: Final[str] = "!H"
    _HEADER_SIZE: Final[int] = struct.calcsize(_HEADER_FORMAT)

    def __init__(self, *, session: WebTransportSession, registry: dict[int, type[Any]], serializer: Serializer) -> None:
        """Initialize the structured datagram transport."""
        if len(set(registry.values())) != len(registry):
            raise ConfigurationError(message="Types in the structured datagram registry must be unique.")

        self._session = weakref.ref(session)
        self._registry = registry
        self._serializer = serializer

        self._class_to_id = {v: k for k, v in registry.items()}
        self._closed = False
        self._handler_ref: Any = None
        self._incoming_obj_queue: asyncio.Queue[Any] | None = None
        self._is_initialized = False
        self._sentinel = object()

    @property
    def is_closed(self) -> bool:
        """Check if the structured datagram transport is closed."""
        session = self._session()
        return self._closed or session is None or session.is_closed

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        self.initialize()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the async context manager."""
        await self.close()

    async def close(self) -> None:
        """Close the structured transport and unsubscribe from events."""
        if self._closed:
            return

        self._closed = True
        if self._handler_ref is not None:
            session = self._session()
            if session is not None:
                try:
                    session.events.off(event_type=EventType.DATAGRAM_RECEIVED, handler=self._handler_ref)
                except (ValueError, KeyError):
                    pass
            self._handler_ref = None

        if self._incoming_obj_queue is not None:
            self._incoming_obj_queue.put_nowait(item=self._sentinel)

    def initialize(self, *, queue_size: int = _DEFAULT_QUEUE_SIZE) -> None:
        """Initialize the resources for the transport synchronously."""
        if self._is_initialized:
            return

        self._incoming_obj_queue = asyncio.Queue(maxsize=queue_size)

        session = self._session()
        if session is not None:
            if session.is_closed:
                raise SessionError(message="Cannot initialize transport, parent session is closed.")

            weak_self = weakref.ref(self)

            async def handler(event: Event) -> None:
                transport = weak_self()
                if transport is None:
                    return
                await transport._on_datagram_received(event=event)

            self._handler_ref = handler
            session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=handler)
        else:
            raise SessionError(message="Cannot initialize transport, parent session is already gone.")

        self._is_initialized = True

    async def receive_obj(self, *, timeout: float | None = None) -> Any:
        """Receive and deserialize a Python object from a datagram."""
        if self.is_closed:
            raise SessionError(message="Structured transport is closed.")
        if not self._is_initialized or self._incoming_obj_queue is None:
            raise SessionError(message="Structured transport has not been initialized.")

        try:
            async with asyncio.timeout(delay=timeout):
                obj = await self._incoming_obj_queue.get()
            if obj is self._sentinel:
                raise SessionError(message="Structured transport was closed while receiving.")
            return obj
        except asyncio.TimeoutError:
            raise TimeoutError(message=f"Receive object timeout after {timeout}s") from None

    async def send_obj(self, *, obj: Any) -> None:
        """Serialize and send a Python object as a datagram."""
        session = self._session()
        if session is None or session.is_closed:
            raise SessionError(message="Session is closed, cannot send object.")
        if not self._is_initialized:
            raise SessionError(message="Structured transport has not been initialized.")

        obj_type = type(obj)
        type_id = self._class_to_id.get(obj_type)
        if type_id is None:
            raise SerializationError(message=f"Object of type '{obj_type.__name__}' is not registered.")

        header = struct.pack(self._HEADER_FORMAT, type_id)
        payload = self._serializer.serialize(obj=obj)

        await session.send_datagram(data=b"".join((header, payload)))

    async def _on_datagram_received(self, *, event: Event) -> None:
        """Handle incoming raw datagrams and place them in the object queue."""
        if self._closed or not isinstance(event.data, dict) or self._incoming_obj_queue is None:
            return

        datagram: bytes | None = event.data.get("data")
        if datagram is None:
            return

        try:
            view = memoryview(datagram)
            if len(view) < self._HEADER_SIZE:
                return

            header_view = view[: self._HEADER_SIZE]
            payload_view = view[self._HEADER_SIZE :]

            type_id = struct.unpack(self._HEADER_FORMAT, header_view)[0]
            message_class = self._registry.get(type_id)

            if message_class is None:
                raise SerializationError(message=f"Received unknown message type ID: {type_id}")

            obj = self._serializer.deserialize(data=payload_view, obj_type=message_class)

            try:
                self._incoming_obj_queue.put_nowait(item=obj)
            except asyncio.QueueFull:
                session = self._session()
                session_id = session.session_id if session is not None else "unknown"
                _logger.warning("Structured datagram queue full for session %s; dropping datagram.", session_id)

        except (struct.error, SerializationError) as e:
            _logger.warning("Failed to deserialize structured datagram: %s", e)
        except Exception as e:
            _logger.error("Error in datagram receive handler: %s", e, exc_info=True)
