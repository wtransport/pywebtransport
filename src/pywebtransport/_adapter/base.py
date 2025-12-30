"""Shared protocol adapter logic for client and server."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from typing import Any

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import (
    ConnectionTerminated,
    DatagramFrameReceived,
    HandshakeCompleted,
    QuicEvent,
    StreamDataReceived,
    StreamReset,
)
from aioquic.quic.logger import QuicLoggerTrace

from pywebtransport._adapter.pending import PendingRequestManager
from pywebtransport._protocol.events import (
    CloseQuicConnection,
    CreateH3Session,
    CreateQuicStream,
    Effect,
    EmitConnectionEvent,
    EmitSessionEvent,
    EmitStreamEvent,
    InternalBindH3Session,
    InternalBindQuicStream,
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalFailH3Session,
    InternalFailQuicStream,
    InternalReturnStreamData,
    LogH3Frame,
    NotifyRequestDone,
    NotifyRequestFailed,
    ProcessProtocolEvent,
    ProtocolEvent,
    RescheduleQuicTimer,
    ResetQuicStream,
    SendH3Capsule,
    SendH3Datagram,
    SendH3Goaway,
    SendH3Headers,
    SendQuicData,
    SendQuicDatagram,
    StopQuicStream,
    TransportConnectionTerminated,
    TransportDatagramFrameReceived,
    TransportHandshakeCompleted,
    TransportQuicParametersReceived,
    TransportQuicTimerFired,
    TransportStreamDataReceived,
    TransportStreamReset,
    TriggerQuicTimer,
)
from pywebtransport._protocol.webtransport_engine import WebTransportEngine
from pywebtransport.config import ClientConfig, ServerConfig
from pywebtransport.constants import DEFAULT_MAX_EVENT_QUEUE_SIZE, ErrorCodes
from pywebtransport.exceptions import ConnectionError
from pywebtransport.types import Buffer, EventType
from pywebtransport.utils import get_logger

__all__: list[str] = []

logger = get_logger(name=__name__)


class WebTransportCommonProtocol(QuicConnectionProtocol):
    """Base adapter translating quic events to internal protocol events."""

    _quic_logger: QuicLoggerTrace | None = None

    def __init__(
        self,
        *,
        quic: QuicConnection,
        config: ClientConfig | ServerConfig,
        is_client: bool,
        stream_handler: Any = None,
        loop: asyncio.AbstractEventLoop | None = None,
        max_event_queue_size: int = DEFAULT_MAX_EVENT_QUEUE_SIZE,
    ) -> None:
        """Initialize the common protocol adapter."""
        super().__init__(quic=quic, stream_handler=stream_handler)
        self._loop = loop if loop is not None else asyncio.get_running_loop()
        self._config = config
        self._is_client = is_client
        self._max_event_queue_size = max_event_queue_size
        self._timer_handle: asyncio.TimerHandle | None = None

        self._pending_manager = PendingRequestManager()

        self._engine = WebTransportEngine(connection_id=str(quic.host_cid), config=config, is_client=is_client)

        self._resource_gc_timer: asyncio.TimerHandle | None = None
        self._early_event_cleanup_timer: asyncio.TimerHandle | None = None

        self._pending_effects: deque[Effect] = deque()
        self._is_processing_effects = False
        self._status_callback: Callable[[EventType, dict[str, Any]], None] | None = None

    def close_connection(self, *, error_code: int, reason_phrase: str | None = None) -> None:
        """Close the QUIC connection."""
        if self._quic._close_event is not None:
            return

        self._quic.close(error_code=error_code, reason_phrase=reason_phrase if reason_phrase is not None else "")
        self.transmit()
        self._cancel_maintenance_timers()
        self._pending_manager.fail_all(exception=ConnectionError(f"Connection closed: {reason_phrase}"))

    def connection_lost(self, exc: Exception | None) -> None:
        """Handle connection loss."""
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None

        self._cancel_maintenance_timers()

        event_to_send: TransportConnectionTerminated | None = None
        already_closing_locally = self._quic._close_event is not None

        if exc is None and already_closing_locally:
            pass
        else:
            if exc is not None:
                code = getattr(exc, "error_code", ErrorCodes.INTERNAL_ERROR)
                reason = str(exc)
            else:
                code = ErrorCodes.NO_ERROR
                reason = "Connection closed"
            event_to_send = TransportConnectionTerminated(error_code=code, reason_phrase=reason)

        if event_to_send is not None:
            self._push_event_to_engine(event=event_to_send)

        self._pending_manager.fail_all(exception=exc if exc is not None else ConnectionError("Connection lost"))
        super().connection_lost(exc)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Handle connection being made."""
        super().connection_made(transport)
        self._setup_maintenance_timers()

    def create_request(self) -> tuple[int, asyncio.Future[Any]]:
        """Create a new tracked request."""
        return self._pending_manager.create_request()

    def get_next_available_stream_id(self, *, is_unidirectional: bool) -> int:
        """Get the next available stream ID from the QUIC connection."""
        return self._quic.get_next_available_stream_id(is_unidirectional=is_unidirectional)

    def get_server_name(self) -> str | None:
        """Get the server name (SNI) from the QUIC configuration."""
        return self._quic.configuration.server_name

    def handle_timer_now(self) -> None:
        """Handle the QUIC timer expiry."""
        self._quic.handle_timer(now=self._loop.time())

        event = self._quic.next_event()
        while event is not None:
            self.quic_event_received(event=event)
            event = self._quic.next_event()

        self.transmit()

    def log_event(self, *, category: str, event: str, data: dict[str, Any]) -> None:
        """Log an H3 event via the QUIC logger."""
        if self._quic_logger is not None:
            self._quic_logger.log_event(category=category, event=event, data=data)

    def quic_event_received(self, event: QuicEvent) -> None:
        """Translate aioquic events into internal ProtocolEvents."""
        match event:
            case HandshakeCompleted():
                self._on_handshake_completed()
            case ConnectionTerminated(error_code=error_code, reason_phrase=reason_phrase):
                logger.debug(
                    "QUIC ConnectionTerminated event received: code=%#x reason='%s'", error_code, reason_phrase
                )
                self._push_event_to_engine(
                    event=TransportConnectionTerminated(error_code=error_code, reason_phrase=reason_phrase)
                )
            case DatagramFrameReceived(data=data):
                self._push_event_to_engine(event=TransportDatagramFrameReceived(data=data))
            case StreamDataReceived(data=data, end_stream=end_stream, stream_id=stream_id):
                self._push_event_to_engine(
                    event=TransportStreamDataReceived(data=data, end_stream=end_stream, stream_id=stream_id)
                )
            case StreamReset(error_code=error_code, stream_id=stream_id):
                self._push_event_to_engine(event=TransportStreamReset(error_code=error_code, stream_id=stream_id))
            case _:
                pass

    def reset_stream(self, *, stream_id: int, error_code: int) -> None:
        """Reset the sending side of a QUIC stream."""
        if self._quic._close_event is not None:
            return

        try:
            self._quic.reset_stream(stream_id=stream_id, error_code=error_code)
            self.transmit()
        except (AssertionError, ValueError):
            logger.debug("Dropping ResetQuicStream for stream %d: Stream unknown or state conflict.", stream_id)

    def schedule_timer_now(self) -> None:
        """Schedule the next QUIC timer callback."""
        if self._timer_handle is not None:
            self._timer_handle.cancel()

        timer_at = self._quic.get_timer()
        if timer_at is not None:
            self._timer_handle = self._loop.call_at(timer_at, self._handle_timer)

    def send_datagram_frame(self, *, data: Buffer | list[Buffer]) -> None:
        """Send a QUIC datagram frame (supports Scatter/Gather)."""
        if self._quic._close_event is not None:
            logger.debug("Attempted to send datagram while connection is closing.")
            return

        bytes_data: bytes
        if isinstance(data, list):
            bytes_data = b"".join(data)
        else:
            bytes_data = bytes(data)

        self._quic.send_datagram_frame(data=bytes_data)
        self.transmit()

    def send_event(self, *, event: ProtocolEvent) -> None:
        """Send a user-initiated event to the engine."""
        self._push_event_to_engine(event=event)

    def send_stream_data(self, *, stream_id: int, data: bytes, end_stream: bool = False) -> None:
        """Send data on a QUIC stream."""
        if self._quic._close_event is not None:
            if data or not end_stream:
                logger.debug("Attempted to send stream data while connection is closing (stream %d).", stream_id)
                return

        try:
            self._quic.send_stream_data(stream_id=stream_id, data=data, end_stream=end_stream)
            self.transmit()
        except (AssertionError, ValueError):
            logger.debug("Dropping SendQuicData for stream %d: Stream unknown or state conflict.", stream_id)

    def set_status_callback(self, *, callback: Callable[[EventType, dict[str, Any]], None]) -> None:
        """Set the callback for high-level status events."""
        self._status_callback = callback

    def stop_stream(self, *, stream_id: int, error_code: int) -> None:
        """Stop the receiving side of a QUIC stream."""
        try:
            self._quic.stop_stream(stream_id=stream_id, error_code=error_code)
        except (AssertionError, ValueError):
            logger.debug("Dropping StopQuicStream for stream %d: Stream unknown or state conflict.", stream_id)

    def transmit(self) -> None:
        """Transmit pending QUIC packets."""
        transport = self._transport
        if (
            transport is not None
            and hasattr(transport, "is_closing")
            and not transport.is_closing()
            and hasattr(transport, "sendto")
        ):
            packets = self._quic.datagrams_to_send(now=self._loop.time())
            is_client = self._quic.configuration.is_client
            for data, addr in packets:
                try:
                    if is_client:
                        transport.sendto(data)
                    else:
                        transport.sendto(data, addr)
                except (ConnectionRefusedError, OSError) as e:
                    logger.debug("Failed to send UDP packet: %s", e)
                except Exception as e:
                    logger.error("Unexpected error during transmit: %s", e, exc_info=True)

    def _allocate_stream_id(self, *, is_unidirectional: bool) -> int:
        """Atomically allocate and reserve a stream ID."""
        stream_id = self._quic.get_next_available_stream_id(is_unidirectional=is_unidirectional)
        self._quic.send_stream_data(stream_id=stream_id, data=b"", end_stream=False)
        return stream_id

    def _cancel_maintenance_timers(self) -> None:
        """Cancel internal maintenance timers."""
        if self._resource_gc_timer is not None:
            self._resource_gc_timer.cancel()
            self._resource_gc_timer = None
        if self._early_event_cleanup_timer is not None:
            self._early_event_cleanup_timer.cancel()
            self._early_event_cleanup_timer = None

    def _execute_effects(self, *, effects: list[Effect]) -> None:
        """Execute effects returned by the engine."""
        for effect in effects:
            self._pending_effects.append(effect)

        if self._is_processing_effects:
            return

        self._is_processing_effects = True
        try:
            while self._pending_effects:
                effect = self._pending_effects.popleft()
                self._process_single_effect(effect=effect)
        finally:
            self._is_processing_effects = False

    def _handle_early_event_cleanup_timer(self) -> None:
        """Trigger early event cleanup in the engine."""
        self._early_event_cleanup_timer = None
        self._push_event_to_engine(event=InternalCleanupEarlyEvents())
        if self._config.pending_event_ttl > 0:
            self._early_event_cleanup_timer = self._loop.call_later(
                self._config.pending_event_ttl, self._handle_early_event_cleanup_timer
            )

    def _handle_resource_gc_timer(self) -> None:
        """Trigger resource GC in the engine."""
        self._resource_gc_timer = None
        self._push_event_to_engine(event=InternalCleanupResources())
        if self._config.resource_cleanup_interval > 0:
            self._resource_gc_timer = self._loop.call_later(
                self._config.resource_cleanup_interval, self._handle_resource_gc_timer
            )

    def _handle_timer(self) -> None:
        """Handle the QUIC timer expiry by injecting an event."""
        self._timer_handle = None
        self._push_event_to_engine(event=TransportQuicTimerFired())

    def _on_handshake_completed(self) -> None:
        """Handle QUIC handshake completion."""
        self._push_event_to_engine(event=TransportHandshakeCompleted())

        self._quic_logger = getattr(self._quic, "_quic_logger", None)

        control_id = self._quic.get_next_available_stream_id(is_unidirectional=True)
        encoder_id = self._quic.get_next_available_stream_id(is_unidirectional=True)
        decoder_id = self._quic.get_next_available_stream_id(is_unidirectional=True)

        init_effects = self._engine.initialize_h3_transport(
            control_id=control_id, encoder_id=encoder_id, decoder_id=decoder_id
        )
        self._execute_effects(effects=init_effects)

        remote_max_datagram_frame_size = getattr(self._quic, "_remote_max_datagram_frame_size", None)
        if remote_max_datagram_frame_size is not None:
            self._push_event_to_engine(
                event=TransportQuicParametersReceived(remote_max_datagram_frame_size=remote_max_datagram_frame_size)
            )

    def _process_single_effect(self, *, effect: Effect) -> None:
        """Process a single side effect."""
        match effect:
            case SendQuicData(stream_id=sid, data=d, end_stream=es):
                self.send_stream_data(stream_id=sid, data=bytes(d), end_stream=es)

            case SendQuicDatagram(data=d):
                self.send_datagram_frame(data=d)

            case ResetQuicStream(stream_id=sid, error_code=ec):
                self.reset_stream(stream_id=sid, error_code=ec)

            case StopQuicStream(stream_id=sid, error_code=ec):
                self.stop_stream(stream_id=sid, error_code=ec)

            case CloseQuicConnection(error_code=ec, reason=r):
                self.close_connection(error_code=ec, reason_phrase=r)

            case NotifyRequestDone(request_id=rid, result=res):
                self._pending_manager.complete_request(request_id=rid, result=res)

            case NotifyRequestFailed(request_id=rid, exception=exc):
                self._pending_manager.fail_request(request_id=rid, exception=exc)

            case CreateH3Session(request_id=rid, path=p, headers=h):
                try:
                    stream_id = self._allocate_stream_id(is_unidirectional=False)
                    server_name = self.get_server_name()
                    authority = server_name if server_name is not None else ""
                    h3_effects = self._engine.encode_session_request(
                        stream_id=stream_id, path=p, headers=h, authority=authority
                    )
                    self._execute_effects(effects=h3_effects)
                    self._push_event_to_engine(event=InternalBindH3Session(request_id=rid, stream_id=stream_id))
                except Exception as e:
                    self._push_event_to_engine(event=InternalFailH3Session(request_id=rid, exception=e))

            case CreateQuicStream(request_id=rid, session_id=sid, is_unidirectional=uni):
                try:
                    stream_id = self._allocate_stream_id(is_unidirectional=uni)
                    control_stream_id = sid
                    h3_effects = self._engine.encode_stream_creation(
                        stream_id=stream_id, control_stream_id=control_stream_id, is_unidirectional=uni
                    )
                    self._execute_effects(effects=h3_effects)
                    self._push_event_to_engine(
                        event=InternalBindQuicStream(
                            request_id=rid, stream_id=stream_id, session_id=sid, is_unidirectional=uni
                        )
                    )
                except Exception as e:
                    self._push_event_to_engine(
                        event=InternalFailQuicStream(request_id=rid, session_id=sid, is_unidirectional=uni, exception=e)
                    )

            case SendH3Headers(stream_id=sid, status=s, end_stream=end):
                h3_effects = self._engine.encode_headers(stream_id=sid, status=s, end_stream=end)
                self._execute_effects(effects=h3_effects)

            case SendH3Capsule(stream_id=sid, capsule_type=t, capsule_data=d, end_stream=es):
                h3_effects = self._engine.encode_capsule(
                    stream_id=sid, capsule_type=t, capsule_data=bytes(d), end_stream=es
                )
                self._execute_effects(effects=h3_effects)

            case SendH3Datagram(stream_id=sid, data=d):
                h3_effects = self._engine.encode_datagram(stream_id=sid, data=d)
                self._execute_effects(effects=h3_effects)

            case SendH3Goaway():
                h3_effects = self._engine.encode_goaway()
                self._execute_effects(effects=h3_effects)

            case RescheduleQuicTimer():
                self.schedule_timer_now()

            case TriggerQuicTimer():
                self.handle_timer_now()

            case ProcessProtocolEvent(event=evt):
                immediate_effects = self._engine.handle_event(event=evt)
                self._pending_effects.extendleft(reversed(immediate_effects))

            case EmitConnectionEvent(event_type=et, data=d):
                if self._status_callback is not None:
                    self._status_callback(et, d)

            case EmitSessionEvent(event_type=et, data=d):
                if self._status_callback is not None:
                    self._status_callback(et, d)

            case EmitStreamEvent(event_type=et, data=d):
                if self._status_callback is not None:
                    self._status_callback(et, d)

            case LogH3Frame(category=c, event=e, data=d):
                self.log_event(category=c, event=e, data=d)

            case InternalReturnStreamData(stream_id=sid, data=d):
                self._push_event_to_engine(event=InternalReturnStreamData(stream_id=sid, data=d))

            case _:
                pass

    def _push_event_to_engine(self, *, event: ProtocolEvent) -> None:
        """Push an event to the engine and execute resulting effects."""
        effects = self._engine.handle_event(event=event)
        self._execute_effects(effects=effects)
        self.transmit()

    def _setup_maintenance_timers(self) -> None:
        """Start internal maintenance timers."""
        if self._config.resource_cleanup_interval > 0:
            self._resource_gc_timer = self._loop.call_later(
                self._config.resource_cleanup_interval, self._handle_resource_gc_timer
            )
        if self._config.pending_event_ttl > 0:
            self._early_event_cleanup_timer = self._loop.call_later(
                self._config.pending_event_ttl, self._handle_early_event_cleanup_timer
            )
