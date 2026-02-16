"""Monkey patches for upstream aioquic dependencies."""

from __future__ import annotations

from typing import Any, Final

from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import StopSendingReceived
from aioquic.quic.stream import QuicStream

__all__: list[str] = []

_ORIGINAL_IS_FINISHED_GETTER: Final[Any] = QuicStream.is_finished.fget  # type: ignore[attr-defined]


def apply_patches() -> None:
    """Apply protocol compliance patches to upstream aioquic classes."""
    _patch_handle_stop_sending_frame()
    _patch_stream_is_finished()


def _handle_stop_sending_frame_replacement(self: Any, context: Any, frame_type: int, buf: Any) -> None:
    """Replacement method for QuicConnection._handle_stop_sending_frame."""
    stream_id = buf.pull_uint_var()
    error_code = buf.pull_uint_var()

    if self._quic_logger is not None:
        self._quic_logger.log_event(
            category="transport",
            event="packet_received",
            data={"frames": [self._quic_logger.encode_stop_sending_frame(error_code=error_code, stream_id=stream_id)]},
        )

    self._assert_stream_can_send(frame_type, stream_id)
    self._get_or_create_stream(frame_type, stream_id)

    self._events.append(StopSendingReceived(error_code=error_code, stream_id=stream_id))


def _is_finished_replacement(self: Any) -> bool:
    """Replacement getter for QuicStream.is_finished."""
    is_finished = _ORIGINAL_IS_FINISHED_GETTER(self)
    if is_finished:
        is_unidirectional = self.stream_id & 0x02
        if is_unidirectional and not self.sender.reset_pending and self.sender._reset_error_code is None:
            return False
    return is_finished  # type: ignore[no-any-return]


def _patch_handle_stop_sending_frame() -> None:
    """Suppress automatic stream reset generation upon receiving STOP_SENDING frames."""
    QuicConnection._handle_stop_sending_frame = _handle_stop_sending_frame_replacement  # type: ignore[method-assign]


def _patch_stream_is_finished() -> None:
    """Defer stream completion state for local unidirectional streams to allow late reset."""
    QuicStream.is_finished = property(_is_finished_replacement)  # type: ignore[assignment]
