"""Unit tests for the pywebtransport._adapter.patches module."""

from unittest.mock import MagicMock, patch

from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import StopSendingReceived
from aioquic.quic.stream import QuicStream

from pywebtransport._adapter.patches import (
    _handle_stop_sending_frame_replacement,
    _is_finished_replacement,
    apply_patches,
)


class TestPatches:

    def test_apply_patches(self) -> None:
        original_handle = getattr(QuicConnection, "_handle_stop_sending_frame")
        original_finished = getattr(QuicStream, "is_finished")

        try:
            apply_patches()

            assert getattr(QuicConnection, "_handle_stop_sending_frame") is _handle_stop_sending_frame_replacement
            assert getattr(QuicStream, "is_finished").fget is _is_finished_replacement
        finally:
            setattr(QuicConnection, "_handle_stop_sending_frame", original_handle)
            setattr(QuicStream, "is_finished", original_finished)

    def test_handle_stop_sending_frame_no_logger(self) -> None:
        mock_self = MagicMock()
        mock_self._quic_logger = None
        mock_self._events = []
        mock_buf = MagicMock()
        mock_buf.pull_uint_var.side_effect = [8, 456]

        _handle_stop_sending_frame_replacement(mock_self, MagicMock(), 0x05, mock_buf)

        assert len(mock_self._events) == 1
        assert mock_self._events[0].error_code == 456

    def test_handle_stop_sending_frame_with_logger(self) -> None:
        mock_self = MagicMock()
        mock_self._quic_logger = MagicMock()
        mock_self._events = []
        mock_context = MagicMock()
        mock_buf = MagicMock()
        mock_buf.pull_uint_var.side_effect = [4, 123]

        _handle_stop_sending_frame_replacement(mock_self, mock_context, 0x05, mock_buf)

        assert mock_buf.pull_uint_var.call_count == 2
        mock_self._quic_logger.encode_stop_sending_frame.assert_called_once_with(error_code=123, stream_id=4)
        mock_self._quic_logger.log_event.assert_called_once()
        mock_self._assert_stream_can_send.assert_called_once_with(0x05, 4)
        mock_self._get_or_create_stream.assert_called_once_with(0x05, 4)
        assert len(mock_self._events) == 1
        event = mock_self._events[0]
        assert isinstance(event, StopSendingReceived)
        assert event.stream_id == 4
        assert event.error_code == 123

    def test_is_finished_replacement_bidirectional(self) -> None:
        with patch("pywebtransport._adapter.patches.ORIGINAL_IS_FINISHED_GETTER") as mock_getter:
            mock_getter.return_value = True
            mock_stream = MagicMock()
            mock_stream.stream_id = 0

            result = _is_finished_replacement(mock_stream)

            assert result is True

    def test_is_finished_replacement_not_finished(self) -> None:
        with patch("pywebtransport._adapter.patches.ORIGINAL_IS_FINISHED_GETTER") as mock_getter:
            mock_getter.return_value = False
            mock_stream = MagicMock()

            result = _is_finished_replacement(mock_stream)

            assert result is False
            mock_getter.assert_called_once_with(mock_stream)

    def test_is_finished_replacement_uni_active_retention(self) -> None:
        with patch("pywebtransport._adapter.patches.ORIGINAL_IS_FINISHED_GETTER") as mock_getter:
            mock_getter.return_value = True
            mock_stream = MagicMock()
            mock_stream.stream_id = 2
            mock_stream.sender.reset_pending = False
            mock_stream.sender._reset_error_code = None

            result = _is_finished_replacement(mock_stream)

            assert result is False

    def test_is_finished_replacement_uni_reset_code_set(self) -> None:
        with patch("pywebtransport._adapter.patches.ORIGINAL_IS_FINISHED_GETTER") as mock_getter:
            mock_getter.return_value = True
            mock_stream = MagicMock()
            mock_stream.stream_id = 2
            mock_stream.sender.reset_pending = False
            mock_stream.sender._reset_error_code = 123

            result = _is_finished_replacement(mock_stream)

            assert result is True

    def test_is_finished_replacement_uni_reset_pending(self) -> None:
        with patch("pywebtransport._adapter.patches.ORIGINAL_IS_FINISHED_GETTER") as mock_getter:
            mock_getter.return_value = True
            mock_stream = MagicMock()
            mock_stream.stream_id = 2
            mock_stream.sender.reset_pending = True
            mock_stream.sender._reset_error_code = None

            result = _is_finished_replacement(mock_stream)

            assert result is True
