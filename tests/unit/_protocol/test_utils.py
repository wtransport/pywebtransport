"""Unit tests for the pywebtransport._protocol.utils module."""

from contextlib import nullcontext
from typing import Any, ContextManager

import pytest
from pytest_mock import MockerFixture

from pywebtransport import ErrorCodes, ProtocolError
from pywebtransport._protocol import utils as protocol_utils
from pywebtransport.constants import MAX_STREAM_ID
from pywebtransport.types import StreamDirection


class TestUtils:

    @pytest.mark.parametrize(
        "stream_id, is_client, expected",
        [
            (0, True, True),
            (0, False, True),
            (1, True, True),
            (1, False, True),
            (2, True, False),
            (2, False, True),
            (3, True, True),
            (3, False, False),
        ],
    )
    def test_can_receive_data_on_stream(self, stream_id: int, is_client: bool, expected: bool) -> None:
        result = protocol_utils.can_receive_data_on_stream(stream_id=stream_id, is_client=is_client)

        assert result == expected

    @pytest.mark.parametrize(
        "stream_id, is_client, expected",
        [
            (0, True, True),
            (0, False, True),
            (1, True, True),
            (1, False, True),
            (2, True, True),
            (2, False, False),
            (3, True, False),
            (3, False, True),
        ],
    )
    def test_can_send_data_on_stream(self, stream_id: int, is_client: bool, expected: bool) -> None:
        result = protocol_utils.can_send_data_on_stream(stream_id=stream_id, is_client=is_client)

        assert result == expected

    @pytest.mark.parametrize(
        "stream_id, is_client, expected_direction",
        [
            (0, True, StreamDirection.BIDIRECTIONAL),
            (0, False, StreamDirection.BIDIRECTIONAL),
            (1, True, StreamDirection.BIDIRECTIONAL),
            (1, False, StreamDirection.BIDIRECTIONAL),
            (2, True, StreamDirection.SEND_ONLY),
            (2, False, StreamDirection.RECEIVE_ONLY),
            (3, True, StreamDirection.RECEIVE_ONLY),
            (3, False, StreamDirection.SEND_ONLY),
        ],
    )
    def test_get_stream_direction_from_id(
        self, mocker: MockerFixture, stream_id: int, is_client: bool, expected_direction: StreamDirection
    ) -> None:
        mock_validate = mocker.patch("pywebtransport._protocol.utils.validate_stream_id")

        direction = protocol_utils.get_stream_direction_from_id(stream_id=stream_id, is_client=is_client)

        if __debug__:
            mock_validate.assert_called_once_with(stream_id=stream_id)
        assert direction == expected_direction

    def test_get_stream_direction_from_id_unreachable(self, mocker: MockerFixture) -> None:
        mocker.patch("pywebtransport._protocol.utils.validate_stream_id")
        mocker.patch("pywebtransport._protocol.utils.is_bidirectional_stream", return_value=False)
        mocker.patch("pywebtransport._protocol.utils.can_send_data_on_stream", return_value=None)

        with pytest.raises(AssertionError, match="Unreachable code: Invalid stream direction logic"):
            protocol_utils.get_stream_direction_from_id(stream_id=0, is_client=True)

    @pytest.mark.parametrize(
        "http_code, expected_wt_code, expectation",
        [
            (ErrorCodes.WT_APPLICATION_ERROR_FIRST + 0, 0, nullcontext()),
            (ErrorCodes.WT_APPLICATION_ERROR_FIRST + 29, 29, nullcontext()),
            (ErrorCodes.WT_APPLICATION_ERROR_FIRST + 31, 30, nullcontext()),
            (ErrorCodes.WT_APPLICATION_ERROR_FIRST + 12345 + (12345 // 30), 12345, nullcontext()),
            (ErrorCodes.WT_APPLICATION_ERROR_FIRST + 0xFFFFFFFF + (0xFFFFFFFF // 30), 0xFFFFFFFF, nullcontext()),
            (
                ErrorCodes.WT_APPLICATION_ERROR_FIRST - 1,
                0,
                pytest.raises(ValueError, match="not in the WebTransport application range"),
            ),
            (
                ErrorCodes.WT_APPLICATION_ERROR_LAST + 1,
                0,
                pytest.raises(ValueError, match="not in the WebTransport application range"),
            ),
            (ErrorCodes.WT_APPLICATION_ERROR_FIRST + 0x1E, 0, pytest.raises(ValueError, match="reserved codepoint")),
        ],
    )
    def test_http_code_to_webtransport_code(
        self, http_code: int, expected_wt_code: int, expectation: ContextManager[Any]
    ) -> None:
        with expectation:
            result = protocol_utils.http_code_to_webtransport_code(http_error_code=http_code)

            assert result == expected_wt_code

    @pytest.mark.parametrize("stream_id, expected", [(0, True), (1, True), (2, False), (3, False)])
    def test_is_bidirectional_stream(self, stream_id: int, expected: bool) -> None:
        result = protocol_utils.is_bidirectional_stream(stream_id=stream_id)

        assert result == expected

    @pytest.mark.parametrize("stream_id, expected", [(0, True), (2, True), (1, False), (3, False)])
    def test_is_client_initiated_stream(self, stream_id: int, expected: bool) -> None:
        # Accessing protected member for unit testing verification
        result = protocol_utils._is_client_initiated_stream(stream_id=stream_id)

        assert result == expected

    @pytest.mark.parametrize("stream_id, expected", [(0, True), (4, True), (1, False), (2, False)])
    def test_is_request_response_stream(self, stream_id: int, expected: bool) -> None:
        result = protocol_utils.is_request_response_stream(stream_id=stream_id)

        assert result == expected

    @pytest.mark.parametrize("stream_id, expected", [(1, True), (3, True), (0, False), (2, False)])
    def test_is_server_initiated_stream(self, stream_id: int, expected: bool) -> None:
        result = protocol_utils._is_server_initiated_stream(stream_id=stream_id)

        assert result == expected

    @pytest.mark.parametrize("stream_id, expected", [(2, True), (3, True), (0, False), (1, False)])
    def test_is_unidirectional_stream(self, stream_id: int, expected: bool) -> None:
        result = protocol_utils.is_unidirectional_stream(stream_id=stream_id)

        assert result == expected

    @pytest.mark.parametrize(
        "stream_id, expectation",
        [
            (0, nullcontext()),
            (4, nullcontext()),
            (100, nullcontext()),
            (1, pytest.raises(ProtocolError, match="Invalid Session ID format")),
            (2, pytest.raises(ProtocolError, match="Invalid Session ID format")),
            (3, pytest.raises(ProtocolError, match="Invalid Session ID format")),
            (5, pytest.raises(ProtocolError, match="Invalid Session ID format")),
        ],
    )
    def test_validate_control_stream_id(self, stream_id: int, expectation: ContextManager[Any]) -> None:
        with expectation:
            protocol_utils.validate_control_stream_id(stream_id=stream_id)

    @pytest.mark.parametrize(
        "value, expectation",
        [
            (0, nullcontext()),
            (MAX_STREAM_ID, nullcontext()),
            (-1, pytest.raises(ValueError)),
            (MAX_STREAM_ID + 1, pytest.raises(ValueError)),
            ("not-an-int", pytest.raises(TypeError)),
            (None, pytest.raises(TypeError)),
        ],
    )
    def test_validate_stream_id(self, value: Any, expectation: ContextManager[Any]) -> None:
        with expectation:
            protocol_utils.validate_stream_id(stream_id=value)

    @pytest.mark.parametrize(
        "stream_id, expectation",
        [
            (2, nullcontext()),
            (3, nullcontext()),
            (6, nullcontext()),
            (7, nullcontext()),
            (0, pytest.raises(ProtocolError, match="must be unidirectional")),
            (1, pytest.raises(ProtocolError, match="must be unidirectional")),
            (4, pytest.raises(ProtocolError, match="must be unidirectional")),
            (5, pytest.raises(ProtocolError, match="must be unidirectional")),
        ],
    )
    def test_validate_unidirectional_stream_id(self, stream_id: int, expectation: ContextManager[Any]) -> None:
        with expectation:
            protocol_utils.validate_unidirectional_stream_id(stream_id=stream_id)

    @pytest.mark.parametrize(
        "app_error_code, expected_http_code, expectation",
        [
            (0, ErrorCodes.WT_APPLICATION_ERROR_FIRST + 0, nullcontext()),
            (29, ErrorCodes.WT_APPLICATION_ERROR_FIRST + 29, nullcontext()),
            (30, ErrorCodes.WT_APPLICATION_ERROR_FIRST + 30 + 1, nullcontext()),
            (12345, ErrorCodes.WT_APPLICATION_ERROR_FIRST + 12345 + (12345 // 30), nullcontext()),
            (0xFFFFFFFF, ErrorCodes.WT_APPLICATION_ERROR_FIRST + 0xFFFFFFFF + (0xFFFFFFFF // 30), nullcontext()),
            (-1, 0, pytest.raises(ValueError, match="Application error code must be a 32-bit unsigned integer")),
            (
                0xFFFFFFFF + 1,
                0,
                pytest.raises(ValueError, match="Application error code must be a 32-bit unsigned integer"),
            ),
        ],
    )
    def test_webtransport_code_to_http_code(
        self, app_error_code: int, expected_http_code: int, expectation: ContextManager[Any]
    ) -> None:
        with expectation:
            result = protocol_utils.webtransport_code_to_http_code(app_error_code=app_error_code)

            assert result == expected_http_code
