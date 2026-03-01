"""Unit tests for the pywebtransport._driver.abi module."""

import pytest

from pywebtransport._driver import abi


class TestAbi:

    @pytest.mark.parametrize(
        argnames="constant_name, expected_value",
        argvalues=[
            ("ABI_VERSION", 1),
            ("CONNECTION_EFFECTS", 0x00),
            ("CONNECTION_SPAWNED", 0x01),
            ("CONSUMED", 0x02),
            ("TRANSMIT", 0x03),
            ("CLEANUP_H3_STREAM", 0x40),
            ("EMIT_CONNECTION_EVENT", 0x41),
            ("EMIT_SESSION_EVENT", 0x42),
            ("EMIT_STREAM_EVENT", 0x43),
            ("NOTIFY_REQUEST_DONE", 0x44),
            ("NOTIFY_REQUEST_FAILED", 0x45),
            ("CONNECTION_CLOSE", 0x80),
            ("USER_ACCEPT_SESSION", 0x81),
            ("USER_CLOSE_SESSION", 0x82),
            ("USER_CONNECTION_GRACEFUL_CLOSE", 0x83),
            ("USER_CREATE_SESSION", 0x84),
            ("USER_CREATE_STREAM", 0x85),
            ("USER_GET_CONNECTION_DIAGNOSTICS", 0x86),
            ("USER_GET_SESSION_DIAGNOSTICS", 0x87),
            ("USER_GET_STREAM_DIAGNOSTICS", 0x88),
            ("USER_GRANT_DATA_CREDIT", 0x89),
            ("USER_GRANT_STREAMS_CREDIT", 0x8A),
            ("USER_REJECT_SESSION", 0x8B),
            ("USER_RESET_STREAM", 0x8C),
            ("USER_SEND_DATAGRAM", 0x8D),
            ("USER_SEND_STREAM_DATA", 0x8E),
            ("USER_STOP_SENDING", 0x8F),
            ("USER_STREAM_READ", 0x90),
        ],
    )
    def test_abi_constants(self, constant_name: str, expected_value: int) -> None:
        actual_value = getattr(abi, constant_name)

        assert isinstance(actual_value, int)
        assert actual_value == expected_value

    def test_all_declaration(self) -> None:
        assert getattr(abi, "__all__") == []

    def test_opcodes_uniqueness(self) -> None:
        opcodes = [
            value
            for name, value in vars(abi).items()
            if name.isupper() and name != "ABI_VERSION" and isinstance(value, int)
        ]

        assert len(opcodes) == len(set(opcodes))
