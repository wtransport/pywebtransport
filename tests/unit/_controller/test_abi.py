"""Unit tests for the pywebtransport._controller.abi module."""

import pytest

from pywebtransport._controller import abi


class TestAbi:

    @pytest.mark.parametrize(
        argnames="constant_name, expected_value",
        argvalues=[
            ("ABI_VERSION", 6),
            ("COMMAND_COMPLETED", 0x00),
            ("COMMAND_FAILED", 0x01),
            ("CONNECTION_EFFECTS", 0x02),
            ("CONNECTION_SPAWNED", 0x03),
            ("REACTOR_SHUTDOWN", 0x04),
            ("CLEANUP_H3_STREAM", 0x40),
            ("EMIT_CONNECTION_EVENT", 0x41),
            ("EMIT_SESSION_EVENT", 0x42),
            ("EMIT_STREAM_EVENT", 0x43),
            ("EXPORT_TLS_KEYING_MATERIAL", 0x44),
            ("NOTIFY_REQUEST_DONE", 0x45),
            ("NOTIFY_REQUEST_FAILED", 0x46),
            ("USER_ACCEPT_SESSION", 0x80),
            ("USER_CLOSE_CONNECTION", 0x81),
            ("USER_CLOSE_CONNECTION_GRACEFULLY", 0x82),
            ("USER_CLOSE_SESSION", 0x83),
            ("USER_CREATE_SESSION", 0x84),
            ("USER_CREATE_STREAM", 0x85),
            ("USER_EXPORT_KEYING_MATERIAL", 0x86),
            ("USER_GET_CONNECTION_DIAGNOSTICS", 0x87),
            ("USER_GET_SESSION_DIAGNOSTICS", 0x88),
            ("USER_GET_STREAM_DIAGNOSTICS", 0x89),
            ("USER_READ_STREAM", 0x8A),
            ("USER_REJECT_SESSION", 0x8B),
            ("USER_RESET_STREAM", 0x8C),
            ("USER_SEND_DATAGRAM", 0x8D),
            ("USER_SEND_STREAM_DATA", 0x8E),
            ("USER_STOP_SENDING", 0x8F),
        ],
    )
    def test_abi_constants(self, *, constant_name: str, expected_value: int) -> None:
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
