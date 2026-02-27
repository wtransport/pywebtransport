"""FFI Application Binary Interface (ABI) data contract and operation codes."""

from __future__ import annotations

from typing import Final

__all__: list[str] = []

ABI_VERSION: Final[int] = 1
CONNECTION_EFFECTS: Final[int] = 0x00
CONNECTION_SPAWNED: Final[int] = 0x01
CONSUMED: Final[int] = 0x02
TRANSMIT: Final[int] = 0x03
CLEANUP_H3_STREAM: Final[int] = 0x40
EMIT_CONNECTION_EVENT: Final[int] = 0x41
EMIT_SESSION_EVENT: Final[int] = 0x42
EMIT_STREAM_EVENT: Final[int] = 0x43
NOTIFY_REQUEST_DONE: Final[int] = 0x44
NOTIFY_REQUEST_FAILED: Final[int] = 0x45
CONNECTION_CLOSE: Final[int] = 0x80
USER_ACCEPT_SESSION: Final[int] = 0x81
USER_CLOSE_SESSION: Final[int] = 0x82
USER_CONNECTION_GRACEFUL_CLOSE: Final[int] = 0x83
USER_CREATE_SESSION: Final[int] = 0x84
USER_CREATE_STREAM: Final[int] = 0x85
USER_GET_CONNECTION_DIAGNOSTICS: Final[int] = 0x86
USER_GET_SESSION_DIAGNOSTICS: Final[int] = 0x87
USER_GET_STREAM_DIAGNOSTICS: Final[int] = 0x88
USER_GRANT_DATA_CREDIT: Final[int] = 0x89
USER_GRANT_STREAMS_CREDIT: Final[int] = 0x8A
USER_REJECT_SESSION: Final[int] = 0x8B
USER_RESET_STREAM: Final[int] = 0x8C
USER_SEND_DATAGRAM: Final[int] = 0x8D
USER_SEND_STREAM_DATA: Final[int] = 0x8E
USER_STOP_SENDING: Final[int] = 0x8F
USER_STREAM_READ: Final[int] = 0x90
