"""FFI Application Binary Interface (ABI) data contract and operation codes."""

from __future__ import annotations

from typing import Final

__all__: list[str] = []

ABI_VERSION: Final[int] = 5
COMMAND_COMPLETED: Final[int] = 0x00
COMMAND_FAILED: Final[int] = 0x01
CONNECTION_EFFECTS: Final[int] = 0x02
CONNECTION_SPAWNED: Final[int] = 0x03
REACTOR_SHUTDOWN: Final[int] = 0x04
CLEANUP_H3_STREAM: Final[int] = 0x40
EMIT_CONNECTION_EVENT: Final[int] = 0x41
EMIT_SESSION_EVENT: Final[int] = 0x42
EMIT_STREAM_EVENT: Final[int] = 0x43
EXPORT_TLS_KEYING_MATERIAL: Final[int] = 0x44
NOTIFY_REQUEST_DONE: Final[int] = 0x45
NOTIFY_REQUEST_FAILED: Final[int] = 0x46
USER_ACCEPT_SESSION: Final[int] = 0x80
USER_CLOSE_CONNECTION: Final[int] = 0x81
USER_CLOSE_CONNECTION_GRACEFULLY: Final[int] = 0x82
USER_CLOSE_SESSION: Final[int] = 0x83
USER_CREATE_SESSION: Final[int] = 0x84
USER_CREATE_STREAM: Final[int] = 0x85
USER_EXPORT_KEYING_MATERIAL: Final[int] = 0x86
USER_GET_CONNECTION_DIAGNOSTICS: Final[int] = 0x87
USER_GET_SESSION_DIAGNOSTICS: Final[int] = 0x88
USER_GET_STREAM_DIAGNOSTICS: Final[int] = 0x89
USER_GRANT_DATA_CREDIT: Final[int] = 0x8A
USER_GRANT_STREAMS_CREDIT: Final[int] = 0x8B
USER_READ_STREAM: Final[int] = 0x8C
USER_REJECT_SESSION: Final[int] = 0x8D
USER_RESET_STREAM: Final[int] = 0x8E
USER_SEND_DATAGRAM: Final[int] = 0x8F
USER_SEND_STREAM_DATA: Final[int] = 0x90
USER_STOP_SENDING: Final[int] = 0x91
