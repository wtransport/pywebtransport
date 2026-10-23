"""FFI ABI definitions."""

from __future__ import annotations

from typing import Final

__all__: list[str] = []

ABI_VERSION: Final[int] = 8
COMMAND_FAILED: Final[int] = 0x00
CONNECTION_EFFECTS: Final[int] = 0x01
CONNECTION_SPAWNED: Final[int] = 0x02
REACTOR_SHUTDOWN: Final[int] = 0x03
EMIT_CONNECTION_EVENT: Final[int] = 0x40
EMIT_SESSION_EVENT: Final[int] = 0x41
EMIT_STREAM_EVENT: Final[int] = 0x42
NOTIFY_REQUEST_DONE: Final[int] = 0x43
NOTIFY_REQUEST_FAILED: Final[int] = 0x44
USER_ACCEPT_SESSION: Final[int] = 0x80
USER_CLOSE_CONNECTION: Final[int] = 0x81
USER_CLOSE_CONNECTION_GRACEFULLY: Final[int] = 0x82
USER_CLOSE_SESSION: Final[int] = 0x83
USER_CREATE_SESSION: Final[int] = 0x84
USER_CREATE_SESSION_OPTIMISTIC: Final[int] = 0x85
USER_CREATE_STREAM: Final[int] = 0x86
USER_DIAGNOSE_CONNECTION: Final[int] = 0x87
USER_DIAGNOSE_SESSION: Final[int] = 0x88
USER_DIAGNOSE_STREAM: Final[int] = 0x89
USER_EXPORT_KEYING_MATERIAL: Final[int] = 0x8A
USER_READ_DATAGRAM: Final[int] = 0x8B
USER_READ_STREAM: Final[int] = 0x8C
USER_REJECT_SESSION: Final[int] = 0x8D
USER_RESET_STREAM: Final[int] = 0x8E
USER_SEND_DATAGRAM: Final[int] = 0x8F
USER_STOP_SENDING: Final[int] = 0x90
USER_WRITE_STREAM: Final[int] = 0x91
