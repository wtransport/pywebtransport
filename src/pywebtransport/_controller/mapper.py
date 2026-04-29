"""Translate domain models to ABI tuples for the FFI data contract."""

from __future__ import annotations

from typing import Any

from pywebtransport._controller import abi
from pywebtransport._protocol import events

__all__: list[str] = []


def pack_user_event(*, event: events.ProtocolEvent) -> tuple[int, tuple[Any, ...]]:
    """Translate the UserEvent domain model into a tightly packed FFI tuple."""
    match event:
        case events.UserAcceptSession():
            return abi.USER_ACCEPT_SESSION, (event.request_id, event.session_id, event.wt_protocol)
        case events.UserCloseConnection():
            return abi.USER_CLOSE_CONNECTION, (event.request_id, event.error_code, event.reason)
        case events.UserCloseConnectionGracefully():
            return abi.USER_CLOSE_CONNECTION_GRACEFULLY, (event.request_id,)
        case events.UserCloseSession():
            return abi.USER_CLOSE_SESSION, (event.request_id, event.session_id, event.error_code, event.reason)
        case events.UserCreateSession():
            return abi.USER_CREATE_SESSION, (event.request_id, event.path, event.headers, event.wt_available_protocols)
        case events.UserCreateStream():
            return abi.USER_CREATE_STREAM, (event.request_id, event.session_id, event.is_unidirectional)
        case events.UserExportKeyingMaterial():
            return abi.USER_EXPORT_KEYING_MATERIAL, (
                event.request_id,
                event.session_id,
                event.label,
                event.context,
                event.length,
            )
        case events.UserGetConnectionDiagnostics():
            return abi.USER_GET_CONNECTION_DIAGNOSTICS, (event.request_id,)
        case events.UserGetSessionDiagnostics():
            return abi.USER_GET_SESSION_DIAGNOSTICS, (event.request_id, event.session_id)
        case events.UserGetStreamDiagnostics():
            return abi.USER_GET_STREAM_DIAGNOSTICS, (event.request_id, event.stream_id)
        case events.UserGrantDataCredit():
            return abi.USER_GRANT_DATA_CREDIT, (event.request_id, event.session_id, event.max_data)
        case events.UserGrantStreamsCredit():
            return abi.USER_GRANT_STREAMS_CREDIT, (
                event.request_id,
                event.session_id,
                event.is_unidirectional,
                event.max_streams,
            )
        case events.UserReadStream():
            return abi.USER_READ_STREAM, (event.request_id, event.stream_id, event.max_bytes)
        case events.UserRejectSession():
            return abi.USER_REJECT_SESSION, (event.request_id, event.session_id, event.status_code)
        case events.UserResetStream():
            return abi.USER_RESET_STREAM, (event.request_id, event.stream_id, event.error_code)
        case events.UserSendDatagram():
            return abi.USER_SEND_DATAGRAM, (event.request_id, event.session_id, event.data)
        case events.UserSendStreamData():
            return abi.USER_SEND_STREAM_DATA, (event.request_id, event.stream_id, event.data, event.end_stream)
        case events.UserStopSending():
            return abi.USER_STOP_SENDING, (event.request_id, event.stream_id, event.error_code)
        case _:
            raise ValueError(f"rt_event convert invalid actual={type(event).__name__}")
