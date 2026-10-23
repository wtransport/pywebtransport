"""FFI event conversion."""

from __future__ import annotations

from typing import Any

from pywebtransport._controller import abi
from pywebtransport._controller.events import (
    CommandFailed,
    ConnectionEffects,
    ConnectionSpawned,
    Effect,
    EmitConnectionEvent,
    EmitSessionEvent,
    EmitStreamEvent,
    NotifyRequestDone,
    NotifyRequestFailed,
    ProtocolEvent,
    ReactorShutdown,
    RuntimeEvent,
    UserAcceptSession,
    UserCloseConnection,
    UserCloseConnectionGracefully,
    UserCloseSession,
    UserCreateSession,
    UserCreateSessionOptimistic,
    UserCreateStream,
    UserDiagnoseConnection,
    UserDiagnoseSession,
    UserDiagnoseStream,
    UserExportKeyingMaterial,
    UserReadDatagram,
    UserReadStream,
    UserRejectSession,
    UserResetStream,
    UserSendDatagram,
    UserStopSending,
    UserWriteStream,
)

__all__: list[str] = []


def convert_effect(*, event_tuple: tuple[int, Any]) -> Effect:
    opcode, payload = event_tuple

    match opcode:
        case abi.EMIT_CONNECTION_EVENT:
            connection_handle, event_type, error_code, reason = payload
            return EmitConnectionEvent(
                connection_handle=connection_handle, event_type=event_type, error_code=error_code, reason=reason
            )
        case abi.EMIT_SESSION_EVENT:
            (
                session_id,
                event_type,
                path,
                headers,
                wt_available_protocols,
                wt_protocol,
                data,
                is_unidirectional,
                max_data,
                max_streams,
                ready_at,
                status_code,
                error_code,
                reason,
            ) = payload
            return EmitSessionEvent(
                session_id=session_id,
                event_type=event_type,
                path=path,
                headers=headers,
                wt_available_protocols=wt_available_protocols,
                wt_protocol=wt_protocol,
                data=data,
                is_unidirectional=is_unidirectional,
                max_data=max_data,
                max_streams=max_streams,
                ready_at=ready_at,
                status_code=status_code,
                error_code=error_code,
                reason=reason,
            )
        case abi.EMIT_STREAM_EVENT:
            stream_id, event_type, session_id, direction, is_peer_initiated, error_code = payload
            return EmitStreamEvent(
                stream_id=stream_id,
                event_type=event_type,
                session_id=session_id,
                direction=direction,
                is_peer_initiated=is_peer_initiated,
                error_code=error_code,
            )
        case abi.NOTIFY_REQUEST_DONE:
            request_id, result = payload
            return NotifyRequestDone(request_id=request_id, result=result)
        case abi.NOTIFY_REQUEST_FAILED:
            request_id, exception = payload
            return NotifyRequestFailed(request_id=request_id, exception=exception)
        case _:
            raise ValueError(f"rt_event convert invalid actual={opcode}")


def convert_protocol_event(*, event: ProtocolEvent) -> tuple[int, tuple[Any, ...]]:
    match event:
        case UserAcceptSession():
            return abi.USER_ACCEPT_SESSION, (event.request_id, event.session_id, event.headers, event.wt_protocol)
        case UserCloseConnection():
            return abi.USER_CLOSE_CONNECTION, (event.request_id, event.error_code, event.reason)
        case UserCloseConnectionGracefully():
            return abi.USER_CLOSE_CONNECTION_GRACEFULLY, (event.request_id,)
        case UserCloseSession():
            return abi.USER_CLOSE_SESSION, (event.request_id, event.session_id, event.error_code, event.reason)
        case UserCreateSession():
            return abi.USER_CREATE_SESSION, (
                event.request_id,
                event.authority,
                event.path,
                event.headers,
                event.wt_available_protocols,
            )
        case UserCreateSessionOptimistic():
            return abi.USER_CREATE_SESSION_OPTIMISTIC, (
                event.request_id,
                event.authority,
                event.path,
                event.headers,
                event.wt_available_protocols,
            )
        case UserCreateStream():
            return abi.USER_CREATE_STREAM, (event.request_id, event.session_id, event.is_unidirectional)
        case UserDiagnoseConnection():
            return abi.USER_DIAGNOSE_CONNECTION, (event.request_id,)
        case UserDiagnoseSession():
            return abi.USER_DIAGNOSE_SESSION, (event.request_id, event.session_id)
        case UserDiagnoseStream():
            return abi.USER_DIAGNOSE_STREAM, (event.request_id, event.stream_id)
        case UserExportKeyingMaterial():
            return abi.USER_EXPORT_KEYING_MATERIAL, (
                event.request_id,
                event.session_id,
                event.label,
                event.context,
                event.length,
            )
        case UserReadDatagram():
            return abi.USER_READ_DATAGRAM, (event.request_id, event.session_id)
        case UserReadStream():
            return abi.USER_READ_STREAM, (event.request_id, event.stream_id, event.max_size)
        case UserRejectSession():
            return abi.USER_REJECT_SESSION, (event.request_id, event.session_id, event.headers, event.status_code)
        case UserResetStream():
            return abi.USER_RESET_STREAM, (event.request_id, event.stream_id, event.error_code)
        case UserSendDatagram():
            return abi.USER_SEND_DATAGRAM, (event.request_id, event.session_id, event.data)
        case UserStopSending():
            return abi.USER_STOP_SENDING, (event.request_id, event.stream_id, event.error_code)
        case UserWriteStream():
            return abi.USER_WRITE_STREAM, (event.request_id, event.stream_id, event.data, event.end_stream)
        case _:
            raise ValueError(f"rt_event convert invalid actual={type(event).__name__}")


def convert_runtime_event(*, event_tuple: tuple[int, Any]) -> RuntimeEvent:
    opcode, payload = event_tuple

    match opcode:
        case abi.COMMAND_FAILED:
            request_id, exception = payload
            return CommandFailed(request_id=request_id, exception=exception)
        case abi.CONNECTION_EFFECTS:
            handle, effects = payload
            return ConnectionEffects(handle=handle, effects=effects)
        case abi.CONNECTION_SPAWNED:
            handle, remote_address, effects = payload
            return ConnectionSpawned(handle=handle, remote_address=remote_address, effects=effects)
        case abi.REACTOR_SHUTDOWN:
            return ReactorShutdown()
        case _:
            raise ValueError(f"rt_event convert invalid actual={opcode}")
