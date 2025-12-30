"""Handle session-level logic for the protocol engine."""

from __future__ import annotations

import dataclasses
import http
from typing import TYPE_CHECKING

from aioquic._buffer import Buffer as QuicBuffer
from aioquic._buffer import BufferReadError

from pywebtransport import constants
from pywebtransport._protocol.events import (
    CapsuleReceived,
    CloseQuicConnection,
    ConnectStreamClosed,
    CreateQuicStream,
    DatagramReceived,
    Effect,
    EmitSessionEvent,
    EmitStreamEvent,
    NotifyRequestDone,
    NotifyRequestFailed,
    ResetQuicStream,
    SendH3Capsule,
    SendH3Datagram,
    SendH3Headers,
    SendQuicData,
    StopQuicStream,
    UserAcceptSession,
    UserCloseSession,
    UserCreateStream,
    UserGetSessionDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserRejectSession,
    UserSendDatagram,
)
from pywebtransport._protocol.state import ProtocolState, SessionStateData
from pywebtransport._protocol.utils import can_receive_data_on_stream
from pywebtransport.constants import ErrorCodes
from pywebtransport.exceptions import FlowControlError, ProtocolError, SessionError, StreamError
from pywebtransport.types import EventType, SessionId, SessionState, StreamState
from pywebtransport.utils import get_logger, get_timestamp

if TYPE_CHECKING:
    from pywebtransport.config import ClientConfig, ServerConfig


__all__: list[str] = []

logger = get_logger(name=__name__)


class SessionProcessor:
    """Process session-level events and manage state transitions."""

    def __init__(self, *, is_client: bool, config: ClientConfig | ServerConfig) -> None:
        """Initialize the session processor."""
        self._is_client = is_client
        self._config = config

    def handle_accept_session(self, *, event: UserAcceptSession, state: ProtocolState) -> list[Effect]:
        """Handle the UserAcceptSession event (server-only)."""
        effects: list[Effect] = []
        session_id = event.session_id
        session_data = state.sessions.get(session_id)

        if self._is_client:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id, exception=ProtocolError(message="Client cannot accept sessions")
                )
            )
            return effects

        if session_data is None:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(message=f"Session {session_id} not found for acceptance"),
                )
            )
            return effects

        if session_data.state != SessionState.CONNECTING:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(
                        message=f"Session {session_id} is not in connecting state ({session_data.state})"
                    ),
                )
            )
            return effects

        session_data.state = SessionState.CONNECTED
        session_data.ready_at = get_timestamp()

        effects.append(SendH3Headers(stream_id=session_data.session_id, status=http.HTTPStatus.OK, end_stream=False))
        effects.append(
            EmitSessionEvent(
                session_id=session_id,
                event_type=EventType.SESSION_READY,
                data={"session_id": session_id, "ready_at": session_data.ready_at},
            )
        )
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
        logger.info("Accepted session %s", session_id)
        return effects

    def handle_capsule_received(self, *, event: CapsuleReceived, state: ProtocolState) -> list[Effect]:
        """Handle a session-level CapsuleReceived event."""
        effects: list[Effect] = []
        stream_id = event.stream_id

        if stream_id not in state.sessions:
            logger.debug("Received capsule on unknown or closed session stream %d", stream_id)
            return []

        session_id = stream_id
        session_data = state.sessions[session_id]

        if session_data.state == SessionState.CLOSED:
            raise ProtocolError(
                message=f"Data received on closed session {session_id} (stream {stream_id})",
                error_code=ErrorCodes.H3_MESSAGE_ERROR,
            )

        try:
            buf = QuicBuffer(data=bytes(event.capsule_data))
            match event.capsule_type:
                case constants.WT_MAX_DATA_TYPE:
                    new_limit = buf.pull_uint_var()
                    if new_limit > session_data.peer_max_data:
                        logger.debug(
                            "Session %s flow credit received: peer_max_data updating from %d to %d",
                            session_id,
                            session_data.peer_max_data,
                            new_limit,
                        )
                        session_data.peer_max_data = new_limit
                        effects.append(
                            EmitSessionEvent(
                                session_id=session_id,
                                event_type=EventType.SESSION_MAX_DATA_UPDATED,
                                data={"session_id": session_id, "max_data": new_limit},
                            )
                        )

                        effects.extend(self._drain_session_write_buffers(session_id=session_id, state=state))

                    elif new_limit < session_data.peer_max_data:
                        return self._close_session_with_error(
                            session_id=session_id,
                            session_data=session_data,
                            state=state,
                            error_code=ErrorCodes.WT_FLOW_CONTROL_ERROR,
                            reason="Flow control limit decreased for MAX_DATA",
                        )

                case constants.WT_MAX_STREAMS_BIDI_TYPE:
                    new_limit = buf.pull_uint_var()
                    if new_limit > constants.MAX_PROTOCOL_STREAMS_LIMIT:
                        raise ProtocolError(
                            message=f"MAX_STREAMS_BIDI limit exceeds protocol maximum ({new_limit})",
                            error_code=ErrorCodes.FRAME_ENCODING_ERROR,
                        )

                    if new_limit > session_data.peer_max_streams_bidi:
                        session_data.peer_max_streams_bidi = new_limit
                        effects.append(
                            EmitSessionEvent(
                                session_id=session_id,
                                event_type=EventType.SESSION_MAX_STREAMS_BIDI_UPDATED,
                                data={"session_id": session_id, "max_streams_bidi": new_limit},
                            )
                        )

                        if self._is_client:
                            while (
                                session_data.local_streams_bidi_opened < session_data.peer_max_streams_bidi
                                and session_data.pending_bidi_stream_requests
                            ):
                                request_id = session_data.pending_bidi_stream_requests.popleft()
                                session_data.local_streams_bidi_opened += 1
                                effects.append(
                                    CreateQuicStream(
                                        request_id=request_id, session_id=session_id, is_unidirectional=False
                                    )
                                )

                    elif new_limit < session_data.peer_max_streams_bidi:
                        return self._close_session_with_error(
                            session_id=session_id,
                            session_data=session_data,
                            state=state,
                            error_code=ErrorCodes.WT_FLOW_CONTROL_ERROR,
                            reason="Flow control limit decreased for MAX_STREAMS_BIDI",
                        )

                case constants.WT_MAX_STREAMS_UNI_TYPE:
                    new_limit = buf.pull_uint_var()
                    if new_limit > constants.MAX_PROTOCOL_STREAMS_LIMIT:
                        raise ProtocolError(
                            message=f"MAX_STREAMS_UNI limit exceeds protocol maximum ({new_limit})",
                            error_code=ErrorCodes.FRAME_ENCODING_ERROR,
                        )

                    if new_limit > session_data.peer_max_streams_uni:
                        session_data.peer_max_streams_uni = new_limit
                        effects.append(
                            EmitSessionEvent(
                                session_id=session_id,
                                event_type=EventType.SESSION_MAX_STREAMS_UNI_UPDATED,
                                data={"session_id": session_id, "max_streams_uni": new_limit},
                            )
                        )

                        if self._is_client:
                            while (
                                session_data.local_streams_uni_opened < session_data.peer_max_streams_uni
                                and session_data.pending_uni_stream_requests
                            ):
                                request_id = session_data.pending_uni_stream_requests.popleft()
                                session_data.local_streams_uni_opened += 1
                                effects.append(
                                    CreateQuicStream(
                                        request_id=request_id, session_id=session_id, is_unidirectional=True
                                    )
                                )

                    elif new_limit < session_data.peer_max_streams_uni:
                        return self._close_session_with_error(
                            session_id=session_id,
                            session_data=session_data,
                            state=state,
                            error_code=ErrorCodes.WT_FLOW_CONTROL_ERROR,
                            reason="Flow control limit decreased for MAX_STREAMS_UNI",
                        )

                case constants.WT_DATA_BLOCKED_TYPE:
                    logger.debug("Session %s received WT_DATA_BLOCKED from peer", session_id)
                    if self._config.flow_control_window_auto_scale:
                        target_window = self._config.flow_control_window_size
                        new_limit = session_data.peer_data_sent + target_window
                        if new_limit > session_data.local_max_data:
                            session_data.local_max_data = new_limit
                            resp_buf = QuicBuffer(capacity=8)
                            resp_buf.push_uint_var(new_limit)
                            effects.append(
                                SendH3Capsule(
                                    stream_id=session_data.session_id,
                                    capsule_type=constants.WT_MAX_DATA_TYPE,
                                    capsule_data=resp_buf.data,
                                    end_stream=False,
                                )
                            )
                    else:
                        effects.append(
                            EmitSessionEvent(
                                session_id=session_id,
                                event_type=EventType.SESSION_DATA_BLOCKED,
                                data={"session_id": session_id},
                            )
                        )

                case constants.WT_STREAMS_BLOCKED_BIDI_TYPE | constants.WT_STREAMS_BLOCKED_UNI_TYPE:
                    is_uni = event.capsule_type == constants.WT_STREAMS_BLOCKED_UNI_TYPE
                    logger.debug("Session %s received WT_STREAMS_BLOCKED (uni=%s) from peer", session_id, is_uni)

                    if self._config.flow_control_window_auto_scale:
                        new_limit = 0
                        current_limit = 0
                        capsule_type = 0

                        if is_uni:
                            target_window = self._config.stream_flow_control_increment_uni
                            current_usage = session_data.peer_streams_uni_opened
                            current_limit = session_data.local_max_streams_uni
                            new_limit = current_usage + target_window
                            capsule_type = constants.WT_MAX_STREAMS_UNI_TYPE
                            if new_limit > current_limit:
                                session_data.local_max_streams_uni = new_limit
                        else:
                            target_window = self._config.stream_flow_control_increment_bidi
                            current_usage = session_data.peer_streams_bidi_opened
                            current_limit = session_data.local_max_streams_bidi
                            new_limit = current_usage + target_window
                            capsule_type = constants.WT_MAX_STREAMS_BIDI_TYPE
                            if new_limit > current_limit:
                                session_data.local_max_streams_bidi = new_limit

                        if new_limit > current_limit:
                            resp_buf = QuicBuffer(capacity=8)
                            resp_buf.push_uint_var(new_limit)
                            effects.append(
                                SendH3Capsule(
                                    stream_id=session_data.session_id,
                                    capsule_type=capsule_type,
                                    capsule_data=resp_buf.data,
                                    end_stream=False,
                                )
                            )
                    else:
                        effects.append(
                            EmitSessionEvent(
                                session_id=session_id,
                                event_type=EventType.SESSION_STREAMS_BLOCKED,
                                data={"session_id": session_id, "is_unidirectional": is_uni},
                            )
                        )

                case constants.CLOSE_WEBTRANSPORT_SESSION_TYPE:
                    app_code = buf.pull_uint32()
                    reason_bytes = buf.pull_bytes(len(event.capsule_data) - buf.tell())
                    reason = reason_bytes.decode("utf-8", errors="replace")
                    logger.info("Received CLOSE_SESSION for %s: code=%#x reason='%s'", session_id, app_code, reason)

                    session_data.state = SessionState.CLOSED
                    session_data.closed_at = get_timestamp()
                    session_data.close_code = app_code
                    session_data.close_reason = reason
                    effects.append(
                        EmitSessionEvent(
                            session_id=session_id,
                            event_type=EventType.SESSION_CLOSED,
                            data={"session_id": session_id, "code": app_code, "reason": reason},
                        )
                    )
                    effects.extend(
                        self._reset_all_session_streams(session_id=session_id, session_data=session_data, state=state)
                    )

                case constants.DRAIN_WEBTRANSPORT_SESSION_TYPE:
                    logger.info("Received DRAIN_SESSION for %s", session_id)
                    if session_data.state == SessionState.CONNECTED:
                        session_data.state = SessionState.DRAINING
                        effects.append(
                            EmitSessionEvent(
                                session_id=session_id,
                                event_type=EventType.SESSION_DRAINING,
                                data={"session_id": session_id},
                            )
                        )

                case constants.WT_MAX_STREAM_DATA_TYPE | constants.WT_STREAM_DATA_BLOCKED_TYPE:
                    return self._close_session_with_error(
                        session_id=session_id,
                        session_data=session_data,
                        state=state,
                        error_code=ErrorCodes.H3_FRAME_UNEXPECTED,
                        reason=f"Forbidden capsule type received: {hex(event.capsule_type)}",
                    )

                case _:
                    logger.debug("Ignoring unknown capsule type %d for session %s", event.capsule_type, session_id)

        except (BufferReadError, ProtocolError) as e:
            logger.warning("Error processing capsule for session %s: %s", session_id, e, exc_info=True)
            error_code = getattr(e, "error_code", ErrorCodes.PROTOCOL_VIOLATION)
            effects.append(CloseQuicConnection(error_code=error_code, reason=f"Capsule processing error: {e}"))
            try:
                if session_data.state != SessionState.CLOSED:
                    session_data.state = SessionState.CLOSED
                    session_data.closed_at = get_timestamp()
                    session_data.close_reason = "Capsule processing error"
                    effects.append(
                        EmitSessionEvent(
                            session_id=session_id,
                            event_type=EventType.SESSION_CLOSED,
                            data={"session_id": session_id, "code": error_code, "reason": "Capsule processing error"},
                        )
                    )
                    effects.extend(
                        self._reset_all_session_streams(session_id=session_id, session_data=session_data, state=state)
                    )
            except Exception as cleanup_e:
                logger.error("Secondary error during fail-safe cleanup for session %s: %s", session_id, cleanup_e)

        return effects

    def handle_close_session(self, *, event: UserCloseSession, state: ProtocolState) -> list[Effect]:
        """Handle the UserCloseSession event."""
        effects: list[Effect] = []
        session_id = event.session_id
        session_data = state.sessions.get(session_id)

        if session_data is None or session_data.state == SessionState.CLOSED:
            effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
            return effects

        session_data.state = SessionState.CLOSED
        session_data.closed_at = get_timestamp()
        session_data.close_code = event.error_code
        session_data.close_reason = event.reason

        effects.extend(self._reset_all_session_streams(session_id=session_id, session_data=session_data, state=state))

        reason_str = event.reason if event.reason is not None else ""
        reason_bytes = reason_str.encode("utf-8")
        if len(reason_bytes) > constants.MAX_CLOSE_REASON_BYTES:
            reason_bytes = reason_bytes[: constants.MAX_CLOSE_REASON_BYTES]

        buf = QuicBuffer(capacity=4 + len(reason_bytes))
        buf.push_uint32(event.error_code)
        buf.push_bytes(reason_bytes)
        capsule_payload = buf.data

        effects.append(
            SendH3Capsule(
                stream_id=session_data.session_id,
                capsule_type=constants.CLOSE_WEBTRANSPORT_SESSION_TYPE,
                capsule_data=capsule_payload,
                end_stream=True,
            )
        )
        effects.append(
            EmitSessionEvent(
                session_id=session_id,
                event_type=EventType.SESSION_CLOSED,
                data={"session_id": session_id, "code": event.error_code, "reason": event.reason},
            )
        )
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
        logger.info("Closing session %s by user request", session_id)
        return effects

    def handle_connect_stream_closed(self, *, event: ConnectStreamClosed, state: ProtocolState) -> list[Effect]:
        """Handle the clean closure (FIN) of a CONNECT stream."""
        session_id = event.stream_id

        if session_id not in state.sessions:
            return []

        session_data = state.sessions[session_id]
        if session_data.state == SessionState.CLOSED:
            return []

        logger.info("Session %s cleanly closed by peer (CONNECT stream FIN)", session_id)

        return self._close_session_with_error(
            session_id=session_id,
            session_data=session_data,
            state=state,
            error_code=ErrorCodes.NO_ERROR,
            reason="CONNECT stream cleanly closed",
        )

    def handle_create_stream(self, *, event: UserCreateStream, state: ProtocolState) -> list[Effect]:
        """Handle the UserCreateStream event."""
        effects: list[Effect] = []
        session_id = event.session_id
        session_data = state.sessions.get(session_id)

        if session_data is None:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(message=f"Session {session_id} not found for stream creation"),
                )
            )
            return effects

        if session_data.state not in (SessionState.CONNECTED, SessionState.DRAINING):
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(
                        message=f"Session {session_id} is not connected or draining ({session_data.state})"
                    ),
                )
            )
            return effects

        limit_exceeded = False
        if event.is_unidirectional:
            if session_data.local_streams_uni_opened >= session_data.peer_max_streams_uni:
                limit_exceeded = True
        else:
            if session_data.local_streams_bidi_opened >= session_data.peer_max_streams_bidi:
                limit_exceeded = True

        if limit_exceeded:
            error: Exception
            match (self._is_client, event.is_unidirectional):
                case (True, True):
                    logger.debug(
                        "Client uni stream creation for session %s blocked by flow control (%d >= %d)",
                        session_id,
                        session_data.local_streams_uni_opened,
                        session_data.peer_max_streams_uni,
                    )
                    session_data.pending_uni_stream_requests.append(event.request_id)
                    buf = QuicBuffer(capacity=8)
                    buf.push_uint_var(session_data.peer_max_streams_uni)
                    return [
                        SendH3Capsule(
                            stream_id=session_data.session_id,
                            capsule_type=constants.WT_STREAMS_BLOCKED_UNI_TYPE,
                            capsule_data=buf.data,
                            end_stream=False,
                        )
                    ]
                case (True, False):
                    logger.debug(
                        "Client bidi stream creation for session %s blocked by flow control (%d >= %d)",
                        session_id,
                        session_data.local_streams_bidi_opened,
                        session_data.peer_max_streams_bidi,
                    )
                    session_data.pending_bidi_stream_requests.append(event.request_id)
                    buf = QuicBuffer(capacity=8)
                    buf.push_uint_var(session_data.peer_max_streams_bidi)
                    return [
                        SendH3Capsule(
                            stream_id=session_data.session_id,
                            capsule_type=constants.WT_STREAMS_BLOCKED_BIDI_TYPE,
                            capsule_data=buf.data,
                            end_stream=False,
                        )
                    ]
                case (False, True):
                    error = FlowControlError(
                        message="Unidirectional stream limit reached", error_code=ErrorCodes.STREAM_LIMIT_ERROR
                    )
                    effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
                    return effects
                case (False, False):
                    error = FlowControlError(
                        message="Bidirectional stream limit reached", error_code=ErrorCodes.STREAM_LIMIT_ERROR
                    )
                    effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
                    return effects

        if event.is_unidirectional:
            session_data.local_streams_uni_opened += 1
        else:
            session_data.local_streams_bidi_opened += 1

        effects.append(
            CreateQuicStream(
                request_id=event.request_id, session_id=session_id, is_unidirectional=event.is_unidirectional
            )
        )
        return effects

    def handle_datagram_received(self, *, event: DatagramReceived, state: ProtocolState) -> list[Effect]:
        """Handle a DatagramReceived event for a session."""
        effects: list[Effect] = []
        session_id = event.stream_id

        if session_id in state.sessions:
            session_data = state.sessions[session_id]
            if session_data.state in (SessionState.CONNECTED, SessionState.DRAINING):
                session_data.datagrams_received += 1
                session_data.datagram_bytes_received += len(event.data)
                effects.append(
                    EmitSessionEvent(
                        session_id=session_id,
                        event_type=EventType.DATAGRAM_RECEIVED,
                        data={"session_id": session_id, "data": event.data},
                    )
                )
            else:
                logger.debug("Ignoring datagram for non-active session %s state %s", session_id, session_data.state)
        else:
            if state.early_event_count >= self._config.max_total_pending_events:
                logger.warning(
                    "Global early event buffer full (%d), dropping datagram for session %d",
                    state.early_event_count,
                    session_id,
                )
                return []

            session_buffer = state.early_event_buffer.get(session_id, [])
            if len(session_buffer) >= self._config.max_pending_events_per_session:
                logger.warning(
                    "Per-session early event buffer full (%d) for session %d, dropping datagram",
                    len(session_buffer),
                    session_id,
                )
                return []

            logger.debug("Buffering early datagram for unknown session %d", session_id)
            state.early_event_buffer.setdefault(session_id, []).append((get_timestamp(), event))
            state.early_event_count += 1

        return effects

    def handle_get_session_diagnostics(self, *, event: UserGetSessionDiagnostics, state: ProtocolState) -> list[Effect]:
        """Handle the UserGetSessionDiagnostics event."""
        session_data = state.sessions.get(event.session_id)
        if session_data is None:
            return [
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(f"Session {event.session_id} not found for diagnostics"),
                )
            ]

        diag_data = dataclasses.asdict(session_data)
        diag_data["active_streams"] = list(session_data.active_streams)
        diag_data["blocked_streams"] = list(session_data.blocked_streams)

        return [NotifyRequestDone(request_id=event.request_id, result=diag_data)]

    def handle_grant_data_credit(self, *, event: UserGrantDataCredit, state: ProtocolState) -> list[Effect]:
        """Handle the UserGrantDataCredit event."""
        effects: list[Effect] = []
        session_data = state.sessions.get(event.session_id)

        if session_data is None:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id, exception=SessionError(f"Session {event.session_id} not found.")
                )
            )
            return effects

        if event.max_data <= session_data.local_max_data:
            logger.warning(
                "Manual data credit grant (%d) is not greater than current limit (%d). Ignoring.",
                event.max_data,
                session_data.local_max_data,
            )
            effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
            return effects

        session_data.local_max_data = event.max_data
        buf = QuicBuffer(capacity=8)
        buf.push_uint_var(event.max_data)
        effects.append(
            SendH3Capsule(
                stream_id=session_data.session_id,
                capsule_type=constants.WT_MAX_DATA_TYPE,
                capsule_data=buf.data,
                end_stream=False,
            )
        )
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
        return effects

    def handle_grant_streams_credit(self, *, event: UserGrantStreamsCredit, state: ProtocolState) -> list[Effect]:
        """Handle the UserGrantStreamsCredit event."""
        effects: list[Effect] = []
        session_data = state.sessions.get(event.session_id)

        if session_data is None:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id, exception=SessionError(f"Session {event.session_id} not found.")
                )
            )
            return effects

        capsule_type: int
        if event.is_unidirectional:
            if event.max_streams <= session_data.local_max_streams_uni:
                logger.warning(
                    "Manual uni streams credit grant (%d) is not greater than current limit (%d). Ignoring.",
                    event.max_streams,
                    session_data.local_max_streams_uni,
                )
                effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
                return effects
            session_data.local_max_streams_uni = event.max_streams
            capsule_type = constants.WT_MAX_STREAMS_UNI_TYPE
        else:
            if event.max_streams <= session_data.local_max_streams_bidi:
                logger.warning(
                    "Manual bidi streams credit grant (%d) is not greater than current limit (%d). Ignoring.",
                    event.max_streams,
                    session_data.local_max_streams_bidi,
                )
                effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
                return effects
            session_data.local_max_streams_bidi = event.max_streams
            capsule_type = constants.WT_MAX_STREAMS_BIDI_TYPE

        buf = QuicBuffer(capacity=8)
        buf.push_uint_var(event.max_streams)
        effects.append(
            SendH3Capsule(
                stream_id=session_data.session_id, capsule_type=capsule_type, capsule_data=buf.data, end_stream=False
            )
        )
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
        return effects

    def handle_reject_session(self, *, event: UserRejectSession, state: ProtocolState) -> list[Effect]:
        """Handle the UserRejectSession event (server-only)."""
        effects: list[Effect] = []
        session_id = event.session_id
        session_data = state.sessions.get(session_id)

        if self._is_client:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id, exception=ProtocolError(message="Client cannot reject sessions")
                )
            )
            return effects

        if session_data is None:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(message=f"Session {session_id} not found for rejection"),
                )
            )
            return effects

        if session_data.state != SessionState.CONNECTING:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(
                        message=f"Session {session_id} is not in connecting state ({session_data.state})"
                    ),
                )
            )
            return effects

        session_data.state = SessionState.CLOSED
        session_data.closed_at = get_timestamp()
        session_data.close_reason = f"Rejected by application with status {event.status_code}"

        effects.append(SendH3Headers(stream_id=session_data.session_id, status=event.status_code, end_stream=True))
        effects.append(
            EmitSessionEvent(
                session_id=session_id,
                event_type=EventType.SESSION_CLOSED,
                data={"session_id": session_id, "code": event.status_code, "reason": "Rejected by application"},
            )
        )
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
        logger.info("Rejected session %s with status %d", session_id, event.status_code)
        return effects

    def handle_send_datagram(self, *, event: UserSendDatagram, state: ProtocolState) -> list[Effect]:
        """Handle the UserSendDatagram event."""
        effects: list[Effect] = []
        session_id = event.session_id
        session_data = state.sessions.get(session_id)

        if session_data is None:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(message=f"Session {session_id} not found for sending datagram"),
                )
            )
            return effects

        if session_data.state != SessionState.CONNECTED:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(message=f"Session {session_id} is not connected ({session_data.state})"),
                )
            )
            return effects

        data_len = 0
        if isinstance(event.data, list):
            data_len = sum(len(chunk) for chunk in event.data)
        else:
            data_len = len(event.data)

        if data_len > state.remote_max_datagram_frame_size:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=ValueError(
                        f"Datagram size {data_len} exceeds maximum {state.remote_max_datagram_frame_size}"
                    ),
                )
            )
            return effects

        session_data.datagrams_sent += 1
        session_data.datagram_bytes_sent += data_len

        effects.append(SendH3Datagram(stream_id=session_data.session_id, data=event.data))
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
        return effects

    def _close_session_with_error(
        self,
        *,
        session_id: SessionId,
        session_data: SessionStateData,
        state: ProtocolState,
        error_code: int,
        reason: str,
    ) -> list[Effect]:
        """Generate effects to close a session with a specific error."""
        session_data.state = SessionState.CLOSED
        session_data.closed_at = get_timestamp()
        session_data.close_code = error_code
        session_data.close_reason = reason
        effects: list[Effect] = [
            ResetQuicStream(stream_id=session_data.session_id, error_code=error_code),
            EmitSessionEvent(
                session_id=session_id,
                event_type=EventType.SESSION_CLOSED,
                data={"session_id": session_id, "code": error_code, "reason": reason},
            ),
        ]
        effects.extend(self._reset_all_session_streams(session_id=session_id, session_data=session_data, state=state))
        return effects

    def _drain_session_write_buffers(self, *, session_id: int, state: ProtocolState) -> list[Effect]:
        """Drain buffered stream writes after receiving session flow credit."""
        effects: list[Effect] = []
        session_data = state.sessions.get(session_id)
        if session_data is None:
            return []

        available_credit = session_data.peer_max_data - session_data.local_data_sent
        if available_credit <= 0:
            return []

        streams_to_check = list(session_data.blocked_streams)

        for stream_id in streams_to_check:
            if available_credit <= 0:
                break

            stream_data = state.streams.get(stream_id)
            if stream_data is None:
                session_data.blocked_streams.discard(stream_id)
                continue

            if stream_data.state in (StreamState.CLOSED, StreamState.RESET_SENT) or not stream_data.write_buffer:
                session_data.blocked_streams.discard(stream_id)
                continue

            logger.debug(
                "Draining write buffer for stream %d (session %s) with %d credit",
                stream_id,
                session_id,
                available_credit,
            )

            while stream_data.write_buffer and available_credit > 0:
                try:
                    (buffered_data, request_id, buffered_end_stream) = stream_data.write_buffer.popleft()
                except (IndexError, TypeError, ValueError):
                    logger.error("Internal state error: Stream %d write_buffer is malformed.", stream_id)
                    break

                view = memoryview(buffered_data)
                data_len = len(view)

                send_amount = min(data_len, available_credit)

                data_to_send = view[:send_amount]
                remaining_view = view[send_amount:]

                is_final_chunk = (len(remaining_view) == 0) and buffered_end_stream

                effects.append(SendQuicData(stream_id=stream_id, data=data_to_send, end_stream=is_final_chunk))
                session_data.local_data_sent += send_amount
                stream_data.bytes_sent += send_amount
                available_credit -= send_amount

                stream_data.write_buffer_size -= send_amount

                if remaining_view:
                    stream_data.write_buffer.appendleft((remaining_view, request_id, buffered_end_stream))
                    break
                else:
                    if is_final_chunk:
                        original_state = stream_data.state
                        match original_state:
                            case StreamState.HALF_CLOSED_REMOTE | StreamState.RESET_RECEIVED:
                                stream_data.state = StreamState.CLOSED
                                effects.append(
                                    EmitStreamEvent(
                                        stream_id=stream_id,
                                        event_type=EventType.STREAM_CLOSED,
                                        data={"stream_id": stream_id},
                                    )
                                )
                            case StreamState.OPEN:
                                stream_data.state = StreamState.HALF_CLOSED_LOCAL
                        logger.debug("Stream %d send side closed (from buffer drain)", stream_id)

                    effects.append(NotifyRequestDone(request_id=request_id, result=None))

            if not stream_data.write_buffer:
                session_data.blocked_streams.discard(stream_id)

        return effects

    def _reset_all_session_streams(
        self, *, session_id: int, session_data: SessionStateData, state: ProtocolState
    ) -> list[Effect]:
        """Generate effects to reset all streams associated with a session."""
        effects: list[Effect] = []

        session_error = SessionError(message=f"Session {session_id} closed during stream creation")
        while session_data.pending_bidi_stream_requests:
            req_id = session_data.pending_bidi_stream_requests.popleft()
            effects.append(NotifyRequestFailed(request_id=req_id, exception=session_error))
        while session_data.pending_uni_stream_requests:
            req_id = session_data.pending_uni_stream_requests.popleft()
            effects.append(NotifyRequestFailed(request_id=req_id, exception=session_error))

        stream_error = StreamError(message=f"Session {session_id} terminated", error_code=ErrorCodes.WT_SESSION_GONE)

        streams_to_reset = list(session_data.active_streams)

        for stream_id in streams_to_reset:
            stream_data = state.streams.get(stream_id)
            if stream_data is None or stream_data.state == StreamState.CLOSED:
                continue

            while stream_data.pending_read_requests:
                req_id, _ = stream_data.pending_read_requests.popleft()
                effects.append(NotifyRequestFailed(request_id=req_id, exception=stream_error))

            while stream_data.write_buffer:
                _data, req_id, _end = stream_data.write_buffer.popleft()
                effects.append(NotifyRequestFailed(request_id=req_id, exception=stream_error))

            if stream_data.state not in (StreamState.RESET_SENT, StreamState.CLOSED):
                effects.append(ResetQuicStream(stream_id=stream_id, error_code=ErrorCodes.WT_SESSION_GONE))

            if stream_data.state not in (StreamState.RESET_RECEIVED, StreamState.CLOSED):
                if can_receive_data_on_stream(stream_id=stream_id, is_client=self._is_client):
                    effects.append(StopQuicStream(stream_id=stream_id, error_code=ErrorCodes.WT_SESSION_GONE))

            stream_data.state = StreamState.CLOSED
            stream_data.closed_at = session_data.closed_at

            effects.append(
                EmitStreamEvent(stream_id=stream_id, event_type=EventType.STREAM_CLOSED, data={"stream_id": stream_id})
            )

        session_data.active_streams.clear()
        session_data.blocked_streams.clear()

        return effects
