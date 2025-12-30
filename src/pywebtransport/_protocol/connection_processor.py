"""Handle connection-level logic for the protocol engine."""

from __future__ import annotations

import http
from typing import TYPE_CHECKING

from pywebtransport import constants
from pywebtransport._protocol.events import (
    CleanupH3Stream,
    CloseQuicConnection,
    ConnectionClose,
    CreateH3Session,
    Effect,
    EmitConnectionEvent,
    EmitSessionEvent,
    GoawayReceived,
    HeadersReceived,
    InternalBindH3Session,
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalFailH3Session,
    NotifyRequestDone,
    NotifyRequestFailed,
    ProcessProtocolEvent,
    ResetQuicStream,
    SendH3Capsule,
    SendH3Goaway,
    SendH3Headers,
    TransportConnectionTerminated,
    TransportQuicParametersReceived,
    UserConnectionGracefulClose,
    UserCreateSession,
    UserGetConnectionDiagnostics,
)
from pywebtransport._protocol.state import ProtocolState, SessionInitData, SessionStateData
from pywebtransport.constants import ErrorCodes
from pywebtransport.exceptions import ConnectionError, ProtocolError, SessionError
from pywebtransport.types import ConnectionId, ConnectionState, EventType, SessionState, StreamState
from pywebtransport.utils import get_header_as_str, get_logger, get_timestamp

if TYPE_CHECKING:
    from pywebtransport.config import ClientConfig, ServerConfig


__all__: list[str] = []

logger = get_logger(name=__name__)


class ConnectionProcessor:
    """Process connection-level events and manage state transitions."""

    def __init__(self, *, is_client: bool, config: ClientConfig | ServerConfig, connection_id: ConnectionId) -> None:
        """Initialize the connection processor."""
        self._is_client = is_client
        self._config = config
        self._connection_id = connection_id

    def handle_cleanup_early_events(self, *, event: InternalCleanupEarlyEvents, state: ProtocolState) -> list[Effect]:
        """Handle the InternalCleanupEarlyEvents event to expire buffered data."""
        effects: list[Effect] = []
        now = get_timestamp()
        timeout = self._config.pending_event_ttl

        streams_to_remove: list[int] = []

        for stream_id, events in state.early_event_buffer.items():
            valid_events = []
            for timestamp, evt in events:
                if now - timestamp < timeout:
                    valid_events.append((timestamp, evt))
                else:
                    state.early_event_count -= 1

            if not valid_events:
                streams_to_remove.append(stream_id)
            else:
                state.early_event_buffer[stream_id] = valid_events

        for stream_id in streams_to_remove:
            state.early_event_buffer.pop(stream_id, None)
            if stream_id not in state.sessions:
                logger.debug("Early event buffer timed out for stream %d, resetting", stream_id)
                effects.append(
                    ResetQuicStream(stream_id=stream_id, error_code=constants.ErrorCodes.WT_BUFFERED_STREAM_REJECTED)
                )

        return effects

    def handle_cleanup_resources(self, *, event: InternalCleanupResources, state: ProtocolState) -> list[Effect]:
        """Handle the InternalCleanupResources event."""
        effects: list[Effect] = []

        closed_session_ids = {sid for sid, sdata in state.sessions.items() if sdata.state == SessionState.CLOSED}
        closed_stream_ids = {stid for stid, stdata in state.streams.items() if stdata.state == StreamState.CLOSED}

        for sid in closed_session_ids:
            logger.debug("Cleaning up closed session %s from state", sid)
            session_data = state.sessions.pop(sid, None)
            if session_data is not None:
                effects.append(CleanupH3Stream(stream_id=sid))

                for stid in session_data.active_streams:
                    if stid in state.streams:
                        state.streams.pop(stid, None)
                    effects.append(CleanupH3Stream(stream_id=stid))

        for stid in closed_stream_ids:
            if stid in state.streams:
                logger.debug("Cleaning up closed stream %d from state", stid)
                state.streams.pop(stid, None)
                effects.append(CleanupH3Stream(stream_id=stid))

        return effects

    def handle_connection_close(self, *, event: ConnectionClose, state: ProtocolState) -> list[Effect]:
        """Handle the ConnectionClose event."""
        effects: list[Effect] = []
        if state.connection_state not in (ConnectionState.CLOSED, ConnectionState.CLOSING):
            state.connection_state = ConnectionState.CLOSING
            state.closed_at = get_timestamp()
            effects.append(CloseQuicConnection(error_code=event.error_code, reason=event.reason))
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
        return effects

    def handle_connection_terminated(
        self, *, event: TransportConnectionTerminated, state: ProtocolState
    ) -> list[Effect]:
        """Handle the TransportConnectionTerminated event."""
        if state.connection_state == ConnectionState.CLOSED:
            return []

        state.connection_state = ConnectionState.CLOSED
        state.closed_at = get_timestamp()

        effects: list[Effect] = []
        error = ConnectionError(message=f"Connection terminated: {event.reason_phrase}", error_code=event.error_code)

        state.pending_session_configs.clear()
        state.pending_requests.clear()

        for stream_data in state.streams.values():
            while stream_data.pending_read_requests:
                req_id, _ = stream_data.pending_read_requests.popleft()
                effects.append(NotifyRequestFailed(request_id=req_id, exception=error))
            while stream_data.write_buffer:
                _data, req_id, _end = stream_data.write_buffer.popleft()
                effects.append(NotifyRequestFailed(request_id=req_id, exception=error))

        effects.append(
            EmitConnectionEvent(
                event_type=EventType.CONNECTION_CLOSED,
                data={
                    "connection_id": self._connection_id,
                    "reason": event.reason_phrase,
                    "error_code": event.error_code,
                },
            )
        )

        return effects

    def handle_create_session(self, *, event: UserCreateSession, state: ProtocolState) -> list[Effect]:
        """Handle the UserCreateSession event (client-only)."""
        if not self._is_client:
            return [
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=ProtocolError(message="Server cannot create sessions using this method"),
                )
            ]

        if state.connection_state != ConnectionState.CONNECTED:
            return [
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=ConnectionError(
                        message=f"Cannot create session, connection state is {state.connection_state}"
                    ),
                )
            ]

        state.pending_session_configs[event.request_id] = SessionInitData(
            path=event.path, headers=event.headers, created_at=get_timestamp()
        )

        return [CreateH3Session(request_id=event.request_id, path=event.path, headers=event.headers)]

    def handle_get_connection_diagnostics(
        self, *, event: UserGetConnectionDiagnostics, state: ProtocolState
    ) -> list[Effect]:
        """Handle the UserGetConnectionDiagnostics event."""
        diagnostics_data = {
            "connection_id": self._connection_id,
            "state": state.connection_state,
            "is_client": state.is_client,
            "connected_at": state.connected_at,
            "closed_at": state.closed_at,
            "max_datagram_size": state.max_datagram_size,
            "remote_max_datagram_frame_size": state.remote_max_datagram_frame_size,
            "session_count": len(state.sessions),
            "stream_count": len(state.streams),
        }
        return [NotifyRequestDone(request_id=event.request_id, result=diagnostics_data)]

    def handle_goaway_received(self, *, event: GoawayReceived, state: ProtocolState) -> list[Effect]:
        """Handle the H3 GOAWAY signal by draining all active sessions."""
        effects: list[Effect] = []

        if state.connection_state not in (ConnectionState.CLOSING, ConnectionState.CLOSED):
            state.connection_state = ConnectionState.CLOSING
            state.closed_at = get_timestamp()

        for session_id, session_data in state.sessions.items():
            if session_data.state == SessionState.CONNECTED:
                session_data.state = SessionState.DRAINING
                effects.append(
                    SendH3Capsule(
                        stream_id=session_id,
                        capsule_type=constants.DRAIN_WEBTRANSPORT_SESSION_TYPE,
                        capsule_data=b"",
                        end_stream=False,
                    )
                )
                effects.append(
                    EmitSessionEvent(
                        session_id=session_id, event_type=EventType.SESSION_DRAINING, data={"session_id": session_id}
                    )
                )
        return effects

    def handle_graceful_close(self, *, event: UserConnectionGracefulClose, state: ProtocolState) -> list[Effect]:
        """Handle the user request for a graceful H3 GOAWAY shutdown."""
        effects: list[Effect] = []

        if not state.local_goaway_sent:
            state.local_goaway_sent = True
            effects.append(SendH3Goaway())

            if state.connection_state not in (ConnectionState.CLOSING, ConnectionState.CLOSED):
                state.connection_state = ConnectionState.CLOSING
                state.closed_at = get_timestamp()

        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
        return effects

    def handle_headers_received(self, *, event: HeadersReceived, state: ProtocolState) -> list[Effect]:
        """Handle the HeadersReceived event."""
        effects: list[Effect] = []
        now = get_timestamp()
        stream_id = event.stream_id

        if self._is_client:
            request_id = state.pending_requests.pop(stream_id, None)
            if request_id is None:
                logger.warning("Received headers on unknown client stream %d (no pending request)", stream_id)
                return []

            init_data = state.pending_session_configs.pop(request_id, None)
            if init_data is None:
                logger.error("Internal State Error: Missing init data for request %d", request_id)
                return [
                    NotifyRequestFailed(
                        request_id=request_id,
                        exception=SessionError("Internal state inconsistency: Session init data missing"),
                    )
                ]

            status = get_header_as_str(headers=event.headers, key=":status")
            if status == str(http.HTTPStatus.OK):
                session_id = stream_id
                session_data = SessionStateData(
                    session_id=session_id,
                    state=SessionState.CONNECTED,
                    path=init_data.path,
                    headers=init_data.headers,
                    created_at=init_data.created_at,
                    local_max_data=self._config.initial_max_data,
                    peer_max_data=state.peer_initial_max_data,
                    local_max_streams_bidi=self._config.initial_max_streams_bidi,
                    peer_max_streams_bidi=state.peer_initial_max_streams_bidi,
                    local_max_streams_uni=self._config.initial_max_streams_uni,
                    peer_max_streams_uni=state.peer_initial_max_streams_uni,
                    ready_at=now,
                )
                state.sessions[session_id] = session_data

                effects.append(
                    EmitSessionEvent(
                        session_id=session_id,
                        event_type=EventType.SESSION_READY,
                        data={
                            "session_id": session_id,
                            "ready_at": now,
                            "path": session_data.path,
                            "headers": session_data.headers,
                        },
                    )
                )
                effects.append(NotifyRequestDone(request_id=request_id, result=session_id))

                if stream_id in state.early_event_buffer:
                    buffered_events = state.early_event_buffer.pop(stream_id)
                    state.early_event_count -= len(buffered_events)
                    for _, early_event in buffered_events:
                        effects.append(ProcessProtocolEvent(event=early_event))

            else:
                status_val = status if status is not None else "Unknown"
                reason = f"Session creation failed with status {status_val!r}"
                error = ConnectionError(message=reason, error_code=ErrorCodes.H3_REQUEST_REJECTED)
                effects.append(NotifyRequestFailed(request_id=request_id, exception=error))

        else:
            if stream_id in state.sessions:
                logger.debug("Received trailers on existing session stream %d, ignoring.", stream_id)
                return []

            if state.connection_state != ConnectionState.CONNECTED:
                logger.debug(
                    "Rejecting new session on stream %d: connection state is %s", stream_id, state.connection_state
                )
                effects.append(SendH3Headers(stream_id=stream_id, status=http.HTTPStatus.TOO_MANY_REQUESTS))
                return effects

            method = get_header_as_str(headers=event.headers, key=":method")
            protocol = get_header_as_str(headers=event.headers, key=":protocol")

            if method != "CONNECT" or protocol != "webtransport":
                logger.debug("Rejecting non-WebTransport request on stream %d", stream_id)
                effects.append(SendH3Headers(stream_id=stream_id, status=http.HTTPStatus.BAD_REQUEST))
                return effects

            max_sess = self._config.max_sessions
            if max_sess > 0 and len(state.sessions) >= max_sess:
                logger.warning("Session limit (%d) reached, rejecting new session on stream %d", max_sess, stream_id)
                effects.append(SendH3Headers(stream_id=stream_id, status=http.HTTPStatus.TOO_MANY_REQUESTS))
                return effects

            session_id = stream_id
            raw_path = get_header_as_str(headers=event.headers, key=":path", default="/")
            path = raw_path if raw_path is not None and raw_path else "/"

            session_data = SessionStateData(
                session_id=session_id,
                state=SessionState.CONNECTING,
                path=path,
                headers=event.headers,
                created_at=now,
                local_max_data=self._config.initial_max_data,
                peer_max_data=state.peer_initial_max_data,
                local_max_streams_bidi=self._config.initial_max_streams_bidi,
                peer_max_streams_bidi=state.peer_initial_max_streams_bidi,
                local_max_streams_uni=self._config.initial_max_streams_uni,
                peer_max_streams_uni=state.peer_initial_max_streams_uni,
            )
            state.sessions[session_id] = session_data

            effects.append(
                EmitSessionEvent(
                    session_id=session_id,
                    event_type=EventType.SESSION_REQUEST,
                    data={"session_id": session_id, "path": path, "headers": event.headers},
                )
            )

            if stream_id in state.early_event_buffer:
                buffered_events = state.early_event_buffer.pop(stream_id)
                state.early_event_count -= len(buffered_events)
                for _, early_event in buffered_events:
                    effects.append(ProcessProtocolEvent(event=early_event))

        return effects

    def handle_internal_bind_h3_session(self, *, event: InternalBindH3Session, state: ProtocolState) -> list[Effect]:
        """Handle the InternalBindH3Session event (Client-side, Phase 2 of creation)."""
        state.pending_requests[event.stream_id] = event.request_id
        return []

    def handle_internal_fail_h3_session(self, *, event: InternalFailH3Session, state: ProtocolState) -> list[Effect]:
        """Handle the InternalFailH3Session event."""
        logger.error("H3 Session creation failed for request %d: %s", event.request_id, event.exception)

        state.pending_session_configs.pop(event.request_id, None)

        return [NotifyRequestFailed(request_id=event.request_id, exception=event.exception)]

    def handle_transport_parameters_received(
        self, *, event: TransportQuicParametersReceived, state: ProtocolState
    ) -> list[Effect]:
        """Handle the TransportQuicParametersReceived event."""
        logger.debug(
            "Received transport parameters: remote_max_datagram_frame_size=%d", event.remote_max_datagram_frame_size
        )
        state.remote_max_datagram_frame_size = event.remote_max_datagram_frame_size
        return []
