"""Handle stream-level logic for the protocol engine."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from aioquic._buffer import Buffer as QuicBuffer

from pywebtransport import constants
from pywebtransport._protocol.events import (
    Effect,
    EmitStreamEvent,
    InternalBindQuicStream,
    InternalFailQuicStream,
    InternalReturnStreamData,
    NotifyRequestDone,
    NotifyRequestFailed,
    ResetQuicStream,
    SendH3Capsule,
    SendQuicData,
    StopQuicStream,
    TransportStreamReset,
    UserGetStreamDiagnostics,
    UserResetStream,
    UserSendStreamData,
    UserStopStream,
    UserStreamRead,
    WebTransportStreamDataReceived,
)
from pywebtransport._protocol.state import ProtocolState, SessionStateData, StreamStateData
from pywebtransport._protocol.utils import (
    get_stream_direction_from_id,
    http_code_to_webtransport_code,
    webtransport_code_to_http_code,
)
from pywebtransport.constants import ErrorCodes
from pywebtransport.exceptions import SessionError, StreamError
from pywebtransport.types import Buffer, EventType, SessionState, StreamDirection, StreamState
from pywebtransport.utils import ensure_buffer, get_logger, get_timestamp

if TYPE_CHECKING:
    from pywebtransport.config import ClientConfig, ServerConfig


__all__: list[str] = []

logger = get_logger(name=__name__)


class StreamProcessor:
    """Process stream-level events and manage state transitions."""

    def __init__(self, *, is_client: bool, config: ClientConfig | ServerConfig) -> None:
        """Initialize the stream processor."""
        self._is_client = is_client
        self._config = config

    def handle_get_stream_diagnostics(self, *, event: UserGetStreamDiagnostics, state: ProtocolState) -> list[Effect]:
        """Handle the UserGetStreamDiagnostics event."""
        stream_id = event.stream_id
        stream_data = state.streams.get(stream_id)
        if stream_data is None:
            return [
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=StreamError(message=f"Stream {stream_id} not found for diagnostics", stream_id=stream_id),
                )
            ]
        data_dict = dataclasses.asdict(stream_data)
        data_dict["read_buffer"] = bytes(0)
        data_dict["read_buffer_size"] = stream_data.read_buffer_size
        return [NotifyRequestDone(request_id=event.request_id, result=data_dict)]

    def handle_internal_bind_quic_stream(self, *, event: InternalBindQuicStream, state: ProtocolState) -> list[Effect]:
        """Handle the InternalBindQuicStream event."""
        effects: list[Effect] = []
        session_id = event.session_id
        session_data = state.sessions.get(session_id)

        if session_data is None:
            effects.append(
                NotifyRequestFailed(
                    request_id=event.request_id,
                    exception=SessionError(f"Session {session_id} not found during stream bind"),
                )
            )
            return effects

        stream_id = event.stream_id
        direction = StreamDirection.SEND_ONLY if event.is_unidirectional else StreamDirection.BIDIRECTIONAL

        stream_data = StreamStateData(
            stream_id=stream_id,
            session_id=session_id,
            direction=direction,
            state=StreamState.OPEN,
            created_at=get_timestamp(),
        )

        state.streams[stream_id] = stream_data
        session_data.active_streams.add(stream_id)

        effects.append(NotifyRequestDone(request_id=event.request_id, result=stream_id))
        effects.append(
            EmitStreamEvent(
                stream_id=stream_id,
                event_type=EventType.STREAM_OPENED,
                data={"stream_id": stream_id, "session_id": session_id, "direction": direction},
            )
        )
        return effects

    def handle_internal_fail_quic_stream(self, *, event: InternalFailQuicStream, state: ProtocolState) -> list[Effect]:
        """Handle the InternalFailQuicStream event."""
        session_data = state.sessions.get(event.session_id)
        if session_data is not None:
            if event.is_unidirectional:
                if session_data.local_streams_uni_opened > 0:
                    session_data.local_streams_uni_opened -= 1
            else:
                if session_data.local_streams_bidi_opened > 0:
                    session_data.local_streams_bidi_opened -= 1

        return [NotifyRequestFailed(request_id=event.request_id, exception=event.exception)]

    def handle_return_stream_data(self, *, event: InternalReturnStreamData, state: ProtocolState) -> list[Effect]:
        """Handle the InternalReturnStreamData event."""
        stream_data = state.streams.get(event.stream_id)
        if stream_data is not None:
            stream_data.read_buffer.appendleft(event.data)
            stream_data.read_buffer_size += len(event.data)
        return []

    def handle_reset_stream(self, *, event: UserResetStream, state: ProtocolState) -> list[Effect]:
        """Handle the UserResetStream event to reset the sending side."""
        effects: list[Effect] = []
        stream_id = event.stream_id
        stream_data = state.streams.get(stream_id)

        if stream_data is None:
            error = StreamError(message=f"Stream {stream_id} not found for reset", stream_id=stream_id)
            effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
            return effects

        if stream_data.state in (StreamState.HALF_CLOSED_LOCAL, StreamState.CLOSED, StreamState.RESET_SENT):
            effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
            return effects

        original_state = stream_data.state

        stream_data.state = StreamState.RESET_SENT
        stream_data.closed_at = get_timestamp()
        stream_data.close_code = event.error_code

        http_error_code = webtransport_code_to_http_code(app_error_code=event.error_code)
        effects.append(ResetQuicStream(stream_id=stream_id, error_code=http_error_code))
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))

        while stream_data.write_buffer:
            _data_chunk, req_id, _end_stream = stream_data.write_buffer.popleft()
            error = StreamError(
                message=f"Stream {stream_id} reset by application", stream_id=stream_id, error_code=event.error_code
            )
            effects.append(NotifyRequestFailed(request_id=req_id, exception=error))
        stream_data.write_buffer_size = 0

        session_data = state.sessions.get(stream_data.session_id)
        if session_data is not None:
            session_data.blocked_streams.discard(stream_id)

        match original_state:
            case StreamState.HALF_CLOSED_REMOTE | StreamState.RESET_RECEIVED:
                stream_data.state = StreamState.CLOSED
                if session_data is not None:
                    session_data.active_streams.discard(stream_id)
                effects.append(
                    EmitStreamEvent(
                        stream_id=stream_id, event_type=EventType.STREAM_CLOSED, data={"stream_id": stream_id}
                    )
                )

        logger.debug(
            "Stream %d reset locally with code %d (mapped to %x)", stream_id, event.error_code, http_error_code
        )
        return effects

    def handle_send_stream_data(self, *, event: UserSendStreamData, state: ProtocolState) -> list[Effect]:
        """Handle the UserSendStreamData event to send data."""
        effects: list[Effect] = []
        stream_id = event.stream_id
        stream_data = state.streams.get(stream_id)

        if stream_data is None:
            error = StreamError(message=f"Stream {stream_id} not found for sending data", stream_id=stream_id)
            effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
            return effects

        if stream_data.state in (StreamState.HALF_CLOSED_LOCAL, StreamState.CLOSED, StreamState.RESET_SENT):
            error = StreamError(
                message=f"Stream {stream_id} is not writable (state: {stream_data.state})", stream_id=stream_id
            )
            effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
            return effects

        session_id = stream_data.session_id
        session_data = state.sessions.get(session_id)
        if session_data is None:
            logger.error("Internal state error: Stream %d exists but session %s does not", stream_id, session_id)
            error = StreamError(message="Internal state error: Session not found", stream_id=stream_id)
            effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
            return effects

        try:
            buffer_data = ensure_buffer(data=event.data)
        except TypeError as exc:
            logger.warning("Stream %d received invalid data type: %s", stream_id, exc)
            return [NotifyRequestFailed(request_id=event.request_id, exception=exc)]

        data_len = len(buffer_data)
        max_buffer_size = self._config.max_stream_write_buffer
        current_buffer_size = stream_data.write_buffer_size

        if current_buffer_size + data_len > max_buffer_size:
            error = StreamError(
                message=(
                    f"Stream {stream_id} write buffer full "
                    f"({current_buffer_size} + {data_len} > {max_buffer_size} bytes)"
                ),
                stream_id=stream_id,
            )
            return [NotifyRequestFailed(request_id=event.request_id, exception=error)]

        if stream_data.write_buffer:
            logger.debug("Stream %d write added to existing write buffer", stream_id)
            stream_data.write_buffer.append((buffer_data, event.request_id, event.end_stream))
            stream_data.write_buffer_size += data_len
            session_data.blocked_streams.add(stream_id)
            return []

        available_credit = session_data.peer_max_data - session_data.local_data_sent

        if data_len <= available_credit:
            session_data.local_data_sent += data_len
            stream_data.bytes_sent += data_len
            effects.append(SendQuicData(stream_id=stream_id, data=buffer_data, end_stream=event.end_stream))
            effects.append(NotifyRequestDone(request_id=event.request_id, result=None))

            if event.end_stream:
                original_state = stream_data.state
                match original_state:
                    case StreamState.HALF_CLOSED_REMOTE | StreamState.RESET_RECEIVED:
                        stream_data.state = StreamState.CLOSED
                        session_data.active_streams.discard(stream_id)
                        session_data.blocked_streams.discard(stream_id)
                        effects.append(
                            EmitStreamEvent(
                                stream_id=stream_id, event_type=EventType.STREAM_CLOSED, data={"stream_id": stream_id}
                            )
                        )
                    case StreamState.OPEN:
                        stream_data.state = StreamState.HALF_CLOSED_LOCAL
                logger.debug("Stream %d send side closed", stream_id)

            return effects

        elif available_credit > 0:
            data_to_send_now = buffer_data[:available_credit]
            remaining_data = buffer_data[available_credit:]

            session_data.local_data_sent += available_credit
            stream_data.bytes_sent += available_credit
            effects.append(SendQuicData(stream_id=stream_id, data=data_to_send_now, end_stream=False))

            logger.debug(
                "Stream %d partial send: sent %d bytes, buffering %d bytes",
                stream_id,
                available_credit,
                len(remaining_data),
            )
            stream_data.write_buffer.append((remaining_data, event.request_id, event.end_stream))
            stream_data.write_buffer_size += len(remaining_data)
            session_data.blocked_streams.add(stream_id)

            buf = QuicBuffer(capacity=8)
            buf.push_uint_var(session_data.peer_max_data)
            effects.append(
                SendH3Capsule(
                    stream_id=session_data.session_id,
                    capsule_type=constants.WT_DATA_BLOCKED_TYPE,
                    capsule_data=buf.data,
                    end_stream=False,
                )
            )
            return effects

        else:
            logger.debug(
                "Stream %d write blocked by session flow control (%d > %d)", stream_id, data_len, available_credit
            )
            stream_data.write_buffer.append((buffer_data, event.request_id, event.end_stream))
            stream_data.write_buffer_size += data_len
            session_data.blocked_streams.add(stream_id)

            buf = QuicBuffer(capacity=8)
            buf.push_uint_var(session_data.peer_max_data)
            effects.append(
                SendH3Capsule(
                    stream_id=session_data.session_id,
                    capsule_type=constants.WT_DATA_BLOCKED_TYPE,
                    capsule_data=buf.data,
                    end_stream=False,
                )
            )
            return effects

    def handle_stop_stream(self, *, event: UserStopStream, state: ProtocolState) -> list[Effect]:
        """Handle the UserStopStream event to stop the receiving side."""
        effects: list[Effect] = []
        stream_id = event.stream_id
        stream_data = state.streams.get(stream_id)

        if stream_data is None:
            error = StreamError(message=f"Stream {stream_id} not found for stop", stream_id=stream_id)
            effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
            return effects

        if stream_data.state in (StreamState.HALF_CLOSED_REMOTE, StreamState.CLOSED, StreamState.RESET_RECEIVED):
            effects.append(NotifyRequestDone(request_id=event.request_id, result=None))
            return effects

        original_state = stream_data.state
        stream_data.state = StreamState.RESET_RECEIVED
        stream_data.closed_at = get_timestamp()
        stream_data.close_code = event.error_code

        http_error_code = webtransport_code_to_http_code(app_error_code=event.error_code)
        effects.append(StopQuicStream(stream_id=stream_id, error_code=http_error_code))
        effects.append(NotifyRequestDone(request_id=event.request_id, result=None))

        while stream_data.pending_read_requests:
            req_id, _ = stream_data.pending_read_requests.popleft()
            error = StreamError(
                message=f"Stream {stream_id} stopped by application", stream_id=stream_id, error_code=event.error_code
            )
            effects.append(NotifyRequestFailed(request_id=req_id, exception=error))

        session_data = state.sessions.get(stream_data.session_id)

        match original_state:
            case StreamState.HALF_CLOSED_LOCAL | StreamState.RESET_SENT:
                stream_data.state = StreamState.CLOSED
                if session_data is not None:
                    session_data.active_streams.discard(stream_id)
                    session_data.blocked_streams.discard(stream_id)
                effects.append(
                    EmitStreamEvent(
                        stream_id=stream_id, event_type=EventType.STREAM_CLOSED, data={"stream_id": stream_id}
                    )
                )

        logger.debug(
            "Stream %d receive side stopped locally with code %d (mapped to %x)",
            stream_id,
            event.error_code,
            http_error_code,
        )
        return effects

    def handle_stream_read(self, *, event: UserStreamRead, state: ProtocolState) -> list[Effect]:
        """Handle the UserStreamRead event to read data."""
        effects: list[Effect] = []
        stream_id = event.stream_id
        stream_data = state.streams.get(stream_id)

        if stream_data is None:
            error = StreamError(message=f"Stream {stream_id} not found for reading", stream_id=stream_id)
            effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
            return effects

        if stream_data.read_buffer_size > 0:
            target = self._calculate_read_amount(
                requested_bytes=event.max_bytes, buffer_size=stream_data.read_buffer_size
            )
            data_to_return = self._read_from_buffer(stream_data=stream_data, max_bytes=target)
            effects.append(NotifyRequestDone(request_id=event.request_id, result=data_to_return))
            return effects

        if stream_data.state in (StreamState.RESET_RECEIVED, StreamState.CLOSED):
            error = StreamError(
                message=f"Stream {stream_id} receive side closed (state: {stream_data.state})", stream_id=stream_id
            )
            effects.append(NotifyRequestFailed(request_id=event.request_id, exception=error))
            return effects

        if stream_data.state == StreamState.HALF_CLOSED_REMOTE:
            effects.append(NotifyRequestDone(request_id=event.request_id, result=b""))
            return effects

        stream_data.pending_read_requests.append((event.request_id, event.max_bytes))
        return effects

    def handle_transport_stream_reset(self, *, event: TransportStreamReset, state: ProtocolState) -> list[Effect]:
        """Handle a transport-level stream reset from the peer."""
        effects: list[Effect] = []
        stream_id = event.stream_id
        stream_data = state.streams.get(stream_id)

        if stream_data is None or stream_data.state == StreamState.CLOSED:
            return []

        logger.debug("Stream %d reset by peer with code %d", stream_id, event.error_code)

        app_error_code = event.error_code
        if ErrorCodes.WT_APPLICATION_ERROR_FIRST <= event.error_code <= ErrorCodes.WT_APPLICATION_ERROR_LAST:
            try:
                app_error_code = http_code_to_webtransport_code(http_error_code=event.error_code)
            except ValueError:
                logger.warning(
                    "Received reserved H3 error code %x on stream %d, using as-is.", event.error_code, stream_id
                )

        stream_data.closed_at = get_timestamp()
        stream_data.close_code = app_error_code

        while stream_data.pending_read_requests:
            req_id, _ = stream_data.pending_read_requests.popleft()
            error = StreamError(
                message=f"Stream {stream_id} reset by peer", stream_id=stream_id, error_code=app_error_code
            )
            effects.append(NotifyRequestFailed(request_id=req_id, exception=error))

        while stream_data.write_buffer:
            _data_chunk, req_id, _end_stream = stream_data.write_buffer.popleft()
            error = StreamError(
                message=f"Stream {stream_id} reset by peer", stream_id=stream_id, error_code=app_error_code
            )
            effects.append(NotifyRequestFailed(request_id=req_id, exception=error))
        stream_data.write_buffer_size = 0

        stream_data.state = StreamState.CLOSED

        session_data = state.sessions.get(stream_data.session_id)
        if session_data is not None:
            session_data.active_streams.discard(stream_id)
            session_data.blocked_streams.discard(stream_id)

        effects.append(
            EmitStreamEvent(stream_id=stream_id, event_type=EventType.STREAM_CLOSED, data={"stream_id": stream_id})
        )

        return effects

    def handle_webtransport_stream_data(
        self, *, event: WebTransportStreamDataReceived, state: ProtocolState
    ) -> list[Effect]:
        """Handle WebTransport data received on an established stream."""
        effects: list[Effect] = []
        stream_id = event.stream_id
        stream_data = state.streams.get(stream_id)
        session_data: SessionStateData | None = None

        if stream_data is None:
            if self._is_client:
                logger.warning("Client received WT data for unknown stream %d, ignoring.", stream_id)
                return []

            session_id = event.session_id
            if state.sessions.get(session_id) is None:
                if state.early_event_count >= self._config.max_total_pending_events:
                    logger.warning(
                        "Global early event buffer full (%d), rejecting stream %d", state.early_event_count, stream_id
                    )
                    return [
                        ResetQuicStream(
                            stream_id=stream_id, error_code=constants.ErrorCodes.WT_BUFFERED_STREAM_REJECTED
                        )
                    ]

                session_buffer = state.early_event_buffer.get(session_id, [])
                if len(session_buffer) >= self._config.max_pending_events_per_session:
                    logger.warning(
                        "Per-session early event buffer full (%d) for session %d, rejecting stream %d",
                        len(session_buffer),
                        session_id,
                        stream_id,
                    )
                    return [
                        ResetQuicStream(
                            stream_id=stream_id, error_code=constants.ErrorCodes.WT_BUFFERED_STREAM_REJECTED
                        )
                    ]

                logger.debug("Buffering early event for stream %d on unknown session %d", stream_id, session_id)
                state.early_event_buffer.setdefault(session_id, []).append((get_timestamp(), event))
                state.early_event_count += 1
                return []

            session_data = state.sessions.get(session_id)
            if session_data is None:
                logger.warning(
                    "Received WT data for stream %d with unknown session %s, ignoring.", stream_id, session_id
                )
                return effects

            if session_data.state not in (SessionState.CONNECTED, SessionState.DRAINING):
                logger.debug(
                    "Ignoring new stream %d for session %s in state %s", stream_id, session_id, session_data.state
                )
                return []

            direction = get_stream_direction_from_id(stream_id=stream_id, is_client=self._is_client)

            match direction:
                case StreamDirection.RECEIVE_ONLY:
                    if session_data.peer_streams_uni_opened >= session_data.local_max_streams_uni:
                        logger.warning(
                            "Session %s unidirectional stream limit (%d) reached, ignoring stream %d",
                            session_id,
                            session_data.local_max_streams_uni,
                            stream_id,
                        )
                        return []
                    session_data.peer_streams_uni_opened += 1

                    credit_effect = self._check_and_send_stream_credit(
                        session_data=session_data, is_unidirectional=True
                    )
                    if credit_effect is not None:
                        effects.append(credit_effect)

                case StreamDirection.BIDIRECTIONAL:
                    if session_data.peer_streams_bidi_opened >= session_data.local_max_streams_bidi:
                        logger.warning(
                            "Session %s bidirectional stream limit (%d) reached, ignoring stream %d",
                            session_id,
                            session_data.local_max_streams_bidi,
                            stream_id,
                        )
                        return []
                    session_data.peer_streams_bidi_opened += 1

                    credit_effect = self._check_and_send_stream_credit(
                        session_data=session_data, is_unidirectional=False
                    )
                    if credit_effect is not None:
                        effects.append(credit_effect)

                case StreamDirection.SEND_ONLY:
                    logger.warning(
                        "Received WT data on server for client-initiated send-only stream %d, ignoring.", stream_id
                    )
                    return []

            logger.debug("Creating new incoming stream %d for session %s", stream_id, session_id)
            stream_data = StreamStateData(
                stream_id=stream_id,
                session_id=session_id,
                direction=direction,
                state=StreamState.OPEN,
                created_at=get_timestamp(),
            )
            state.streams[stream_id] = stream_data
            session_data.active_streams.add(stream_id)

            effects.append(
                EmitStreamEvent(
                    stream_id=stream_id,
                    event_type=EventType.STREAM_OPENED,
                    data={"stream_id": stream_id, "session_id": session_id, "direction": direction},
                )
            )

        if stream_data.state in (StreamState.RESET_RECEIVED, StreamState.CLOSED):
            logger.debug("Ignoring WT data for already closed/reset stream %d", stream_id)
            return []

        if session_data is None:
            session_data = state.sessions.get(stream_data.session_id)
            if session_data is None:
                logger.error(
                    "Internal state error: Stream %d exists but session %s does not. Cannot process data.",
                    stream_id,
                    stream_data.session_id,
                )
                return effects

        if event.data:
            data_len = len(event.data)
            max_buffer_size = self._config.max_stream_read_buffer
            current_buffer_size = stream_data.read_buffer_size

            if current_buffer_size + data_len > max_buffer_size:
                logger.warning(
                    "Stream %d read buffer limit exceeded (%d + %d > %d). Sending STOP_SENDING.",
                    stream_id,
                    current_buffer_size,
                    data_len,
                    max_buffer_size,
                )
                effects.append(StopQuicStream(stream_id=stream_id, error_code=ErrorCodes.WT_FLOW_CONTROL_ERROR))
                return effects

            stream_data.bytes_received += data_len
            stream_data.read_buffer.append(event.data)
            stream_data.read_buffer_size += data_len

            session_data.peer_data_sent += data_len
            credit_effect = self._check_and_send_data_credit(session_data=session_data)
            if credit_effect is not None:
                effects.append(credit_effect)

        while stream_data.pending_read_requests and stream_data.read_buffer_size > 0:
            req_id, max_bytes = stream_data.pending_read_requests.popleft()
            target = self._calculate_read_amount(requested_bytes=max_bytes, buffer_size=stream_data.read_buffer_size)

            data_chunk = self._read_from_buffer(stream_data=stream_data, max_bytes=target)
            effects.append(NotifyRequestDone(request_id=req_id, result=data_chunk))

        if event.stream_ended:
            original_state = stream_data.state
            match original_state:
                case StreamState.HALF_CLOSED_LOCAL | StreamState.RESET_SENT:
                    if stream_data.read_buffer_size == 0:
                        stream_data.state = StreamState.CLOSED
                        stream_data.closed_at = get_timestamp()
                        session_data.active_streams.discard(stream_id)
                        session_data.blocked_streams.discard(stream_id)
                        effects.append(
                            EmitStreamEvent(
                                stream_id=stream_id, event_type=EventType.STREAM_CLOSED, data={"stream_id": stream_id}
                            )
                        )
                    else:
                        stream_data.state = StreamState.HALF_CLOSED_REMOTE
                        logger.debug("Stream %d rx closed, data pending read. Moving to HALF_CLOSED_REMOTE", stream_id)
                case StreamState.OPEN:
                    stream_data.state = StreamState.HALF_CLOSED_REMOTE
                    logger.debug("Stream %d receive side closed by peer (WT data)", stream_id)

            while stream_data.pending_read_requests:
                req_id, _ = stream_data.pending_read_requests.popleft()
                effects.append(NotifyRequestDone(request_id=req_id, result=b""))

        return effects

    def _calculate_read_amount(self, *, requested_bytes: int, buffer_size: int) -> int:
        """Calculate the amount of data to read based on request and buffer size."""
        if requested_bytes <= 0:
            return buffer_size
        return min(requested_bytes, buffer_size)

    def _check_and_send_data_credit(self, *, session_data: SessionStateData) -> Effect | None:
        """Check session data credit and send a MAX_DATA capsule if needed."""
        if not self._config.flow_control_window_auto_scale:
            return None

        configured_window = self._config.flow_control_window_size
        physical_limit = self._config.max_stream_read_buffer
        target_window = min(configured_window, physical_limit)

        current_limit = session_data.local_max_data
        current_usage = session_data.peer_data_sent

        available_credit = current_limit - current_usage
        threshold = target_window // 2

        if available_credit <= threshold:
            new_limit = current_usage + target_window

            logger.debug(
                "Session %s data credit auto-increment: usage=%d available=%d limit=%d new_limit=%d",
                session_data.session_id,
                current_usage,
                available_credit,
                current_limit,
                new_limit,
            )

            session_data.local_max_data = new_limit

            buf = QuicBuffer(capacity=8)
            buf.push_uint_var(new_limit)

            return SendH3Capsule(
                stream_id=session_data.session_id,
                capsule_type=constants.WT_MAX_DATA_TYPE,
                capsule_data=buf.data,
                end_stream=False,
            )
        return None

    def _check_and_send_stream_credit(
        self, *, session_data: SessionStateData, is_unidirectional: bool
    ) -> Effect | None:
        """Check stream credit and send a MAX_STREAMS capsule if needed."""
        if not self._config.flow_control_window_auto_scale:
            return None

        if is_unidirectional:
            target_window = self._config.stream_flow_control_increment_uni
            current_limit = session_data.local_max_streams_uni
            current_usage = session_data.peer_streams_uni_opened
            capsule_type = constants.WT_MAX_STREAMS_UNI_TYPE
        else:
            target_window = self._config.stream_flow_control_increment_bidi
            current_limit = session_data.local_max_streams_bidi
            current_usage = session_data.peer_streams_bidi_opened
            capsule_type = constants.WT_MAX_STREAMS_BIDI_TYPE

        available_credit = current_limit - current_usage
        threshold = target_window // 2

        if available_credit <= threshold:
            new_limit = current_usage + target_window

            logger.debug(
                "Session %s stream credit auto-increment: type=%s usage=%d available=%d limit=%d new_limit=%d",
                session_data.session_id,
                "uni" if is_unidirectional else "bidi",
                current_usage,
                available_credit,
                current_limit,
                new_limit,
            )

            if is_unidirectional:
                session_data.local_max_streams_uni = new_limit
            else:
                session_data.local_max_streams_bidi = new_limit

            buf = QuicBuffer(capacity=8)
            buf.push_uint_var(new_limit)

            return SendH3Capsule(
                stream_id=session_data.session_id, capsule_type=capsule_type, capsule_data=buf.data, end_stream=False
            )
        return None

    def _read_from_buffer(self, *, stream_data: StreamStateData, max_bytes: int) -> bytes:
        """Read up to max_bytes from the stream's read buffer."""
        chunks: list[Buffer] = []
        bytes_collected = 0

        while stream_data.read_buffer and bytes_collected < max_bytes:
            chunk = stream_data.read_buffer.popleft()
            chunk_len = len(chunk)
            needed = max_bytes - bytes_collected

            if chunk_len <= needed:
                chunks.append(chunk)
                bytes_collected += chunk_len
                stream_data.read_buffer_size -= chunk_len
            else:
                part = chunk[:needed]
                remainder = chunk[needed:]
                chunks.append(part)
                bytes_collected += needed
                stream_data.read_buffer_size -= needed
                stream_data.read_buffer.appendleft(remainder)
                break

        return b"".join(chunks)
