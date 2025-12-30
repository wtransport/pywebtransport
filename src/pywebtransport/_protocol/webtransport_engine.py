"""Internal protocol engine for driving the WebTransport state machine."""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any

from aioquic.buffer import encode_uint_var

from pywebtransport import constants
from pywebtransport._protocol.connection_processor import ConnectionProcessor
from pywebtransport._protocol.events import (
    CapsuleReceived,
    ConnectionClose,
    ConnectStreamClosed,
    DatagramReceived,
    Effect,
    EmitConnectionEvent,
    GoawayReceived,
    HeadersReceived,
    InternalBindH3Session,
    InternalBindQuicStream,
    InternalCleanupEarlyEvents,
    InternalCleanupResources,
    InternalFailH3Session,
    InternalFailQuicStream,
    InternalReturnStreamData,
    LogH3Frame,
    NotifyRequestFailed,
    ProtocolEvent,
    RescheduleQuicTimer,
    SendQuicData,
    SendQuicDatagram,
    SettingsReceived,
    TransportConnectionTerminated,
    TransportDatagramFrameReceived,
    TransportHandshakeCompleted,
    TransportQuicParametersReceived,
    TransportQuicTimerFired,
    TransportStreamDataReceived,
    TransportStreamReset,
    TriggerQuicTimer,
    UserAcceptSession,
    UserCloseSession,
    UserConnectionGracefulClose,
    UserCreateSession,
    UserCreateStream,
    UserEvent,
    UserGetConnectionDiagnostics,
    UserGetSessionDiagnostics,
    UserGetStreamDiagnostics,
    UserGrantDataCredit,
    UserGrantStreamsCredit,
    UserRejectSession,
    UserResetStream,
    UserSendDatagram,
    UserSendStreamData,
    UserStopStream,
    UserStreamRead,
    WebTransportStreamDataReceived,
)
from pywebtransport._protocol.h3_engine import WebTransportH3Engine
from pywebtransport._protocol.session_processor import SessionProcessor
from pywebtransport._protocol.state import ProtocolState
from pywebtransport._protocol.stream_processor import StreamProcessor
from pywebtransport.constants import ErrorCodes
from pywebtransport.exceptions import ConnectionError
from pywebtransport.types import Buffer, ConnectionId, ConnectionState, EventType, Headers, StreamId
from pywebtransport.utils import get_logger, get_timestamp, merge_headers

if TYPE_CHECKING:
    from pywebtransport.config import ClientConfig, ServerConfig


__all__: list[str] = []

logger = get_logger(name=__name__)


class WebTransportEngine:
    """Orchestrates the unified protocol state machine."""

    def __init__(self, *, connection_id: ConnectionId, config: ClientConfig | ServerConfig, is_client: bool) -> None:
        """Initialize the WebTransport engine."""
        self._connection_id = connection_id
        self._is_client = is_client
        self._config = config

        self._state = ProtocolState(
            is_client=is_client, connection_state=ConnectionState.IDLE, max_datagram_size=config.max_datagram_size
        )

        self._connection_processor = ConnectionProcessor(
            is_client=is_client, config=config, connection_id=connection_id
        )
        self._session_processor = SessionProcessor(is_client=is_client, config=config)
        self._stream_processor = StreamProcessor(is_client=is_client, config=config)
        self._h3_engine = WebTransportH3Engine(is_client=is_client, config=config)

        self._pending_user_actions: deque[UserEvent[Any]] = deque()

    def cleanup_stream(self, *, stream_id: StreamId) -> None:
        """Clean up H3 state for a closed stream."""
        self._h3_engine.cleanup_stream(stream_id=stream_id)

    def encode_capsule(
        self, *, stream_id: StreamId, capsule_type: int, capsule_data: bytes, end_stream: bool = False
    ) -> list[Effect]:
        """Encode a capsule and return effects to send it."""
        data = self._h3_engine.encode_capsule(stream_id=stream_id, capsule_type=capsule_type, capsule_data=capsule_data)
        return [SendQuicData(stream_id=stream_id, data=data, end_stream=end_stream)]

    def encode_datagram(self, *, stream_id: StreamId, data: Buffer | list[Buffer]) -> list[Effect]:
        """Encode a datagram and return effects to send it."""
        payload = self._h3_engine.encode_datagram(stream_id=stream_id, data=data)
        return [SendQuicDatagram(data=payload)]

    def encode_goaway(self) -> list[Effect]:
        """Encode a GOAWAY frame and return effects to send it."""
        control_id = self._h3_engine._local_control_stream_id
        if control_id is None:
            return []
        data = self._h3_engine.encode_goaway_frame()
        return [SendQuicData(stream_id=control_id, data=data, end_stream=False)]

    def encode_headers(self, *, stream_id: StreamId, status: int, end_stream: bool = False) -> list[Effect]:
        """Encode headers and return effects to send them."""
        headers: Headers = {":status": str(status)}
        return self._h3_engine.encode_headers(stream_id=stream_id, headers=headers, end_stream=end_stream)

    def encode_session_request(
        self, *, stream_id: StreamId, path: str, authority: str, headers: Headers
    ) -> list[Effect]:
        """Encode a WebTransport session establishment request (CONNECT)."""
        initial_headers: Headers = {
            ":method": "CONNECT",
            ":scheme": "https",
            ":authority": authority,
            ":path": path,
            ":protocol": "webtransport",
        }
        final_headers = merge_headers(base=initial_headers, update=headers)
        return self._h3_engine.encode_headers(stream_id=stream_id, headers=final_headers, end_stream=False)

    def encode_stream_creation(
        self, *, stream_id: StreamId, control_stream_id: StreamId, is_unidirectional: bool
    ) -> list[Effect]:
        """Encode the preamble for a new WebTransport stream."""
        return self._h3_engine.encode_webtransport_stream_creation(
            stream_id=stream_id, control_stream_id=control_stream_id, is_unidirectional=is_unidirectional
        )

    def handle_event(self, *, event: ProtocolEvent) -> list[Effect]:
        """Process a single event and return resulting effects."""
        all_effects: list[Effect] = []
        events_to_process: deque[ProtocolEvent] = deque([event])

        while events_to_process:
            current_event = events_to_process.popleft()
            new_effects: list[Effect] = []
            re_queue_pending_actions = False

            match current_event:
                case InternalBindH3Session() as ibhs_ev:
                    new_effects.extend(
                        self._connection_processor.handle_internal_bind_h3_session(event=ibhs_ev, state=self._state)
                    )

                case InternalBindQuicStream() as ibqs_ev:
                    new_effects.extend(
                        self._stream_processor.handle_internal_bind_quic_stream(event=ibqs_ev, state=self._state)
                    )

                case InternalCleanupEarlyEvents() as icee_ev:
                    new_effects.extend(
                        self._connection_processor.handle_cleanup_early_events(event=icee_ev, state=self._state)
                    )

                case InternalCleanupResources() as icr_ev:
                    new_effects.extend(
                        self._connection_processor.handle_cleanup_resources(event=icr_ev, state=self._state)
                    )

                case InternalFailH3Session() as ifhs_ev:
                    new_effects.extend(
                        self._connection_processor.handle_internal_fail_h3_session(event=ifhs_ev, state=self._state)
                    )

                case InternalFailQuicStream() as ifqs_ev:
                    new_effects.extend(
                        self._stream_processor.handle_internal_fail_quic_stream(event=ifqs_ev, state=self._state)
                    )

                case InternalReturnStreamData() as irsd_ev:
                    new_effects.extend(
                        self._stream_processor.handle_return_stream_data(event=irsd_ev, state=self._state)
                    )

                case TransportConnectionTerminated() as tct_ev:
                    new_effects.extend(
                        self._connection_processor.handle_connection_terminated(event=tct_ev, state=self._state)
                    )
                    error = ConnectionError(
                        message=f"Connection terminated before ready: {tct_ev.reason_phrase}",
                        error_code=tct_ev.error_code,
                    )
                    new_effects.extend(self._fail_pending_user_actions(exception=error))

                case TransportDatagramFrameReceived() | TransportStreamDataReceived() as tev:
                    was_settings_received = self._h3_engine._settings_received

                    h3_events, h3_effects = self._h3_engine.handle_transport_event(event=tev, state=self._state)
                    new_effects.extend(h3_effects)
                    events_to_process.extendleft(reversed(h3_events))

                    if self._is_client and not was_settings_received and self._h3_engine._settings_received:
                        logger.debug("Client received peer H3 SETTINGS.")
                        self._state.peer_settings_received = True
                        (readiness_effects, is_ready) = self._check_client_connection_ready()
                        new_effects.extend(readiness_effects)
                        if is_ready:
                            re_queue_pending_actions = True

                case TransportHandshakeCompleted():
                    if self._state.connection_state == ConnectionState.IDLE:
                        logger.debug("State transition: IDLE -> CONNECTING")
                        self._state.connection_state = ConnectionState.CONNECTING

                    if self._state.connection_state == ConnectionState.CONNECTING:
                        logger.debug("TransportHandshakeCompleted received.")
                        self._state.handshake_complete = True

                        if self._is_client:
                            (readiness_effects, is_ready) = self._check_client_connection_ready()
                            new_effects.extend(readiness_effects)
                            if is_ready:
                                re_queue_pending_actions = True
                        else:
                            self._state.connection_state = ConnectionState.CONNECTED
                            self._state.connected_at = get_timestamp()
                            new_effects.append(
                                EmitConnectionEvent(
                                    event_type=EventType.CONNECTION_ESTABLISHED,
                                    data={"connection_id": self._connection_id},
                                )
                            )
                    else:
                        logger.warning(
                            "Received TransportHandshakeCompleted in unexpected state: %s", self._state.connection_state
                        )

                case TransportQuicParametersReceived() as tqpr_ev:
                    new_effects.extend(
                        self._connection_processor.handle_transport_parameters_received(
                            event=tqpr_ev, state=self._state
                        )
                    )

                case TransportQuicTimerFired():
                    new_effects.extend([TriggerQuicTimer(), RescheduleQuicTimer()])

                case TransportStreamReset() as tsr_ev:
                    new_effects.extend(
                        self._stream_processor.handle_transport_stream_reset(event=tsr_ev, state=self._state)
                    )

                case CapsuleReceived() as cr_ev:
                    new_effects.extend(self._session_processor.handle_capsule_received(event=cr_ev, state=self._state))

                case ConnectStreamClosed() as csc_ev:
                    new_effects.extend(
                        self._session_processor.handle_connect_stream_closed(event=csc_ev, state=self._state)
                    )

                case DatagramReceived() as dr_ev:
                    new_effects.extend(self._session_processor.handle_datagram_received(event=dr_ev, state=self._state))

                case GoawayReceived() as gr_ev:
                    new_effects.extend(
                        self._connection_processor.handle_goaway_received(event=gr_ev, state=self._state)
                    )

                case HeadersReceived() as hr_ev:
                    new_effects.extend(
                        self._connection_processor.handle_headers_received(event=hr_ev, state=self._state)
                    )

                case SettingsReceived() as sr_ev:
                    logger.debug("Processing H3 SETTINGS frame.")
                    self._state.peer_initial_max_data = sr_ev.settings.get(constants.SETTINGS_WT_INITIAL_MAX_DATA, 0)
                    self._state.peer_initial_max_streams_bidi = sr_ev.settings.get(
                        constants.SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, 0
                    )
                    self._state.peer_initial_max_streams_uni = sr_ev.settings.get(
                        constants.SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, 0
                    )

                case WebTransportStreamDataReceived() as wtsdr_ev:
                    new_effects.extend(
                        self._stream_processor.handle_webtransport_stream_data(event=wtsdr_ev, state=self._state)
                    )

                case UserAcceptSession() as uas_ev:
                    new_effects.extend(self._session_processor.handle_accept_session(event=uas_ev, state=self._state))

                case UserCloseSession() as ucs_ev:
                    new_effects.extend(self._session_processor.handle_close_session(event=ucs_ev, state=self._state))

                case UserConnectionGracefulClose() as ugcc_ev:
                    new_effects.extend(
                        self._connection_processor.handle_graceful_close(event=ugcc_ev, state=self._state)
                    )

                case UserCreateSession() as ucs_ev:
                    if self._is_client and self._state.connection_state in (
                        ConnectionState.IDLE,
                        ConnectionState.CONNECTING,
                    ):
                        logger.debug("Client not fully connected, buffering UserCreateSession.")
                        self._pending_user_actions.append(ucs_ev)
                    else:
                        new_effects.extend(
                            self._connection_processor.handle_create_session(event=ucs_ev, state=self._state)
                        )

                case UserCreateStream() as ucs_ev:
                    if self._is_client and self._state.connection_state in (
                        ConnectionState.IDLE,
                        ConnectionState.CONNECTING,
                    ):
                        logger.debug("Client not fully connected, buffering UserCreateStream.")
                        self._pending_user_actions.append(ucs_ev)
                    else:
                        new_effects.extend(
                            self._session_processor.handle_create_stream(event=ucs_ev, state=self._state)
                        )

                case UserGetConnectionDiagnostics() as ugcd_ev:
                    new_effects.extend(
                        self._connection_processor.handle_get_connection_diagnostics(event=ugcd_ev, state=self._state)
                    )

                case UserGetSessionDiagnostics() as ugsd_ev:
                    new_effects.extend(
                        self._session_processor.handle_get_session_diagnostics(event=ugsd_ev, state=self._state)
                    )

                case UserGetStreamDiagnostics() as ugstd_ev:
                    new_effects.extend(
                        self._stream_processor.handle_get_stream_diagnostics(event=ugstd_ev, state=self._state)
                    )

                case UserGrantDataCredit() as ugdc_ev:
                    new_effects.extend(
                        self._session_processor.handle_grant_data_credit(event=ugdc_ev, state=self._state)
                    )

                case UserGrantStreamsCredit() as ugsc_ev:
                    new_effects.extend(
                        self._session_processor.handle_grant_streams_credit(event=ugsc_ev, state=self._state)
                    )

                case UserRejectSession() as urs_ev:
                    new_effects.extend(self._session_processor.handle_reject_session(event=urs_ev, state=self._state))

                case UserResetStream() as urs_ev:
                    new_effects.extend(self._stream_processor.handle_reset_stream(event=urs_ev, state=self._state))

                case UserSendDatagram() as usd_ev:
                    new_effects.extend(self._session_processor.handle_send_datagram(event=usd_ev, state=self._state))

                case UserSendStreamData() as ussd_ev:
                    new_effects.extend(self._stream_processor.handle_send_stream_data(event=ussd_ev, state=self._state))

                case UserStopStream() as uss_ev:
                    new_effects.extend(self._stream_processor.handle_stop_stream(event=uss_ev, state=self._state))

                case UserStreamRead() as usr_ev:
                    new_effects.extend(self._stream_processor.handle_stream_read(event=usr_ev, state=self._state))

                case ConnectionClose() as cc_ev:
                    new_effects.extend(
                        self._connection_processor.handle_connection_close(event=cc_ev, state=self._state)
                    )
                    error = ConnectionError(message="Connection closed by application", error_code=ErrorCodes.NO_ERROR)
                    new_effects.extend(self._fail_pending_user_actions(exception=error))

                case _:
                    logger.warning("Unhandled event type in engine's handle_event: %s", type(current_event))

            all_effects.extend(new_effects)

            if re_queue_pending_actions:
                if self._pending_user_actions:
                    logger.debug(
                        "Connection is ready, re-queueing %d pending user actions.", len(self._pending_user_actions)
                    )
                    events_to_process.extendleft(reversed(self._pending_user_actions))
                    self._pending_user_actions.clear()

        all_effects.append(RescheduleQuicTimer())

        return all_effects

    def initialize_h3_transport(
        self, *, control_id: StreamId, encoder_id: StreamId, decoder_id: StreamId
    ) -> list[Effect]:
        """Initialize HTTP/3 unidirectional streams and settings."""
        self._h3_engine.set_local_stream_ids(
            control_stream_id=control_id, encoder_stream_id=encoder_id, decoder_stream_id=decoder_id
        )

        settings_bytes = self._h3_engine.initialize_connection()

        effects: list[Effect] = [
            SendQuicData(
                stream_id=control_id,
                data=encode_uint_var(constants.H3_STREAM_TYPE_CONTROL) + settings_bytes,
                end_stream=False,
            ),
            SendQuicData(
                stream_id=encoder_id, data=encode_uint_var(constants.H3_STREAM_TYPE_QPACK_ENCODER), end_stream=False
            ),
            SendQuicData(
                stream_id=decoder_id, data=encode_uint_var(constants.H3_STREAM_TYPE_QPACK_DECODER), end_stream=False
            ),
            LogH3Frame(category="http", event="stream_type_set", data={"new": "control", "stream_id": control_id}),
            LogH3Frame(
                category="http", event="stream_type_set", data={"new": "qpack_encoder", "stream_id": encoder_id}
            ),
            LogH3Frame(
                category="http", event="stream_type_set", data={"new": "qpack_decoder", "stream_id": decoder_id}
            ),
        ]
        return effects

    def _check_client_connection_ready(self) -> tuple[list[Effect], bool]:
        """Check if the client connection is fully ready (QUIC + H3)."""
        if (
            self._state.connection_state == ConnectionState.CONNECTING
            and self._state.handshake_complete
            and self._state.peer_settings_received
        ):
            logger.debug("Client connection fully ready (QUIC + H3 SETTINGS).")
            self._state.connection_state = ConnectionState.CONNECTED
            self._state.connected_at = get_timestamp()
            effects: list[Effect] = [
                EmitConnectionEvent(
                    event_type=EventType.CONNECTION_ESTABLISHED, data={"connection_id": self._connection_id}
                )
            ]
            return effects, True

        return [], False

    def _fail_pending_user_actions(self, *, exception: Exception) -> list[Effect]:
        """Fail all pending user actions with the given exception."""
        effects: list[Effect] = []
        while self._pending_user_actions:
            action = self._pending_user_actions.popleft()
            effects.append(NotifyRequestFailed(request_id=action.request_id, exception=exception))
        return effects
