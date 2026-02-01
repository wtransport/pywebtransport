//! Unit tests for the `crate::common::types` module.

use bytes::Bytes;
use rstest::*;
use serde_json::to_string;

use super::*;

#[rstest]
#[case(ConnectionState::Closed, "\"closed\"")]
#[case(ConnectionState::Closing, "\"closing\"")]
#[case(ConnectionState::Connected, "\"connected\"")]
#[case(ConnectionState::Connecting, "\"connecting\"")]
#[case(ConnectionState::Draining, "\"draining\"")]
#[case(ConnectionState::Failed, "\"failed\"")]
#[case(ConnectionState::Idle, "\"idle\"")]
fn test_connection_state_serialization_mapping_success(
    #[case] state: ConnectionState,
    #[case] expected_json: &str,
) {
    let res = to_string(&state);

    assert!(
        res.is_ok(),
        "Serialization failed: {:?}",
        res.as_ref().err()
    );
    assert_eq!(res.unwrap_or_default(), expected_json);
}

#[test]
fn test_connection_state_traits_behavior_success() {
    let state = ConnectionState::Connected;

    let state_copy = state;
    let debug_output = format!("{state:?}");

    assert_eq!(state, state_copy);
    assert_eq!(debug_output, "Connected");
}

#[rstest]
#[case(EventType::CapsuleReceived, "\"capsule_received\"")]
#[case(EventType::ConnectionClosed, "\"connection_closed\"")]
#[case(EventType::ConnectionEstablished, "\"connection_established\"")]
#[case(EventType::ConnectionFailed, "\"connection_failed\"")]
#[case(EventType::ConnectionLost, "\"connection_lost\"")]
#[case(EventType::DatagramError, "\"datagram_error\"")]
#[case(EventType::DatagramReceived, "\"datagram_received\"")]
#[case(EventType::DatagramSent, "\"datagram_sent\"")]
#[case(EventType::ProtocolError, "\"protocol_error\"")]
#[case(EventType::SessionClosed, "\"session_closed\"")]
#[case(EventType::SessionDataBlocked, "\"session_data_blocked\"")]
#[case(EventType::SessionDraining, "\"session_draining\"")]
#[case(EventType::SessionMaxDataUpdated, "\"session_max_data_updated\"")]
#[case(
    EventType::SessionMaxStreamsBidiUpdated,
    "\"session_max_streams_bidi_updated\""
)]
#[case(
    EventType::SessionMaxStreamsUniUpdated,
    "\"session_max_streams_uni_updated\""
)]
#[case(EventType::SessionReady, "\"session_ready\"")]
#[case(EventType::SessionRequest, "\"session_request\"")]
#[case(EventType::SessionStreamsBlocked, "\"session_streams_blocked\"")]
#[case(EventType::SettingsReceived, "\"settings_received\"")]
#[case(EventType::StreamClosed, "\"stream_closed\"")]
#[case(EventType::StreamDataReceived, "\"stream_data_received\"")]
#[case(EventType::StreamError, "\"stream_error\"")]
#[case(EventType::StreamOpened, "\"stream_opened\"")]
#[case(EventType::TimeoutError, "\"timeout_error\"")]
fn test_event_type_serialization_mapping_success(
    #[case] event: EventType,
    #[case] expected_json: &str,
) {
    let res = to_string(&event);

    assert!(
        res.is_ok(),
        "Serialization failed: {:?}",
        res.as_ref().err()
    );
    assert_eq!(res.unwrap_or_default(), expected_json);
}

#[test]
fn test_event_type_traits_behavior_success() {
    let event = EventType::StreamOpened;

    let event_copy = event;
    let debug_output = format!("{event:?}");

    assert_eq!(event, event_copy);
    assert_eq!(debug_output, "StreamOpened");
}

#[rstest]
#[case(SessionState::Closed, "\"closed\"")]
#[case(SessionState::Closing, "\"closing\"")]
#[case(SessionState::Connected, "\"connected\"")]
#[case(SessionState::Connecting, "\"connecting\"")]
#[case(SessionState::Draining, "\"draining\"")]
fn test_session_state_serialization_mapping_success(
    #[case] state: SessionState,
    #[case] expected_json: &str,
) {
    let res = to_string(&state);

    assert!(
        res.is_ok(),
        "Serialization failed: {:?}",
        res.as_ref().err()
    );
    assert_eq!(res.unwrap_or_default(), expected_json);
}

#[test]
fn test_session_state_traits_behavior_success() {
    let state = SessionState::Connected;

    let state_copy = state;

    assert_eq!(state, state_copy);
}

#[rstest]
#[case(StreamDirection::Bidirectional, "\"bidirectional\"")]
#[case(StreamDirection::ReceiveOnly, "\"receive_only\"")]
#[case(StreamDirection::SendOnly, "\"send_only\"")]
fn test_stream_direction_serialization_mapping_success(
    #[case] direction: StreamDirection,
    #[case] expected_json: &str,
) {
    let res = to_string(&direction);

    assert!(
        res.is_ok(),
        "Serialization failed: {:?}",
        res.as_ref().err()
    );
    assert_eq!(res.unwrap_or_default(), expected_json);
}

#[test]
fn test_stream_direction_traits_behavior_success() {
    let direction = StreamDirection::Bidirectional;

    let direction_copy = direction;

    assert_eq!(direction, direction_copy);
}

#[rstest]
#[case(StreamState::Closed, "\"closed\"")]
#[case(StreamState::HalfClosedLocal, "\"half_closed_local\"")]
#[case(StreamState::HalfClosedRemote, "\"half_closed_remote\"")]
#[case(StreamState::Open, "\"open\"")]
#[case(StreamState::ResetReceived, "\"reset_received\"")]
#[case(StreamState::ResetSent, "\"reset_sent\"")]
fn test_stream_state_serialization_mapping_success(
    #[case] state: StreamState,
    #[case] expected_json: &str,
) {
    let res = to_string(&state);

    assert!(
        res.is_ok(),
        "Serialization failed: {:?}",
        res.as_ref().err()
    );
    assert_eq!(res.unwrap_or_default(), expected_json);
}

#[test]
fn test_stream_state_traits_behavior_success() {
    let state = StreamState::ResetSent;

    let state_copy = state;

    assert_eq!(state, state_copy);
}

#[test]
fn test_type_aliases_instantiation_and_usage_success() {
    let conn_id: ConnectionId = "conn-uuid-1".to_owned();
    let err_code: ErrorCode = 404;
    let req_id: RequestId = 1001;
    let sess_id: SessionId = 500;
    let stream_id: StreamId = 8;
    let headers: Headers = vec![(Bytes::from("content-type"), Bytes::from("application/json"))];

    assert_eq!(conn_id, "conn-uuid-1");
    assert_eq!(err_code, 404);
    assert_eq!(req_id, 1001);
    assert_eq!(sess_id, 500);
    assert_eq!(stream_id, 8);
    assert_eq!(headers.len(), 1);
    assert_eq!(
        headers.first().map(|(k, _)| k),
        Some(&Bytes::from("content-type"))
    );
}
