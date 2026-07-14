//! Unit tests for the `crate::common::types` module.

use bytes::Bytes;

use super::*;

#[test]
fn test_connection_state_traits_behavior_success() {
    let state = ConnectionState::Connected;

    let state_copy = state;
    let debug_output = format!("{state:?}");

    assert_eq!(state, state_copy);
    assert_eq!(debug_output, "Connected");
}

#[test]
fn test_error_source_traits_behavior_success() {
    let source = ErrorSource::Session;

    let source_copy = source;
    let debug_output = format!("{source:?}");

    assert_eq!(source, source_copy);
    assert_eq!(debug_output, "Session");
}

#[test]
fn test_event_type_traits_behavior_success() {
    let event = EventType::StreamOpened;

    let event_copy = event;
    let debug_output = format!("{event:?}");

    assert_eq!(event, event_copy);
    assert_eq!(debug_output, "StreamOpened");

    let pending_event = EventType::SessionPending;

    let pending_copy = pending_event;
    let pending_debug = format!("{pending_event:?}");

    assert_eq!(pending_event, pending_copy);
    assert_eq!(pending_debug, "SessionPending");
}

#[test]
fn test_session_state_traits_behavior_success() {
    let state = SessionState::Connected;

    let state_copy = state;
    let debug_output = format!("{state:?}");

    assert_eq!(state, state_copy);
    assert_eq!(debug_output, "Connected");
}

#[test]
fn test_stream_direction_traits_behavior_success() {
    let direction = StreamDirection::Bidirectional;

    let direction_copy = direction;
    let debug_output = format!("{direction:?}");

    assert_eq!(direction, direction_copy);
    assert_eq!(debug_output, "Bidirectional");
}

#[test]
fn test_stream_state_traits_behavior_success() {
    let state = StreamState::ResetSent;

    let state_copy = state;
    let debug_output = format!("{state:?}");

    assert_eq!(state, state_copy);
    assert_eq!(debug_output, "ResetSent");
}

#[test]
fn test_type_aliases_instantiation_and_usage_success() {
    let conn_handle: ConnectionHandle = 42;
    let err_code: ErrorCode = 404;
    let req_id: RequestId = 1001;
    let sess_id: SessionId = 500;
    let stream_id: StreamId = 8;
    let headers: Headers = vec![(Bytes::from("content-type"), Bytes::from("application/json"))];

    assert_eq!(conn_handle, 42);
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
