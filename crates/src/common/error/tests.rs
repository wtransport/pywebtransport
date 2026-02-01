//! Unit tests for the `crate::common::error` module.

use std::io;

use rstest::*;

use super::*;

#[rstest]
#[case(None, "auth failed", "Authentication failed: auth failed (code: None)")]
#[case(
    Some(100),
    "bad creds",
    "Authentication failed: bad creds (code: Some(100))"
)]
fn test_authentication_error_formatting_success(
    #[case] code: Option<u64>,
    #[case] msg: String,
    #[case] expected: String,
) {
    let error = WebTransportError::Authentication(code, msg);

    assert_eq!(error.to_string(), expected);
}

#[rstest]
#[case(None, "conn lost", "Connection error: conn lost (code: None)")]
#[case(Some(200), "timeout", "Connection error: timeout (code: Some(200))")]
fn test_connection_error_formatting_success(
    #[case] code: Option<u64>,
    #[case] msg: String,
    #[case] expected: String,
) {
    let error = WebTransportError::Connection(code, msg);

    assert_eq!(error.to_string(), expected);
}

#[test]
fn test_datagram_error_formatting_success() {
    let error = WebTransportError::Datagram(None, "too large".to_owned());

    assert_eq!(error.to_string(), "Datagram error: too large (code: None)");
}

#[test]
fn test_error_debug_trait_implementation_success() {
    let error = WebTransportError::Datagram(None, "debug check".to_owned());

    let debug_str = format!("{error:?}");

    assert!(debug_str.contains("Datagram"));
    assert!(debug_str.contains("debug check"));
}

#[test]
fn test_flow_control_error_formatting_success() {
    let error = WebTransportError::FlowControl(Some(42), "blocked".to_owned());

    assert_eq!(
        error.to_string(),
        "Flow control error: blocked (code: Some(42))"
    );
}

#[test]
fn test_h3_protocol_error_formatting_success() {
    let error = WebTransportError::H3(Some(1), "frame error".to_owned());

    assert_eq!(
        error.to_string(),
        "HTTP/3 protocol error: frame error (code: Some(1))"
    );
}

#[test]
fn test_io_error_conversion_preserves_message_success() {
    let inner_error = io::Error::new(io::ErrorKind::ConnectionReset, "connection reset by peer");

    let error = WebTransportError::from(inner_error);

    assert!(matches!(error, WebTransportError::Io(_)));
    assert_eq!(error.to_string(), "I/O error: connection reset by peer");
}

#[test]
fn test_quic_protocol_error_formatting_success() {
    let error = WebTransportError::Quic(None, "handshake failed".to_owned());

    assert_eq!(
        error.to_string(),
        "QUIC protocol error: handshake failed (code: None)"
    );
}

#[test]
fn test_session_error_formatting_with_id_success() {
    let session_id = 123;
    let error_code = 404;

    let error = WebTransportError::Session(
        session_id,
        Some(error_code),
        "session terminated".to_owned(),
    );

    assert_eq!(
        error.to_string(),
        "Session 123 error: session terminated (code: Some(404))"
    );
}

#[test]
fn test_stream_error_formatting_with_id_success() {
    let stream_id = 5;

    let error = WebTransportError::Stream(stream_id, None, "stream reset".to_owned());

    assert_eq!(
        error.to_string(),
        "Stream 5 error: stream reset (code: None)"
    );
}

#[test]
fn test_timeout_error_formatting_success() {
    let error = WebTransportError::Timeout(None, "operation timed out".to_owned());

    assert_eq!(
        error.to_string(),
        "Timeout: operation timed out (code: None)"
    );
}

#[test]
fn test_unknown_error_formatting_success() {
    let error = WebTransportError::Unknown(Some(999), "mystery".to_owned());

    assert_eq!(
        error.to_string(),
        "Unknown error: mystery (code: Some(999))"
    );
}
