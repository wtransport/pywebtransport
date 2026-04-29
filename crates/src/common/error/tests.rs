//! Unit tests for the `crate::common::error` module.

use std::borrow::Cow;

use super::*;

#[test]
fn test_configuration_error_formatting() {
    let error = WebTransportError::Configuration(Some(400), Cow::Borrowed("invalid setting"));

    assert_eq!(
        error.to_string(),
        "Configuration error: invalid setting (code: Some(400))"
    );
}

#[test]
fn test_connection_error_formatting() {
    let error = WebTransportError::Connection(None, Cow::Owned("connection reset".to_owned()));

    assert_eq!(
        error.to_string(),
        "Connection error: connection reset (code: None)"
    );
}

#[test]
fn test_debug_formatting() {
    let error = WebTransportError::Protocol(None, Cow::Borrowed("debug check"));
    let debug_str = format!("{error:?}");

    assert!(debug_str.contains("Protocol"));
    assert!(debug_str.contains("debug check"));
}

#[test]
fn test_protocol_error_formatting() {
    let error = WebTransportError::Protocol(Some(1), Cow::Borrowed("frame error"));

    assert_eq!(
        error.to_string(),
        "Protocol error: frame error (code: Some(1))"
    );
}

#[test]
fn test_stream_error_formatting() {
    let error = WebTransportError::Stream(12, Some(256), Cow::Borrowed("invalid stream data"));

    assert_eq!(
        error.to_string(),
        "Stream error: invalid stream data (stream: 12, code: Some(256))"
    );
}

#[test]
fn test_unknown_error_formatting() {
    let error = WebTransportError::Unknown(Some(999), Cow::Owned("mystery".to_owned()));

    assert_eq!(
        error.to_string(),
        "Unknown error: mystery (code: Some(999))"
    );
}
