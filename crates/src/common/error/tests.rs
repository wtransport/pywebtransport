//! Unit tests for the `crate::common::error` module.

use super::*;

#[test]
fn test_configuration_error_formatting() {
    let error = WebTransportError::Configuration(Some(400), "invalid setting".to_owned());

    assert_eq!(
        error.to_string(),
        "Configuration error: invalid setting (code: Some(400))"
    );
}

#[test]
fn test_connection_error_formatting() {
    let error = WebTransportError::Connection(None, "connection reset".to_owned());

    assert_eq!(
        error.to_string(),
        "Connection error: connection reset (code: None)"
    );
}

#[test]
fn test_debug_formatting() {
    let error = WebTransportError::Protocol(None, "debug check".to_owned());
    let debug_str = format!("{error:?}");

    assert!(debug_str.contains("Protocol"));
    assert!(debug_str.contains("debug check"));
}

#[test]
fn test_protocol_error_formatting() {
    let error = WebTransportError::Protocol(Some(1), "frame error".to_owned());

    assert_eq!(
        error.to_string(),
        "Protocol error: frame error (code: Some(1))"
    );
}

#[test]
fn test_unknown_error_formatting() {
    let error = WebTransportError::Unknown(Some(999), "mystery".to_owned());

    assert_eq!(
        error.to_string(),
        "Unknown error: mystery (code: Some(999))"
    );
}
