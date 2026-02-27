//! WebTransport logic error definitions.

use thiserror::Error;

use crate::common::types::ErrorCode;

/// Enumeration of WebTransport protocol errors.
#[derive(Debug, Error)]
pub enum WebTransportError {
    /// Local configuration validation failure.
    #[error("Configuration error: {1} (code: {0:?})")]
    Configuration(Option<ErrorCode>, String),

    /// Connection establishment and state failure.
    #[error("Connection error: {1} (code: {0:?})")]
    Connection(Option<ErrorCode>, String),

    /// Violation of protocol specifications.
    #[error("Protocol error: {1} (code: {0:?})")]
    Protocol(Option<ErrorCode>, String),

    /// Uncategorized internal implementation failure.
    #[error("Unknown error: {1} (code: {0:?})")]
    Unknown(Option<ErrorCode>, String),
}

#[cfg(test)]
mod tests;
