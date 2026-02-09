//! WebTransport logic error definitions.

use thiserror::Error;

use crate::common::types::{ErrorCode, ErrorSource};

/// Enumeration of WebTransport protocol errors.
#[derive(Debug, Error)]
pub enum WebTransportError {
    /// Violation of protocol specifications.
    #[error("Protocol error: {1} (code: {0:?})")]
    Protocol(Option<ErrorCode>, String),

    /// Uncategorized internal implementation failure.
    #[error("Unknown error: {1} (code: {0:?})")]
    Unknown(Option<ErrorCode>, String),
}

impl WebTransportError {
    // Returns the architectural source of the error.
    pub(crate) fn source(&self) -> ErrorSource {
        match self {
            Self::Protocol(_, _) => ErrorSource::Protocol,
            Self::Unknown(_, _) => ErrorSource::Unspecified,
        }
    }
}

#[cfg(test)]
mod tests;
