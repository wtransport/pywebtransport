//! WebTransport logic error definitions.

use std::io;

use thiserror::Error;

use crate::common::types::{ErrorCode, SessionId, StreamId};

/// Enumeration of WebTransport protocol errors.
#[derive(Debug, Error)]
pub enum WebTransportError {
    /// Cryptographic authentication or credential validation failure.
    #[error("Authentication failed: {1} (code: {0:?})")]
    Authentication(Option<ErrorCode>, String),

    /// Transport connection termination or state failure.
    #[error("Connection error: {1} (code: {0:?})")]
    Connection(Option<ErrorCode>, String),

    /// Datagram payload or transmission constraint violation.
    #[error("Datagram error: {1} (code: {0:?})")]
    Datagram(Option<ErrorCode>, String),

    /// Transport flow control limit violation.
    #[error("Flow control error: {1} (code: {0:?})")]
    FlowControl(Option<ErrorCode>, String),

    /// HTTP/3 protocol violation.
    #[error("HTTP/3 protocol error: {1} (code: {0:?})")]
    H3(Option<ErrorCode>, String),

    /// Underlying input/output operation failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// QUIC protocol violation.
    #[error("QUIC protocol error: {1} (code: {0:?})")]
    Quic(Option<ErrorCode>, String),

    /// WebTransport session lifecycle error.
    #[error("Session {0} error: {2} (code: {1:?})")]
    Session(SessionId, Option<ErrorCode>, String),

    /// Data stream lifecycle error.
    #[error("Stream {0} error: {2} (code: {1:?})")]
    Stream(StreamId, Option<ErrorCode>, String),

    /// Asynchronous operation timeout.
    #[error("Timeout: {1} (code: {0:?})")]
    Timeout(Option<ErrorCode>, String),

    /// Unclassified internal system error.
    #[error("Unknown error: {1} (code: {0:?})")]
    Unknown(Option<ErrorCode>, String),
}

#[cfg(test)]
mod tests;
