//! WebTransport logic error definitions.

use std::borrow::Cow;
use thiserror::Error;

use crate::common::types::{ErrorCode, StreamId};

// Enumeration of WebTransport protocol errors.
#[derive(Debug, Error)]
pub(crate) enum WebTransportError {
    #[error("Configuration error: {1} (code: {0:?})")]
    Configuration(Option<ErrorCode>, Cow<'static, str>),
    #[error("Connection error: {1} (code: {0:?})")]
    Connection(Option<ErrorCode>, Cow<'static, str>),
    #[error("Protocol error: {1} (code: {0:?})")]
    Protocol(Option<ErrorCode>, Cow<'static, str>),
    #[error("Stream error: {2} (stream: {0}, code: {1:?})")]
    Stream(StreamId, Option<ErrorCode>, Cow<'static, str>),
    #[error("Unknown error: {1} (code: {0:?})")]
    Unknown(Option<ErrorCode>, Cow<'static, str>),
}

#[cfg(test)]
mod tests;
