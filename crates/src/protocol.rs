//! Protocol module declarations.

pub(crate) use crate::protocol::connection::ConnectionDiagnostics;
pub(crate) use crate::protocol::session::SessionDiagnostics;
pub(crate) use crate::protocol::stream::StreamDiagnostics;

pub(crate) mod engine;
pub(crate) mod events;
pub(crate) mod utils;

mod connection;
mod h3;
mod qpack;
mod session;
mod stream;
