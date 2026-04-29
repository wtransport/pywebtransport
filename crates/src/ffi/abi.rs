//! FFI Application Binary Interface (ABI) definitions and operational codes.

use pyo3::prelude::*;

pub(super) const ABI_VERSION: u8 = 4;
pub(super) const COMMAND_COMPLETED: u8 = 0x00;
pub(super) const COMMAND_FAILED: u8 = 0x01;
pub(super) const CONNECTION_EFFECTS: u8 = 0x02;
pub(super) const CONNECTION_SPAWNED: u8 = 0x03;
pub(super) const REACTOR_SHUTDOWN: u8 = 0x04;
pub(super) const CLEANUP_H3_STREAM: u8 = 0x40;
pub(super) const EMIT_CONNECTION_EVENT: u8 = 0x41;
pub(super) const EMIT_SESSION_EVENT: u8 = 0x42;
pub(super) const EMIT_STREAM_EVENT: u8 = 0x43;
pub(super) const EXPORT_TLS_KEYING_MATERIAL: u8 = 0x44;
pub(super) const NOTIFY_REQUEST_DONE: u8 = 0x45;
pub(super) const NOTIFY_REQUEST_FAILED: u8 = 0x46;
pub(super) const USER_ACCEPT_SESSION: u8 = 0x80;
pub(super) const USER_CLOSE_CONNECTION: u8 = 0x81;
pub(super) const USER_CLOSE_CONNECTION_GRACEFULLY: u8 = 0x82;
pub(super) const USER_CLOSE_SESSION: u8 = 0x83;
pub(super) const USER_CREATE_SESSION: u8 = 0x84;
pub(super) const USER_CREATE_STREAM: u8 = 0x85;
pub(super) const USER_EXPORT_KEYING_MATERIAL: u8 = 0x86;
pub(super) const USER_GET_CONNECTION_DIAGNOSTICS: u8 = 0x87;
pub(super) const USER_GET_SESSION_DIAGNOSTICS: u8 = 0x88;
pub(super) const USER_GET_STREAM_DIAGNOSTICS: u8 = 0x89;
pub(super) const USER_GRANT_DATA_CREDIT: u8 = 0x8A;
pub(super) const USER_GRANT_STREAMS_CREDIT: u8 = 0x8B;
pub(super) const USER_READ_STREAM: u8 = 0x8C;
pub(super) const USER_REJECT_SESSION: u8 = 0x8D;
pub(super) const USER_RESET_STREAM: u8 = 0x8E;
pub(super) const USER_SEND_DATAGRAM: u8 = 0x8F;
pub(super) const USER_SEND_STREAM_DATA: u8 = 0x90;
pub(super) const USER_STOP_SENDING: u8 = 0x91;

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ABI_VERSION", ABI_VERSION)?;
    Ok(())
}
