//! Common type definitions and state enumerations.

use bytes::Bytes;

// WebTransport connection handle.
pub(crate) type ConnectionHandle = u64;
// Protocol error code.
pub(crate) type ErrorCode = u64;
// Canonicalized HTTP/3 header byte pairs.
pub(crate) type Headers = Vec<(Bytes, Bytes)>;
// Asynchronous operation correlation identifier.
pub(crate) type RequestId = u64;
// WebTransport session identifier.
pub(crate) type SessionId = u64;
// WebTransport stream identifier.
pub(crate) type StreamId = u64;

// Connection lifecycle states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionState {
    Closed,
    Closing,
    Connected,
    Connecting,
    Idle,
}

// Architectural source context of an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorSource {
    Connection,
    Datagram,
    Session,
    Stream,
    Unspecified,
}

// System event type definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EventType {
    ConnectionClosed,
    ConnectionEstablished,
    DatagramReceived,
    SessionClosed,
    SessionDataBlocked,
    SessionDraining,
    SessionMaxDataUpdated,
    SessionMaxStreamsBidiUpdated,
    SessionMaxStreamsUniUpdated,
    SessionPending,
    SessionReady,
    SessionRequest,
    SessionStreamsBlocked,
    StopSendingReceived,
    StreamClosed,
    StreamOpened,
    StreamResetReceived,
}

// WebTransport session lifecycle states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionState {
    Closed,
    Closing,
    Connected,
    Connecting,
    Draining,
}

// Stream data flow direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamDirection {
    Bidirectional,
    ReceiveOnly,
    SendOnly,
}

// WebTransport stream lifecycle states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamState {
    Closed,
    HalfClosedLocal,
    HalfClosedRemote,
    Open,
    ResetReceived,
    ResetSent,
}

#[cfg(test)]
mod tests;
