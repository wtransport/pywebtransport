//! Common types.

use bytes::Bytes;

pub(crate) type ConnectionHandle = u64;
pub(crate) type ErrorCode = u64;
pub(crate) type Headers = Vec<(Bytes, Bytes)>;
pub(crate) type RequestId = u64;
pub(crate) type SessionId = u64;
pub(crate) type StreamId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionState {
    Closed,
    Closing,
    Connected,
    Connecting,
    Idle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorSource {
    Connection,
    Datagram,
    Session,
    Stream,
    Unspecified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EventType {
    ConnectionClosed,
    ConnectionEstablished,
    SessionClosed,
    SessionDataBlocked,
    SessionDraining,
    SessionMaxDataUpdated,
    SessionMaxStreamsBidiUpdated,
    SessionMaxStreamsUniUpdated,
    SessionPending,
    SessionReady,
    SessionRequestReceived,
    SessionStreamsBlocked,
    StreamClosed,
    StreamOpened,
    StreamResetReceived,
    StreamStopSendingReceived,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionState {
    Closed,
    Closing,
    Connected,
    Connecting,
    Draining,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamDirection {
    Bidirectional,
    ReceiveOnly,
    SendOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamReceiveState {
    DataRead,
    DataReceived,
    Receive,
    ResetRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamSendState {
    DataReceived,
    DataSent,
    Ready,
    ResetSent,
}

#[cfg(test)]
mod tests;
