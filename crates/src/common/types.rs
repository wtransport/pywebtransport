//! Common type definitions and state enumerations.

use bytes::Bytes;
use serde::Serialize;

/// Connection diagnostic identifier.
pub type ConnectionId = String;

/// Application-level error code.
pub type ErrorCode = u64;

/// Canonicalized HTTP/3 header byte pairs.
pub type Headers = Vec<(Bytes, Bytes)>;

/// Asynchronous operation correlation identifier.
pub type RequestId = u64;

/// WebTransport session identifier.
pub type SessionId = u64;

/// WebTransport stream identifier.
pub type StreamId = u64;

/// Connection lifecycle states.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    /// Initial idle state.
    Idle,
    /// Connection handshake in progress.
    Connecting,
    /// Connection established and ready.
    Connected,
    /// Connection closure sequence initiated.
    Closing,
    /// Connection draining state.
    Draining,
    /// Connection fully closed.
    Closed,
    /// Connection termination due to failure.
    Failed,
}

/// System event type definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    /// Capsule received on a stream.
    CapsuleReceived,
    /// Connection closed.
    ConnectionClosed,
    /// Connection established.
    ConnectionEstablished,
    /// Connection failed.
    ConnectionFailed,
    /// Connection lost.
    ConnectionLost,
    /// Datagram error detected.
    DatagramError,
    /// Datagram received.
    DatagramReceived,
    /// Datagram sent.
    DatagramSent,
    /// Protocol error detected.
    ProtocolError,
    /// Session blocked by data flow control.
    SessionDataBlocked,
    /// Session closed.
    SessionClosed,
    /// Session entering draining state.
    SessionDraining,
    /// Session max bidirectional streams limit updated.
    SessionMaxStreamsBidiUpdated,
    /// Session max unidirectional streams limit updated.
    SessionMaxStreamsUniUpdated,
    /// Session max data limit updated.
    SessionMaxDataUpdated,
    /// Session ready.
    SessionReady,
    /// New session request.
    SessionRequest,
    /// Session blocked by stream limits.
    SessionStreamsBlocked,
    /// H3 Settings received.
    SettingsReceived,
    /// Stream closed.
    StreamClosed,
    /// Stream data received.
    StreamDataReceived,
    /// Stream logic error.
    StreamError,
    /// Stream opened.
    StreamOpened,
    /// Asynchronous operation timeout.
    TimeoutError,
}

/// WebTransport session lifecycle states.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Session handshake in progress.
    Connecting,
    /// Session established and active.
    Connected,
    /// Session closure initiated.
    Closing,
    /// Session draining state.
    Draining,
    /// Session fully closed.
    Closed,
}

/// Stream data flow direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamDirection {
    /// Bidirectional flow.
    Bidirectional,
    /// Outbound only.
    SendOnly,
    /// Inbound only.
    ReceiveOnly,
}

/// WebTransport stream lifecycle states.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamState {
    /// Stream open and active.
    Open,
    /// Local write side closed.
    HalfClosedLocal,
    /// Remote write side closed.
    HalfClosedRemote,
    /// Reset signal sent.
    ResetSent,
    /// Reset signal received.
    ResetReceived,
    /// Stream fully closed.
    Closed,
}

#[cfg(test)]
mod tests;
