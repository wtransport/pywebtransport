//! FFI conversion logic for common types and enumerations.

use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::common::types::{
    ConnectionState, EventType, SessionState, StreamDirection, StreamState,
};

impl<'py> IntoPyObject<'py> for ConnectionState {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    // ConnectionState to Python string conversion.
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::Idle => "idle",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Closing => "closing",
            Self::Draining => "draining",
            Self::Closed => "closed",
            Self::Failed => "failed",
        };

        Ok(PyString::new(py, s))
    }
}

impl<'py> IntoPyObject<'py> for EventType {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    // EventType to Python string conversion.
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::CapsuleReceived => "capsule_received",
            Self::ConnectionClosed => "connection_closed",
            Self::ConnectionEstablished => "connection_established",
            Self::ConnectionFailed => "connection_failed",
            Self::ConnectionLost => "connection_lost",
            Self::DatagramError => "datagram_error",
            Self::DatagramReceived => "datagram_received",
            Self::DatagramSent => "datagram_sent",
            Self::ProtocolError => "protocol_error",
            Self::SessionClosed => "session_closed",
            Self::SessionDataBlocked => "session_data_blocked",
            Self::SessionDraining => "session_draining",
            Self::SessionMaxDataUpdated => "session_max_data_updated",
            Self::SessionMaxStreamsBidiUpdated => "session_max_streams_bidi_updated",
            Self::SessionMaxStreamsUniUpdated => "session_max_streams_uni_updated",
            Self::SessionReady => "session_ready",
            Self::SessionRequest => "session_request",
            Self::SessionStreamsBlocked => "session_streams_blocked",
            Self::SettingsReceived => "settings_received",
            Self::StreamClosed => "stream_closed",
            Self::StreamDataReceived => "stream_data_received",
            Self::StreamError => "stream_error",
            Self::StreamOpened => "stream_opened",
            Self::TimeoutError => "timeout_error",
        };

        Ok(PyString::new(py, s))
    }
}

impl<'py> IntoPyObject<'py> for SessionState {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    // SessionState to Python string conversion.
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Closing => "closing",
            Self::Draining => "draining",
            Self::Closed => "closed",
        };

        Ok(PyString::new(py, s))
    }
}

impl<'py> IntoPyObject<'py> for StreamDirection {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    // StreamDirection to Python string conversion.
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::Bidirectional => "bidirectional",
            Self::SendOnly => "send_only",
            Self::ReceiveOnly => "receive_only",
        };

        Ok(PyString::new(py, s))
    }
}

impl<'py> IntoPyObject<'py> for StreamState {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    // StreamState to Python string conversion.
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::Open => "open",
            Self::HalfClosedLocal => "half_closed_local",
            Self::HalfClosedRemote => "half_closed_remote",
            Self::ResetSent => "reset_sent",
            Self::ResetReceived => "reset_received",
            Self::Closed => "closed",
        };

        Ok(PyString::new(py, s))
    }
}
