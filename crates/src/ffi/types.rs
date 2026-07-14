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

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::Closed => "closed",
            Self::Closing => "closing",
            Self::Connected => "connected",
            Self::Connecting => "connecting",
            Self::Idle => "idle",
        };

        Ok(PyString::intern(py, s))
    }
}

impl<'py> IntoPyObject<'py> for EventType {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::ConnectionClosed => "connection_closed",
            Self::ConnectionEstablished => "connection_established",
            Self::DatagramReceived => "datagram_received",
            Self::SessionClosed => "session_closed",
            Self::SessionDataBlocked => "session_data_blocked",
            Self::SessionDraining => "session_draining",
            Self::SessionMaxDataUpdated => "session_max_data_updated",
            Self::SessionMaxStreamsBidiUpdated => "session_max_streams_bidi_updated",
            Self::SessionMaxStreamsUniUpdated => "session_max_streams_uni_updated",
            Self::SessionPending => "session_pending",
            Self::SessionReady => "session_ready",
            Self::SessionRequest => "session_request",
            Self::SessionStreamsBlocked => "session_streams_blocked",
            Self::StopSendingReceived => "stop_sending_received",
            Self::StreamClosed => "stream_closed",
            Self::StreamOpened => "stream_opened",
            Self::StreamResetReceived => "stream_reset_received",
        };

        Ok(PyString::intern(py, s))
    }
}

impl<'py> IntoPyObject<'py> for SessionState {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::Closed => "closed",
            Self::Closing => "closing",
            Self::Connected => "connected",
            Self::Connecting => "connecting",
            Self::Draining => "draining",
        };

        Ok(PyString::intern(py, s))
    }
}

impl<'py> IntoPyObject<'py> for StreamDirection {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::Bidirectional => "bidirectional",
            Self::ReceiveOnly => "receive_only",
            Self::SendOnly => "send_only",
        };

        Ok(PyString::intern(py, s))
    }
}

impl<'py> IntoPyObject<'py> for StreamState {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            Self::Closed => "closed",
            Self::HalfClosedLocal => "half_closed_local",
            Self::HalfClosedRemote => "half_closed_remote",
            Self::Open => "open",
            Self::ResetReceived => "reset_received",
            Self::ResetSent => "reset_sent",
        };

        Ok(PyString::intern(py, s))
    }
}
