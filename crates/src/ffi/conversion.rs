//! FFI conversion logic for events.

use bytes::Bytes;
use pyo3::IntoPyObjectExt;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use tracing::debug;

use crate::common::types::{ErrorSource, Headers};
use crate::ffi::abi::{
    COMMAND_FAILED, CONNECTION_EFFECTS, CONNECTION_SPAWNED, EMIT_CONNECTION_EVENT,
    EMIT_SESSION_EVENT, EMIT_STREAM_EVENT, NOTIFY_REQUEST_DONE, NOTIFY_REQUEST_FAILED,
    REACTOR_SHUTDOWN, USER_ACCEPT_SESSION, USER_CLOSE_CONNECTION, USER_CLOSE_CONNECTION_GRACEFULLY,
    USER_CLOSE_SESSION, USER_CREATE_SESSION, USER_CREATE_SESSION_OPTIMISTIC, USER_CREATE_STREAM,
    USER_DIAGNOSE_CONNECTION, USER_DIAGNOSE_SESSION, USER_DIAGNOSE_STREAM,
    USER_EXPORT_KEYING_MATERIAL, USER_READ_DATAGRAM, USER_READ_STREAM, USER_REJECT_SESSION,
    USER_RESET_STREAM, USER_SEND_DATAGRAM, USER_STOP_SENDING, USER_WRITE_STREAM,
};
use crate::ffi::error;
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};
use crate::protocol::{ConnectionDiagnostics, SessionDiagnostics, StreamDiagnostics};
use crate::runtime::channel::RuntimeEvent;

impl<'py> IntoPyObject<'py> for Effect {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Effect::EmitConnectionEvent {
                connection_handle,
                event_type,
                error_code,
                reason,
            } => {
                let payload = PyTuple::new(
                    py,
                    &[
                        connection_handle.into_pyobject(py)?.into_any(),
                        event_type.into_pyobject(py)?.into_any(),
                        error_code.into_pyobject(py)?.into_any(),
                        reason.into_pyobject(py)?.into_any(),
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        EMIT_CONNECTION_EVENT.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            Effect::EmitSessionEvent {
                session_id,
                event_type,
                path,
                headers,
                wt_available_protocols,
                wt_protocol,
                data,
                is_unidirectional,
                max_data,
                max_streams,
                ready_at,
                status_code,
                error_code,
                reason,
            } => {
                let headers = match headers {
                    Some(headers) => headers_to_py(py, &headers)?.into_any(),
                    None => py.None().into_bound(py).into_any(),
                };

                let data = match data {
                    Some(data) => PyBytes::new(py, &data).into_any(),
                    None => py.None().into_bound(py).into_any(),
                };

                let payload = PyTuple::new(
                    py,
                    &[
                        session_id.into_pyobject(py)?.into_any(),
                        event_type.into_pyobject(py)?.into_any(),
                        path.into_pyobject(py)?.into_any(),
                        headers,
                        wt_available_protocols.into_pyobject(py)?.into_any(),
                        wt_protocol.into_pyobject(py)?.into_any(),
                        data,
                        is_unidirectional.into_pyobject(py)?.into_any(),
                        max_data.into_pyobject(py)?.into_any(),
                        max_streams.into_pyobject(py)?.into_any(),
                        ready_at.into_pyobject(py)?.into_any(),
                        status_code.into_pyobject(py)?.into_any(),
                        error_code.into_pyobject(py)?.into_any(),
                        reason.into_pyobject(py)?.into_any(),
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        EMIT_SESSION_EVENT.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            Effect::EmitStreamEvent {
                stream_id,
                event_type,
                session_id,
                direction,
                is_peer_initiated,
                error_code,
            } => {
                let payload = PyTuple::new(
                    py,
                    &[
                        stream_id.into_pyobject(py)?.into_any(),
                        event_type.into_pyobject(py)?.into_any(),
                        session_id.into_pyobject(py)?.into_any(),
                        direction.into_pyobject(py)?.into_any(),
                        is_peer_initiated.into_pyobject(py)?.into_any(),
                        error_code.into_pyobject(py)?.into_any(),
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        EMIT_STREAM_EVENT.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            Effect::NotifyRequestDone { request_id, result } => {
                let result = match result {
                    RequestResult::ConnectionDiagnostics(diagnostics) => {
                        connection_diagnostics_to_py(py, &diagnostics)?
                    }
                    RequestResult::ConnectionHandle(handle) => handle.into_pyobject(py)?.into_any(),
                    RequestResult::Datagram(bytes)
                    | RequestResult::KeyingMaterial(bytes)
                    | RequestResult::ReadData(bytes) => PyBytes::new(py, &bytes).into_any(),
                    RequestResult::None => py.None().into_bound(py).into_any(),
                    RequestResult::SessionDiagnostics(diagnostics) => {
                        session_diagnostics_to_py(py, &diagnostics)?
                    }
                    RequestResult::SessionId(session_id) => {
                        session_id.into_pyobject(py)?.into_any()
                    }
                    RequestResult::StreamId(stream_id) => stream_id.into_pyobject(py)?.into_any(),
                    RequestResult::StreamDiagnostics(diagnostics) => {
                        stream_diagnostics_to_py(py, &diagnostics)?
                    }
                };

                let payload =
                    PyTuple::new(py, &[request_id.into_pyobject(py)?.into_any(), result])?;

                Ok(PyTuple::new(
                    py,
                    &[
                        NOTIFY_REQUEST_DONE.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            Effect::NotifyRequestFailed {
                request_id,
                source,
                error_code,
                reason,
            } => {
                let py_exception = error::create_py_exception(py, source, error_code, &reason);
                let payload = PyTuple::new(
                    py,
                    &[
                        request_id.into_pyobject(py)?.into_any(),
                        py_exception.into_bound_py_any(py)?,
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        NOTIFY_REQUEST_FAILED.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            _ => {
                debug!("rt_event convert failed actual={self:?}");
                Err(PyRuntimeError::new_err("rt_event convert failed"))
            }
        }
    }
}

impl<'py> IntoPyObject<'py> for RuntimeEvent {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            RuntimeEvent::CommandFailed {
                request_id,
                error_code,
                reason,
            } => {
                let py_exception =
                    error::create_py_exception(py, ErrorSource::Connection, error_code, &reason);
                let payload = PyTuple::new(
                    py,
                    &[
                        request_id.into_pyobject(py)?.into_any(),
                        py_exception.into_bound_py_any(py)?,
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        COMMAND_FAILED.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            RuntimeEvent::ConnectionEffects { handle, effects } => {
                let payload = PyTuple::new(
                    py,
                    &[
                        handle.0.into_pyobject(py)?.into_any(),
                        effects.into_pyobject(py)?.into_any(),
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        CONNECTION_EFFECTS.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            RuntimeEvent::ConnectionSpawned {
                handle,
                remote_address,
                effects,
            } => {
                let remote_address = PyTuple::new(
                    py,
                    &[
                        remote_address
                            .ip()
                            .to_string()
                            .into_pyobject(py)?
                            .into_any(),
                        remote_address.port().into_pyobject(py)?.into_any(),
                    ],
                )?;
                let payload = PyTuple::new(
                    py,
                    &[
                        handle.0.into_pyobject(py)?.into_any(),
                        remote_address.into_any(),
                        effects.into_pyobject(py)?.into_any(),
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        CONNECTION_SPAWNED.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            RuntimeEvent::ReactorShutdown => {
                let payload = PyTuple::empty(py);

                Ok(PyTuple::new(
                    py,
                    &[
                        REACTOR_SHUTDOWN.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for ProtocolEvent {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let event = obj
            .extract::<Bound<'_, PyTuple>>()
            .map_err(|e| PyValueError::new_err(format!("rt_event convert invalid err={e}")))?;

        let opcode = event.get_item(0)?.extract::<u8>()?;
        let payload = event
            .get_item(1)?
            .extract::<Bound<'_, PyTuple>>()
            .map_err(|e| PyValueError::new_err(format!("rt_event convert invalid err={e}")))?;

        let extract_bytes_at_index = |index: usize| -> PyResult<Bytes> {
            let value = payload.get_item(index)?;
            extract_bytes(&value).map_err(|e| {
                PyValueError::new_err(format!("rt_event convert invalid actual={index} err={e}"))
            })
        };

        match opcode {
            USER_ACCEPT_SESSION => Ok(ProtocolEvent::UserAcceptSession {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                headers: extract_headers(&payload.get_item(2)?)?,
                wt_protocol: payload.get_item(3)?.extract()?,
            }),
            USER_CLOSE_CONNECTION => Ok(ProtocolEvent::UserCloseConnection {
                request_id: payload.get_item(0)?.extract()?,
                error_code: payload.get_item(1)?.extract()?,
                reason: payload
                    .get_item(2)?
                    .extract::<Option<String>>()?
                    .map(Into::into),
            }),
            USER_CLOSE_CONNECTION_GRACEFULLY => Ok(ProtocolEvent::UserCloseConnectionGracefully {
                request_id: payload.get_item(0)?.extract()?,
            }),
            USER_CLOSE_SESSION => Ok(ProtocolEvent::UserCloseSession {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                error_code: payload.get_item(2)?.extract()?,
                reason: payload
                    .get_item(3)?
                    .extract::<Option<String>>()?
                    .map(Into::into),
            }),
            USER_CREATE_SESSION => Ok(ProtocolEvent::UserCreateSession {
                request_id: payload.get_item(0)?.extract()?,
                authority: payload.get_item(1)?.extract()?,
                path: payload.get_item(2)?.extract()?,
                headers: extract_headers(&payload.get_item(3)?)?,
                wt_available_protocols: payload.get_item(4)?.extract()?,
            }),
            USER_CREATE_SESSION_OPTIMISTIC => Ok(ProtocolEvent::UserCreateSessionOptimistic {
                request_id: payload.get_item(0)?.extract()?,
                authority: payload.get_item(1)?.extract()?,
                path: payload.get_item(2)?.extract()?,
                headers: extract_headers(&payload.get_item(3)?)?,
                wt_available_protocols: payload.get_item(4)?.extract()?,
            }),
            USER_CREATE_STREAM => Ok(ProtocolEvent::UserCreateStream {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                is_unidirectional: payload.get_item(2)?.extract()?,
            }),
            USER_DIAGNOSE_CONNECTION => Ok(ProtocolEvent::UserDiagnoseConnection {
                request_id: payload.get_item(0)?.extract()?,
            }),
            USER_DIAGNOSE_SESSION => Ok(ProtocolEvent::UserDiagnoseSession {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
            }),
            USER_DIAGNOSE_STREAM => Ok(ProtocolEvent::UserDiagnoseStream {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
            }),
            USER_EXPORT_KEYING_MATERIAL => Ok(ProtocolEvent::UserExportKeyingMaterial {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                label: payload.get_item(2)?.extract()?,
                context: extract_bytes_at_index(3)?,
                length: payload.get_item(4)?.extract()?,
            }),
            USER_READ_DATAGRAM => Ok(ProtocolEvent::UserReadDatagram {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
            }),
            USER_READ_STREAM => Ok(ProtocolEvent::UserReadStream {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
                max_size: payload
                    .get_item(2)?
                    .extract::<Option<u64>>()?
                    .unwrap_or(u64::MAX),
            }),
            USER_REJECT_SESSION => Ok(ProtocolEvent::UserRejectSession {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                headers: extract_headers(&payload.get_item(2)?)?,
                status_code: payload.get_item(3)?.extract()?,
            }),
            USER_RESET_STREAM => Ok(ProtocolEvent::UserResetStream {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
                error_code: payload.get_item(2)?.extract()?,
            }),
            USER_SEND_DATAGRAM => Ok(ProtocolEvent::UserSendDatagram {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                data: extract_bytes_at_index(2)?,
            }),
            USER_STOP_SENDING => Ok(ProtocolEvent::UserStopSending {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
                error_code: payload.get_item(2)?.extract()?,
            }),
            USER_WRITE_STREAM => Ok(ProtocolEvent::UserWriteStream {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
                data: extract_bytes_at_index(2)?,
                end_stream: payload.get_item(3)?.extract()?,
            }),
            _ => Err(PyValueError::new_err(format!(
                "rt_event convert invalid actual={opcode}"
            ))),
        }
    }
}

fn connection_diagnostics_to_py<'py>(
    py: Python<'py>,
    diagnostics: &ConnectionDiagnostics,
) -> PyResult<Bound<'py, PyAny>> {
    let py_diagnostics = PyDict::new(py);
    py_diagnostics.set_item("close_code", diagnostics.close_code)?;
    py_diagnostics.set_item("close_reason", &diagnostics.close_reason)?;
    py_diagnostics.set_item("closed_at", diagnostics.closed_at)?;
    py_diagnostics.set_item("connected_at", diagnostics.connected_at)?;
    py_diagnostics.set_item("connection_handle", diagnostics.connection_handle)?;
    py_diagnostics.set_item("handshake_completed", diagnostics.handshake_completed)?;
    py_diagnostics.set_item("is_client", diagnostics.is_client)?;
    py_diagnostics.set_item("local_goaway_sent", diagnostics.local_goaway_sent)?;
    py_diagnostics.set_item("peer_goaway_received", diagnostics.peer_goaway_received)?;
    py_diagnostics.set_item("peer_initial_max_data", diagnostics.peer_initial_max_data)?;
    py_diagnostics.set_item(
        "peer_initial_max_streams_bidi",
        diagnostics.peer_initial_max_streams_bidi,
    )?;
    py_diagnostics.set_item(
        "peer_initial_max_streams_uni",
        diagnostics.peer_initial_max_streams_uni,
    )?;
    py_diagnostics.set_item(
        "peer_max_datagram_frame_size",
        diagnostics.peer_max_datagram_frame_size,
    )?;
    py_diagnostics.set_item("peer_settings_received", diagnostics.peer_settings_received)?;
    py_diagnostics.set_item("state", diagnostics.state)?;

    Ok(py_diagnostics.into_any())
}

fn extract_bytes(object: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(buffer) = object.extract::<PyBuffer<u8>>() {
        Ok(Bytes::from(buffer.to_vec(object.py())?))
    } else if let Ok(object) = object.extract::<Bound<'_, PyString>>() {
        Ok(Bytes::copy_from_slice(object.to_str()?.as_bytes()))
    } else {
        Err(PyValueError::new_err("mem_buffer convert invalid"))
    }
}

fn extract_headers(object: &Bound<'_, PyAny>) -> PyResult<Headers> {
    let mut headers = Vec::new();

    if let Ok(object) = object.extract::<Bound<'_, PyDict>>() {
        for (key, value) in object {
            process_header_item(&key, &value, &mut headers)?;
        }
    } else if let Ok(object) = object.extract::<Bound<'_, PyList>>() {
        for item in object {
            let item = item.extract::<Bound<'_, PyTuple>>().map_err(|e| {
                PyValueError::new_err(format!("h3_headers convert invalid err={e}"))
            })?;

            if item.len() != 2 {
                return Err(PyValueError::new_err(format!(
                    "h3_headers convert invalid actual={}",
                    item.len()
                )));
            }

            process_header_item(&item.get_item(0)?, &item.get_item(1)?, &mut headers)?;
        }
    } else {
        return Err(PyValueError::new_err("h3_headers convert invalid"));
    }

    Ok(headers)
}

fn headers_to_py<'py>(py: Python<'py>, headers: &Headers) -> PyResult<Bound<'py, PyList>> {
    let py_headers = PyList::empty(py);

    for (key, value) in headers {
        let key = PyBytes::new(py, key);
        let value = PyBytes::new(py, value);

        py_headers.append(PyTuple::new(py, &[key, value])?)?;
    }

    Ok(py_headers)
}

fn process_header_item(
    key: &Bound<'_, PyAny>,
    value: &Bound<'_, PyAny>,
    headers: &mut Headers,
) -> PyResult<()> {
    let key = extract_bytes(key)?;
    let value = extract_bytes(value)?;

    let key = if key.iter().any(u8::is_ascii_uppercase) {
        Bytes::from(key.to_ascii_lowercase())
    } else {
        key
    };

    headers.push((key, value));

    Ok(())
}

fn session_diagnostics_to_py<'py>(
    py: Python<'py>,
    diagnostics: &SessionDiagnostics,
) -> PyResult<Bound<'py, PyAny>> {
    let py_diagnostics = PyDict::new(py);

    py_diagnostics.set_item("close_code", diagnostics.close_code)?;
    py_diagnostics.set_item("close_reason", &diagnostics.close_reason)?;
    py_diagnostics.set_item("closed_at", diagnostics.closed_at)?;
    py_diagnostics.set_item("created_at", diagnostics.created_at)?;
    py_diagnostics.set_item("data_consumed", diagnostics.data_consumed)?;
    py_diagnostics.set_item("data_received", diagnostics.data_received)?;
    py_diagnostics.set_item("data_sent", diagnostics.data_sent)?;
    py_diagnostics.set_item(
        "flow_control_negotiated",
        diagnostics.flow_control_negotiated,
    )?;
    py_diagnostics.set_item("headers", headers_to_py(py, &diagnostics.headers)?)?;
    py_diagnostics.set_item("is_client", diagnostics.is_client)?;
    py_diagnostics.set_item("local_max_data", diagnostics.local_max_data)?;
    py_diagnostics.set_item("local_max_streams_bidi", diagnostics.local_max_streams_bidi)?;
    py_diagnostics.set_item("local_max_streams_uni", diagnostics.local_max_streams_uni)?;
    py_diagnostics.set_item(
        "local_streams_bidi_opened",
        diagnostics.local_streams_bidi_opened,
    )?;
    py_diagnostics.set_item(
        "local_streams_uni_opened",
        diagnostics.local_streams_uni_opened,
    )?;
    py_diagnostics.set_item("path", &diagnostics.path)?;
    py_diagnostics.set_item("peer_max_data", diagnostics.peer_max_data)?;
    py_diagnostics.set_item("peer_max_streams_bidi", diagnostics.peer_max_streams_bidi)?;
    py_diagnostics.set_item("peer_max_streams_uni", diagnostics.peer_max_streams_uni)?;
    py_diagnostics.set_item(
        "peer_streams_bidi_closed",
        diagnostics.peer_streams_bidi_closed,
    )?;
    py_diagnostics.set_item(
        "peer_streams_bidi_opened",
        diagnostics.peer_streams_bidi_opened,
    )?;
    py_diagnostics.set_item(
        "peer_streams_uni_closed",
        diagnostics.peer_streams_uni_closed,
    )?;
    py_diagnostics.set_item(
        "peer_streams_uni_opened",
        diagnostics.peer_streams_uni_opened,
    )?;
    py_diagnostics.set_item("ready_at", diagnostics.ready_at)?;
    py_diagnostics.set_item("session_id", diagnostics.session_id)?;
    py_diagnostics.set_item("state", diagnostics.state)?;
    py_diagnostics.set_item(
        "wt_available_protocols",
        &diagnostics.wt_available_protocols,
    )?;
    py_diagnostics.set_item("wt_protocol", &diagnostics.wt_protocol)?;

    Ok(py_diagnostics.into_any())
}

fn stream_diagnostics_to_py<'py>(
    py: Python<'py>,
    diagnostics: &StreamDiagnostics,
) -> PyResult<Bound<'py, PyAny>> {
    let py_diagnostics = PyDict::new(py);
    py_diagnostics.set_item("created_at", diagnostics.created_at)?;
    py_diagnostics.set_item("direction", diagnostics.direction)?;
    py_diagnostics.set_item("is_peer_initiated", diagnostics.is_peer_initiated)?;
    py_diagnostics.set_item("receive_error_code", diagnostics.receive_error_code)?;
    py_diagnostics.set_item("receive_state", diagnostics.receive_state)?;
    py_diagnostics.set_item("send_error_code", diagnostics.send_error_code)?;
    py_diagnostics.set_item("send_state", diagnostics.send_state)?;
    py_diagnostics.set_item("session_id", diagnostics.session_id)?;
    py_diagnostics.set_item("stream_id", diagnostics.stream_id)?;

    Ok(py_diagnostics.into_any())
}
