//! FFI conversion logic between Python objects and Rust protocol types.

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::{Borrowed, Bound, IntoPyObjectExt};
use tracing::debug;

use crate::common::types::Headers;
use crate::ffi::abi;
use crate::ffi::error::create_py_exception;
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};
use crate::protocol::{ConnectionDiagnostics, SessionDiagnostics, StreamDiagnostics};

impl<'py> IntoPyObject<'py> for Effect {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Effect::CleanupH3Stream { stream_id } => {
                let payload = PyTuple::new(py, &[stream_id.into_pyobject(py)?.into_any()])?;

                Ok(PyTuple::new(
                    py,
                    &[
                        abi::CLEANUP_H3_STREAM.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
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
                        abi::EMIT_CONNECTION_EVENT.into_pyobject(py)?.into_any(),
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
                error_code,
                reason,
            } => {
                let headers = match headers {
                    Some(h) => headers_to_py(py, &h)?.into_any(),
                    None => py.None().into_bound(py).into_any(),
                };

                let data = match data {
                    Some(d) => PyBytes::new(py, &d).into_any(),
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
                        error_code.into_pyobject(py)?.into_any(),
                        reason.into_pyobject(py)?.into_any(),
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        abi::EMIT_SESSION_EVENT.into_pyobject(py)?.into_any(),
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
                        abi::EMIT_STREAM_EVENT.into_pyobject(py)?.into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            Effect::ExportTlsKeyingMaterial {
                request_id,
                label,
                context,
                length,
            } => {
                let context = PyBytes::new(py, &context).into_any();

                let payload = PyTuple::new(
                    py,
                    &[
                        request_id.into_pyobject(py)?.into_any(),
                        label.into_pyobject(py)?.into_any(),
                        context,
                        length.into_pyobject(py)?.into_any(),
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        abi::EXPORT_TLS_KEYING_MATERIAL
                            .into_pyobject(py)?
                            .into_any(),
                        payload.into_any(),
                    ],
                )?
                .into_any())
            }
            Effect::NotifyRequestDone { request_id, result } => {
                let result = match result {
                    RequestResult::ConnectionDiagnostics(diag) => {
                        connection_diagnostics_to_py(py, &diag)?
                    }
                    RequestResult::KeyingMaterial(bytes) | RequestResult::ReadData(bytes) => {
                        PyBytes::new(py, &bytes).into_any()
                    }
                    RequestResult::None => py.None().into_bound(py).into_any(),
                    RequestResult::SessionDiagnostics(diag) => {
                        session_diagnostics_to_py(py, &diag)?
                    }
                    RequestResult::SessionId(sid) | RequestResult::StreamId(sid) => {
                        sid.into_pyobject(py)?.into_any()
                    }
                    RequestResult::StreamDiagnostics(diag) => stream_diagnostics_to_py(py, &diag)?,
                };

                let payload =
                    PyTuple::new(py, &[request_id.into_pyobject(py)?.into_any(), result])?;

                Ok(PyTuple::new(
                    py,
                    &[
                        abi::NOTIFY_REQUEST_DONE.into_pyobject(py)?.into_any(),
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
                let py_exc = create_py_exception(py, source, error_code, &reason);
                let payload = PyTuple::new(
                    py,
                    &[
                        request_id.into_pyobject(py)?.into_any(),
                        py_exc.into_bound_py_any(py)?,
                    ],
                )?;

                Ok(PyTuple::new(
                    py,
                    &[
                        abi::NOTIFY_REQUEST_FAILED.into_pyobject(py)?.into_any(),
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

impl<'a, 'py> FromPyObject<'a, 'py> for ProtocolEvent {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let bound = ob.as_borrowed();
        let tuple = bound
            .extract::<Bound<'_, PyTuple>>()
            .map_err(|e| PyValueError::new_err(format!("rt_event convert invalid err={e}")))?;

        let opcode = tuple.get_item(0)?.extract::<u8>()?;
        let payload = tuple
            .get_item(1)?
            .extract::<Bound<'_, PyTuple>>()
            .map_err(|e| PyValueError::new_err(format!("rt_event convert invalid err={e}")))?;

        let extract_bytes_at_index = |idx: usize| -> PyResult<Bytes> {
            let val = payload.get_item(idx)?;
            extract_bytes(&val).map_err(|e| {
                PyValueError::new_err(format!("rt_event convert invalid actual={idx} err={e}"))
            })
        };

        match opcode {
            abi::USER_ACCEPT_SESSION => Ok(ProtocolEvent::UserAcceptSession {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                wt_protocol: payload.get_item(2)?.extract()?,
            }),
            abi::USER_CLOSE_CONNECTION => Ok(ProtocolEvent::UserCloseConnection {
                request_id: payload.get_item(0)?.extract()?,
                error_code: payload.get_item(1)?.extract()?,
                reason: payload
                    .get_item(2)?
                    .extract::<Option<String>>()?
                    .map(Into::into),
            }),
            abi::USER_CLOSE_CONNECTION_GRACEFULLY => {
                Ok(ProtocolEvent::UserCloseConnectionGracefully {
                    request_id: payload.get_item(0)?.extract()?,
                })
            }
            abi::USER_CLOSE_SESSION => Ok(ProtocolEvent::UserCloseSession {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                error_code: payload.get_item(2)?.extract()?,
                reason: payload
                    .get_item(3)?
                    .extract::<Option<String>>()?
                    .map(Into::into),
            }),
            abi::USER_CREATE_SESSION => Ok(ProtocolEvent::UserCreateSession {
                request_id: payload.get_item(0)?.extract()?,
                authority: payload.get_item(1)?.extract()?,
                path: payload.get_item(2)?.extract()?,
                headers: extract_headers(&payload.get_item(3)?)?,
                wt_available_protocols: payload.get_item(4)?.extract()?,
            }),
            abi::USER_CREATE_SESSION_OPTIMISTIC => Ok(ProtocolEvent::UserCreateSessionOptimistic {
                request_id: payload.get_item(0)?.extract()?,
                authority: payload.get_item(1)?.extract()?,
                path: payload.get_item(2)?.extract()?,
                headers: extract_headers(&payload.get_item(3)?)?,
                wt_available_protocols: payload.get_item(4)?.extract()?,
            }),
            abi::USER_CREATE_STREAM => Ok(ProtocolEvent::UserCreateStream {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                is_unidirectional: payload.get_item(2)?.extract()?,
            }),
            abi::USER_EXPORT_KEYING_MATERIAL => Ok(ProtocolEvent::UserExportKeyingMaterial {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                label: payload.get_item(2)?.extract()?,
                context: extract_bytes_at_index(3)?,
                length: payload.get_item(4)?.extract()?,
            }),
            abi::USER_GET_CONNECTION_DIAGNOSTICS => {
                Ok(ProtocolEvent::UserGetConnectionDiagnostics {
                    request_id: payload.get_item(0)?.extract()?,
                })
            }
            abi::USER_GET_SESSION_DIAGNOSTICS => Ok(ProtocolEvent::UserGetSessionDiagnostics {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
            }),
            abi::USER_GET_STREAM_DIAGNOSTICS => Ok(ProtocolEvent::UserGetStreamDiagnostics {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
            }),
            abi::USER_READ_STREAM => Ok(ProtocolEvent::UserReadStream {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
                max_bytes: payload
                    .get_item(2)?
                    .extract::<Option<u64>>()?
                    .unwrap_or(u64::MAX),
            }),
            abi::USER_REJECT_SESSION => Ok(ProtocolEvent::UserRejectSession {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                status_code: payload.get_item(2)?.extract()?,
            }),
            abi::USER_RESET_STREAM => Ok(ProtocolEvent::UserResetStream {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
                error_code: payload.get_item(2)?.extract()?,
            }),
            abi::USER_SEND_DATAGRAM => Ok(ProtocolEvent::UserSendDatagram {
                request_id: payload.get_item(0)?.extract()?,
                session_id: payload.get_item(1)?.extract()?,
                data: extract_bytes_at_index(2)?,
            }),
            abi::USER_SEND_STREAM_DATA => Ok(ProtocolEvent::UserSendStreamData {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
                data: extract_bytes_at_index(2)?,
                end_stream: payload.get_item(3)?.extract()?,
            }),
            abi::USER_STOP_SENDING => Ok(ProtocolEvent::UserStopSending {
                request_id: payload.get_item(0)?.extract()?,
                stream_id: payload.get_item(1)?.extract()?,
                error_code: payload.get_item(2)?.extract()?,
            }),
            _ => Err(PyValueError::new_err(format!(
                "rt_event convert invalid actual={opcode}"
            ))),
        }
    }
}

// Converts ConnectionDiagnostics into a Python dictionary.
fn connection_diagnostics_to_py<'py>(
    py: Python<'py>,
    diag: &ConnectionDiagnostics,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("close_code", diag.close_code)?;
    dict.set_item("close_reason", &diag.close_reason)?;
    dict.set_item("closed_at", diag.closed_at)?;
    dict.set_item("connected_at", diag.connected_at)?;
    dict.set_item("connection_handle", diag.connection_handle)?;
    dict.set_item("early_event_count", diag.early_event_count)?;
    dict.set_item("handshake_complete", diag.handshake_complete)?;
    dict.set_item("is_client", diag.is_client)?;
    dict.set_item("local_goaway_sent", diag.local_goaway_sent)?;
    dict.set_item("peer_goaway_received", diag.peer_goaway_received)?;
    dict.set_item("peer_initial_max_data", diag.peer_initial_max_data)?;
    dict.set_item(
        "peer_initial_max_streams_bidi",
        diag.peer_initial_max_streams_bidi,
    )?;
    dict.set_item(
        "peer_initial_max_streams_uni",
        diag.peer_initial_max_streams_uni,
    )?;
    dict.set_item(
        "peer_max_datagram_frame_size",
        diag.peer_max_datagram_frame_size,
    )?;
    dict.set_item("peer_settings_received", diag.peer_settings_received)?;
    dict.set_item("pending_request_count", diag.pending_request_count)?;
    dict.set_item("session_count", diag.session_count)?;
    dict.set_item("state", diag.state)?;
    dict.set_item("stream_count", diag.stream_count)?;

    Ok(dict.into_any())
}

// Bytes extraction from Python object using buffer protocol or UTF-8 encoding.
fn extract_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(buffer) = obj.extract::<PyBuffer<u8>>() {
        Ok(Bytes::from(buffer.to_vec(obj.py())?))
    } else if let Ok(s) = obj.extract::<Bound<'_, PyString>>() {
        Ok(Bytes::copy_from_slice(s.to_str()?.as_bytes()))
    } else {
        Err(PyValueError::new_err("mem_buffer convert invalid"))
    }
}

// HTTP/3 header extraction from Python dictionary or list.
fn extract_headers(obj: &Bound<'_, PyAny>) -> PyResult<Headers> {
    let mut headers = Vec::new();

    if let Ok(dict) = obj.extract::<Bound<'_, PyDict>>() {
        for (k, v) in dict {
            process_header_item(&k, &v, &mut headers)?;
        }
    } else if let Ok(list) = obj.extract::<Bound<'_, PyList>>() {
        for item in list {
            let tuple = item.extract::<Bound<'_, PyTuple>>().map_err(|e| {
                PyValueError::new_err(format!("h3_headers convert invalid err={e}"))
            })?;

            if tuple.len() != 2 {
                return Err(PyValueError::new_err(format!(
                    "h3_headers convert invalid actual={}",
                    tuple.len()
                )));
            }

            process_header_item(&tuple.get_item(0)?, &tuple.get_item(1)?, &mut headers)?;
        }
    } else {
        return Err(PyValueError::new_err("h3_headers convert invalid"));
    }

    Ok(headers)
}

// Converts a Rust Headers vector into a Python list of tuples.
fn headers_to_py<'py>(py: Python<'py>, headers: &Headers) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);

    for (k, v) in headers {
        let key = PyBytes::new(py, k);
        let val = PyBytes::new(py, v);
        let tuple = PyTuple::new(py, &[key, val])?;

        list.append(tuple)?;
    }

    Ok(list)
}

// Parses a single Python header key-value pair and appends it to the accumulator.
fn process_header_item(
    key: &Bound<'_, PyAny>,
    value: &Bound<'_, PyAny>,
    acc: &mut Headers,
) -> PyResult<()> {
    let key_bytes = extract_bytes(key)?;
    let val_bytes = extract_bytes(value)?;

    let key_lower = if key_bytes.iter().any(u8::is_ascii_uppercase) {
        Bytes::from(key_bytes.to_ascii_lowercase())
    } else {
        key_bytes
    };

    acc.push((key_lower, val_bytes));

    Ok(())
}

// Converts SessionDiagnostics into a Python dictionary.
fn session_diagnostics_to_py<'py>(
    py: Python<'py>,
    diag: &SessionDiagnostics,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new(py);

    let mut active_streams_vec: Vec<_> = diag.active_streams.iter().copied().collect();
    active_streams_vec.sort_unstable();
    let active_streams_list = PyList::empty(py);
    for v in active_streams_vec {
        active_streams_list.append(v.into_pyobject(py)?)?;
    }
    dict.set_item("active_streams", active_streams_list)?;

    let mut blocked_streams_vec: Vec<_> = diag.blocked_streams.iter().copied().collect();
    blocked_streams_vec.sort_unstable();
    let blocked_streams_list = PyList::empty(py);
    for v in blocked_streams_vec {
        blocked_streams_list.append(v.into_pyobject(py)?)?;
    }
    dict.set_item("blocked_streams", blocked_streams_list)?;

    dict.set_item("close_code", diag.close_code)?;
    dict.set_item("close_reason", &diag.close_reason)?;
    dict.set_item("closed_at", diag.closed_at)?;
    dict.set_item("created_at", diag.created_at)?;
    dict.set_item("datagram_bytes_received", diag.datagram_bytes_received)?;
    dict.set_item("datagram_bytes_sent", diag.datagram_bytes_sent)?;
    dict.set_item("datagrams_received", diag.datagrams_received)?;
    dict.set_item("datagrams_sent", diag.datagrams_sent)?;
    dict.set_item("flow_control_negotiated", diag.flow_control_negotiated)?;
    dict.set_item("headers", headers_to_py(py, &diag.headers)?)?;
    dict.set_item("is_client", diag.is_client)?;
    dict.set_item("local_data_consumed", diag.local_data_consumed)?;
    dict.set_item("local_data_received", diag.local_data_received)?;
    dict.set_item("local_data_sent", diag.local_data_sent)?;
    dict.set_item("local_max_data", diag.local_max_data)?;
    dict.set_item("local_max_streams_bidi", diag.local_max_streams_bidi)?;
    dict.set_item("local_max_streams_uni", diag.local_max_streams_uni)?;
    dict.set_item("local_streams_bidi_opened", diag.local_streams_bidi_opened)?;
    dict.set_item("local_streams_uni_opened", diag.local_streams_uni_opened)?;
    dict.set_item("path", &diag.path)?;
    dict.set_item("peer_max_data", diag.peer_max_data)?;
    dict.set_item("peer_max_streams_bidi", diag.peer_max_streams_bidi)?;
    dict.set_item("peer_max_streams_uni", diag.peer_max_streams_uni)?;
    dict.set_item("peer_streams_bidi_closed", diag.peer_streams_bidi_closed)?;
    dict.set_item("peer_streams_bidi_opened", diag.peer_streams_bidi_opened)?;
    dict.set_item("peer_streams_uni_closed", diag.peer_streams_uni_closed)?;
    dict.set_item("peer_streams_uni_opened", diag.peer_streams_uni_opened)?;

    let pending_bidi = PyList::empty(py);
    for v in &diag.pending_bidi_stream_requests {
        pending_bidi.append(v.into_pyobject(py)?)?;
    }
    dict.set_item("pending_bidi_stream_requests", pending_bidi)?;

    let pending_uni = PyList::empty(py);
    for v in &diag.pending_uni_stream_requests {
        pending_uni.append(v.into_pyobject(py)?)?;
    }
    dict.set_item("pending_uni_stream_requests", pending_uni)?;

    dict.set_item("ready_at", diag.ready_at)?;
    dict.set_item("session_id", diag.session_id)?;
    dict.set_item("state", diag.state)?;
    dict.set_item("wt_protocol", &diag.wt_protocol)?;

    Ok(dict.into_any())
}

// Converts StreamDiagnostics into a Python dictionary.
fn stream_diagnostics_to_py<'py>(
    py: Python<'py>,
    diag: &StreamDiagnostics,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("bytes_received", diag.bytes_received)?;
    dict.set_item("bytes_sent", diag.bytes_sent)?;
    dict.set_item("close_code", diag.close_code)?;
    dict.set_item("close_reason", &diag.close_reason)?;
    dict.set_item("closed_at", diag.closed_at)?;
    dict.set_item("created_at", diag.created_at)?;
    dict.set_item("direction", diag.direction)?;
    dict.set_item("is_peer_initiated", diag.is_peer_initiated)?;
    dict.set_item("read_buffer_size", diag.read_buffer_size)?;
    dict.set_item("session_id", diag.session_id)?;
    dict.set_item("state", diag.state)?;
    dict.set_item("stream_id", diag.stream_id)?;
    dict.set_item("write_buffer_size", diag.write_buffer_size)?;

    Ok(dict.into_any())
}
