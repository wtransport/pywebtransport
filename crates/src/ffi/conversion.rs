//! FFI conversion logic between Python objects and Rust protocol types.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::{Borrowed, Bound};

use crate::common::types::Headers;
use crate::ffi::error::create_py_exception;
use crate::protocol::events::{Effect, ProtocolEvent, RequestResult};

// Effect vector to Python list conversion.
pub(super) fn effects_to_py(py: Python<'_>, effects: Vec<Effect>) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for effect in effects {
        list.append(effect.into_pyobject(py)?)?;
    }

    Ok(list.into())
}

// Bytes extraction from Python object using buffer protocol or UTF-8 encoding.
pub(super) fn extract_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(buffer) = obj.extract::<PyBuffer<u8>>() {
        Ok(Bytes::from(buffer.to_vec(obj.py())?))
    } else if let Ok(s) = obj.extract::<Bound<'_, PyString>>() {
        Ok(Bytes::copy_from_slice(s.to_str()?.as_bytes()))
    } else {
        Err(PyValueError::new_err(
            "Expected bytes, bytearray, memoryview, or str",
        ))
    }
}

// Extraction of bytes or list of bytes into single buffer.
pub(super) fn extract_bytes_or_list(obj: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(b) = extract_bytes(obj) {
        Ok(b)
    } else if let Ok(list) = obj.extract::<Bound<'_, PyList>>() {
        let mut buf = BytesMut::new();
        for item in list.iter() {
            let b = extract_bytes(&item).map_err(|e| {
                PyValueError::new_err(format!(
                    "Datagram list items must be bytes-like or str: {e}"
                ))
            })?;
            buf.extend_from_slice(&b);
        }

        Ok(buf.freeze())
    } else {
        Err(PyValueError::new_err(
            "Datagram data must be bytes-like or list[bytes-like]",
        ))
    }
}

// HTTP/3 header extraction from Python dictionary or list.
pub(super) fn extract_headers(obj: &Bound<'_, PyAny>) -> PyResult<Headers> {
    let mut headers = Vec::new();

    if let Ok(dict) = obj.extract::<Bound<'_, PyDict>>() {
        for (k, v) in dict {
            process_header_item(&k, &v, &mut headers)?;
        }
    } else if let Ok(list) = obj.extract::<Bound<'_, PyList>>() {
        for item in list {
            let tuple = item.extract::<Bound<'_, PyTuple>>().map_err(|e| {
                PyValueError::new_err(format!("Headers list must contain tuples: {e}"))
            })?;
            if tuple.len() != 2 {
                return Err(PyValueError::new_err("Header tuple must have 2 elements"));
            }
            process_header_item(&tuple.get_item(0)?, &tuple.get_item(1)?, &mut headers)?;
        }
    } else {
        return Err(PyValueError::new_err("Headers must be a dict or list"));
    }

    Ok(headers)
}

impl<'a, 'py> FromPyObject<'a, 'py> for ProtocolEvent {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let bound = ob.as_borrowed();
        let type_name = bound.get_type().name()?;

        let get = |name: &str| bound.getattr(name);
        let get_bytes = |name: &str| -> PyResult<Bytes> {
            let val = bound.getattr(name)?;
            extract_bytes(&val)
                .map_err(|e| PyValueError::new_err(format!("Field {name} must be bytes-like: {e}")))
        };

        match type_name.to_str()? {
            "InternalBindH3Session" => Ok(ProtocolEvent::InternalBindH3Session {
                request_id: get("request_id")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
            }),
            "InternalBindQuicStream" => Ok(ProtocolEvent::InternalBindQuicStream {
                request_id: get("request_id")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
                is_unidirectional: get("is_unidirectional")?.extract()?,
            }),
            "InternalCleanupEarlyEvents" => Ok(ProtocolEvent::InternalCleanupEarlyEvents),
            "InternalCleanupResources" => Ok(ProtocolEvent::InternalCleanupResources),
            "InternalFailH3Session" => {
                let exc = get("exception")?;
                let (reason, error_code) = extract_exception_details(&exc);

                Ok(ProtocolEvent::InternalFailH3Session {
                    request_id: get("request_id")?.extract()?,
                    error_code,
                    reason,
                })
            }
            "InternalFailQuicStream" => {
                let exc = get("exception")?;
                let (reason, error_code) = extract_exception_details(&exc);

                Ok(ProtocolEvent::InternalFailQuicStream {
                    request_id: get("request_id")?.extract()?,
                    session_id: get("session_id")?.extract()?,
                    is_unidirectional: get("is_unidirectional")?.extract()?,
                    error_code,
                    reason,
                })
            }
            "InternalReturnStreamData" => Ok(ProtocolEvent::InternalReturnStreamData {
                stream_id: get("stream_id")?.extract()?,
                data: get_bytes("data")?,
            }),
            "TransportConnectionTerminated" => Ok(ProtocolEvent::TransportConnectionTerminated {
                error_code: get("error_code")?.extract()?,
                reason_phrase: get("reason_phrase")?.extract()?,
            }),
            "TransportDatagramFrameReceived" => Ok(ProtocolEvent::TransportDatagramFrameReceived {
                data: get_bytes("data")?,
            }),
            "TransportHandshakeCompleted" => Ok(ProtocolEvent::TransportHandshakeCompleted),
            "TransportQuicParametersReceived" => {
                Ok(ProtocolEvent::TransportQuicParametersReceived {
                    remote_max_datagram_frame_size: get("remote_max_datagram_frame_size")?
                        .extract()?,
                })
            }
            "TransportQuicTimerFired" => Ok(ProtocolEvent::TransportQuicTimerFired),
            "TransportStreamDataReceived" => Ok(ProtocolEvent::TransportStreamDataReceived {
                data: get_bytes("data")?,
                end_stream: get("end_stream")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
            }),
            "TransportStreamReset" => Ok(ProtocolEvent::TransportStreamReset {
                error_code: get("error_code")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
            }),
            "CapsuleReceived" => Ok(ProtocolEvent::CapsuleReceived {
                capsule_data: get_bytes("capsule_data")?,
                capsule_type: get("capsule_type")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
            }),
            "ConnectStreamClosed" => Ok(ProtocolEvent::ConnectStreamClosed {
                stream_id: get("stream_id")?.extract()?,
            }),
            "DatagramReceived" => Ok(ProtocolEvent::DatagramReceived {
                data: get_bytes("data")?,
                stream_id: get("stream_id")?.extract()?,
            }),
            "GoawayReceived" => Ok(ProtocolEvent::GoawayReceived),
            "HeadersReceived" => Ok(ProtocolEvent::HeadersReceived {
                headers: extract_headers(&get("headers")?)?,
                stream_id: get("stream_id")?.extract()?,
                stream_ended: get("stream_ended")?.extract()?,
            }),
            "SettingsReceived" => Ok(ProtocolEvent::SettingsReceived {
                settings: get("settings")?.extract::<HashMap<u64, u64>>()?,
            }),
            "WebTransportStreamDataReceived" => Ok(ProtocolEvent::WebTransportStreamDataReceived {
                data: get_bytes("data")?,
                session_id: get("session_id")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
                stream_ended: get("stream_ended")?.extract()?,
            }),
            "ConnectionClose" => Ok(ProtocolEvent::ConnectionClose {
                request_id: get("request_id")?.extract()?,
                error_code: get("error_code")?.extract()?,
                reason: get("reason")?.extract()?,
            }),
            "UserAcceptSession" => Ok(ProtocolEvent::UserAcceptSession {
                request_id: get("request_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
            }),
            "UserCloseSession" => Ok(ProtocolEvent::UserCloseSession {
                request_id: get("request_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
                error_code: get("error_code")?.extract()?,
                reason: get("reason")?.extract()?,
            }),
            "UserConnectionGracefulClose" => Ok(ProtocolEvent::UserConnectionGracefulClose {
                request_id: get("request_id")?.extract()?,
            }),
            "UserCreateSession" => Ok(ProtocolEvent::UserCreateSession {
                request_id: get("request_id")?.extract()?,
                path: get("path")?.extract()?,
                headers: extract_headers(&get("headers")?)?,
            }),
            "UserCreateStream" => Ok(ProtocolEvent::UserCreateStream {
                request_id: get("request_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
                is_unidirectional: get("is_unidirectional")?.extract()?,
            }),
            "UserGetConnectionDiagnostics" => Ok(ProtocolEvent::UserGetConnectionDiagnostics {
                request_id: get("request_id")?.extract()?,
            }),
            "UserGetSessionDiagnostics" => Ok(ProtocolEvent::UserGetSessionDiagnostics {
                request_id: get("request_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
            }),
            "UserGetStreamDiagnostics" => Ok(ProtocolEvent::UserGetStreamDiagnostics {
                request_id: get("request_id")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
            }),
            "UserGrantDataCredit" => Ok(ProtocolEvent::UserGrantDataCredit {
                request_id: get("request_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
                max_data: get("max_data")?.extract()?,
            }),
            "UserGrantStreamsCredit" => Ok(ProtocolEvent::UserGrantStreamsCredit {
                request_id: get("request_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
                max_streams: get("max_streams")?.extract()?,
                is_unidirectional: get("is_unidirectional")?.extract()?,
            }),
            "UserRejectSession" => Ok(ProtocolEvent::UserRejectSession {
                request_id: get("request_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
                status_code: get("status_code")?.extract()?,
            }),
            "UserResetStream" => Ok(ProtocolEvent::UserResetStream {
                request_id: get("request_id")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
                error_code: get("error_code")?.extract()?,
            }),
            "UserSendDatagram" => Ok(ProtocolEvent::UserSendDatagram {
                request_id: get("request_id")?.extract()?,
                session_id: get("session_id")?.extract()?,
                data: extract_bytes_or_list(&get("data")?)?,
            }),
            "UserSendStreamData" => Ok(ProtocolEvent::UserSendStreamData {
                request_id: get("request_id")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
                data: get_bytes("data")?,
                end_stream: get("end_stream")?.extract()?,
            }),
            "UserStopStream" => Ok(ProtocolEvent::UserStopStream {
                request_id: get("request_id")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
                error_code: get("error_code")?.extract()?,
            }),
            "UserStreamRead" => Ok(ProtocolEvent::UserStreamRead {
                request_id: get("request_id")?.extract()?,
                stream_id: get("stream_id")?.extract()?,
                max_bytes: get("max_bytes")?
                    .extract::<Option<u64>>()?
                    .unwrap_or(u64::MAX),
            }),
            _ => Err(PyValueError::new_err(format!(
                "Unknown ProtocolEvent type: {type_name}"
            ))),
        }
    }
}

impl<'py> IntoPyObject<'py> for ProtocolEvent {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let events_mod = PyModule::import(py, "pywebtransport._protocol.events")?;

        let make =
            |cls_name: &str, kwargs: Vec<(&str, Py<PyAny>)>| -> PyResult<Bound<'py, PyAny>> {
                let cls = events_mod.getattr(cls_name)?;
                let dict = PyDict::new(py);
                for (k, v) in kwargs {
                    dict.set_item(k, v)?;
                }
                cls.call((), Some(&dict))
            };

        match self {
            ProtocolEvent::InternalBindH3Session {
                request_id,
                stream_id,
            } => make(
                "InternalBindH3Session",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::InternalBindQuicStream {
                request_id,
                stream_id,
                session_id,
                is_unidirectional,
            } => make(
                "InternalBindQuicStream",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "is_unidirectional",
                        PyBool::new(py, is_unidirectional)
                            .to_owned()
                            .into_any()
                            .unbind(),
                    ),
                ],
            ),
            ProtocolEvent::InternalCleanupEarlyEvents => make("InternalCleanupEarlyEvents", vec![]),
            ProtocolEvent::InternalCleanupResources => make("InternalCleanupResources", vec![]),
            ProtocolEvent::InternalFailH3Session {
                request_id,
                error_code,
                reason,
            } => make(
                "InternalFailH3Session",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "exception",
                        create_py_exception(py, error_code, reason)
                            .value(py)
                            .clone()
                            .into_any()
                            .unbind(),
                    ),
                ],
            ),
            ProtocolEvent::InternalFailQuicStream {
                request_id,
                session_id,
                is_unidirectional,
                error_code,
                reason,
            } => make(
                "InternalFailQuicStream",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "is_unidirectional",
                        PyBool::new(py, is_unidirectional)
                            .to_owned()
                            .into_any()
                            .unbind(),
                    ),
                    (
                        "exception",
                        create_py_exception(py, error_code, reason)
                            .value(py)
                            .clone()
                            .into_any()
                            .unbind(),
                    ),
                ],
            ),
            ProtocolEvent::InternalReturnStreamData { stream_id, data } => make(
                "InternalReturnStreamData",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", PyBytes::new(py, &data).into_any().unbind()),
                ],
            ),
            ProtocolEvent::TransportConnectionTerminated {
                error_code,
                reason_phrase,
            } => make(
                "TransportConnectionTerminated",
                vec![
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "reason_phrase",
                        PyString::new(py, &reason_phrase).into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::TransportDatagramFrameReceived { data } => make(
                "TransportDatagramFrameReceived",
                vec![("data", PyBytes::new(py, &data).into_any().unbind())],
            ),
            ProtocolEvent::TransportHandshakeCompleted => {
                make("TransportHandshakeCompleted", vec![])
            }
            ProtocolEvent::TransportQuicParametersReceived {
                remote_max_datagram_frame_size,
            } => make(
                "TransportQuicParametersReceived",
                vec![(
                    "remote_max_datagram_frame_size",
                    remote_max_datagram_frame_size
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                )],
            ),
            ProtocolEvent::TransportQuicTimerFired => make("TransportQuicTimerFired", vec![]),
            ProtocolEvent::TransportStreamDataReceived {
                stream_id,
                data,
                end_stream,
            } => make(
                "TransportStreamDataReceived",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", PyBytes::new(py, &data).into_any().unbind()),
                    (
                        "end_stream",
                        PyBool::new(py, end_stream).to_owned().into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::TransportStreamReset {
                stream_id,
                error_code,
            } => make(
                "TransportStreamReset",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::CapsuleReceived {
                stream_id,
                capsule_type,
                capsule_data,
            } => make(
                "CapsuleReceived",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "capsule_type",
                        capsule_type.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "capsule_data",
                        PyBytes::new(py, &capsule_data).into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::ConnectStreamClosed { stream_id } => make(
                "ConnectStreamClosed",
                vec![(
                    "stream_id",
                    stream_id.into_pyobject(py)?.into_any().unbind(),
                )],
            ),
            ProtocolEvent::DatagramReceived { stream_id, data } => make(
                "DatagramReceived",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", PyBytes::new(py, &data).into_any().unbind()),
                ],
            ),
            ProtocolEvent::GoawayReceived => make("GoawayReceived", vec![]),
            ProtocolEvent::HeadersReceived {
                stream_id,
                headers,
                stream_ended,
            } => make(
                "HeadersReceived",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("headers", headers_to_py(py, headers)?.into_any().unbind()),
                    (
                        "stream_ended",
                        PyBool::new(py, stream_ended).to_owned().into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::SettingsReceived { settings } => make(
                "SettingsReceived",
                vec![("settings", settings.into_pyobject(py)?.into_any().unbind())],
            ),
            ProtocolEvent::WebTransportStreamDataReceived {
                session_id,
                stream_id,
                data,
                stream_ended,
            } => make(
                "WebTransportStreamDataReceived",
                vec![
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", PyBytes::new(py, &data).into_any().unbind()),
                    (
                        "stream_ended",
                        PyBool::new(py, stream_ended).to_owned().into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::ConnectionClose {
                request_id,
                error_code,
                reason,
            } => make(
                "ConnectionClose",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("reason", reason.into_pyobject(py)?.into_any().unbind()),
                ],
            ),
            ProtocolEvent::UserAcceptSession {
                request_id,
                session_id,
            } => make(
                "UserAcceptSession",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserCloseSession {
                request_id,
                session_id,
                error_code,
                reason,
            } => make(
                "UserCloseSession",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("reason", reason.into_pyobject(py)?.into_any().unbind()),
                ],
            ),
            ProtocolEvent::UserConnectionGracefulClose { request_id } => make(
                "UserConnectionGracefulClose",
                vec![(
                    "request_id",
                    request_id.into_pyobject(py)?.into_any().unbind(),
                )],
            ),
            ProtocolEvent::UserCreateSession {
                request_id,
                path,
                headers,
            } => make(
                "UserCreateSession",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("path", PyString::new(py, &path).into_any().unbind()),
                    ("headers", headers_to_py(py, headers)?.into_any().unbind()),
                ],
            ),
            ProtocolEvent::UserCreateStream {
                request_id,
                session_id,
                is_unidirectional,
            } => make(
                "UserCreateStream",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "is_unidirectional",
                        PyBool::new(py, is_unidirectional)
                            .to_owned()
                            .into_any()
                            .unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserGetConnectionDiagnostics { request_id } => make(
                "UserGetConnectionDiagnostics",
                vec![(
                    "request_id",
                    request_id.into_pyobject(py)?.into_any().unbind(),
                )],
            ),
            ProtocolEvent::UserGetSessionDiagnostics {
                request_id,
                session_id,
            } => make(
                "UserGetSessionDiagnostics",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserGetStreamDiagnostics {
                request_id,
                stream_id,
            } => make(
                "UserGetStreamDiagnostics",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserGrantDataCredit {
                request_id,
                session_id,
                max_data,
            } => make(
                "UserGrantDataCredit",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("max_data", max_data.into_pyobject(py)?.into_any().unbind()),
                ],
            ),
            ProtocolEvent::UserGrantStreamsCredit {
                request_id,
                session_id,
                max_streams,
                is_unidirectional,
            } => make(
                "UserGrantStreamsCredit",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "max_streams",
                        max_streams.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "is_unidirectional",
                        PyBool::new(py, is_unidirectional)
                            .to_owned()
                            .into_any()
                            .unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserRejectSession {
                request_id,
                session_id,
                status_code,
            } => make(
                "UserRejectSession",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "status_code",
                        status_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserResetStream {
                request_id,
                stream_id,
                error_code,
            } => make(
                "UserResetStream",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserSendDatagram {
                request_id,
                session_id,
                data,
            } => make(
                "UserSendDatagram",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", PyBytes::new(py, &data).into_any().unbind()),
                ],
            ),
            ProtocolEvent::UserSendStreamData {
                request_id,
                stream_id,
                data,
                end_stream,
            } => make(
                "UserSendStreamData",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", PyBytes::new(py, &data).into_any().unbind()),
                    (
                        "end_stream",
                        PyBool::new(py, end_stream).to_owned().into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserStopStream {
                request_id,
                stream_id,
                error_code,
            } => make(
                "UserStopStream",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            ProtocolEvent::UserStreamRead {
                request_id,
                stream_id,
                max_bytes,
            } => make(
                "UserStreamRead",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "max_bytes",
                        max_bytes.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
        }
    }
}

impl<'py> IntoPyObject<'py> for Effect {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let events_mod = PyModule::import(py, "pywebtransport._protocol.events")?;

        let make =
            |cls_name: &str, kwargs: Vec<(&str, Py<PyAny>)>| -> PyResult<Bound<'py, PyAny>> {
                let cls = events_mod.getattr(cls_name)?;
                let dict = PyDict::new(py);
                for (k, v) in kwargs {
                    dict.set_item(k, v)?;
                }

                cls.call((), Some(&dict))
            };

        match self {
            Effect::CleanupH3Stream { stream_id } => make(
                "CleanupH3Stream",
                vec![(
                    "stream_id",
                    stream_id.into_pyobject(py)?.into_any().unbind(),
                )],
            ),
            Effect::CloseQuicConnection { error_code, reason } => make(
                "CloseQuicConnection",
                vec![
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("reason", reason.into_pyobject(py)?.into_any().unbind()),
                ],
            ),
            Effect::CreateH3Session {
                request_id,
                path,
                headers,
            } => make(
                "CreateH3Session",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("path", PyString::new(py, &path).into_any().unbind()),
                    ("headers", headers_to_py(py, headers)?.into_any().unbind()),
                ],
            ),
            Effect::CreateQuicStream {
                request_id,
                session_id,
                is_unidirectional,
            } => make(
                "CreateQuicStream",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "is_unidirectional",
                        PyBool::new(py, is_unidirectional)
                            .to_owned()
                            .into_any()
                            .unbind(),
                    ),
                ],
            ),
            Effect::EmitConnectionEvent {
                event_type,
                connection_id,
                error_code,
                reason,
            } => make(
                "EmitConnectionEvent",
                vec![
                    (
                        "event_type",
                        event_type.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", {
                        let d = PyDict::new(py);
                        d.set_item("connection_id", connection_id)?;
                        if let Some(ec) = error_code {
                            d.set_item("error_code", ec)?;
                        }
                        if let Some(r) = reason {
                            d.set_item("reason", r)?;
                        }
                        d.into_any().unbind()
                    }),
                ],
            ),
            Effect::EmitSessionEvent {
                event_type,
                session_id,
                code,
                data,
                headers,
                is_unidirectional,
                max_data,
                max_streams,
                path,
                ready_at,
                reason,
            } => make(
                "EmitSessionEvent",
                vec![
                    (
                        "event_type",
                        event_type.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "session_id",
                        session_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", {
                        let d = PyDict::new(py);
                        d.set_item("session_id", session_id)?;
                        if let Some(c) = code {
                            d.set_item("code", c)?;
                        }
                        if let Some(dat) = data {
                            d.set_item("data", PyBytes::new(py, &dat))?;
                        }
                        if let Some(h) = headers {
                            let list = headers_to_py(py, h)?;
                            d.set_item("headers", list)?;
                        }
                        if let Some(uni) = is_unidirectional {
                            d.set_item("is_unidirectional", uni)?;
                        }
                        if let Some(md) = max_data {
                            d.set_item("max_data", md)?;
                        }
                        if let Some(ms) = max_streams {
                            d.set_item("max_streams", ms)?;
                        }
                        if let Some(p) = path {
                            d.set_item("path", p)?;
                        }
                        if let Some(ra) = ready_at {
                            d.set_item("ready_at", ra)?;
                        }
                        if let Some(r) = reason {
                            d.set_item("reason", r)?;
                        }
                        d.into_any().unbind()
                    }),
                ],
            ),
            Effect::EmitStreamEvent {
                event_type,
                stream_id,
                direction,
                session_id,
            } => make(
                "EmitStreamEvent",
                vec![
                    (
                        "event_type",
                        event_type.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", {
                        let d = PyDict::new(py);
                        d.set_item("stream_id", stream_id)?;
                        if let Some(dir) = direction {
                            d.set_item("direction", dir.into_pyobject(py)?.into_any().unbind())?;
                        }
                        if let Some(sid) = session_id {
                            d.set_item("session_id", sid)?;
                        }
                        d.into_any().unbind()
                    }),
                ],
            ),
            Effect::LogH3Frame {
                category,
                event,
                data,
            } => {
                let json_mod = PyModule::import(py, "json")?;
                let dict_data = json_mod.call_method1("loads", (data,))?;

                make(
                    "LogH3Frame",
                    vec![
                        ("category", PyString::new(py, &category).into_any().unbind()),
                        ("event", PyString::new(py, &event).into_any().unbind()),
                        ("data", dict_data.into_any().unbind()),
                    ],
                )
            }
            Effect::NotifyRequestDone { request_id, result } => {
                let py_result = match result {
                    RequestResult::None => py.None().into_any(),
                    RequestResult::SessionId(sid) | RequestResult::StreamId(sid) => {
                        sid.into_pyobject(py)?.into_any().unbind()
                    }
                    RequestResult::ReadData(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                    RequestResult::Diagnostics(s) => {
                        let json_mod = PyModule::import(py, "json")?;
                        json_mod.call_method1("loads", (s,))?.into_any().unbind()
                    }
                };

                make(
                    "NotifyRequestDone",
                    vec![
                        (
                            "request_id",
                            request_id.into_pyobject(py)?.into_any().unbind(),
                        ),
                        ("result", py_result),
                    ],
                )
            }
            Effect::NotifyRequestFailed {
                request_id,
                error_code,
                reason,
            } => make(
                "NotifyRequestFailed",
                vec![
                    (
                        "request_id",
                        request_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "exception",
                        create_py_exception(py, error_code, reason)
                            .value(py)
                            .clone()
                            .into_any()
                            .unbind(),
                    ),
                ],
            ),
            Effect::ProcessProtocolEvent { event } => make(
                "ProcessProtocolEvent",
                vec![("event", (*event).into_pyobject(py)?.into_any().unbind())],
            ),
            Effect::RescheduleQuicTimer => make("RescheduleQuicTimer", vec![]),
            Effect::ResetQuicStream {
                stream_id,
                error_code,
            } => make(
                "ResetQuicStream",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            Effect::SendH3Capsule {
                stream_id,
                capsule_type,
                capsule_data,
                end_stream,
            } => make(
                "SendH3Capsule",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "capsule_type",
                        capsule_type.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "capsule_data",
                        PyBytes::new(py, &capsule_data).into_any().unbind(),
                    ),
                    (
                        "end_stream",
                        PyBool::new(py, end_stream).to_owned().into_any().unbind(),
                    ),
                ],
            ),
            Effect::SendH3Datagram { stream_id, data } => make(
                "SendH3Datagram",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", PyBytes::new(py, &data).into_any().unbind()),
                ],
            ),
            Effect::SendH3Goaway => make("SendH3Goaway", vec![]),
            Effect::SendH3Headers {
                stream_id,
                status,
                end_stream,
            } => make(
                "SendH3Headers",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("status", status.into_pyobject(py)?.into_any().unbind()),
                    (
                        "end_stream",
                        PyBool::new(py, end_stream).to_owned().into_any().unbind(),
                    ),
                ],
            ),
            Effect::SendQuicData {
                stream_id,
                data,
                end_stream,
            } => make(
                "SendQuicData",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    ("data", PyBytes::new(py, &data).into_any().unbind()),
                    (
                        "end_stream",
                        PyBool::new(py, end_stream).to_owned().into_any().unbind(),
                    ),
                ],
            ),
            Effect::SendQuicDatagram { data } => make(
                "SendQuicDatagram",
                vec![("data", PyBytes::new(py, &data).into_any().unbind())],
            ),
            Effect::StopQuicStream {
                stream_id,
                error_code,
            } => make(
                "StopQuicStream",
                vec![
                    (
                        "stream_id",
                        stream_id.into_pyobject(py)?.into_any().unbind(),
                    ),
                    (
                        "error_code",
                        error_code.into_pyobject(py)?.into_any().unbind(),
                    ),
                ],
            ),
            Effect::TriggerQuicTimer => make("TriggerQuicTimer", vec![]),
        }
    }
}

// Exception detail extraction.
fn extract_exception_details(exc: &Bound<'_, PyAny>) -> (String, Option<u64>) {
    let reason = exc.to_string();
    let error_code = exc
        .getattr("error_code")
        .ok()
        .and_then(|v| v.extract::<Option<u64>>().ok())
        .flatten();
    (reason, error_code)
}

// Rust Headers to Python list conversion.
fn headers_to_py(py: Python<'_>, headers: Headers) -> PyResult<Bound<'_, PyList>> {
    let list = PyList::empty(py);
    for (k, v) in headers {
        let key = PyBytes::new(py, &k);
        let val = PyBytes::new(py, &v);
        let tuple = PyTuple::new(py, &[key, val])?;
        list.append(tuple)?;
    }
    Ok(list)
}

// Single header item processing.
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
