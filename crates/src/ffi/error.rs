//! FFI error mapping and conversion logic.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::common::error::WebTransportError;
use crate::common::types::ErrorCode;

// Python exception instance creation based on error classification.
pub(super) fn create_py_exception(
    py: Python<'_>,
    code: Option<ErrorCode>,
    reason: String,
) -> PyErr {
    let class_name = match code {
        Some(c) if (0x1000_0000..=0x10FF_FFFF).contains(&c) => "WebTransportError",
        Some(c) if (0x1100_0000..=0x11FF_FFFF).contains(&c) => "ConnectionError",
        Some(c) if (0x1200_0000..=0x12FF_FFFF).contains(&c) => "SessionError",
        Some(c) if (0x1300_0000..=0x13FF_FFFF).contains(&c) => "StreamError",
        _ => "WebTransportError",
    };

    let kwargs = make_kwargs(py, code);

    instantiate_py_exception(py, class_name, reason, &kwargs)
}

impl From<WebTransportError> for PyErr {
    fn from(err: WebTransportError) -> PyErr {
        Python::attach(|py| {
            let (class_name, kwargs) = match &err {
                WebTransportError::Authentication(code, _) => {
                    ("AuthenticationError", make_kwargs(py, *code))
                }
                WebTransportError::Connection(code, _) => {
                    ("ConnectionError", make_kwargs(py, *code))
                }
                WebTransportError::Datagram(code, _) => ("DatagramError", make_kwargs(py, *code)),
                WebTransportError::FlowControl(code, _) => {
                    ("FlowControlError", make_kwargs(py, *code))
                }
                WebTransportError::H3(code, _) | WebTransportError::Quic(code, _) => {
                    ("ProtocolError", make_kwargs(py, *code))
                }
                WebTransportError::Io(_) => ("WebTransportError", make_kwargs(py, None)),
                WebTransportError::Session(sid, code, _) => {
                    let dict = make_kwargs(py, *code);
                    dict.set_item("session_id", sid).ok();
                    ("SessionError", dict)
                }
                WebTransportError::Stream(sid, code, _) => {
                    let dict = make_kwargs(py, *code);
                    dict.set_item("stream_id", sid).ok();
                    ("StreamError", dict)
                }
                WebTransportError::Timeout(code, _) => ("TimeoutError", make_kwargs(py, *code)),
                WebTransportError::Unknown(code, _) => {
                    ("WebTransportError", make_kwargs(py, *code))
                }
            };

            instantiate_py_exception(py, class_name, err.to_string(), &kwargs)
        })
    }
}

// Dynamic Python exception class instantiation.
fn instantiate_py_exception(
    py: Python<'_>,
    class_name: &str,
    reason: String,
    kwargs: &Bound<'_, PyDict>,
) -> PyErr {
    match py.import("pywebtransport.exceptions") {
        Ok(m) => match m.getattr(class_name) {
            Ok(cls) => match cls.call((reason,), Some(kwargs)) {
                Ok(instance) => PyErr::from_value(instance),
                Err(e) => e,
            },
            Err(e) => e,
        },
        Err(e) => e,
    }
}

// Keyword arguments dictionary construction.
fn make_kwargs(py: Python<'_>, code: Option<ErrorCode>) -> Bound<'_, PyDict> {
    let dict = PyDict::new(py);
    if let Some(c) = code {
        dict.set_item("error_code", c).ok();
    }
    dict
}
