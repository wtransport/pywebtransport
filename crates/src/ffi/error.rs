//! FFI error mapping and conversion logic.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::common::error::WebTransportError;
use crate::common::types::{ErrorCode, ErrorSource};

// Python exception instance creation based on error classification.
pub(super) fn create_py_exception(
    py: Python<'_>,
    source: ErrorSource,
    code: Option<ErrorCode>,
    reason: String,
) -> PyErr {
    let class_name = match source {
        ErrorSource::Connection => "ConnectionError",
        ErrorSource::Session => "SessionError",
        ErrorSource::Stream => "StreamError",
        ErrorSource::Datagram => "DatagramError",
        ErrorSource::Protocol => "ProtocolError",
        ErrorSource::FlowControl => "FlowControlError",
        ErrorSource::Unspecified => "WebTransportError",
    };

    let kwargs = make_kwargs(py, code);

    instantiate_py_exception(py, class_name, reason, &kwargs)
}

impl From<WebTransportError> for PyErr {
    fn from(err: WebTransportError) -> PyErr {
        Python::attach(|py| {
            let (class_name, code, reason) = match &err {
                WebTransportError::Configuration(c, r) => ("ConfigurationError", *c, r.clone()),
                WebTransportError::Connection(c, r) => ("ConnectionError", *c, r.clone()),
                WebTransportError::Protocol(c, r) => ("ProtocolError", *c, r.clone()),
                WebTransportError::Stream(_, c, r) => ("StreamError", *c, r.clone()),
                WebTransportError::Unknown(c, r) => ("WebTransportError", *c, r.clone()),
            };

            let kwargs = make_kwargs(py, code);

            instantiate_py_exception(py, class_name, reason, &kwargs)
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
