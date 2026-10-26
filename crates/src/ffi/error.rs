//! FFI conversion logic for common errors.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;

use crate::common::error::WebTransportError;
use crate::common::types::{ErrorCode, ErrorSource};

static EXCEPTION_MODULE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

pub(super) fn create_py_exception(
    py: Python<'_>,
    source: ErrorSource,
    error_code: Option<ErrorCode>,
    reason: &str,
) -> PyErr {
    let class_name = match source {
        ErrorSource::Connection => "ConnectionError",
        ErrorSource::Datagram => "DatagramError",
        ErrorSource::Session => "SessionError",
        ErrorSource::Stream => "StreamError",
        ErrorSource::Unspecified => "WebTransportError",
    };

    let kwargs = make_kwargs(py, error_code);

    instantiate_py_exception(py, class_name, reason, &kwargs)
}

impl From<WebTransportError> for PyErr {
    fn from(error: WebTransportError) -> PyErr {
        Python::attach(|py| {
            let (class_name, error_code, reason) = match error {
                WebTransportError::Configuration(error_code, reason) => {
                    ("ConfigurationError", error_code, reason)
                }
                WebTransportError::Protocol(error_code, reason) => {
                    ("ProtocolError", error_code, reason)
                }
                WebTransportError::Stream(_, error_code, reason) => {
                    ("StreamError", error_code, reason)
                }
                WebTransportError::Unknown(error_code, reason) => {
                    ("WebTransportError", error_code, reason)
                }
            };

            let kwargs = make_kwargs(py, error_code);

            instantiate_py_exception(py, class_name, &reason, &kwargs)
        })
    }
}

fn instantiate_py_exception(
    py: Python<'_>,
    class_name: &str,
    reason: &str,
    kwargs: &Bound<'_, PyDict>,
) -> PyErr {
    let module = EXCEPTION_MODULE
        .get_or_try_init(py, || {
            py.import("pywebtransport.exceptions").map(Into::into)
        })
        .and_then(|module| module.bind(py).getattr(class_name));

    match module {
        Ok(class) => match class.call((reason,), Some(kwargs)) {
            Ok(instance) => PyErr::from_value(instance),
            Err(e) => e,
        },
        Err(e) => e,
    }
}

fn make_kwargs(py: Python<'_>, error_code: Option<ErrorCode>) -> Bound<'_, PyDict> {
    let kwargs = PyDict::new(py);
    if let Some(error_code) = error_code {
        kwargs.set_item("error_code", error_code).ok();
    }

    kwargs
}
