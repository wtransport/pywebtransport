//! `PyWebTransport` engine.

use pyo3::prelude::*;

pub(crate) mod common;
pub(crate) mod protocol;
pub(crate) mod runtime;
pub(crate) mod tls;
pub(crate) mod transport;

mod ffi;

#[pymodule]
fn _pywebtransport(_py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    ffi::register(module)
}
