//! Root crate definition and Python module registration.

use pyo3::prelude::*;

pub(crate) mod common;
pub(crate) mod protocol;
pub(crate) mod runtime;
pub(crate) mod tls;
pub(crate) mod transport;

mod ffi;

// Python module initialization.
#[pymodule]
fn _pywebtransport(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    ffi::register(m)
}
