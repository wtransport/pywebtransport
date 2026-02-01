//! Root crate definition and Python module registration.

use pyo3::prelude::*;

pub mod common;
pub mod tls;

pub(crate) mod protocol;

mod ffi;

// Python module initialization.
#[pymodule]
fn _wtransport(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    ffi::register(m)
}
