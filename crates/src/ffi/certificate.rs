//! FFI bindings for TLS certificates.

use std::path::Path;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::tls::certificate;

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(generate_self_signed_cert, module)?)?;

    Ok(())
}

#[pyfunction]
#[pyo3(signature = (*, hostname, output_dir=".", validity_days=365))]
fn generate_self_signed_cert(
    hostname: &str,
    output_dir: &str,
    validity_days: i64,
) -> PyResult<(String, String, String)> {
    if hostname.is_empty() {
        return Err(PyValueError::new_err("hostname validate failed"));
    }

    certificate::generate_self_signed_cert(hostname, Path::new(output_dir), validity_days)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}
