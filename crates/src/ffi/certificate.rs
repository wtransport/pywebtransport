//! TLS certificate generation bindings.

use std::path::Path;

use pyo3::prelude::*;

use crate::tls;

// Certificate function registration.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_self_signed_cert, m)?)?;

    Ok(())
}

// Self-signed X.509 certificate generation.
#[pyfunction]
#[pyo3(signature = (*, hostname, output_dir=".", validity_days=365))]
fn generate_self_signed_cert(
    hostname: &str,
    output_dir: &str,
    validity_days: i64,
) -> PyResult<(String, String)> {
    if hostname.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Hostname cannot be empty",
        ));
    }

    tls::certificate::generate_self_signed_cert(hostname, Path::new(output_dir), validity_days)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}
