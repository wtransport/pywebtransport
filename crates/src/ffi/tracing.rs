//! Tracing initialization and subscriber setup for the FFI boundary.

use pyo3::prelude::*;

// Registers the tracing FFI functions into the Python module.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_tracing, m)?)?;

    Ok(())
}

// Initializes the global tracing subscriber driven by the RUST_LOG environment variable.
#[pyfunction]
#[pyo3(signature = ())]
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init()
        .ok();
}
