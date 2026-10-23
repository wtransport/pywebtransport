//! FFI bindings for tracing initialization.

use std::io;

use pyo3::prelude::*;
use tracing_subscriber::EnvFilter;

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(init_tracing, module)?)?;

    Ok(())
}

#[pyfunction]
#[pyo3(signature = ())]
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(io::stderr)
        .try_init()
        .ok();
}
