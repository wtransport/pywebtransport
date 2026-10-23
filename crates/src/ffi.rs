//! FFI module declarations.

use pyo3::prelude::*;

mod abi;
mod certificate;
mod config;
mod constants;
mod conversion;
mod endpoint;
mod error;
mod tracing;
mod types;
mod waker;

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    abi::register(module)?;
    certificate::register(module)?;
    constants::register(module)?;
    endpoint::register(module)?;
    tracing::register(module)?;
    waker::register(module)?;

    Ok(())
}
