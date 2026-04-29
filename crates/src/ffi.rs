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

// FFI sub-module registration.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    abi::register(m)?;
    certificate::register(m)?;
    constants::register(m)?;
    endpoint::register(m)?;
    tracing::register(m)?;
    waker::register(m)?;

    Ok(())
}
