//! FFI module declarations.

use pyo3::prelude::*;

mod certificate;
mod config;
mod constants;
mod conversion;
mod engine;
mod error;
mod types;

// FFI sub-module registration.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    certificate::register(m)?;
    constants::register(m)?;
    engine::register(m)?;

    Ok(())
}
