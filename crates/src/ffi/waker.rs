//! Cross-language async waker mechanism using OS-level file descriptors.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::prelude::*;
use tracing::error;

#[cfg(unix)]
mod sys {
    use std::fs::File;
    use std::io::{self, Write};
    use std::mem::ManuallyDrop;
    use std::os::fd::{FromRawFd, RawFd};

    #[inline]
    pub(super) fn write_waker(fd: usize) -> io::Result<()> {
        let raw_fd = RawFd::try_from(fd).map_err(|_e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "waker file descriptor exceeds OS limits",
            )
        })?;

        let mut file = ManuallyDrop::new(unsafe { File::from_raw_fd(raw_fd) });

        file.write_all(&[1])
    }
}

#[cfg(windows)]
mod sys {
    use std::io::{self, Write};
    use std::mem::ManuallyDrop;
    use std::net::TcpStream;
    use std::os::windows::io::{FromRawSocket, RawSocket};

    #[inline]
    pub(super) fn write_waker(fd: usize) -> io::Result<()> {
        let raw_socket = RawSocket::try_from(fd).map_err(|_e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "waker socket handle exceeds OS limits",
            )
        })?;

        let mut stream = ManuallyDrop::new(unsafe { TcpStream::from_raw_socket(raw_socket) });

        stream.write_all(&[1])
    }
}

// Python-facing handle for managing the cross-language wake-up mechanism.
#[pyclass(name = "Waker", module = "pywebtransport._wtransport")]
pub(super) struct PyWaker {
    state: Arc<WakerState>,
}

#[pymethods]
impl PyWaker {
    #[new]
    fn new(fd: usize) -> Self {
        Self {
            state: Arc::new(WakerState {
                fd,
                signaled: AtomicBool::new(false),
            }),
        }
    }

    fn clear(&self) {
        self.state.clear();
    }
}

#[allow(
    clippy::multiple_inherent_impl,
    reason = "PyO3 requires Rust-native methods to reside outside the #[pymethods] block."
)]
impl PyWaker {
    // Generates a thread-safe callback for the Tokio reactor to invoke.
    pub(super) fn clone_waker_callback(&self) -> Arc<dyn Fn() + Send + Sync> {
        let state = Arc::clone(&self.state);

        Arc::new(move || state.wake())
    }
}

// Registers the FFI classes to the parent Python module.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWaker>()?;

    Ok(())
}

// Shared state between the Python FFI plane and the Rust Tokio reactor.
struct WakerState {
    fd: usize,
    signaled: AtomicBool,
}

impl WakerState {
    // Clears the signaled flag to allow subsequent wake-ups.
    fn clear(&self) {
        self.signaled.store(false, Ordering::Release);
    }

    // Triggers the OS-level file descriptor write if not already signaled.
    fn wake(&self) {
        if !self.signaled.swap(true, Ordering::AcqRel)
            && let Err(e) = sys::write_waker(self.fd)
            && e.kind() != std::io::ErrorKind::WouldBlock
        {
            error!("Waker system call failed on handle {}: {}", self.fd, e);
        }
    }
}
