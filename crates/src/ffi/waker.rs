//! FFI bindings for the waker.

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::prelude::*;
use tracing::debug;

#[cfg(unix)]
mod sys {
    use std::fs::File;
    use std::io::{self, Error, ErrorKind, Write};
    use std::mem::ManuallyDrop;
    use std::os::fd::{FromRawFd, RawFd};

    #[inline]
    pub(super) fn write_waker(fd: usize) -> io::Result<()> {
        let fd = RawFd::try_from(fd)
            .map_err(|_e| Error::new(ErrorKind::InvalidInput, "sys_fd convert invalid"))?;

        let mut file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });

        file.write_all(&[1])
    }
}

#[cfg(windows)]
mod sys {
    use std::io::{self, Error, ErrorKind, Write};
    use std::mem::ManuallyDrop;
    use std::net::TcpStream;
    use std::os::windows::io::{FromRawSocket, RawSocket};

    #[inline]
    pub(super) fn write_waker(fd: usize) -> io::Result<()> {
        let fd = RawSocket::try_from(fd)
            .map_err(|_e| Error::new(ErrorKind::InvalidInput, "sys_socket convert invalid"))?;

        let mut stream = ManuallyDrop::new(unsafe { TcpStream::from_raw_socket(fd) });

        stream.write_all(&[1])
    }
}

#[pyclass(name = "Waker", module = "pywebtransport._pywebtransport")]
pub(super) struct PyWaker {
    state: Arc<WakerState>,
}

#[pymethods]
impl PyWaker {
    #[new]
    #[pyo3(signature = (*, fd))]
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
    pub(super) fn clone_waker_callback(&self) -> Arc<dyn Fn() + Send + Sync> {
        let state = Arc::clone(&self.state);

        Arc::new(move || state.wake())
    }
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyWaker>()?;

    Ok(())
}

struct WakerState {
    fd: usize,
    signaled: AtomicBool,
}

impl WakerState {
    fn clear(&self) {
        self.signaled.store(false, Ordering::Release);
    }

    fn wake(&self) {
        if !self.signaled.swap(true, Ordering::AcqRel)
            && let Err(e) = sys::write_waker(self.fd)
            && e.kind() != ErrorKind::WouldBlock
        {
            debug!("rt_waker send failed err={e:?}");
        }
    }
}
