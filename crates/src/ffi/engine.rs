//! FFI bindings for WebTransport protocol engine.

use std::cell::RefCell;

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::common::types::StreamId;
use crate::ffi::conversion::{effects_to_py, extract_bytes_or_list, extract_headers};
use crate::protocol::engine::WebTransportEngine as InnerEngine;
use crate::protocol::events::ProtocolEvent;

// Python module registration.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<WebTransportEngine>()?;

    Ok(())
}

// Python wrapper for Rust WebTransportEngine.
#[pyclass(
    name = "WebTransportEngine",
    module = "pywebtransport._wtransport",
    unsendable
)]
struct WebTransportEngine {
    inner: RefCell<InnerEngine>,
}

#[pymethods]
impl WebTransportEngine {
    // Engine initialization with configuration extraction.
    #[new]
    #[pyo3(signature = (connection_id, config, is_client))]
    fn new(connection_id: String, config: &Bound<'_, PyAny>, is_client: bool) -> PyResult<Self> {
        let max_datagram_size = config.getattr("max_datagram_size")?.extract::<u64>()?;
        let flow_control_window_size = config
            .getattr("flow_control_window_size")?
            .extract::<u64>()?;
        let max_sessions = config.getattr("max_sessions")?.extract::<u64>()?;
        let initial_max_data = config.getattr("initial_max_data")?.extract::<u64>()?;
        let initial_max_streams_bidi = config
            .getattr("initial_max_streams_bidi")?
            .extract::<u64>()?;
        let initial_max_streams_uni = config
            .getattr("initial_max_streams_uni")?
            .extract::<u64>()?;
        let stream_read_buffer_size = config.getattr("max_stream_read_buffer")?.extract::<u64>()?;
        let stream_write_buffer_size = config
            .getattr("max_stream_write_buffer")?
            .extract::<u64>()?;
        let flow_control_window_auto_scale = config
            .getattr("flow_control_window_auto_scale")?
            .extract::<bool>()?;
        let max_capsule_size = config.getattr("max_capsule_size")?.extract::<u64>()?;

        let inner = InnerEngine::new(
            connection_id,
            is_client,
            max_datagram_size,
            flow_control_window_size,
            max_sessions,
            initial_max_data,
            initial_max_streams_bidi,
            initial_max_streams_uni,
            stream_read_buffer_size,
            stream_write_buffer_size,
            flow_control_window_auto_scale,
            max_capsule_size,
        )?;

        Ok(WebTransportEngine {
            inner: RefCell::new(inner),
        })
    }

    // H3 stream state cleanup.
    fn cleanup_stream(&self, stream_id: StreamId) {
        self.inner.borrow_mut().cleanup_stream(stream_id);
    }

    // Capsule encoding and effect generation.
    #[staticmethod]
    #[pyo3(signature = (stream_id, capsule_type, capsule_data, end_stream=false))]
    fn encode_capsule(
        py: Python<'_>,
        stream_id: StreamId,
        capsule_type: u64,
        capsule_data: &Bound<'_, PyBytes>,
        end_stream: bool,
    ) -> PyResult<Py<PyAny>> {
        let bytes_data = Bytes::copy_from_slice(capsule_data.as_bytes());
        let effects = InnerEngine::encode_capsule(stream_id, capsule_type, bytes_data, end_stream)?;

        effects_to_py(py, effects)
    }

    // Datagram encoding and effect generation.
    #[staticmethod]
    fn encode_datagram(
        py: Python<'_>,
        stream_id: StreamId,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let bytes_data = extract_bytes_or_list(data)?;
        let effects = InnerEngine::encode_datagram(stream_id, &bytes_data)?;

        effects_to_py(py, effects)
    }

    // GOAWAY frame encoding and effect generation.
    fn encode_goaway(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let effects = self.inner.borrow_mut().encode_goaway();

        effects_to_py(py, effects)
    }

    // Headers encoding and effect generation.
    #[pyo3(signature = (stream_id, status, end_stream=false))]
    fn encode_headers(
        &self,
        py: Python<'_>,
        stream_id: StreamId,
        status: u16,
        end_stream: bool,
    ) -> PyResult<Py<PyAny>> {
        let effects = self
            .inner
            .borrow_mut()
            .encode_headers(stream_id, status, end_stream)?;

        effects_to_py(py, effects)
    }

    // Session establishment request encoding.
    fn encode_session_request(
        &self,
        py: Python<'_>,
        stream_id: StreamId,
        path: String,
        authority: String,
        headers: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let headers_vec = extract_headers(headers)?;
        let effects = self.inner.borrow_mut().encode_session_request(
            stream_id,
            path,
            authority,
            &headers_vec,
        )?;

        effects_to_py(py, effects)
    }

    // Stream creation preamble encoding.
    fn encode_stream_creation(
        &self,
        py: Python<'_>,
        stream_id: StreamId,
        control_stream_id: StreamId,
        is_unidirectional: bool,
    ) -> PyResult<Py<PyAny>> {
        let effects = self.inner.borrow_mut().encode_stream_creation(
            stream_id,
            control_stream_id,
            is_unidirectional,
        );

        effects_to_py(py, effects)
    }

    // Protocol event handling.
    fn handle_event(&self, py: Python<'_>, event: ProtocolEvent, now: f64) -> PyResult<Py<PyAny>> {
        let effects = self.inner.borrow_mut().handle_event(event, now);

        effects_to_py(py, effects)
    }

    // HTTP/3 transport initialization.
    fn initialize_h3_transport(
        &self,
        py: Python<'_>,
        control_id: StreamId,
        encoder_id: StreamId,
        decoder_id: StreamId,
    ) -> PyResult<Py<PyAny>> {
        let effects = self
            .inner
            .borrow_mut()
            .initialize_h3_transport(control_id, encoder_id, decoder_id)?;

        effects_to_py(py, effects)
    }
}
