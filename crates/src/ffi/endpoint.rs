//! FFI bindings for the WebTransport transport multiplexer and state machine.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use quinn_proto::{ConnectionHandle, Endpoint as QuinnEndpoint, Transmit};

use crate::common::config::{
    RustClientConfig, RustServerConfig, TransportConfig as WtTransportConfig,
};
use crate::ffi::abi;
use crate::ffi::conversion::extract_bytes;
use crate::protocol::events::ProtocolEvent;
use crate::transport::config::{build_client_config, build_endpoint_config, build_server_config};
use crate::transport::endpoint::{EndpointEvent, TransportEndpoint};

// Registers the endpoint FFI classes to the parent Python module for initialization.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEndpoint>()?;

    Ok(())
}

/// Python wrapper for the WebTransport endpoint scheduling state machine.
#[pyclass(name = "Endpoint", module = "pywebtransport._wtransport", unsendable)]
struct PyEndpoint {
    inner: TransportEndpoint,
    time_mapper: TimeMapper,
}

impl fmt::Debug for PyEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyEndpoint")
            .field("time_mapper", &self.time_mapper)
            .finish_non_exhaustive()
    }
}

#[pymethods]
impl PyEndpoint {
    #[new]
    #[pyo3(signature = (is_client, config, now))]
    fn new(is_client: bool, config: &Bound<'_, PyAny>, now: f64) -> PyResult<Self> {
        let inner = if is_client {
            let rust_config = extract_client_config(config)?;
            let quic_crypto = build_client_config(&rust_config)?;
            let endpoint_config = Arc::new(quinn_proto::EndpointConfig::default());
            let quinn_endpoint = QuinnEndpoint::new(endpoint_config, None, false, None);

            TransportEndpoint::new_client(quinn_endpoint, rust_config.transport, quic_crypto)?
        } else {
            let rust_config = extract_server_config(config)?;
            let server_crypto = build_server_config(&rust_config)?;
            let endpoint_config = Arc::new(build_endpoint_config(&rust_config));
            let quinn_endpoint =
                QuinnEndpoint::new(endpoint_config, Some(Arc::new(server_crypto)), false, None);

            TransportEndpoint::new_server(
                quinn_endpoint,
                rust_config.transport.clone(),
                rust_config,
            )?
        };
        let time_mapper = TimeMapper::new(now);

        Ok(Self { inner, time_mapper })
    }

    #[pyo3(signature = (remote, server_name, now))]
    fn connect(
        &mut self,
        remote: &Bound<'_, PyAny>,
        server_name: &str,
        now: f64,
    ) -> PyResult<usize> {
        let remote_addr = extract_socket_addr(remote)?;
        let now_instant = self.time_mapper.resolve_instant(now);

        let handle = self.inner.connect(remote_addr, server_name, now_instant)?;

        Ok(handle.0)
    }

    #[pyo3(signature = (data, remote, local, now))]
    fn handle_datagram<'py>(
        &mut self,
        py: Python<'py>,
        data: &Bound<'_, PyAny>,
        remote: &Bound<'_, PyAny>,
        local: Option<&Bound<'_, PyAny>>,
        now: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes_data = extract_bytes(data)?;
        let bytes_mut = BytesMut::from(bytes_data.as_ref());
        let remote_addr = extract_socket_addr(remote)?;
        let local_addr = match local {
            Some(ob) => Some(extract_socket_addr(ob)?),
            None => None,
        };
        let now_instant = self.time_mapper.resolve_instant(now);

        let event =
            self.inner
                .handle_datagram(bytes_mut, remote_addr, local_addr, now, now_instant);

        Ok(map_endpoint_event(py, event, self.inner.transmit_workspace())?.into_any())
    }

    #[pyo3(signature = (now))]
    fn handle_timeout<'py>(&mut self, py: Python<'py>, now: f64) -> PyResult<Bound<'py, PyAny>> {
        let now_instant = self.time_mapper.resolve_instant(now);

        let results = self.inner.handle_timeout(now_instant, now);
        let list = PyList::empty(py);

        for (handle, effects) in results {
            let item = PyTuple::new(
                py,
                &[
                    handle.0.into_pyobject(py)?.into_any(),
                    effects.into_pyobject(py)?.into_any(),
                ],
            )?;

            list.append(item)?;
        }

        Ok(list.into_any())
    }

    #[pyo3(signature = (handle, event, now))]
    fn handle_user_event<'py>(
        &mut self,
        py: Python<'py>,
        handle: usize,
        event: &Bound<'_, PyAny>,
        now: f64,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let protocol_event: ProtocolEvent = event.extract()?;
        let now_instant = self.time_mapper.resolve_instant(now);
        let conn_handle = ConnectionHandle(handle);

        let Some(endpoint_event) =
            self.inner
                .handle_user_event(conn_handle, protocol_event, now, now_instant)
        else {
            return Ok(None);
        };

        Ok(Some(
            map_endpoint_event(py, endpoint_event, self.inner.transmit_workspace())?.into_any(),
        ))
    }

    #[pyo3(signature = (now))]
    fn poll_transmit<'py>(
        &mut self,
        py: Python<'py>,
        now: f64,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let now_instant = self.time_mapper.resolve_instant(now);

        let Some(transmit) = self.inner.poll_transmit(now_instant) else {
            return Ok(None);
        };

        let result = map_endpoint_event(
            py,
            EndpointEvent::Transmit(transmit),
            self.inner.transmit_workspace(),
        )?;

        Ok(Some(result.into_any()))
    }

    #[pyo3(name = "get_remote_address", signature = (handle))]
    fn remote_address(&self, handle: usize) -> Option<(String, u16)> {
        let conn_handle = ConnectionHandle(handle);

        self.inner
            .remote_address(conn_handle)
            .map(|addr| (addr.ip().to_string(), addr.port()))
    }

    #[pyo3(signature = ())]
    fn timeout(&mut self) -> Option<f64> {
        let t = self.inner.timeout()?;

        Some(self.time_mapper.resolve_py_time(t))
    }
}

// Synchronizes Python's monotonic event loop time with Rust's Instant.
#[derive(Debug)]
struct TimeMapper {
    base_instant: Instant,
    base_py_time: f64,
    last_instant: Instant,
}

impl TimeMapper {
    fn new(py_time: f64) -> Self {
        let base_instant = Instant::now();

        Self {
            base_instant,
            base_py_time: py_time,
            last_instant: base_instant,
        }
    }

    fn resolve_instant(&mut self, py_time: f64) -> Instant {
        let delta = py_time - self.base_py_time;

        let instant = if delta >= 0.0 {
            self.base_instant + Duration::from_secs_f64(delta)
        } else {
            self.base_instant
                .checked_sub(Duration::from_secs_f64(-delta))
                .unwrap_or(self.base_instant)
        };

        self.last_instant = self.last_instant.max(instant);

        self.last_instant
    }

    fn resolve_py_time(&self, instant: Instant) -> f64 {
        if instant >= self.base_instant {
            self.base_py_time + instant.duration_since(self.base_instant).as_secs_f64()
        } else {
            self.base_py_time - self.base_instant.duration_since(instant).as_secs_f64()
        }
    }
}

// Constructs pure Rust client configuration with robust absence handling.
fn extract_client_config(config: &Bound<'_, PyAny>) -> PyResult<RustClientConfig> {
    let ca_certs = config
        .getattr("ca_certs")
        .ok()
        .and_then(|v| v.extract::<String>().ok())
        .map(PathBuf::from);
    let certfile = config
        .getattr("certfile")
        .ok()
        .and_then(|v| v.extract::<String>().ok())
        .map(PathBuf::from);
    let connect_timeout_f64: f64 = config.getattr("connect_timeout")?.extract()?;
    let headers_attr = config.getattr("headers")?;
    let keyfile = config
        .getattr("keyfile")
        .ok()
        .and_then(|v| v.extract::<String>().ok())
        .map(PathBuf::from);
    let max_connection_retries: u64 = config.getattr("max_connection_retries")?.extract()?;
    let max_retry_delay_f64: f64 = config.getattr("max_retry_delay")?.extract()?;
    let retry_backoff: f64 = config.getattr("retry_backoff")?.extract()?;
    let retry_delay_f64: f64 = config.getattr("retry_delay")?.extract()?;
    let transport = extract_transport_config(config)?;
    let user_agent = config
        .getattr("user_agent")
        .ok()
        .and_then(|v| v.extract::<String>().ok());
    let verify_mode_attr = config.getattr("verify_mode")?;

    let headers =
        if let Ok(hash_map) = headers_attr.extract::<std::collections::HashMap<String, String>>() {
            hash_map.into_iter().collect()
        } else {
            headers_attr
                .extract::<Vec<(String, String)>>()
                .unwrap_or_default()
        };
    let verify_server_certificate = extract_verify_mode(&verify_mode_attr, true)?;

    Ok(RustClientConfig {
        ca_certs,
        certfile,
        connect_timeout: Duration::from_secs_f64(connect_timeout_f64.max(0.0)),
        headers,
        keyfile,
        max_connection_retries,
        max_retry_delay: Duration::from_secs_f64(max_retry_delay_f64.max(0.0)),
        retry_backoff,
        retry_delay: Duration::from_secs_f64(retry_delay_f64.max(0.0)),
        transport,
        user_agent,
        verify_server_certificate,
    })
}

// Constructs pure Rust server configuration from dynamic Python properties.
fn extract_server_config(config: &Bound<'_, PyAny>) -> PyResult<RustServerConfig> {
    let bind_host: String = config.getattr("bind_host")?.extract()?;
    let bind_port: u16 = config.getattr("bind_port")?.extract()?;
    let certfile_str: String = config.getattr("certfile")?.extract()?;
    let keyfile_str: String = config.getattr("keyfile")?.extract()?;
    let transport = extract_transport_config(config)?;
    let verify_mode_attr = config.getattr("verify_mode")?;

    let enable_stateless_retry = config
        .getattr("enable_stateless_retry")
        .and_then(|v| v.extract::<bool>())
        .unwrap_or(true);
    let require_client_auth = extract_verify_mode(&verify_mode_attr, false)?;

    Ok(RustServerConfig {
        bind_host,
        bind_port,
        certfile: PathBuf::from(certfile_str),
        enable_stateless_retry,
        keyfile: PathBuf::from(keyfile_str),
        require_client_auth,
        transport,
    })
}

// Extracts a SocketAddr using PyO3 generic extraction for maximum safety.
fn extract_socket_addr(ob: &Bound<'_, PyAny>) -> PyResult<SocketAddr> {
    if let Ok((ip_str, port, flowinfo, scope_id)) = ob.extract::<(String, u16, u32, u32)>() {
        let ip: std::net::Ipv6Addr = ip_str
            .parse()
            .map_err(|e| PyValueError::new_err(format!("Invalid IPv6 address: {e}")))?;

        Ok(SocketAddr::V6(std::net::SocketAddrV6::new(
            ip, port, flowinfo, scope_id,
        )))
    } else if let Ok((ip_str, port)) = ob.extract::<(String, u16)>() {
        let ip = ip_str
            .parse()
            .map_err(|e| PyValueError::new_err(format!("Invalid IP address: {e}")))?;

        Ok(SocketAddr::new(ip, port))
    } else {
        Err(PyValueError::new_err(
            "Expected a tuple of (ip_str, port) or (ip_str, port, flowinfo, scope_id) for network address",
        ))
    }
}

// Safely dismantles the FFI type barrier for transport sub-configuration.
fn extract_transport_config(config: &Bound<'_, PyAny>) -> PyResult<WtTransportConfig> {
    let t_opt = config.getattr("transport").ok();
    let source = if let Some(tr) = &t_opt { tr } else { config };

    let alpn_protocols: Vec<String> = source.getattr("alpn_protocols")?.extract()?;
    let close_timeout_f64: f64 = source.getattr("close_timeout")?.extract()?;
    let congestion_control_algorithm: String =
        source.getattr("congestion_control_algorithm")?.extract()?;
    let connection_idle_timeout_f64: f64 = source.getattr("connection_idle_timeout")?.extract()?;
    let flow_control_window_auto_scale: bool = source
        .getattr("flow_control_window_auto_scale")?
        .extract()?;
    let flow_control_window_size: u64 = source.getattr("flow_control_window_size")?.extract()?;
    let initial_max_data: u64 = source.getattr("initial_max_data")?.extract()?;
    let initial_max_streams_bidi: u64 = source.getattr("initial_max_streams_bidi")?.extract()?;
    let initial_max_streams_uni: u64 = source.getattr("initial_max_streams_uni")?.extract()?;
    let keep_alive_opt: Option<f64> = source.getattr("keep_alive")?.extract()?;
    let max_capsule_size: u64 = source.getattr("max_capsule_size")?.extract()?;
    let max_connections: u64 = source.getattr("max_connections")?.extract()?;
    let max_datagram_size: u64 = source.getattr("max_datagram_size")?.extract()?;
    let max_event_history_size: u64 = source.getattr("max_event_history_size")?.extract()?;
    let max_event_listeners: u64 = source.getattr("max_event_listeners")?.extract()?;
    let max_event_queue_size: u64 = source.getattr("max_event_queue_size")?.extract()?;
    let max_message_size: u64 = source.getattr("max_message_size")?.extract()?;
    let max_pending_events_per_session: u64 = source
        .getattr("max_pending_events_per_session")?
        .extract()?;
    let max_sessions: u64 = source.getattr("max_sessions")?.extract()?;
    let max_stream_read_buffer: u64 = source.getattr("max_stream_read_buffer")?.extract()?;
    let max_stream_write_buffer: u64 = source.getattr("max_stream_write_buffer")?.extract()?;
    let max_total_pending_events: u64 = source.getattr("max_total_pending_events")?.extract()?;
    let pending_event_ttl_f64: f64 = source.getattr("pending_event_ttl")?.extract()?;
    let read_timeout_attr = source.getattr("read_timeout")?;
    let resource_cleanup_interval_f64: f64 =
        source.getattr("resource_cleanup_interval")?.extract()?;
    let stream_creation_timeout_f64: f64 = source.getattr("stream_creation_timeout")?.extract()?;
    let transport_streams_cap: u64 = source.getattr("transport_streams_cap")?.extract()?;
    let write_timeout_attr = source.getattr("write_timeout")?;

    let read_timeout_opt = if read_timeout_attr.is_none() {
        None
    } else {
        Some(read_timeout_attr.extract::<f64>()?)
    };
    let write_timeout_opt = if write_timeout_attr.is_none() {
        None
    } else {
        Some(write_timeout_attr.extract::<f64>()?)
    };

    Ok(WtTransportConfig {
        alpn_protocols,
        close_timeout: Duration::from_secs_f64(close_timeout_f64.max(0.0)),
        congestion_control_algorithm,
        connection_idle_timeout: Duration::from_secs_f64(connection_idle_timeout_f64.max(0.0)),
        flow_control_window_auto_scale,
        flow_control_window_size,
        initial_max_data,
        initial_max_streams_bidi,
        initial_max_streams_uni,
        keep_alive: keep_alive_opt.map(|f| Duration::from_secs_f64(f.max(0.0))),
        max_capsule_size,
        max_connections,
        max_datagram_size,
        max_event_history_size,
        max_event_listeners,
        max_event_queue_size,
        max_message_size,
        max_pending_events_per_session,
        max_sessions,
        max_stream_read_buffer,
        max_stream_write_buffer,
        max_total_pending_events,
        pending_event_ttl: Duration::from_secs_f64(pending_event_ttl_f64.max(0.0)),
        read_timeout: read_timeout_opt.map(|f| Duration::from_secs_f64(f.max(0.0))),
        resource_cleanup_interval: Duration::from_secs_f64(resource_cleanup_interval_f64.max(0.0)),
        stream_creation_timeout: Duration::from_secs_f64(stream_creation_timeout_f64.max(0.0)),
        transport_streams_cap,
        write_timeout: write_timeout_opt.map(|f| Duration::from_secs_f64(f.max(0.0))),
    })
}

// Extracts verify mode from Python object with default value fallback.
fn extract_verify_mode(attr: &Bound<'_, PyAny>, default_val: bool) -> PyResult<bool> {
    if attr.is_none() {
        Ok(default_val)
    } else {
        let mode_int = attr.extract::<i32>().map_err(|e| {
            PyValueError::new_err(format!(
                "verify_mode must be an integer (e.g. ssl.CERT_NONE): {e}"
            ))
        })?;

        Ok(mode_int != 0)
    }
}

// Maps a terminal endpoint event to an ABI tagged Python tuple.
fn map_endpoint_event<'py>(
    py: Python<'py>,
    event: EndpointEvent,
    workspace: &[u8],
) -> PyResult<Bound<'py, PyTuple>> {
    match event {
        EndpointEvent::ConnectionEffects { handle, effects } => {
            let payload = PyTuple::new(
                py,
                &[
                    handle.0.into_pyobject(py)?.into_any(),
                    effects.into_pyobject(py)?.into_any(),
                ],
            )?;

            PyTuple::new(
                py,
                &[
                    abi::CONNECTION_EFFECTS.into_pyobject(py)?.into_any(),
                    payload.into_any(),
                ],
            )
        }
        EndpointEvent::ConnectionSpawned { handle, effects } => {
            let payload = PyTuple::new(
                py,
                &[
                    handle.0.into_pyobject(py)?.into_any(),
                    effects.into_pyobject(py)?.into_any(),
                ],
            )?;

            PyTuple::new(
                py,
                &[
                    abi::CONNECTION_SPAWNED.into_pyobject(py)?.into_any(),
                    payload.into_any(),
                ],
            )
        }
        EndpointEvent::Consumed => {
            let payload = PyTuple::empty(py);

            PyTuple::new(
                py,
                &[
                    abi::CONSUMED.into_pyobject(py)?.into_any(),
                    payload.into_any(),
                ],
            )
        }
        EndpointEvent::Transmit(transmit) => {
            let payload = map_transmit(py, &transmit, workspace)?;

            PyTuple::new(
                py,
                &[
                    abi::TRANSMIT.into_pyobject(py)?.into_any(),
                    payload.into_any(),
                ],
            )
        }
    }
}

// Extracts zero-copy network payload slices mapping transmission metadata into a tuple.
fn map_transmit<'py>(
    py: Python<'py>,
    transmit: &Transmit,
    workspace: &[u8],
) -> PyResult<Bound<'py, PyTuple>> {
    let ip_str = transmit.destination.ip().to_string();
    let port = transmit.destination.port();

    let dest_tuple = PyTuple::new(
        py,
        &[
            ip_str.into_pyobject(py)?.into_any(),
            port.into_pyobject(py)?.into_any(),
        ],
    )?;
    let payload = workspace
        .get(0..transmit.size)
        .ok_or_else(|| PyValueError::new_err("Transmit size exceeds workspace capacity"))?;

    PyTuple::new(
        py,
        &[dest_tuple.into_any(), PyBytes::new(py, payload).into_any()],
    )
}
