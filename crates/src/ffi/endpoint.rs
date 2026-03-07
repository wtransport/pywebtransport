//! FFI bindings for the WebTransport multiplexer and Tokio reactor proxy.

use std::fmt;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use quinn_proto::Endpoint as QuinnEndpoint;
use tracing::error;

use crate::common::config::{
    RustClientConfig, RustServerConfig, TransportConfig as WtTransportConfig,
};
use crate::ffi::abi;
use crate::ffi::waker::PyWaker;
use crate::protocol::events::ProtocolEvent;
use crate::runtime::channel::{IpcChannels, RuntimeCommand, RuntimeCommandTx, RuntimeEventRx};
use crate::runtime::reactor::Reactor;
use crate::transport::config::{build_client_config, build_endpoint_config, build_server_config};
use crate::transport::endpoint::TransportEndpoint;

// Registers the FFI classes to the parent Python module.
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEndpoint>()?;

    Ok(())
}

// FFI proxy for the threaded Tokio reactor.
#[pyclass(name = "Endpoint", module = "pywebtransport._wtransport")]
struct PyEndpoint {
    local_addrs: Vec<SocketAddr>,
    command_tx: RuntimeCommandTx,
    event_rx: RuntimeEventRx,
}

#[pymethods]
impl PyEndpoint {
    // Initializes the endpoint proxy and spawns the Tokio reactor background thread.
    #[new]
    #[pyo3(signature = (is_client, config, waker))]
    fn new(is_client: bool, config: &Bound<'_, PyAny>, waker: &PyWaker) -> PyResult<Self> {
        let (endpoint, std_sockets) = if is_client {
            let rust_config = extract_client_config(config)?;
            let quic_crypto = build_client_config(&rust_config)?;
            let endpoint_config = Arc::new(quinn_proto::EndpointConfig::default());
            let quinn_endpoint = QuinnEndpoint::new(endpoint_config, None, false, None);

            let transport_endpoint =
                TransportEndpoint::new_client(quinn_endpoint, rust_config.transport, quic_crypto)?;

            let mut sockets = Vec::new();

            if let Ok(s) = std::net::UdpSocket::bind("[::]:0") {
                s.set_nonblocking(true)?;
                sockets.push(s);
            }

            if let Ok(s) = std::net::UdpSocket::bind("0.0.0.0:0") {
                s.set_nonblocking(true)?;
                sockets.push(s);
            }

            if sockets.is_empty() {
                return Err(PyRuntimeError::new_err(
                    "Failed to bind any client sockets (IPv4 or IPv6)",
                ));
            }

            (transport_endpoint, sockets)
        } else {
            let rust_config = extract_server_config(config)?;
            let bind_addr_tuple = (rust_config.bind_host.clone(), rust_config.bind_port);
            let server_crypto = build_server_config(&rust_config)?;
            let endpoint_config = Arc::new(build_endpoint_config(&rust_config));
            let quinn_endpoint =
                QuinnEndpoint::new(endpoint_config, Some(Arc::new(server_crypto)), false, None);

            let transport_endpoint = TransportEndpoint::new_server(
                quinn_endpoint,
                rust_config.transport.clone(),
                rust_config,
            )?;

            let mut sockets = Vec::new();
            let mut bound_v4 = false;
            let mut bound_v6 = false;

            match bind_addr_tuple.to_socket_addrs() {
                Ok(addrs) => {
                    for addr in addrs {
                        if addr.is_ipv4() && bound_v4 {
                            continue;
                        }
                        if addr.is_ipv6() && bound_v6 {
                            continue;
                        }

                        if let Ok(s) = std::net::UdpSocket::bind(addr) {
                            s.set_nonblocking(true)?;
                            sockets.push(s);
                            if addr.is_ipv4() {
                                bound_v4 = true;
                            }
                            if addr.is_ipv6() {
                                bound_v6 = true;
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "DNS resolution failed for bind host: {e}"
                    )));
                }
            }

            if sockets.is_empty() {
                return Err(PyRuntimeError::new_err(format!(
                    "Failed to bind server to any resolved address for {}:{}",
                    bind_addr_tuple.0, bind_addr_tuple.1
                )));
            }

            (transport_endpoint, sockets)
        };

        let local_addrs: Vec<SocketAddr> = std_sockets
            .iter()
            .filter_map(|s| s.local_addr().ok())
            .collect();

        let IpcChannels {
            command_tx,
            command_rx,
            event_tx,
            event_rx,
        } = IpcChannels::new().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to initialize IPC channels: {e}"))
        })?;

        let waker_callback = waker.clone_waker_callback();

        std::thread::Builder::new()
            .name("wtransport-reactor".into())
            .spawn(move || {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => {
                        rt.block_on(async move {
                            let mut tokio_sockets = Vec::new();

                            for s in std_sockets {
                                match tokio::net::UdpSocket::from_std(s) {
                                    Ok(ts) => tokio_sockets.push(ts),
                                    Err(e) => error!("Failed to convert std socket to tokio: {e}"),
                                }
                            }

                            if tokio_sockets.is_empty() {
                                error!("No valid tokio sockets could be created.");
                                return;
                            }

                            let reactor = Reactor::new(
                                command_rx,
                                endpoint,
                                event_tx,
                                tokio_sockets,
                                waker_callback,
                            )
                            .unwrap_or_else(|e| {
                                error!("Failed to initialize the Tokio reactor: {e}");
                                std::process::exit(1);
                            });

                            reactor.run().await;
                        });
                    }
                    Err(e) => {
                        error!("Failed to build Tokio runtime: {e}");
                    }
                }
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to spawn reactor thread: {e}")))?;

        Ok(Self {
            local_addrs,
            command_tx,
            event_rx,
        })
    }

    // Dispatches a graceful shutdown instruction to the underlying Tokio reactor.
    #[pyo3(signature = ())]
    fn close(&self) {
        self.command_tx.try_send(RuntimeCommand::Shutdown).ok();
    }

    // Dispatches an outbound connection creation command to the reactor.
    #[pyo3(signature = (request_id, remote, server_name))]
    fn connect(
        &self,
        request_id: u64,
        remote: &Bound<'_, PyAny>,
        server_name: String,
    ) -> PyResult<()> {
        if self.event_rx.len() > (self.event_rx.capacity() * 9) / 10 {
            return Err(PyRuntimeError::new_err(
                "IPC event queue is dangerously full. The Python event loop is severely lagging. Please yield.",
            ));
        }

        let remote_addr = extract_socket_addr(remote)?;

        self.command_tx
            .try_send(RuntimeCommand::CreateConnection {
                request_id,
                remote: remote_addr,
                server_name,
            })
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to dispatch command to reactor: {e}"))
            })?;

        Ok(())
    }

    // Forwards a Python-triggered protocol event down to the Quic endpoint state machine.
    #[pyo3(signature = (handle, event))]
    fn handle_user_event(&self, handle: usize, event: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.event_rx.len() > (self.event_rx.capacity() * 9) / 10 {
            return Err(PyRuntimeError::new_err(
                "IPC event queue is dangerously full. The Python event loop is severely lagging. Please yield.",
            ));
        }

        let protocol_event: ProtocolEvent = event.extract()?;
        let conn_handle = quinn_proto::ConnectionHandle(handle);

        self.command_tx
            .try_send(RuntimeCommand::Protocol {
                handle: conn_handle,
                event: protocol_event,
            })
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to dispatch protocol event: {e}"))
            })?;

        Ok(())
    }

    // Retrieves the bound IP addresses and ports of the active UDP sockets.
    #[pyo3(name = "get_local_addresses", signature = ())]
    fn local_addresses(&self) -> Vec<(String, u16)> {
        self.local_addrs
            .iter()
            .map(|addr| (addr.ip().to_string(), addr.port()))
            .collect()
    }

    // Drains the IPC event queue and returns all pending network events to Python.
    #[pyo3(signature = ())]
    fn poll_runtime_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let py_list = PyList::empty(py);

        while let Some(event) = self.event_rx.pop() {
            match event {
                crate::runtime::channel::RuntimeEvent::CommandCompleted {
                    request_id,
                    handle,
                    remote_address,
                } => {
                    let addr_tuple = PyTuple::new(
                        py,
                        &[
                            remote_address
                                .ip()
                                .to_string()
                                .into_pyobject(py)?
                                .into_any(),
                            remote_address.port().into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    let payload = PyTuple::new(
                        py,
                        &[
                            request_id.into_pyobject(py)?.into_any(),
                            handle.0.into_pyobject(py)?.into_any(),
                            addr_tuple.into_any(),
                        ],
                    )?;
                    let event_tuple = PyTuple::new(
                        py,
                        &[
                            abi::COMMAND_COMPLETED.into_pyobject(py)?.into_any(),
                            payload.into_any(),
                        ],
                    )?;

                    py_list.append(event_tuple)?;
                }
                crate::runtime::channel::RuntimeEvent::CommandFailed {
                    request_id,
                    error_code,
                    reason,
                } => {
                    let payload = PyTuple::new(
                        py,
                        &[
                            request_id.into_pyobject(py)?.into_any(),
                            error_code.into_pyobject(py)?.into_any(),
                            reason.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    let event_tuple = PyTuple::new(
                        py,
                        &[
                            abi::COMMAND_FAILED.into_pyobject(py)?.into_any(),
                            payload.into_any(),
                        ],
                    )?;

                    py_list.append(event_tuple)?;
                }
                crate::runtime::channel::RuntimeEvent::ConnectionEffects { handle, effects } => {
                    let payload = PyTuple::new(
                        py,
                        &[
                            handle.0.into_pyobject(py)?.into_any(),
                            effects.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    let event_tuple = PyTuple::new(
                        py,
                        &[
                            abi::CONNECTION_EFFECTS.into_pyobject(py)?.into_any(),
                            payload.into_any(),
                        ],
                    )?;

                    py_list.append(event_tuple)?;
                }
                crate::runtime::channel::RuntimeEvent::ConnectionSpawned {
                    handle,
                    remote_address,
                    effects,
                } => {
                    let addr_tuple = PyTuple::new(
                        py,
                        &[
                            remote_address
                                .ip()
                                .to_string()
                                .into_pyobject(py)?
                                .into_any(),
                            remote_address.port().into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    let payload = PyTuple::new(
                        py,
                        &[
                            handle.0.into_pyobject(py)?.into_any(),
                            addr_tuple.into_any(),
                            effects.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    let event_tuple = PyTuple::new(
                        py,
                        &[
                            abi::CONNECTION_SPAWNED.into_pyobject(py)?.into_any(),
                            payload.into_any(),
                        ],
                    )?;

                    py_list.append(event_tuple)?;
                }
                crate::runtime::channel::RuntimeEvent::ReactorShutDown => {
                    let payload = PyTuple::empty(py);
                    let event_tuple = PyTuple::new(
                        py,
                        &[
                            abi::REACTOR_SHUTDOWN.into_pyobject(py)?.into_any(),
                            payload.into_any(),
                        ],
                    )?;

                    py_list.append(event_tuple)?;
                }
            }
        }

        Ok(py_list)
    }
}

impl fmt::Debug for PyEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyEndpoint")
            .field("proxy_state", &"Active")
            .finish()
    }
}

// Extracts and constructs the client configuration.
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
    let connection_attempt_delay_f64: f64 =
        config.getattr("connection_attempt_delay")?.extract()?;
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
        connection_attempt_delay: Duration::from_secs_f64(connection_attempt_delay_f64.max(0.0)),
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

// Extracts and constructs the server configuration.
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

// Extracts a network address tuple into a socket address.
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

// Extracts the nested transport sub-configuration.
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

// Extracts the certificate verification mode.
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
