//! FFI bindings for the WebTransport multiplexer and Tokio reactor proxy.

use std::fmt;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::mpsc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use quinn_proto::Endpoint as QuinnEndpoint;
use tracing::debug;

use crate::common::config::{RustClientConfig, RustServerConfig};
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
#[pyclass(name = "Endpoint", module = "pywebtransport._pywebtransport")]
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
    fn new(
        py: Python<'_>,
        is_client: bool,
        config: &Bound<'_, PyAny>,
        waker: &PyWaker,
    ) -> PyResult<Self> {
        let (endpoint, std_sockets) = if is_client {
            let rust_config = RustClientConfig::try_from(config)?;
            let quic_config = build_client_config(&rust_config)?;
            let endpoint_config = Arc::new(quinn_proto::EndpointConfig::default());
            let quinn_endpoint = QuinnEndpoint::new(endpoint_config, None, false, None);

            let transport_endpoint =
                TransportEndpoint::new_client(quinn_endpoint, rust_config.base, quic_config)?;

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
                return Err(PyRuntimeError::new_err("sys_socket create failed"));
            }

            (transport_endpoint, sockets)
        } else {
            let rust_config = RustServerConfig::try_from(config)?;
            let bind_addr_tuple = (rust_config.bind_host.clone(), rust_config.bind_port);
            let server_crypto = build_server_config(&rust_config)?;
            let endpoint_config = Arc::new(build_endpoint_config(&rust_config));
            let quinn_endpoint =
                QuinnEndpoint::new(endpoint_config, Some(Arc::new(server_crypto)), false, None);

            let transport_endpoint = TransportEndpoint::new_server(
                quinn_endpoint,
                rust_config.base.clone(),
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
                        "hostname resolve failed err={e}"
                    )));
                }
            }

            if sockets.is_empty() {
                return Err(PyRuntimeError::new_err("sys_socket create failed"));
            }

            (transport_endpoint, sockets)
        };

        let local_addrs: Vec<SocketAddr> = std_sockets
            .iter()
            .filter_map(|s| s.local_addr().ok())
            .collect();

        let IpcChannels {
            command_rx,
            command_tx,
            event_rx,
            event_tx,
        } = IpcChannels::new();

        let waker_callback = waker.clone_waker_callback();
        let (tx, rx) = mpsc::sync_channel::<Result<(), String>>(0);

        std::thread::Builder::new()
            .name("pywebtransport-reactor".into())
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
                                    Err(e) => {
                                        debug!("sys_socket convert failed err={e:?}");
                                    }
                                }
                            }

                            if tokio_sockets.is_empty() {
                                debug!("sys_socket convert failed");
                                tx.send(Err("sys_socket convert failed".into())).ok();
                                return;
                            }

                            let reactor = Reactor::new(
                                command_rx,
                                endpoint,
                                event_tx,
                                tokio_sockets,
                                waker_callback,
                            );

                            tx.send(Ok(())).ok();

                            reactor.run().await;
                        });
                    }
                    Err(e) => {
                        debug!("rt create failed err={e:?}");
                        tx.send(Err(format!("rt create failed err={e}"))).ok();
                    }
                }
            })
            .map_err(|e| PyRuntimeError::new_err(format!("sys_thread create failed err={e}")))?;

        py.detach(move || rx.recv())
            .unwrap_or_else(|e| Err(format!("rt resolve failed err={e}")))
            .map_err(PyRuntimeError::new_err)?;

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
            return Err(PyRuntimeError::new_err("rt_channel validate exceeded"));
        }

        let remote_addr = extract_socket_addr(remote)?;

        self.command_tx
            .try_send(RuntimeCommand::CreateConnection {
                request_id,
                remote_address: remote_addr,
                server_name,
            })
            .map_err(|e| PyRuntimeError::new_err(format!("rt_event send failed err={e}")))?;

        Ok(())
    }

    // Forwards a Python-triggered protocol event down to the Quic endpoint state machine.
    #[pyo3(signature = (handle, event))]
    fn handle_user_event(&self, handle: usize, event: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.event_rx.len() > (self.event_rx.capacity() * 9) / 10 {
            return Err(PyRuntimeError::new_err("rt_channel validate exceeded"));
        }

        let protocol_event: ProtocolEvent = event.extract()?;
        let conn_handle = quinn_proto::ConnectionHandle(handle);

        self.command_tx
            .try_send(RuntimeCommand::Protocol {
                handle: conn_handle,
                event: protocol_event,
            })
            .map_err(|e| PyRuntimeError::new_err(format!("rt_event send failed err={e}")))?;

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

// Extracts a network address tuple into a socket address.
fn extract_socket_addr(ob: &Bound<'_, PyAny>) -> PyResult<SocketAddr> {
    if let Ok((ip_str, port, flowinfo, scope_id)) = ob.extract::<(String, u16, u32, u32)>() {
        let ip: std::net::Ipv6Addr = ip_str
            .parse()
            .map_err(|e| PyValueError::new_err(format!("ipv6_address convert failed err={e}")))?;

        Ok(SocketAddr::V6(std::net::SocketAddrV6::new(
            ip, port, flowinfo, scope_id,
        )))
    } else if let Ok((ip_str, port)) = ob.extract::<(String, u16)>() {
        let ip = ip_str
            .parse()
            .map_err(|e| PyValueError::new_err(format!("ip_address convert failed err={e}")))?;

        Ok(SocketAddr::new(ip, port))
    } else {
        Err(PyValueError::new_err("socket_address validate invalid"))
    }
}
