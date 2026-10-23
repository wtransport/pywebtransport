//! FFI bindings for the endpoint.

use std::fmt::{self, Debug, Formatter};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6, ToSocketAddrs, UdpSocket};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{Builder, JoinHandle};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use quinn_proto::{ConnectionHandle as QuicConnectionHandle, Endpoint as QuicEndpoint};
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::runtime::Builder as TokioBuilder;
use tracing::debug;

use crate::common::config::{RustClientConfig, RustServerConfig};
use crate::ffi::waker::PyWaker;
use crate::protocol::events::ProtocolEvent;
use crate::runtime::channel::{RuntimeChannels, RuntimeCommand, RuntimeCommandTx, RuntimeEventRx};
use crate::runtime::reactor::Reactor;
use crate::transport::config;
use crate::transport::endpoint::TransportEndpoint;

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyEndpoint>()?;

    Ok(())
}

#[pyclass(name = "Endpoint", module = "pywebtransport._pywebtransport")]
struct PyEndpoint {
    command_tx: RuntimeCommandTx,
    event_rx: RuntimeEventRx,
    local_addresses: Vec<SocketAddr>,
    reactor_thread: Mutex<Option<JoinHandle<()>>>,
}

#[pymethods]
impl PyEndpoint {
    #[new]
    #[pyo3(signature = (*, config, is_client, waker))]
    fn new(
        py: Python<'_>,
        config: &Bound<'_, PyAny>,
        is_client: bool,
        waker: &PyWaker,
    ) -> PyResult<Self> {
        let (endpoint, std_sockets) = if is_client {
            let rust_config = RustClientConfig::try_from(config)?;
            let quic_config = config::build_client_config(&rust_config)?;
            let endpoint_config = Arc::new(config::build_endpoint_config(&rust_config.base)?);
            let quic_endpoint = QuicEndpoint::new(endpoint_config, None, false, None);

            let transport_endpoint =
                TransportEndpoint::new_client(quic_endpoint, rust_config.base, quic_config)?;

            let mut sockets = Vec::new();

            if let Ok(socket) = UdpSocket::bind("[::]:0") {
                socket.set_nonblocking(true)?;
                sockets.push(socket);
            }

            if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
                socket.set_nonblocking(true)?;
                sockets.push(socket);
            }

            if sockets.is_empty() {
                return Err(PyRuntimeError::new_err("sys_socket create failed"));
            }

            (transport_endpoint, sockets)
        } else {
            let rust_config = RustServerConfig::try_from(config)?;
            let bind_address_tuple = (rust_config.bind_host.clone(), rust_config.bind_port);
            let server_crypto = config::build_server_config(&rust_config)?;
            let endpoint_config = Arc::new(config::build_endpoint_config(&rust_config.base)?);
            let quic_endpoint =
                QuicEndpoint::new(endpoint_config, Some(Arc::new(server_crypto)), false, None);

            let transport_endpoint = TransportEndpoint::new_server(
                quic_endpoint,
                rust_config.base.clone(),
                rust_config,
            )?;

            let mut sockets = Vec::new();
            let mut bound_v4 = false;
            let mut bound_v6 = false;

            match bind_address_tuple.to_socket_addrs() {
                Ok(addresses) => {
                    for address in addresses {
                        if address.is_ipv4() && bound_v4 {
                            continue;
                        }
                        if address.is_ipv6() && bound_v6 {
                            continue;
                        }

                        if let Ok(socket) = UdpSocket::bind(address) {
                            socket.set_nonblocking(true)?;
                            sockets.push(socket);
                            if address.is_ipv4() {
                                bound_v4 = true;
                            }
                            if address.is_ipv6() {
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

        let local_addresses: Vec<SocketAddr> = std_sockets
            .iter()
            .filter_map(|socket| socket.local_addr().ok())
            .collect();

        let RuntimeChannels {
            command_rx,
            command_tx,
            event_rx,
            event_tx,
        } = RuntimeChannels::new();

        let waker_callback = waker.clone_waker_callback();
        let (tx, rx) = mpsc::sync_channel::<Result<(), String>>(0);

        let reactor_thread = Builder::new()
            .name("pywebtransport-reactor".into())
            .spawn(
                move || match TokioBuilder::new_current_thread().enable_all().build() {
                    Ok(runtime) => {
                        runtime.block_on(async move {
                            let mut tokio_sockets = Vec::new();

                            for socket in std_sockets {
                                match TokioUdpSocket::from_std(socket) {
                                    Ok(tokio_socket) => tokio_sockets.push(tokio_socket),
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
                },
            )
            .map_err(|e| PyRuntimeError::new_err(format!("sys_thread create failed err={e}")))?;

        py.detach(move || rx.recv())
            .unwrap_or_else(|e| Err(format!("rt resolve failed err={e}")))
            .map_err(PyRuntimeError::new_err)?;

        Ok(Self {
            command_tx,
            event_rx,
            local_addresses,
            reactor_thread: Mutex::new(Some(reactor_thread)),
        })
    }

    #[pyo3(signature = ())]
    fn close(&self) {
        self.command_tx
            .blocking_send(RuntimeCommand::ShutdownReactor)
            .ok();

        if let Ok(mut reactor_thread) = self.reactor_thread.lock()
            && let Some(handle) = reactor_thread.take()
        {
            handle.join().ok();
        }
    }

    #[pyo3(signature = (*, request_id, remote_address, server_name))]
    fn connect(
        &self,
        request_id: u64,
        remote_address: &Bound<'_, PyAny>,
        server_name: String,
    ) -> PyResult<()> {
        if self.event_rx.len() > (self.event_rx.capacity() * 9) / 10 {
            return Err(PyRuntimeError::new_err("rt_channel validate exceeded"));
        }

        let remote_address = extract_socket_address(remote_address)?;

        self.command_tx
            .try_send(RuntimeCommand::CreateConnection {
                request_id,
                remote_address,
                server_name,
            })
            .map_err(|e| PyRuntimeError::new_err(format!("rt_event send failed err={e}")))?;

        Ok(())
    }

    #[pyo3(signature = (*, handle, request_id, event))]
    fn handle_protocol_event(
        &self,
        handle: usize,
        request_id: u64,
        event: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if self.event_rx.len() > (self.event_rx.capacity() * 9) / 10 {
            return Err(PyRuntimeError::new_err("rt_channel validate exceeded"));
        }

        let protocol_event: ProtocolEvent = event.extract()?;
        let connection_handle = QuicConnectionHandle(handle);

        self.command_tx
            .try_send(RuntimeCommand::ProtocolEvent {
                request_id,
                handle: connection_handle,
                event: protocol_event,
            })
            .map_err(|e| PyRuntimeError::new_err(format!("rt_event send failed err={e}")))?;

        Ok(())
    }

    #[pyo3(name = "get_local_addresses", signature = ())]
    fn local_addresses(&self) -> Vec<(String, u16)> {
        self.local_addresses
            .iter()
            .map(|address| (address.ip().to_string(), address.port()))
            .collect()
    }

    #[pyo3(signature = ())]
    fn poll_runtime_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let py_list = PyList::empty(py);

        while let Some(event) = self.event_rx.pop() {
            py_list.append(event.into_pyobject(py)?)?;
        }

        Ok(py_list)
    }
}

impl Debug for PyEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyEndpoint")
            .field("proxy_state", &"Active")
            .finish()
    }
}

fn extract_socket_address(object: &Bound<'_, PyAny>) -> PyResult<SocketAddr> {
    if let Ok((ip_str, port, flowinfo, scope_id)) = object.extract::<(String, u16, u32, u32)>() {
        let ip: Ipv6Addr = ip_str
            .parse()
            .map_err(|e| PyValueError::new_err(format!("ipv6_address convert failed err={e}")))?;

        Ok(SocketAddr::V6(SocketAddrV6::new(
            ip, port, flowinfo, scope_id,
        )))
    } else if let Ok((ip_str, port)) = object.extract::<(String, u16)>() {
        let ip = ip_str
            .parse()
            .map_err(|e| PyValueError::new_err(format!("ip_address convert failed err={e}")))?;

        Ok(SocketAddr::new(ip, port))
    } else {
        Err(PyValueError::new_err("socket_address validate invalid"))
    }
}
