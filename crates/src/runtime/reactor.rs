//! Tokio-based event loop orchestrating the WebTransport I/O and state machines.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use quinn_proto::Transmit;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::debug;

use crate::runtime::channel::{RuntimeCommand, RuntimeCommandRx, RuntimeEvent, RuntimeEventTx};
use crate::transport::endpoint::{TransportEndpoint, TransportEvent};

// Callback type for cross-thread Python asyncio wake-ups.
pub(crate) type WakerCallback = Arc<dyn Fn() + Send + Sync>;

// Inbound UDP channel capacity between socket listeners and the reactor.
const UDP_CHANNEL_CAPACITY: usize = 8192;
// UDP slab allocation block capacity.
const UDP_SLAB_CAPACITY: usize = 65536;
// UDP slab remaining threshold before reallocation.
const UDP_SLAB_THRESHOLD: usize = 2048;

// Core event loop engine managing asynchronous I/O and multiplexing.
pub(crate) struct Reactor {
    command_rx: RuntimeCommandRx,
    datagram_rx: mpsc::Receiver<(BytesMut, SocketAddr, Option<SocketAddr>)>,
    endpoint: TransportEndpoint,
    event_tx: RuntimeEventTx,
    events_emitted: bool,
    sockets: Vec<Arc<UdpSocket>>,
    start_instant: Instant,
    waker: WakerCallback,
}

impl Reactor {
    // Initializes the Tokio reactor with the given I/O and IPC components.
    pub(crate) fn new(
        command_rx: RuntimeCommandRx,
        endpoint: TransportEndpoint,
        event_tx: RuntimeEventTx,
        sockets: Vec<UdpSocket>,
        waker: WakerCallback,
    ) -> Self {
        let (datagram_tx, datagram_rx) = mpsc::channel(UDP_CHANNEL_CAPACITY);
        let mut arc_sockets = Vec::new();

        for socket in sockets {
            let socket = Arc::new(socket);
            arc_sockets.push(Arc::clone(&socket));

            let tx = datagram_tx.clone();
            let sock_clone = Arc::clone(&socket);

            tokio::spawn(async move {
                let mut buf = BytesMut::with_capacity(UDP_SLAB_CAPACITY);

                loop {
                    if buf.capacity() < UDP_SLAB_THRESHOLD {
                        buf.reserve(UDP_SLAB_CAPACITY);
                    }

                    if let Ok((len, remote_addr)) = sock_clone.recv_buf_from(&mut buf).await {
                        let data = buf.split_to(len);
                        let local_addr = sock_clone.local_addr().ok();

                        if tx.send((data, remote_addr, local_addr)).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }

        Self {
            command_rx,
            datagram_rx,
            endpoint,
            event_tx,
            events_emitted: false,
            sockets: arc_sockets,
            start_instant: Instant::now(),
            waker,
        }
    }

    // Starts the continuous non-blocking event loop.
    pub(crate) async fn run(mut self) {
        loop {
            let timeout = self.endpoint.timeout();
            let sleep_fut = async {
                if let Some(t) = timeout {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(t)).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };

            tokio::select! {
                cmd_opt = self.command_rx.recv() => {
                    let Some(cmd) = cmd_opt else { break; };

                    if !self.handle_command(cmd).await {
                        break;
                    }
                }
                datagram_opt = self.datagram_rx.recv() => {
                    let Some((data, remote, local)) = datagram_opt else { break; };

                    self.handle_datagram(remote, local, data).await;
                }
                () = sleep_fut => {
                    self.handle_timeout();
                }
            }

            self.flush_transmits().await;

            if self.events_emitted {
                (self.waker)();
                self.events_emitted = false;
            }
        }

        self.notify_shutdown();
    }

    // Drains pending network transmissions from the endpoint.
    async fn flush_transmits(&mut self) {
        let now_instant = Instant::now();

        while let Some(transmit) = self.endpoint.poll_transmit(now_instant) {
            self.send_transmit(&transmit).await;
        }
    }

    // Processes an inbound control instruction from the FFI plane.
    async fn handle_command(&mut self, command: RuntimeCommand) -> bool {
        let (now, now_instant) = self.now_context();

        match command {
            RuntimeCommand::CreateConnection {
                request_id,
                remote_address,
                server_name,
            } => match self
                .endpoint
                .connect(remote_address, &server_name, now_instant)
            {
                Ok((handle, remote_address)) => {
                    if self
                        .event_tx
                        .push(RuntimeEvent::CommandCompleted {
                            request_id,
                            handle,
                            remote_address,
                        })
                        .is_ok()
                    {
                        self.events_emitted = true;
                    } else {
                        debug!("rt_event send failed");
                    }
                }
                Err(e) => {
                    if self
                        .event_tx
                        .push(RuntimeEvent::CommandFailed {
                            request_id,
                            error_code: None,
                            reason: e.to_string().into(),
                        })
                        .is_ok()
                    {
                        self.events_emitted = true;
                    } else {
                        debug!("rt_event send failed");
                    }
                }
            },
            RuntimeCommand::Protocol { handle, event } => {
                if let Some(transport_event) =
                    self.endpoint
                        .handle_user_event(handle, event, now, now_instant)
                {
                    self.process_transport_event(transport_event).await;
                }
            }
            RuntimeCommand::Shutdown => {
                return false;
            }
        }

        true
    }

    // Processes an inbound UDP datagram payload.
    async fn handle_datagram(
        &mut self,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        data: BytesMut,
    ) {
        let (now, now_instant) = self.now_context();

        let transport_event = self
            .endpoint
            .handle_datagram(remote, local, data, now, now_instant);

        self.process_transport_event(transport_event).await;
    }

    // Triggers the multiplexer timeout evaluations.
    fn handle_timeout(&mut self) {
        let (now, now_instant) = self.now_context();

        let results = self.endpoint.handle_timeout(now, now_instant);

        for (handle, effects) in results {
            if !effects.is_empty() {
                if self
                    .event_tx
                    .push(RuntimeEvent::ConnectionEffects { handle, effects })
                    .is_ok()
                {
                    self.events_emitted = true;
                } else {
                    debug!("rt_event send failed");
                }
            }
        }
    }

    // Emits the final shutdown acknowledgment to Python.
    fn notify_shutdown(&self) {
        if self.event_tx.push(RuntimeEvent::ReactorShutDown).is_err() {
            debug!("rt_event send failed");
        }

        (self.waker)();
    }

    // Retrieves the current absolute monotonic timestamps.
    fn now_context(&self) -> (f64, Instant) {
        let now_instant = Instant::now();
        let now = now_instant.duration_since(self.start_instant).as_secs_f64();

        (now, now_instant)
    }

    // Translates and dispatches an aggregated endpoint event.
    async fn process_transport_event(&mut self, event: TransportEvent) {
        match event {
            TransportEvent::ConnectionEffects { handle, effects } => {
                if !effects.is_empty() {
                    if self
                        .event_tx
                        .push(RuntimeEvent::ConnectionEffects { handle, effects })
                        .is_ok()
                    {
                        self.events_emitted = true;
                    } else {
                        debug!("rt_event send failed");
                    }
                }
            }
            TransportEvent::ConnectionSpawned {
                handle,
                effects,
                remote_address,
            } => {
                if self
                    .event_tx
                    .push(RuntimeEvent::ConnectionSpawned {
                        handle,
                        remote_address,
                        effects,
                    })
                    .is_ok()
                {
                    self.events_emitted = true;
                } else {
                    debug!("rt_event send failed");
                }
            }
            TransportEvent::Consumed => {}
            TransportEvent::Transmit(transmit) => {
                self.send_transmit(&transmit).await;
            }
        }
    }

    // Executes a physical UDP transmission using the socket.
    async fn send_transmit(&self, transmit: &Transmit) {
        let addr = transmit.destination;
        let size = transmit.size;
        let workspace = self.endpoint.transmit_workspace();

        if let Some(data) = workspace.get(..size) {
            let is_ipv4 = addr.is_ipv4();
            let target_socket = self
                .sockets
                .iter()
                .find(|s| s.local_addr().is_ok_and(|l| l.is_ipv4() == is_ipv4))
                .or_else(|| self.sockets.first());

            if let Some(socket) = target_socket
                && let Err(e) = socket.send_to(data, addr).await
            {
                debug!("udp_datagram send failed err={e:?}");
            }
        }
    }
}

#[cfg(test)]
mod tests;
