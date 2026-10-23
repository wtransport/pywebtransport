//! Runtime reactor.

use std::cmp;
use std::future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use quinn_proto::Transmit as QuicTransmit;
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::mpsc::{self, Receiver};
use tokio::time::{self, Instant as TokioInstant};
use tracing::debug;

use crate::runtime::channel::{RuntimeCommand, RuntimeCommandRx, RuntimeEvent, RuntimeEventTx};
use crate::transport::endpoint::{TransportEndpoint, TransportEvent};

pub(crate) type WakerCallback = Arc<dyn Fn() + Send + Sync>;

const UDP_CHANNEL_CAPACITY: usize = 8192;
const UDP_POLL_DATAGRAMS: usize = 64;
const UDP_SLAB_CAPACITY: usize = 65536;
const UDP_SLAB_THRESHOLD: usize = 2048;

pub(crate) struct Reactor {
    command_rx: RuntimeCommandRx,
    datagram_rx: Receiver<(SocketAddr, Option<SocketAddr>, BytesMut)>,
    endpoint: TransportEndpoint,
    event_tx: RuntimeEventTx,
    events_emitted: bool,
    sockets: Vec<BoundSocket>,
    start_instant: Instant,
    waker: WakerCallback,
}

impl Reactor {
    pub(crate) fn new(
        command_rx: RuntimeCommandRx,
        endpoint: TransportEndpoint,
        event_tx: RuntimeEventTx,
        sockets: Vec<UdpSocket>,
        waker: WakerCallback,
    ) -> Self {
        let (datagram_tx, datagram_rx) = mpsc::channel(UDP_CHANNEL_CAPACITY);
        let mut bound_sockets = Vec::new();

        for socket in sockets {
            let is_ipv4 = socket.local_addr().is_ok_and(|address| address.is_ipv4());
            let socket = Arc::new(socket);
            bound_sockets.push(BoundSocket {
                is_ipv4,
                socket: Arc::clone(&socket),
            });

            let datagram_tx = datagram_tx.clone();

            tokio::spawn(async move {
                let mut buffer = BytesMut::with_capacity(UDP_SLAB_CAPACITY);

                loop {
                    if buffer.capacity() < UDP_SLAB_THRESHOLD {
                        buffer.reserve(UDP_SLAB_CAPACITY);
                    }

                    if let Ok((len, remote_address)) = socket.recv_buf_from(&mut buffer).await {
                        let data = buffer.split_to(len);
                        let local_address = socket.local_addr().ok();

                        if datagram_tx
                            .send((remote_address, local_address, data))
                            .await
                            .is_err()
                        {
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
            sockets: bound_sockets,
            start_instant: Instant::now(),
            waker,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let timeout = self.endpoint.poll_timeout();
            let sleep_future = async {
                if let Some(timeout) = timeout {
                    time::sleep_until(TokioInstant::from_std(timeout)).await;
                } else {
                    future::pending::<()>().await;
                }
            };

            select! {
                command_option = self.command_rx.recv() => {
                    let Some(command) = command_option else { break; };

                    if !self.handle_command(command).await {
                        break;
                    }
                }
                datagram_option = self.datagram_rx.recv() => {
                    let Some((remote_address, local_address, data)) = datagram_option else { break; };

                    self.handle_datagram(remote_address, local_address, data).await;

                    for _ in 1..UDP_POLL_DATAGRAMS {
                        if let Ok((remote_address, local_address, data)) = self.datagram_rx.try_recv() {
                            self.handle_datagram(remote_address, local_address, data).await;
                        } else {
                            break;
                        }
                    }
                }
                () = sleep_future => {
                    self.handle_timeout();
                }
            }

            self.flush_transmits().await;

            if self.events_emitted {
                (self.waker)();
                self.events_emitted = false;
            }
        }

        self.flush_transmits().await;

        if self.events_emitted {
            (self.waker)();
        }

        self.notify_shutdown();
    }

    async fn flush_transmits(&mut self) {
        let now_instant = Instant::now();

        while let Some(transmit) = self.endpoint.poll_transmit(now_instant) {
            self.send_transmit(&transmit).await;
        }
    }

    async fn handle_command(&mut self, command: RuntimeCommand) -> bool {
        let (now, now_instant) = self.now_context();

        match command {
            RuntimeCommand::CreateConnection {
                request_id,
                remote_address,
                server_name,
            } => {
                let transport_event =
                    self.endpoint
                        .connect(request_id, remote_address, &server_name, now_instant);

                self.process_transport_event(transport_event).await;
            }
            RuntimeCommand::ProtocolEvent {
                request_id,
                handle,
                event,
            } => {
                let transport_event = self.endpoint.handle_protocol_event(
                    handle,
                    request_id,
                    event,
                    now,
                    now_instant,
                );

                self.process_transport_event(transport_event).await;
            }
            RuntimeCommand::ShutdownReactor => {
                let (now, now_instant) = self.now_context();
                let results = self.endpoint.shutdown(now, now_instant);

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

                return false;
            }
        }

        true
    }

    async fn handle_datagram(
        &mut self,
        remote_address: SocketAddr,
        local_address: Option<SocketAddr>,
        data: BytesMut,
    ) {
        let (now, now_instant) = self.now_context();

        let transport_event =
            self.endpoint
                .handle_datagram(remote_address, local_address, data, now, now_instant);

        self.process_transport_event(transport_event).await;
    }

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

    fn notify_shutdown(&self) {
        if self.event_tx.push(RuntimeEvent::ReactorShutdown).is_err() {
            debug!("rt_event send failed");
        }

        (self.waker)();
    }

    fn now_context(&self) -> (f64, Instant) {
        let now_instant = Instant::now();
        let now = now_instant.duration_since(self.start_instant).as_secs_f64();

        (now, now_instant)
    }

    async fn process_transport_event(&mut self, event: TransportEvent) {
        match event {
            TransportEvent::CommandFailed {
                request_id,
                error_code,
                reason,
            } => {
                if self
                    .event_tx
                    .push(RuntimeEvent::CommandFailed {
                        request_id,
                        error_code,
                        reason,
                    })
                    .is_ok()
                {
                    self.events_emitted = true;
                } else {
                    debug!("rt_event send failed");
                }
            }
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
                remote_address,
                effects,
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

    async fn send_transmit(&self, transmit: &QuicTransmit) {
        let address = transmit.destination;
        let size = transmit.size;
        let segment_size = transmit.segment_size.unwrap_or(size);
        let workspace = self.endpoint.transmit_workspace();

        if let Some(data) = workspace.get(..size) {
            let is_ipv4 = address.is_ipv4();
            let target_socket = self
                .sockets
                .iter()
                .find(|bound_socket| bound_socket.is_ipv4 == is_ipv4)
                .map(|bound_socket| &bound_socket.socket)
                .or_else(|| {
                    self.sockets
                        .first()
                        .map(|bound_socket| &bound_socket.socket)
                });

            if let Some(socket) = target_socket {
                let mut offset = 0;
                while offset < size {
                    let chunk_len = cmp::min(segment_size, size - offset);
                    let Some(chunk) = data.get(offset..offset + chunk_len) else {
                        break;
                    };

                    if let Err(e) = socket.send_to(chunk, address).await {
                        debug!("sys_socket send failed err={e:?}");
                    }

                    offset += chunk_len;
                }
            }
        }
    }
}

struct BoundSocket {
    is_ipv4: bool,
    socket: Arc<UdpSocket>,
}

#[cfg(test)]
mod tests;
