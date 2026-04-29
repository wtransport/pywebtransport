//! Cross-thread IPC channels for FFI and Tokio reactor communication.

use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use quinn_proto::ConnectionHandle;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::common::types::{ErrorCode, RequestId};
use crate::protocol::events::{Effect, ProtocolEvent};

// Receiving end of the command channel.
pub(crate) type RuntimeCommandRx = Receiver<RuntimeCommand>;
// Transmission end of the command channel.
pub(crate) type RuntimeCommandTx = Sender<RuntimeCommand>;
// Receiving end of the event channel.
pub(crate) type RuntimeEventRx = Arc<ArrayQueue<RuntimeEvent>>;
// Transmission end of the event channel.
pub(crate) type RuntimeEventTx = Arc<ArrayQueue<RuntimeEvent>>;

// Capacity of the IPC conduit between the FFI and Tokio reactor.
const IPC_CHANNEL_CAPACITY: usize = 65536;

// A unified IPC channel tuple bridging the FFI boundary and the Tokio runtime.
pub(crate) struct IpcChannels {
    pub(crate) command_rx: RuntimeCommandRx,
    pub(crate) command_tx: RuntimeCommandTx,
    pub(crate) event_rx: RuntimeEventRx,
    pub(crate) event_tx: RuntimeEventTx,
}

impl IpcChannels {
    // Initializes the bounded IPC channels with strict directional impedance matching.
    pub(crate) fn new() -> Self {
        let (command_tx, command_rx) = mpsc::channel(IPC_CHANNEL_CAPACITY);
        let event_queue = Arc::new(ArrayQueue::new(IPC_CHANNEL_CAPACITY));

        Self {
            command_rx,
            command_tx,
            event_rx: Arc::clone(&event_queue),
            event_tx: event_queue,
        }
    }
}

// User-driven instructions sent from the Python control plane to the Rust Tokio reactor.
#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    CreateConnection {
        request_id: RequestId,
        remote_address: SocketAddr,
        server_name: String,
    },
    Protocol {
        handle: ConnectionHandle,
        event: ProtocolEvent,
    },
    Shutdown,
}

// Outbound notifications and data emitted from the Rust reactor to the Python control plane.
#[derive(Debug)]
pub(crate) enum RuntimeEvent {
    CommandCompleted {
        request_id: RequestId,
        handle: ConnectionHandle,
        remote_address: SocketAddr,
    },
    CommandFailed {
        request_id: RequestId,
        error_code: Option<ErrorCode>,
        reason: Cow<'static, str>,
    },
    ConnectionEffects {
        handle: ConnectionHandle,
        effects: Vec<Effect>,
    },
    ConnectionSpawned {
        handle: ConnectionHandle,
        remote_address: SocketAddr,
        effects: Vec<Effect>,
    },
    ReactorShutDown,
}

#[cfg(test)]
mod tests;
