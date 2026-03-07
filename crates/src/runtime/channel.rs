//! Cross-thread IPC channels for FFI and Tokio reactor communication.

use std::net::SocketAddr;
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use quinn_proto::ConnectionHandle;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::common::constants;
use crate::common::error::WebTransportError;
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

// A unified IPC channel tuple bridging the FFI boundary and the Tokio runtime.
pub(crate) struct IpcChannels {
    pub(crate) command_tx: RuntimeCommandTx,
    pub(crate) command_rx: RuntimeCommandRx,
    pub(crate) event_tx: RuntimeEventTx,
    pub(crate) event_rx: RuntimeEventRx,
}

impl IpcChannels {
    // Initializes the bounded IPC channels with strict directional impedance matching.
    pub(crate) fn new() -> Result<Self, WebTransportError> {
        let capacity = usize::try_from(constants::RUNTIME_IPC_CHANNEL_CAPACITY).map_err(|e| {
            WebTransportError::Unknown(
                Some(constants::ERR_LIB_INTERNAL_ERROR),
                format!("RUNTIME_IPC_CHANNEL_CAPACITY exceeds system pointer size: {e}"),
            )
        })?;

        let (command_tx, command_rx) = mpsc::channel(capacity);
        let event_queue = Arc::new(ArrayQueue::new(capacity));

        Ok(Self {
            command_tx,
            command_rx,
            event_tx: Arc::clone(&event_queue),
            event_rx: event_queue,
        })
    }
}

// User-driven instructions sent from the Python control plane to the Rust Tokio reactor.
#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    CreateConnection {
        request_id: RequestId,
        remote: SocketAddr,
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
        reason: String,
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
