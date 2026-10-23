//! Runtime channels.

use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use quinn_proto::ConnectionHandle as QuicConnectionHandle;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::common::types::{ErrorCode, RequestId};
use crate::protocol::events::{Effect, ProtocolEvent};

pub(crate) type RuntimeCommandRx = Receiver<RuntimeCommand>;
pub(crate) type RuntimeCommandTx = Sender<RuntimeCommand>;
pub(crate) type RuntimeEventRx = Arc<ArrayQueue<RuntimeEvent>>;
pub(crate) type RuntimeEventTx = Arc<ArrayQueue<RuntimeEvent>>;

const RUNTIME_CHANNEL_CAPACITY: usize = 65536;

pub(crate) struct RuntimeChannels {
    pub(crate) command_rx: RuntimeCommandRx,
    pub(crate) command_tx: RuntimeCommandTx,
    pub(crate) event_rx: RuntimeEventRx,
    pub(crate) event_tx: RuntimeEventTx,
}

impl RuntimeChannels {
    pub(crate) fn new() -> Self {
        let (command_tx, command_rx) = mpsc::channel(RUNTIME_CHANNEL_CAPACITY);
        let event_queue = Arc::new(ArrayQueue::new(RUNTIME_CHANNEL_CAPACITY));

        Self {
            command_rx,
            command_tx,
            event_rx: Arc::clone(&event_queue),
            event_tx: event_queue,
        }
    }
}

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    CreateConnection {
        request_id: RequestId,
        remote_address: SocketAddr,
        server_name: String,
    },
    ProtocolEvent {
        request_id: RequestId,
        handle: QuicConnectionHandle,
        event: ProtocolEvent,
    },
    ShutdownReactor,
}

#[derive(Debug)]
pub(crate) enum RuntimeEvent {
    CommandFailed {
        request_id: RequestId,
        error_code: Option<ErrorCode>,
        reason: Cow<'static, str>,
    },
    ConnectionEffects {
        handle: QuicConnectionHandle,
        effects: Vec<Effect>,
    },
    ConnectionSpawned {
        handle: QuicConnectionHandle,
        remote_address: SocketAddr,
        effects: Vec<Effect>,
    },
    ReactorShutdown,
}

#[cfg(test)]
mod tests;
