//! Unit tests for the `crate::runtime::channel` module.

use std::net::{Ipv4Addr, SocketAddr};

use quinn_proto::ConnectionHandle;

use super::*;
use crate::common::types::{ErrorCode, RequestId};
use crate::protocol::events::ProtocolEvent;

fn create_dummy_socket_addr() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443)
}

#[test]
fn test_ipc_channels_creation_and_messaging() {
    let Ok(mut channels) = IpcChannels::new() else {
        assert_eq!("ok", "err", "Failed to create IPC channels");
        unreachable!()
    };
    let command = RuntimeCommand::Shutdown;
    let event = RuntimeEvent::ReactorShutDown;

    let Ok(()) = channels.command_tx.try_send(command) else {
        assert_eq!("ok", "err", "Failed to send command");
        unreachable!()
    };
    let Ok(received_command) = channels.command_rx.try_recv() else {
        assert_eq!("ok", "err", "Failed to receive command");
        unreachable!()
    };
    let Ok(()) = channels.event_tx.push(event) else {
        assert_eq!("ok", "err", "Failed to push event");
        unreachable!()
    };
    let Some(received_event) = channels.event_rx.pop() else {
        assert_eq!("some", "none", "Failed to pop event");
        unreachable!()
    };

    assert!(matches!(received_command, RuntimeCommand::Shutdown));
    assert!(matches!(received_event, RuntimeEvent::ReactorShutDown));
}

#[test]
fn test_runtime_command_variants_debug() {
    let addr = create_dummy_socket_addr();
    let cmd_create = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(1u64),
        remote: addr,
        server_name: "localhost".to_owned(),
    };
    let cmd_protocol = RuntimeCommand::Protocol {
        handle: ConnectionHandle(0),
        event: ProtocolEvent::InternalCleanupResources,
    };
    let cmd_shutdown = RuntimeCommand::Shutdown;

    assert!(!format!("{cmd_create:?}").is_empty());
    assert!(!format!("{cmd_protocol:?}").is_empty());
    assert!(!format!("{cmd_shutdown:?}").is_empty());
}

#[test]
fn test_runtime_event_variants_debug() {
    let addr = create_dummy_socket_addr();
    let evt_completed = RuntimeEvent::CommandCompleted {
        request_id: RequestId::from(1u64),
        handle: ConnectionHandle(0),
        remote_address: addr,
    };
    let evt_failed = RuntimeEvent::CommandFailed {
        request_id: RequestId::from(2u64),
        error_code: Some(ErrorCode::from(0u64)),
        reason: "error".to_owned(),
    };
    let evt_effects = RuntimeEvent::ConnectionEffects {
        handle: ConnectionHandle(0),
        effects: vec![],
    };
    let evt_spawned = RuntimeEvent::ConnectionSpawned {
        handle: ConnectionHandle(1),
        remote_address: addr,
        effects: vec![],
    };
    let evt_shutdown = RuntimeEvent::ReactorShutDown;

    assert!(!format!("{evt_completed:?}").is_empty());
    assert!(!format!("{evt_failed:?}").is_empty());
    assert!(!format!("{evt_effects:?}").is_empty());
    assert!(!format!("{evt_spawned:?}").is_empty());
    assert!(!format!("{evt_shutdown:?}").is_empty());
}
