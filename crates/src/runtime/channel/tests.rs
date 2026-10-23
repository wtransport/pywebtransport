//! Unit tests for the `crate::runtime::channel` module.

use std::net::{Ipv4Addr, SocketAddr};

use quinn_proto::ConnectionHandle as QuicConnectionHandle;

use super::*;
use crate::common::types::{ErrorCode, RequestId};
use crate::protocol::events::ProtocolEvent;

fn create_dummy_socket_address() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443)
}

#[test]
fn test_runtime_channels_creation_and_messaging() {
    let mut channels = RuntimeChannels::new();
    let command = RuntimeCommand::ShutdownReactor;
    let event = RuntimeEvent::ReactorShutdown;

    let Ok(()) = channels.command_tx.try_send(command) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(received_command) = channels.command_rx.try_recv() else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Ok(()) = channels.event_tx.push(event) else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let Some(received_event) = channels.event_rx.pop() else {
        assert_eq!("some", "none");
        unreachable!()
    };

    assert!(matches!(received_command, RuntimeCommand::ShutdownReactor));
    assert!(matches!(received_event, RuntimeEvent::ReactorShutdown));
}

#[test]
fn test_runtime_command_variants_debug() {
    let remote_address = create_dummy_socket_address();
    let command_create = RuntimeCommand::CreateConnection {
        request_id: RequestId::from(1u64),
        remote_address,
        server_name: "localhost".into(),
    };
    let command_protocol = RuntimeCommand::ProtocolEvent {
        request_id: RequestId::from(2u64),
        handle: QuicConnectionHandle(0),
        event: ProtocolEvent::InternalCleanupResources,
    };
    let command_shutdown = RuntimeCommand::ShutdownReactor;

    assert!(!format!("{command_create:?}").is_empty());
    assert!(!format!("{command_protocol:?}").is_empty());
    assert!(!format!("{command_shutdown:?}").is_empty());
}

#[test]
fn test_runtime_event_variants_debug() {
    let remote_address = create_dummy_socket_address();
    let event_failed = RuntimeEvent::CommandFailed {
        request_id: RequestId::from(2u64),
        error_code: Some(ErrorCode::from(0u64)),
        reason: "error".into(),
    };
    let event_effects = RuntimeEvent::ConnectionEffects {
        handle: QuicConnectionHandle(0),
        effects: vec![],
    };
    let event_spawned = RuntimeEvent::ConnectionSpawned {
        handle: QuicConnectionHandle(1),
        remote_address,
        effects: vec![],
    };
    let event_shutdown = RuntimeEvent::ReactorShutdown;

    assert!(!format!("{event_failed:?}").is_empty());
    assert!(!format!("{event_effects:?}").is_empty());
    assert!(!format!("{event_spawned:?}").is_empty());
    assert!(!format!("{event_shutdown:?}").is_empty());
}
