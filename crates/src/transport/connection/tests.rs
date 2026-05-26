//! Unit tests for the `crate::transport::connection` module.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use quinn_proto::crypto::rustls::QuicClientConfig;
use quinn_proto::{
    ClientConfig, Connection as QuinnConnection, Endpoint, EndpointConfig,
    StreamId as QuinnStreamId, TransportConfig, VarInt,
};
use rstest::rstest;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::version::TLS13;
use rustls::{DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme};

use super::*;
use crate::protocol::engine::{EngineParams, WebTransportEngine};
use crate::protocol::events::{Effect, ProtocolEvent};

#[derive(Debug)]
struct DummyVerifier;

impl ServerCertVerifier for DummyVerifier {
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::RSA_PSS_SHA256]
    }

    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }
}

fn create_dummy_quic_connection() -> QuinnConnection {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let Ok(builder) =
        rustls::ClientConfig::builder_with_provider(provider).with_protocol_versions(&[&TLS13])
    else {
        assert_eq!("ok", "err", "Failed to build rustls client config");
        unreachable!()
    };
    let root_store = RootCertStore::empty();
    let mut crypto = builder
        .with_root_certificates(root_store)
        .with_no_client_auth();
    crypto
        .dangerous()
        .set_certificate_verifier(Arc::new(DummyVerifier));
    let Ok(quic_crypto) = QuicClientConfig::try_from(crypto) else {
        assert_eq!("ok", "err", "Failed to build QuicClientConfig");
        unreachable!()
    };
    let mut client_config = ClientConfig::new(Arc::new(quic_crypto));
    client_config.transport_config(Arc::new(TransportConfig::default()));
    let mut endpoint = Endpoint::new(Arc::new(EndpointConfig::default()), None, false, None);
    let remote = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 443);

    let Ok((_, connection)) = endpoint.connect(Instant::now(), client_config, remote, "localhost")
    else {
        assert_eq!("ok", "err", "Failed to connect quinn endpoint");
        unreachable!()
    };

    connection
}

fn create_test_connection() -> TransportConnection {
    let quic = create_dummy_quic_connection();
    let engine = create_test_engine(true);
    let now = Instant::now();

    TransportConnection::new(quic, engine, None, None, now)
}

fn create_test_engine(is_client: bool) -> WebTransportEngine {
    let params = EngineParams {
        early_event_ttl: 5.0,
        flow_control_window: 1024 * 1024,
        flow_control_window_auto_scale_enabled: true,
        initial_max_data: 10000,
        initial_max_streams_bidi: 10,
        initial_max_streams_uni: 10,
        max_capsule_size: 1024,
        max_field_section_size: 65536,
        max_session_pending_events: 10,
        max_sessions: 10,
        max_stream_read_buffer_size: 1024,
        max_stream_write_buffer_size: 1024,
        max_total_pending_events: 100,
    };

    let Ok(engine) = WebTransportEngine::new(42, is_client, params) else {
        assert_eq!("ok", "err", "Engine initialization failed");
        unreachable!()
    };

    engine
}

#[test]
fn test_flush_stream_finished_buffer_remains_in_map() {
    let mut connection = create_test_connection();
    let q_id = QuinnStreamId::from(VarInt::from_u32(1));
    let buffer = SendBuffer {
        finished: true,
        ..Default::default()
    };
    connection.send_buffers.insert(q_id, buffer);

    connection.flush_stream(q_id);

    let Some(final_buffer) = connection.send_buffers.get(&q_id) else {
        assert_eq!("found", "missing", "SendBuffer should exist");
        unreachable!()
    };
    assert!(final_buffer.chunks.is_empty());
    assert!(final_buffer.finished);
}

#[test]
fn test_handle_timeout_past_early_event_time_updates_next_ttl() {
    let mut connection = create_test_connection();
    let now = Instant::now();
    let interval = Duration::from_secs(2);
    connection.early_event_ttl = Some(interval);
    let Some(past_time) = now.checked_sub(Duration::from_secs(1)) else {
        assert_eq!("valid", "underflow", "Time underflow");
        unreachable!()
    };
    connection.next_early_event_time = Some(past_time);

    connection.handle_timeout(0.0, now);

    assert!(connection.next_early_event_time.is_some_and(|t| t > now));
}

#[test]
fn test_handle_timeout_past_gc_time_updates_next_gc_time() {
    let mut connection = create_test_connection();
    let now = Instant::now();
    let interval = Duration::from_secs(5);
    connection.gc_interval = Some(interval);
    let Some(past_time) = now.checked_sub(Duration::from_secs(1)) else {
        assert_eq!("valid", "underflow", "Time underflow");
        unreachable!()
    };
    connection.next_gc_time = Some(past_time);

    connection.handle_timeout(0.0, now);

    assert!(connection.next_gc_time.is_some_and(|t| t > now));
}

#[test]
fn test_handle_user_event_dispatches_cleanly() {
    let mut connection = create_test_connection();
    let event = ProtocolEvent::InternalCleanupResources;

    connection.handle_user_event(event, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[rstest]
#[case(Some(Duration::from_secs(5)), Some(Duration::from_secs(10)))]
#[case(None, Some(Duration::from_secs(10)))]
#[case(Some(Duration::from_secs(5)), None)]
#[case(None, None)]
fn test_new_initialization_sets_correct_timer_states(
    #[case] gc_interval: Option<Duration>,
    #[case] early_event_ttl: Option<Duration>,
) {
    let quic = create_dummy_quic_connection();
    let engine = create_test_engine(true);
    let now = Instant::now();

    let connection = TransportConnection::new(quic, engine, gc_interval, early_event_ttl, now);

    assert_eq!(
        connection.next_gc_time,
        gc_interval.map(|interval| now + interval)
    );
    assert_eq!(
        connection.next_early_event_time,
        early_event_ttl.map(|interval| now + interval)
    );
}

#[test]
fn test_poll_endpoint_events_delegates_cleanly() {
    let mut connection = create_test_connection();

    let _ = connection.poll_endpoint_events();
}

#[test]
fn test_poll_events_returns_none_initially() {
    let mut connection = create_test_connection();

    assert!(connection.poll_events().is_none());
}

#[test]
fn test_poll_transmit_delegates_cleanly() {
    let mut connection = create_test_connection();
    let mut workspace = Vec::new();

    let _ = connection.poll_transmit(&mut workspace, Instant::now());
}

#[test]
fn test_process_effects_close_quic_connection_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::CloseQuicConnection {
        error_code: 0,
        reason: None,
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_create_h3_session_unconnected_dispatches_fail() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::CreateH3Session {
        request_id: 1,
        path: "/".to_owned(),
        headers: vec![],
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(!connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_create_quic_stream_unconnected_dispatches_fail() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::CreateQuicStream {
        request_id: 1,
        session_id: 2,
        is_unidirectional: true,
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(!connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_export_tls_keying_material_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::ExportTlsKeyingMaterial {
        request_id: 1,
        label: "label".to_owned(),
        context: Bytes::from_static(b"ctx"),
        length: 32,
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    let has_done_or_fail = connection.pending_effects.iter().any(|e| {
        matches!(
            e,
            Effect::NotifyRequestDone { .. } | Effect::NotifyRequestFailed { .. }
        )
    });
    assert!(has_done_or_fail);
}

#[test]
fn test_process_effects_process_protocol_event_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::ProcessProtocolEvent {
        event: Box::new(ProtocolEvent::InternalCleanupResources),
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_reset_quic_stream_removes_send_buffer() {
    let mut connection = create_test_connection();
    let q_id = QuinnStreamId::from(VarInt::from_u32(1));
    let effects = vec![Effect::ResetQuicStream {
        stream_id: 1,
        error_code: 0,
    }];
    connection.send_buffers.insert(q_id, SendBuffer::default());

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(!connection.send_buffers.contains_key(&q_id));
}

#[test]
fn test_process_effects_send_h3_capsule_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::SendH3Capsule {
        stream_id: 1,
        capsule_type: 0,
        capsule_data: Bytes::from_static(b"test"),
        end_stream: false,
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_send_h3_datagram_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::SendH3Datagram {
        stream_id: 1,
        data: Bytes::from_static(b"test"),
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_send_h3_goaway_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::SendH3Goaway];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_send_h3_headers_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::SendH3Headers {
        stream_id: 1,
        headers: vec![(Bytes::from_static(b":status"), Bytes::from_static(b"200"))],
        end_stream: false,
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_send_quic_data_appends_to_send_buffer() {
    let mut connection = create_test_connection();
    let stream_id = 1u64;
    let data = Bytes::from_static(b"payload");
    let effects = vec![Effect::SendQuicData {
        stream_id,
        data,
        end_stream: true,
    }];
    let q_id = QuinnStreamId::from(VarInt::from_u32(1));

    connection.process_effects(effects, 0.0, Instant::now());

    let Some(buffer) = connection.send_buffers.get(&q_id) else {
        assert_eq!("found", "missing", "SendBuffer should exist");
        unreachable!()
    };
    assert!(buffer.finished);
    assert_eq!(buffer.chunks.len(), 1);
}

#[test]
fn test_process_effects_send_quic_datagram_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::SendQuicDatagram {
        header: Bytes::from_static(b"head"),
        payload: Bytes::from_static(b"load"),
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[test]
fn test_process_effects_stop_quic_stream_processes_safely() {
    let mut connection = create_test_connection();
    let effects = vec![Effect::StopQuicStream {
        stream_id: 1,
        error_code: 0,
    }];

    connection.process_effects(effects, 0.0, Instant::now());

    assert!(connection.pending_effects.is_empty());
}

#[rstest]
#[case(Some(Duration::from_secs(5)), Some(Duration::from_secs(10)), 5)]
#[case(Some(Duration::from_secs(10)), Some(Duration::from_secs(5)), 5)]
#[case(None, Some(Duration::from_secs(10)), 10)]
#[case(Some(Duration::from_secs(5)), None, 5)]
fn test_timeout_multiple_timers_returns_earliest_deadline(
    #[case] gc_interval: Option<Duration>,
    #[case] early_event_ttl: Option<Duration>,
    #[case] expected_offset: u64,
) {
    let quic = create_dummy_quic_connection();
    let engine = create_test_engine(true);
    let now = Instant::now();
    let mut connection = TransportConnection::new(quic, engine, gc_interval, early_event_ttl, now);

    let result = connection.timeout();

    assert!(result.is_some_and(|t| t == now + Duration::from_secs(expected_offset)));
}
