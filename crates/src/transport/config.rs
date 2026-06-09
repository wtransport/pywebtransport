//! Provides QUIC transport configuration builders and mapping utilities.

use std::sync::{Arc, LazyLock};

use quinn_proto::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
use rustls::RootCertStore;
use rustls::server::WebPkiClientVerifier;
use rustls::version::TLS13;
use tracing::debug;

use crate::common::config::{RustBaseConfig, RustClientConfig, RustServerConfig};
use crate::common::error::WebTransportError;
use crate::tls::certificate::{NoCertificateVerification, load_certs, load_private_key};

// Datagram queue capacity.
const DATAGRAM_QUEUE_CAPACITY: usize = 64;

// Lazily initialized global cache for native OS root certificates.
static NATIVE_ROOT_CERTS: LazyLock<RootCertStore> = LazyLock::new(|| {
    let mut store = RootCertStore::empty();
    let native_certs = rustls_native_certs::load_native_certs();
    let mut valid_count = 0;

    for cert in native_certs.certs {
        if store.add(cert).is_ok() {
            valid_count += 1;
        }
    }

    if valid_count == 0 {
        store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    store
});

// Constructs the QUIC client configuration entity enforcing TLS 1.3 with explicit crypto provider.
pub(crate) fn build_client_config(
    config: &RustClientConfig,
) -> Result<ClientConfig, WebTransportError> {
    let root_store = if let Some(ca_path) = &config.ca_certs {
        let mut store = RootCertStore::empty();
        let certs = load_certs(ca_path)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

        for cert in certs {
            store
                .add(cert)
                .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
        }

        store
    } else {
        NATIVE_ROOT_CERTS.clone()
    };

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13])
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?
        .with_root_certificates(root_store);

    let mut crypto = if let (Some(cert_path), Some(key_path)) = (&config.certfile, &config.keyfile)
    {
        let certs = load_certs(cert_path)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
        let key = load_private_key(key_path)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?
    } else {
        builder.with_no_client_auth()
    };

    crypto.alpn_protocols = config
        .base
        .alpn_protocols
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();

    if !config.verify_server_certificate {
        crypto
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertificateVerification));
    }

    let quic_crypto = QuicClientConfig::try_from(crypto)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    let mut client_config = ClientConfig::new(Arc::new(quic_crypto));
    let transport_config = build_transport_config(&config.base)?;

    client_config.transport_config(Arc::new(transport_config));

    Ok(client_config)
}

// Constructs the global QUIC endpoint configuration.
pub(crate) fn build_endpoint_config(_config: &RustServerConfig) -> EndpointConfig {
    EndpointConfig::default()
}

// Constructs the QUIC server configuration entity enforcing TLS 1.3 with explicit crypto provider.
pub(crate) fn build_server_config(
    config: &RustServerConfig,
) -> Result<ServerConfig, WebTransportError> {
    let certs = load_certs(&config.certfile)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    let key = load_private_key(&config.keyfile)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13])
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

    let mut crypto = if config.require_client_auth {
        let mut root_store = RootCertStore::empty();

        if let Some(ca_path) = &config.ca_certs {
            let ca_certs = load_certs(ca_path)
                .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

            for cert in ca_certs {
                root_store
                    .add(cert)
                    .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
            }
        }

        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(root_store), provider)
            .build()
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?
    };

    crypto.alpn_protocols = config
        .base
        .alpn_protocols
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();

    let quic_crypto = QuicServerConfig::try_from(crypto)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_crypto));
    let transport_config = build_transport_config(&config.base)?;

    server_config.transport_config(Arc::new(transport_config));

    Ok(server_config)
}

// Maps WebTransport configuration to the underlying QUIC transport configuration.
fn build_transport_config(
    base_config: &RustBaseConfig,
) -> Result<TransportConfig, WebTransportError> {
    let mut config = TransportConfig::default();

    match base_config.congestion_control_algorithm.as_str() {
        "bbr" => {
            config.congestion_controller_factory(Arc::new(
                quinn_proto::congestion::BbrConfig::default(),
            ));
        }
        "cubic" => {
            config.congestion_controller_factory(Arc::new(
                quinn_proto::congestion::CubicConfig::default(),
            ));
        }
        "reno" => {
            config.congestion_controller_factory(Arc::new(
                quinn_proto::congestion::NewRenoConfig::default(),
            ));
        }
        unknown => {
            debug!("cfg_congestion_control_algorithm validate invalid actual={unknown}");
            return Err(WebTransportError::Configuration(
                None,
                "cfg_congestion_control_algorithm validate invalid".into(),
            ));
        }
    }

    if base_config.max_datagram_size > 0 {
        let total_size = base_config
            .max_datagram_size
            .saturating_mul(DATAGRAM_QUEUE_CAPACITY as u64);
        let buffer_capacity = usize::try_from(total_size).unwrap_or(usize::MAX);

        config.datagram_receive_buffer_size(Some(buffer_capacity));
        config.datagram_send_buffer_size(buffer_capacity);
    } else {
        config.datagram_receive_buffer_size(None);
        config.datagram_send_buffer_size(0);
    }

    config.keep_alive_interval(base_config.keep_alive_interval);

    let bidi_varint = quinn_proto::VarInt::try_from(base_config.quic_max_concurrent_bidi_streams)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.max_concurrent_bidi_streams(bidi_varint);

    let uni_varint = quinn_proto::VarInt::try_from(base_config.quic_max_concurrent_uni_streams)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.max_concurrent_uni_streams(uni_varint);

    let idle_timeout = quinn_proto::IdleTimeout::try_from(base_config.connection_idle_timeout)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.max_idle_timeout(Some(idle_timeout));

    let receive_window = quinn_proto::VarInt::try_from(base_config.quic_receive_window)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.receive_window(receive_window);

    config.send_window(base_config.quic_send_window);

    let stream_receive_window =
        quinn_proto::VarInt::try_from(base_config.quic_stream_receive_window)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.stream_receive_window(stream_receive_window);

    Ok(config)
}

#[cfg(test)]
mod tests;
