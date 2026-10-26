//! QUIC configuration.

use std::sync::{Arc, LazyLock};

use quinn_proto::congestion::{
    BbrConfig as QuicBbrConfig, CubicConfig as QuicCubicConfig, NewRenoConfig as QuicNewRenoConfig,
};
use quinn_proto::crypto::rustls::{
    QuicClientConfig as QuicCryptoClientConfig, QuicServerConfig as QuicCryptoServerConfig,
};
use quinn_proto::{
    ClientConfig as QuicClientConfig, EndpointConfig as QuicEndpointConfig,
    IdleTimeout as QuicIdleTimeout, ServerConfig as QuicServerConfig,
    TransportConfig as QuicTransportConfig, VarInt as QuicVarInt,
};
use rustls::crypto::ring;
use rustls::server::WebPkiClientVerifier;
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tracing::debug;
use webpki_roots::TLS_SERVER_ROOTS;

use crate::common::config::{RustBaseConfig, RustClientConfig, RustServerConfig};
use crate::common::constants::ALPN_PROTOCOLS;
use crate::common::error::WebTransportError;
use crate::tls::certificate::{self, NoCertificateVerification};

const DATAGRAM_QUEUE_CAPACITY: usize = 64;

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
        store.extend(TLS_SERVER_ROOTS.iter().cloned());
    }

    store
});

pub(crate) fn build_client_config(
    config: &RustClientConfig,
) -> Result<QuicClientConfig, WebTransportError> {
    let root_store = if let Some(ca_path) = &config.ca_certs {
        let mut store = RootCertStore::empty();
        let certs = certificate::load_certs(ca_path)
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

    let provider = Arc::new(ring::default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13])
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?
        .with_root_certificates(root_store);

    let mut crypto = if let (Some(cert_path), Some(key_path)) = (&config.certfile, &config.keyfile)
    {
        let certs = certificate::load_certs(cert_path)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
        let key = certificate::load_private_key(key_path)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?
    } else {
        builder.with_no_client_auth()
    };

    crypto.alpn_protocols = ALPN_PROTOCOLS
        .iter()
        .map(|protocol| protocol.as_bytes().to_vec())
        .collect();

    if !config.verify_server_certificate {
        crypto
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertificateVerification));
    }

    let crypto = QuicCryptoClientConfig::try_from(crypto)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    let mut client_config = QuicClientConfig::new(Arc::new(crypto));
    let transport_config = build_transport_config(&config.base)?;

    client_config.transport_config(Arc::new(transport_config));

    Ok(client_config)
}

pub(crate) fn build_endpoint_config(
    config: &RustBaseConfig,
) -> Result<QuicEndpointConfig, WebTransportError> {
    let mut endpoint_config = QuicEndpointConfig::default();

    let max_udp_payload_size = u16::try_from(config.max_udp_payload_size)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

    endpoint_config
        .max_udp_payload_size(max_udp_payload_size)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

    Ok(endpoint_config)
}

pub(crate) fn build_server_config(
    config: &RustServerConfig,
) -> Result<QuicServerConfig, WebTransportError> {
    let certs = certificate::load_certs(&config.certfile)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    let key = certificate::load_private_key(&config.keyfile)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

    let provider = Arc::new(ring::default_provider());
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13])
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;

    let mut crypto = if config.require_client_auth {
        let mut root_store = RootCertStore::empty();

        if let Some(ca_path) = &config.ca_certs {
            let ca_certs = certificate::load_certs(ca_path)
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

    crypto.alpn_protocols = ALPN_PROTOCOLS
        .iter()
        .map(|protocol| protocol.as_bytes().to_vec())
        .collect();

    let crypto = QuicCryptoServerConfig::try_from(crypto)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    let mut server_config = QuicServerConfig::with_crypto(Arc::new(crypto));
    let transport_config = build_transport_config(&config.base)?;

    server_config.transport_config(Arc::new(transport_config));

    Ok(server_config)
}

fn build_transport_config(
    base_config: &RustBaseConfig,
) -> Result<QuicTransportConfig, WebTransportError> {
    let mut config = QuicTransportConfig::default();

    match base_config.congestion_control_algorithm.as_str() {
        "bbr" => {
            config.congestion_controller_factory(Arc::new(QuicBbrConfig::default()));
        }
        "cubic" => {
            config.congestion_controller_factory(Arc::new(QuicCubicConfig::default()));
        }
        "reno" => {
            config.congestion_controller_factory(Arc::new(QuicNewRenoConfig::default()));
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

    let max_streams_bidi = QuicVarInt::try_from(base_config.max_streams_bidi)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.max_concurrent_bidi_streams(max_streams_bidi);

    let max_streams_uni = QuicVarInt::try_from(base_config.max_streams_uni)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.max_concurrent_uni_streams(max_streams_uni);

    let max_idle_timeout = QuicIdleTimeout::try_from(base_config.max_idle_timeout)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.max_idle_timeout(Some(max_idle_timeout));

    let connection_receive_window = QuicVarInt::try_from(base_config.connection_receive_window)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.receive_window(connection_receive_window);

    config.send_window(base_config.connection_send_window);

    let stream_receive_window = QuicVarInt::try_from(base_config.stream_receive_window)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string().into()))?;
    config.stream_receive_window(stream_receive_window);

    Ok(config)
}

#[cfg(test)]
mod tests;
