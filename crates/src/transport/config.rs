//! Provides QUIC transport configuration builders and mapping utilities.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use quinn_proto::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::version::TLS13;
use rustls::{DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme};

use crate::common::config::{
    RustClientConfig, RustServerConfig, TransportConfig as WtTransportConfig,
};
use crate::common::constants;
use crate::common::error::WebTransportError;

// Constructs the QUIC client configuration entity enforcing TLS 1.3 with explicit crypto provider.
pub(crate) fn build_client_config(
    config: &RustClientConfig,
) -> Result<ClientConfig, WebTransportError> {
    let mut root_store = RootCertStore::empty();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13])
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;

    if let Some(ca_path) = &config.ca_certs {
        let certs = load_certs(ca_path)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;

        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;
        }
    }

    let builder = builder.with_root_certificates(root_store);
    let mut crypto = if let (Some(cert_path), Some(key_path)) = (&config.certfile, &config.keyfile)
    {
        let certs = load_certs(cert_path)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;
        let key = load_private_key(key_path)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;

        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?
    } else {
        builder.with_no_client_auth()
    };

    crypto.alpn_protocols = config
        .transport
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
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;
    let mut client_config = ClientConfig::new(Arc::new(quic_crypto));
    let transport_config = build_transport_config(&config.transport)?;

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
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;
    let key = load_private_key(&config.keyfile)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13])
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;

    let mut crypto = if config.require_client_auth {
        let root_store = RootCertStore::empty();
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(root_store), provider)
            .build()
            .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;

        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?
    };

    crypto.alpn_protocols = config
        .transport
        .alpn_protocols
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();

    let quic_crypto = QuicServerConfig::try_from(crypto)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;
    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_crypto));
    let transport_config = build_transport_config(&config.transport)?;

    server_config.transport_config(Arc::new(transport_config));

    Ok(server_config)
}

// Bypasses server certificate verification for insecure client connections.
#[derive(Debug)]
struct NoCertificateVerification;

impl ServerCertVerifier for NoCertificateVerification {
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
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

// Maps WebTransport configuration to the underlying QUIC transport configuration.
fn build_transport_config(
    wt_config: &WtTransportConfig,
) -> Result<TransportConfig, WebTransportError> {
    let mut config = TransportConfig::default();
    let idle_timeout = quinn_proto::IdleTimeout::try_from(wt_config.connection_idle_timeout)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;
    let bidi_limit = constants::WT_SESSION_CONTROL_BIDI_STREAM_COUNT
        .saturating_add(wt_config.initial_max_streams_bidi)
        .min(wt_config.transport_streams_cap);
    let uni_limit = constants::H3_MIN_UNI_STREAM_COUNT
        .saturating_add(wt_config.initial_max_streams_uni)
        .min(wt_config.transport_streams_cap);
    let bidi_varint = quinn_proto::VarInt::try_from(bidi_limit)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;
    let uni_varint = quinn_proto::VarInt::try_from(uni_limit)
        .map_err(|e| WebTransportError::Configuration(None, e.to_string()))?;

    config.max_idle_timeout(Some(idle_timeout));
    config.keep_alive_interval(wt_config.keep_alive);
    config.max_concurrent_bidi_streams(bidi_varint);
    config.max_concurrent_uni_streams(uni_varint);

    if wt_config.max_datagram_size > 0 {
        let total_size = wt_config
            .max_datagram_size
            .saturating_mul(constants::DATAGRAM_QUEUE_CAPACITY);
        let buffer_capacity = usize::try_from(total_size).unwrap_or(usize::MAX);

        config.datagram_receive_buffer_size(Some(buffer_capacity));
        config.datagram_send_buffer_size(buffer_capacity);
    } else {
        config.datagram_receive_buffer_size(None);
        config.datagram_send_buffer_size(0);
    }

    match wt_config.congestion_control_algorithm.as_str() {
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
            return Err(WebTransportError::Configuration(
                None,
                format!("unsupported congestion control algorithm: {unknown}"),
            ));
        }
    }

    Ok(config)
}

// Extracts PEM certificate chains from the filesystem.
fn load_certs(path: &Path) -> std::io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()
}

// Extracts a PEM private key from the filesystem.
fn load_private_key(path: &Path) -> std::io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "private key not found")
    })
}

#[cfg(test)]
mod tests;
