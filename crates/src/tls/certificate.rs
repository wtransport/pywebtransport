//! X509 certificate generation loading and validation subsystem.

use std::fs::{self, File};
use std::io::{self, BufReader};
use std::net::IpAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::str::FromStr;

use rcgen::string::Ia5String;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use time::{Duration, OffsetDateTime};

// Bypasses server certificate verification for insecure client connections.
#[derive(Debug)]
pub(crate) struct NoCertificateVerification;

impl ServerCertVerifier for NoCertificateVerification {
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
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

// Self-signed certificate generation and persistence.
pub(crate) fn generate_self_signed_cert(
    hostname: &str,
    output_dir: &Path,
    validity_days: i64,
) -> io::Result<(String, String, String)> {
    let ca_params = build_ca_params(validity_days);
    let ca_key_pair = KeyPair::generate().map_err(io::Error::other)?;
    let ca_cert = ca_params
        .self_signed(&ca_key_pair)
        .map_err(io::Error::other)?;

    let ca_issuer = Issuer::new(ca_params, &ca_key_pair);

    let params = build_cert_params(hostname, validity_days)?;
    let key_pair = KeyPair::generate().map_err(io::Error::other)?;
    let cert = params
        .signed_by(&key_pair, &ca_issuer)
        .map_err(io::Error::other)?;

    if !output_dir.exists() {
        fs::create_dir_all(output_dir)?;
    }

    let safe_hostname = hostname.replace(['/', '\\'], "_");
    let ca_filename = format!("{safe_hostname}_ca.crt");
    let cert_filename = format!("{safe_hostname}.crt");
    let key_filename = format!("{safe_hostname}.key");

    let ca_path = output_dir.join(ca_filename);
    let cert_path = output_dir.join(cert_filename);
    let key_path = output_dir.join(key_filename);

    fs::write(&ca_path, ca_cert.pem())?;
    fs::write(&cert_path, cert.pem())?;
    fs::write(&key_path, key_pair.serialize_pem())?;

    #[cfg(unix)]
    {
        let mut perms = fs::metadata(&key_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&key_path, perms)?;
    }

    Ok((
        ca_path.to_string_lossy().into_owned(),
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
    ))
}

// Extracts PEM certificate chains from the filesystem.
pub(crate) fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()
}

// Extracts a PEM private key from the filesystem.
pub(crate) fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "cfg_keyfile resolve failed"))
}

// Root CA parameter construction.
fn build_ca_params(validity_days: i64) -> CertificateParams {
    let mut params = CertificateParams::default();

    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CountryName, "US");
    distinguished_name.push(DnType::StateOrProvinceName, "CA");
    distinguished_name.push(DnType::LocalityName, "San Francisco");
    distinguished_name.push(DnType::OrganizationName, "WTransport");
    distinguished_name.push(DnType::CommonName, "WTransport Root CA");
    params.distinguished_name = distinguished_name;

    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let now = OffsetDateTime::now_utc();
    params.not_before = now - Duration::hours(1);
    params.not_after = now + Duration::days(validity_days);

    params
}

// Certificate parameter construction with defaults.
fn build_cert_params(hostname: &str, validity_days: i64) -> io::Result<CertificateParams> {
    let mut params = CertificateParams::default();

    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CountryName, "US");
    distinguished_name.push(DnType::StateOrProvinceName, "CA");
    distinguished_name.push(DnType::LocalityName, "San Francisco");
    distinguished_name.push(DnType::OrganizationName, "WTransport");
    distinguished_name.push(DnType::CommonName, hostname);
    params.distinguished_name = distinguished_name;

    let subject_alt_name = if let Ok(ip) = IpAddr::from_str(hostname) {
        SanType::IpAddress(ip)
    } else {
        let ia5 = Ia5String::try_from(hostname)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        SanType::DnsName(ia5)
    };

    params.subject_alt_names = vec![subject_alt_name];

    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

    let now = OffsetDateTime::now_utc();
    params.not_before = now - Duration::hours(1);
    params.not_after = now + Duration::days(validity_days);

    Ok(params)
}

#[cfg(test)]
mod tests;
