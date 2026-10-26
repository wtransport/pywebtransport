//! TLS certificates.

use std::fs;
use std::io::{self, Error, ErrorKind};
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
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use time::{Duration, OffsetDateTime};

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

pub(crate) fn generate_self_signed_cert(
    hostname: &str,
    output_dir: &Path,
    validity_days: i64,
) -> io::Result<(String, String, String)> {
    let ca_parameters = build_ca_parameters(validity_days);
    let ca_key_pair = KeyPair::generate().map_err(Error::other)?;
    let ca_cert = ca_parameters
        .self_signed(&ca_key_pair)
        .map_err(Error::other)?;

    let ca_issuer = Issuer::new(ca_parameters, &ca_key_pair);

    let parameters = build_cert_parameters(hostname, validity_days)?;
    let key_pair = KeyPair::generate().map_err(Error::other)?;
    let cert = parameters
        .signed_by(&key_pair, &ca_issuer)
        .map_err(Error::other)?;

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
        let mut permissions = fs::metadata(&key_path)?.permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(&key_path, permissions)?;
    }

    Ok((
        ca_path.to_string_lossy().into_owned(),
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
    ))
}

pub(crate) fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(path)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
        .map(|result| result.map_err(|e| Error::new(ErrorKind::InvalidData, e)))
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| Error::new(ErrorKind::InvalidData, e))
}

fn build_ca_parameters(validity_days: i64) -> CertificateParams {
    let mut parameters = CertificateParams::default();

    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CountryName, "US");
    distinguished_name.push(DnType::StateOrProvinceName, "CA");
    distinguished_name.push(DnType::LocalityName, "San Francisco");
    distinguished_name.push(DnType::OrganizationName, "WTransport");
    distinguished_name.push(DnType::CommonName, "WTransport Root CA");
    parameters.distinguished_name = distinguished_name;

    parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    parameters.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let now = OffsetDateTime::now_utc();
    parameters.not_before = now - Duration::hours(1);
    parameters.not_after = now + Duration::days(validity_days);

    parameters
}

fn build_cert_parameters(hostname: &str, validity_days: i64) -> io::Result<CertificateParams> {
    let mut parameters = CertificateParams::default();

    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CountryName, "US");
    distinguished_name.push(DnType::StateOrProvinceName, "CA");
    distinguished_name.push(DnType::LocalityName, "San Francisco");
    distinguished_name.push(DnType::OrganizationName, "WTransport");
    distinguished_name.push(DnType::CommonName, hostname);
    parameters.distinguished_name = distinguished_name;

    let subject_alt_name = if let Ok(hostname) = IpAddr::from_str(hostname) {
        SanType::IpAddress(hostname)
    } else {
        let hostname =
            Ia5String::try_from(hostname).map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        SanType::DnsName(hostname)
    };

    parameters.subject_alt_names = vec![subject_alt_name];

    parameters.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

    let now = OffsetDateTime::now_utc();
    parameters.not_before = now - Duration::hours(1);
    parameters.not_after = now + Duration::days(validity_days);

    Ok(parameters)
}

#[cfg(test)]
mod tests;
