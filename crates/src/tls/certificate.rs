//! X.509 certificate generation logic.

use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use rcgen::string::Ia5String;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};
use time::{Duration, OffsetDateTime};

/// Self-signed certificate generation and persistence.
pub fn generate_self_signed_cert(
    hostname: &str,
    output_dir: &Path,
    validity_days: i64,
) -> io::Result<(String, String)> {
    let params = build_cert_params(hostname, validity_days)?;
    let key_pair =
        KeyPair::generate().map_err(|e| io::Error::other(format!("Key generation failed: {e}")))?;

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| io::Error::other(format!("Cert generation failed: {e}")))?;

    if !output_dir.exists() {
        fs::create_dir_all(output_dir)?;
    }

    let cert_filename = format!("{hostname}.crt");
    let key_filename = format!("{hostname}.key");

    let cert_path = output_dir.join(cert_filename);
    let key_path = output_dir.join(key_filename);

    fs::write(&cert_path, cert.pem())?;
    fs::write(&key_path, key_pair.serialize_pem())?;

    #[cfg(unix)]
    {
        let mut perms = fs::metadata(&key_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&key_path, perms)?;
    }

    Ok((
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
    ))
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

    let ia5 = Ia5String::try_from(hostname).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid hostname: {e}"),
        )
    })?;

    params.subject_alt_names = vec![SanType::DnsName(ia5)];

    let now = OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now + Duration::days(validity_days);

    Ok(params)
}

#[cfg(test)]
mod tests;
