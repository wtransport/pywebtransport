//! Unit tests for the `crate::tls::certificate` module.

use std::env;
use std::fs;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use rcgen::SanType;
use rstest::rstest;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

use super::*;

#[rstest]
#[case("localhost")]
#[case("valid-host.com")]
#[case("webtransport.server")]
fn test_build_cert_parameters_hostname_acceptance_success(#[case] hostname: &str) {
    let result = build_cert_parameters(hostname, 1);

    assert!(result.is_ok(), "should succeed");
}

#[test]
fn test_build_cert_parameters_ip_address_hostname_success() {
    let hostname = "127.0.0.1";

    let result = build_cert_parameters(hostname, 1);

    assert!(result.is_ok(), "should succeed");

    let Ok(parameters) = result else { return };

    assert!(matches!(
        parameters.subject_alt_names.first(),
        Some(SanType::IpAddress(_))
    ));
}

#[test]
fn test_build_cert_parameters_logic_defaults_success() {
    let hostname = "example.com";
    let validity_days = 30;

    let result = build_cert_parameters(hostname, validity_days);

    assert!(result.is_ok(), "should succeed");

    let Ok(parameters) = result else { return };

    assert!(!format!("{:?}", parameters.distinguished_name).is_empty());
    assert_eq!(parameters.subject_alt_names.len(), 1);
    assert!(parameters.not_after > parameters.not_before);
}

#[test]
fn test_build_cert_parameters_utf8_hostname_rejection_failure() {
    let hostname = "测试.com";

    let result = build_cert_parameters(hostname, 1);

    assert!(result.is_err(), "should fail");

    if let Err(e) = result {
        assert_eq!(e.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn test_generate_self_signed_cert_io_operations_success() {
    let temp_dir = env::temp_dir().join("pywebtransport_cert_gen_test");
    drop(fs::remove_dir_all(&temp_dir));
    let hostname = "test-node";

    let result = generate_self_signed_cert(hostname, &temp_dir, 1);

    assert!(result.is_ok(), "should succeed");

    let Ok((ca_path_str, cert_path_str, key_path_str)) = result else {
        return;
    };
    let ca_path = Path::new(&ca_path_str);
    let cert_path = Path::new(&cert_path_str);
    let key_path = Path::new(&key_path_str);

    assert!(ca_path.exists());
    assert!(cert_path.exists());
    assert!(key_path.exists());

    let ca_content = fs::read_to_string(ca_path).unwrap_or_default();
    let cert_content = fs::read_to_string(cert_path).unwrap_or_default();
    let key_content = fs::read_to_string(key_path).unwrap_or_default();

    assert!(ca_content.contains("BEGIN CERTIFICATE"));
    assert!(cert_content.contains("BEGIN CERTIFICATE"));
    assert!(key_content.contains("PRIVATE KEY"));

    drop(fs::remove_dir_all(&temp_dir));
}

#[cfg(unix)]
#[test]
fn test_generate_self_signed_cert_key_permissions_unix_success() {
    let temp_dir = env::temp_dir().join("pywebtransport_perm_test");
    drop(fs::remove_dir_all(&temp_dir));
    let hostname = "secure-node";

    let result = generate_self_signed_cert(hostname, &temp_dir, 1);

    assert!(result.is_ok(), "should succeed");

    let Ok((_, _, key_path_str)) = result else {
        return;
    };
    let metadata_result = fs::metadata(key_path_str);

    assert!(metadata_result.is_ok(), "should succeed");

    let Ok(metadata) = metadata_result else {
        return;
    };
    let mode = metadata.permissions().mode();

    assert_eq!(mode & 0o777, 0o600);

    drop(fs::remove_dir_all(&temp_dir));
}

#[rstest]
fn test_load_certs_fails_with_invalid_path() {
    let path = Path::new("/non/existent/path/certs.pem");

    let result = load_certs(path);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[test]
fn test_load_certs_succeeds_with_valid_pem() {
    let temp_dir = env::temp_dir().join("pywebtransport_load_certs_test");
    drop(fs::remove_dir_all(&temp_dir));
    let hostname = "load-certs-node";

    let Ok((ca_path_str, _, _)) = generate_self_signed_cert(hostname, &temp_dir, 1) else {
        assert_eq!("ok", "err");
        unreachable!()
    };

    let result = load_certs(Path::new(&ca_path_str));

    let Ok(certs) = result else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    assert_eq!(certs.len(), 1);

    drop(fs::remove_dir_all(&temp_dir));
}

#[rstest]
fn test_load_private_key_fails_with_invalid_path() {
    let path = Path::new("/non/existent/path/key.pem");

    let result = load_private_key(path);

    let Err(_) = result else {
        assert_eq!("err", "ok");
        unreachable!()
    };
}

#[test]
fn test_no_certificate_verification_accepts_all() {
    let verifier = NoCertificateVerification;
    let cert = CertificateDer::from(vec![0x30, 0x03, 0x02, 0x01, 0x00]);
    let Ok(server_name) = ServerName::try_from("localhost") else {
        assert_eq!("ok", "err");
        unreachable!()
    };
    let now = UnixTime::now();

    let schemes = verifier.supported_verify_schemes();
    match verifier.verify_server_cert(&cert, &[], &server_name, &[], now) {
        Ok(_) => {}
        Err(e) => {
            assert_eq!(format!("{e:?}"), "");
        }
    }

    assert!(!schemes.is_empty());
}
