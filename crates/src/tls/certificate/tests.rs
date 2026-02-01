//! Unit tests for the `crate::tls::certificate` module.

use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use rstest::*;

use super::*;

#[rstest]
#[case("localhost")]
#[case("valid-host.com")]
#[case("webtransport.server")]
fn test_build_cert_params_hostname_acceptance_success(#[case] hostname: &str) {
    let res = build_cert_params(hostname, 1);

    if let Err(e) = &res {
        assert!(
            res.is_ok(),
            "Hostname '{hostname}' should be accepted but failed: {e}"
        );
    }
}

#[test]
fn test_build_cert_params_logic_defaults_success() {
    let hostname = "example.com";
    let validity_days = 30;

    let res = build_cert_params(hostname, validity_days);

    if let Err(e) = &res {
        assert!(res.is_ok(), "Failed to build params: {e}");
    }

    let Ok(params) = res else { return };

    assert!(!format!("{:?}", params.distinguished_name).is_empty());
    assert_eq!(params.subject_alt_names.len(), 1);
    assert!(params.not_after > params.not_before);
}

#[test]
fn test_build_cert_params_utf8_hostname_rejection_failure() {
    let hostname = "测试.com";

    let res = build_cert_params(hostname, 1);

    if res.is_ok() {
        assert!(res.is_err(), "UTF-8 hostname should be rejected");
    }

    if let Err(e) = res {
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
    }
}

#[test]
fn test_generate_self_signed_cert_io_operations_success() {
    let temp_dir = std::env::temp_dir().join("pywebtransport_cert_gen_test");
    let _ = fs::remove_dir_all(&temp_dir).ok();
    let hostname = "test-node";

    let res = generate_self_signed_cert(hostname, &temp_dir, 1);

    if let Err(e) = &res {
        assert!(res.is_ok(), "Generation failed: {e}");
    }

    let Ok((cert_path_str, key_path_str)) = res else {
        return;
    };
    let cert_path = Path::new(&cert_path_str);
    let key_path = Path::new(&key_path_str);

    assert!(cert_path.exists());
    assert!(key_path.exists());

    let cert_content = fs::read_to_string(cert_path).unwrap_or_default();
    let key_content = fs::read_to_string(key_path).unwrap_or_default();

    assert!(cert_content.contains("BEGIN CERTIFICATE"));
    assert!(key_content.contains("PRIVATE KEY"));

    let _ = fs::remove_dir_all(&temp_dir).ok();
}

#[cfg(unix)]
#[test]
fn test_generate_self_signed_cert_key_permissions_unix_success() {
    let temp_dir = std::env::temp_dir().join("pywebtransport_perm_test");
    let _ = fs::remove_dir_all(&temp_dir).ok();
    let hostname = "secure-node";

    let res = generate_self_signed_cert(hostname, &temp_dir, 1);

    if let Err(e) = &res {
        assert!(res.is_ok(), "Generation failed: {e}");
    }

    let Ok((_, key_path_str)) = res else { return };
    let metadata_res = fs::metadata(&key_path_str);

    if let Err(e) = &metadata_res {
        assert!(metadata_res.is_ok(), "Failed to read metadata: {e}");
    }

    let Ok(metadata) = metadata_res else { return };
    let mode = metadata.permissions().mode();

    assert_eq!(mode & 0o777, 0o600);

    let _ = fs::remove_dir_all(&temp_dir).ok();
}
