//! Build configuration for vendored C dependencies and FFI binding generation.

use std::env;
use std::path::{Path, PathBuf};

// Build process execution, compiling C libraries and generating Rust bindings.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let vendor_dir = manifest_dir.join("vendor");
    let lsqpack_dir = vendor_dir.join("ls-qpack");
    let xxhash_dir = vendor_dir.join("xxhash");
    let wincompat_dir = lsqpack_dir.join("wincompat");

    let lsqpack_c = lsqpack_dir.join("lsqpack.c");
    let xxhash_c = xxhash_dir.join("xxhash.c");
    let lsqpack_h = lsqpack_dir.join("lsqpack.h");
    let lsxpack_header_h = lsqpack_dir.join("lsxpack_header.h");

    let is_windows = env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");

    let mut build = cc::Build::new();
    build
        .file(&lsqpack_c)
        .file(&xxhash_c)
        .include(&lsqpack_dir)
        .include(&xxhash_dir)
        .define("XXH_INLINE_ALL", None)
        .warnings(false);

    if is_windows {
        build.include(&wincompat_dir);
    }

    build.compile("lsqpack");

    println!("cargo:rerun-if-changed={}", lsqpack_c.display());
    println!("cargo:rerun-if-changed={}", xxhash_c.display());
    println!("cargo:rerun-if-changed={}", lsqpack_h.display());
    println!("cargo:rerun-if-changed={}", lsxpack_header_h.display());

    let mut builder = bindgen::Builder::default()
        .header(path_to_str(&lsqpack_h)?)
        .header(path_to_str(&lsxpack_header_h)?)
        .clang_arg(format!("-I{}", lsqpack_dir.display()))
        .clang_arg(format!("-I{}", xxhash_dir.display()));

    if is_windows {
        builder = builder.clang_arg(format!("-I{}", wincompat_dir.display()));
    }

    let bindings = builder
        .allowlist_function("lsqpack_.*")
        .allowlist_type("lsqpack_.*")
        .allowlist_function("lsxpack_.*")
        .allowlist_type("lsxpack_.*")
        .allowlist_var("LSQPACK_.*")
        .rustified_enum("lsqpack_read_header_status")
        .rustified_enum("lsqpack_enc_status")
        .use_core()
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()?;

    let out_path = PathBuf::from(env::var("OUT_DIR")?);
    bindings.write_to_file(out_path.join("bindings.rs"))?;

    Ok(())
}

// Path to string conversion helper.
fn path_to_str(p: &Path) -> Result<&str, std::io::Error> {
    p.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "build_path convert failed actual={} expected=utf8",
                p.display()
            ),
        )
    })
}
