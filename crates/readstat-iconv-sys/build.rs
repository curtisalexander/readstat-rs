//! Build script for `readstat-iconv-sys`.
//!
//! Windows-only: compiles the vendored win-iconv static library and makes FFI
//! bindings available. On non-Windows targets (and Emscripten, which supplies
//! its own iconv) the build script is a no-op.
//!
//! win-iconv is a public-domain iconv implementation backed by the Win32
//! conversion APIs (`MultiByteToWideChar` / `WideCharToMultiByte`), so — unlike
//! the GNU libiconv (LGPL-2.1) vendored previously — statically linking it
//! imposes no copyleft obligations on downstream Windows binaries.
//!
//! Like `readstat-sys`, the Rust bindings are **pre-generated per target** and
//! checked in under `src/bindings/bindings_windows_<arch>.rs`. Unlike
//! `readstat-sys`, one file serves both the MSVC and GNU flavors: the iconv
//! surface is enum-free (`void*`, `size_t`, `char*` only), so its ABI is
//! identical under either toolchain. The default
//! build copies the file matching the current target into `OUT_DIR`, so
//! **consumers need no `libclang`**. Enable the `buildtime_bindgen` feature to
//! regenerate the bindings from `wrapper.h` (requires `libclang`); the result
//! is written to `OUT_DIR`, and additionally to the checked-in path (so a
//! maintainer can commit the diff) when `READSTAT_REGEN_BINDINGS` is set. The
//! CI `regen-iconv` job runs this on a Windows runner.

fn main() {
    use std::env;
    use std::fs;
    use std::path::PathBuf;

    // Gate on the *target* OS, not the host: a build script is compiled for the
    // host, so `#[cfg(windows)]` would mis-handle cross-compilation (building a
    // Windows target from Linux/macOS, or vice versa). win-iconv is only linked
    // when the target is Windows; every other target (including Emscripten,
    // which supplies its own iconv) is a no-op.
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "windows" {
        return;
    }

    let project_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    let root = project_dir.join("vendor").join("win-iconv");

    // Fail early with a clear message if the vendored win-iconv sources are
    // missing (typically a `git clone` without `--recursive`). The published
    // crate bundles these files, so this only guards the source-checkout path.
    assert!(
        root.join("win_iconv.c").exists(),
        "Vendored win-iconv sources not found under {}.\n\
         Run `git submodule update --init --recursive` to fetch them.",
        root.display()
    );

    cc::Build::new()
        .file(root.join("win_iconv.c"))
        .include(&root)
        .warnings(false)
        .compile("iconv");

    println!("cargo:rerun-if-changed=wrapper.h");
    // Emitting any rerun-if-changed directive disables cargo's default
    // whole-package tracking, so watch the vendored win-iconv sources explicitly.
    println!("cargo:rerun-if-changed=vendor/win-iconv/win_iconv.c");
    println!("cargo:rerun-if-changed=vendor/win-iconv/iconv.h");
    println!("cargo:rustc-link-lib=static=iconv");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!(
        "cargo:rustc-link-search=native={}",
        out_path.to_str().unwrap()
    );

    // Copy and communicate headers so `readstat-sys` can locate `iconv.h` via
    // the `DEP_ICONV_INCLUDE` environment variable (set from `cargo:include`).
    fs::create_dir_all(out_path.join("include")).unwrap();
    fs::copy(
        root.join("iconv.h"),
        out_path.join("include").join("iconv.h"),
    )
    .unwrap();
    println!("cargo:include={}/include", out_path.to_str().unwrap());

    // Per-target pre-generated bindings. iconv's allowlisted surface is tiny
    // and uses only platform-stable C types, but we key by arch to match the
    // `readstat-sys` convention and leave room for a future Windows-on-ARM
    // target.
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let pregenerated = project_dir
        .join("src")
        .join("bindings")
        .join(format!("bindings_windows_{target_arch}.rs"));

    println!("cargo:rerun-if-env-changed=READSTAT_REGEN_BINDINGS");

    if cfg!(feature = "buildtime_bindgen") {
        // Regeneration path — requires `libclang`. Always writes to OUT_DIR
        // (for this compile); the checked-in file is refreshed ONLY when
        // `READSTAT_REGEN_BINDINGS` is set — enabling the feature alone (e.g.
        // a workspace-wide `--all-features` build) must not silently rewrite
        // committed bindings, whose exact output depends on the local
        // libclang version.
        #[cfg(feature = "buildtime_bindgen")]
        {
            let bindings = bindgen::Builder::default()
                .header("wrapper.h")
                // Restrict to the iconv surface so the output excludes Windows
                // system-header types and stays platform-stable.
                .allowlist_function(".*iconv.*")
                .allowlist_type(".*iconv.*")
                .allowlist_var(".*iconv.*")
                .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
                .generate()
                .expect("Unable to generate bindings");

            bindings
                .write_to_file(out_path.join("bindings.rs"))
                .expect("Couldn't write bindings to OUT_DIR!");

            if env::var_os("READSTAT_REGEN_BINDINGS").is_some() {
                if let Some(parent) = pregenerated.parent() {
                    fs::create_dir_all(parent).expect("Couldn't create src/bindings directory");
                }
                bindings
                    .write_to_file(&pregenerated)
                    .expect("Couldn't refresh per-target pre-generated bindings file");
            }
        }
    } else {
        // Default path — copy the checked-in bindings; no bindgen, no libclang.
        assert!(
            pregenerated.exists(),
            "{} is missing; run `READSTAT_REGEN_BINDINGS=1 cargo build -p readstat-iconv-sys \
             --features buildtime_bindgen` (requires libclang) to regenerate it",
            pregenerated.display()
        );
        fs::copy(&pregenerated, out_path.join("bindings.rs"))
            .expect("Couldn't copy pre-generated bindings into OUT_DIR");
        println!("cargo:rerun-if-changed={}", pregenerated.display());
    }
}
