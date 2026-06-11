use std::env;
use std::path::PathBuf;

#[allow(clippy::too_many_lines)]
fn main() {
    let target = env::var("TARGET").unwrap();
    let is_emscripten = target.contains("emscripten");

    let project_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    let src = project_dir.join("vendor").join("ReadStat").join("src");
    let sas = src.join("sas");
    let spss = src.join("spss");
    let stata = src.join("stata");
    let txt = src.join("txt");

    // Fail early with a clear message if the vendored ReadStat sources are
    // missing (typically a `git clone` without `--recursive`). The published
    // crate bundles these files, so this check still passes for crates.io
    // consumers — it only guards the source-checkout path.
    assert!(
        src.join("CKHashTable.c").exists(),
        "Vendored ReadStat sources not found under {}.\n\
         Run `git submodule update --init --recursive` to fetch them.",
        src.display()
    );

    let mut cc = cc::Build::new();

    // Core ReadStat files (always needed)
    cc.file(src.join("CKHashTable.c"))
        .file(src.join("readstat_bits.c"))
        .file(src.join("readstat_convert.c"))
        .file(src.join("readstat_error.c"))
        .file(src.join("readstat_io_unistd.c"))
        .file(src.join("readstat_malloc.c"))
        .file(src.join("readstat_metadata.c"))
        .file(src.join("readstat_parser.c"))
        .file(src.join("readstat_value.c"))
        .file(src.join("readstat_variable.c"))
        .file(src.join("readstat_writer.c"));

    // SAS format support (always needed)
    cc.file(sas.join("ieee.c"))
        .file(sas.join("readstat_sas.c"))
        .file(sas.join("readstat_sas7bcat_read.c"))
        .file(sas.join("readstat_sas7bcat_write.c"))
        .file(sas.join("readstat_sas7bdat_read.c"))
        .file(sas.join("readstat_sas7bdat_write.c"))
        .file(sas.join("readstat_sas_rle.c"))
        .file(sas.join("readstat_xport.c"))
        .file(sas.join("readstat_xport_read.c"))
        .file(sas.join("readstat_xport_parse_format.c"))
        .file(sas.join("readstat_xport_write.c"));

    // SPSS, Stata, and txt format support — skip for Emscripten builds
    // to reduce wasm binary size and avoid Windows command-line length
    // limits when archiving many object files with emar.bat
    if !is_emscripten {
        cc.file(spss.join("readstat_por.c"))
            .file(spss.join("readstat_por_parse.c"))
            .file(spss.join("readstat_por_read.c"))
            .file(spss.join("readstat_por_write.c"))
            .file(spss.join("readstat_sav.c"))
            .file(spss.join("readstat_sav_compress.c"))
            .file(spss.join("readstat_sav_parse.c"))
            .file(spss.join("readstat_sav_parse_timestamp.c"))
            .file(spss.join("readstat_sav_read.c"))
            .file(spss.join("readstat_sav_write.c"))
            .file(spss.join("readstat_spss.c"))
            .file(spss.join("readstat_spss_parse.c"))
            .file(spss.join("readstat_sav_parse_mr_name.c"))
            .file(spss.join("readstat_zsav_compress.c"))
            .file(spss.join("readstat_zsav_read.c"))
            .file(spss.join("readstat_zsav_write.c"))
            .file(stata.join("readstat_dta.c"))
            .file(stata.join("readstat_dta_parse_timestamp.c"))
            .file(stata.join("readstat_dta_read.c"))
            .file(stata.join("readstat_dta_write.c"))
            .file(txt.join("commands_util.c"))
            .file(txt.join("readstat_copy.c"))
            .file(txt.join("readstat_sas_commands_read.c"))
            .file(txt.join("readstat_spss_commands_read.c"))
            .file(txt.join("readstat_schema.c"))
            .file(txt.join("readstat_stata_dictionary_read.c"))
            .file(txt.join("readstat_txt_read.c"));
    }

    cc.include(&src).warnings(false);

    // AddressSanitizer: instrument ReadStat C code when requested.
    // Uses a targeted env var so third-party sys crates (e.g. zstd-sys)
    // are not affected — global CFLAGS would break their linking.
    if env::var("READSTAT_SANITIZE_ADDRESS").is_ok() {
        if target.contains("windows-msvc") {
            cc.flag("/fsanitize=address");
        } else {
            cc.flag("-fsanitize=address");
            cc.flag("-fno-omit-frame-pointer");
        }
    }

    // Include iconv.h — Emscripten provides its own
    if !is_emscripten && let Some(include) = env::var_os("DEP_ICONV_INCLUDE") {
        cc.include(include);
    }

    // Include zlib.h — Emscripten provides its own
    if !is_emscripten && let Some(include) = env::var_os("DEP_Z_INCLUDE") {
        cc.include(include);
    }

    // Linking
    // Note: zlib linking is handled by the libz-sys crate dependency
    if is_emscripten {
        // Emscripten provides iconv and zlib — no extra link directives needed.
    } else if target.contains("windows-msvc") {
        // Ensure LIBCLANG_PATH is set so bindgen can find libclang.dll —
        // only needed when bindgen actually runs. Default consumer builds
        // (pre-gen bindings) skip this entirely.
        //
        // The default below is a Windows filesystem path, so it only makes sense
        // when the *host* is Windows. When cross-compiling to windows-msvc from a
        // non-Windows host (e.g. cargo-xwin on Linux/macOS), libclang lives
        // elsewhere and the user/CI sets LIBCLANG_PATH themselves — don't assert
        // a path that can't exist there.
        let host_is_windows = env::var("HOST").unwrap_or_default().contains("windows");
        if cfg!(feature = "buildtime_bindgen")
            && host_is_windows
            && env::var_os("LIBCLANG_PATH").is_none()
        {
            let default = PathBuf::from(r"C:\Program Files\LLVM\lib");
            assert!(
                default.exists(),
                "\n\
                \n  error: LIBCLANG_PATH is not set and the default path does not exist:\
                \n           {}\
                \n\
                \n  bindgen requires libclang to generate Rust bindings.\
                \n  Install LLVM from https://releases.llvm.org/download.html\
                \n  then set the LIBCLANG_PATH environment variable.\
                \n\
                \n  PowerShell (user-level, persistent):\
                \n    [Environment]::SetEnvironmentVariable(\"LIBCLANG_PATH\", \"C:\\Program Files\\LLVM\\lib\", \"User\")\
                \n\
                \n  After setting the variable, restart your terminal.\
                \n",
                default.display()
            );
            // SAFETY: This runs in a build script which is single-threaded.
            unsafe { env::set_var("LIBCLANG_PATH", &default) };
        }
        println!("cargo:rustc-link-lib=static=iconv");
    } else if target.contains("apple-darwin") {
        println!("cargo:rustc-link-lib=iconv");
    }

    cc.compile("readstat");

    println!("cargo:rerun-if-changed=wrapper.h");
    // Emitting any rerun-if-changed directive disables cargo's default
    // whole-package change tracking, so watch the vendored C sources explicitly
    // — otherwise editing them (or bumping the submodule) won't trigger a
    // rebuild. Also watch the sanitizer env var, which toggles the C cflags.
    println!("cargo:rerun-if-changed=vendor/ReadStat/src");
    println!("cargo:rerun-if-env-changed=READSTAT_SANITIZE_ADDRESS");
    println!("cargo:rustc-link-lib=static=readstat");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Per-target pre-generated bindings. The C ABI surface differs across
    // platforms in ways that don't generalize: MSVC emits C enums as
    // `signed int` while GCC/Clang emit `unsigned int`; Windows `c_long`
    // is 32 bits vs 64 bits on 64-bit Unix; and union/padding layout
    // rules differ between the Itanium and Microsoft C++ ABIs. Each
    // (os, arch) combination needs its own bindgen output.
    //
    // Emscripten/wasm32 has no pre-gen — its sysroot can't be reproduced
    // outside an emsdk install — so wasm32 consumers must enable
    // `buildtime_bindgen`.
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let pregenerated_bindings = if is_emscripten {
        None
    } else {
        Some(
            project_dir
                .join("src")
                .join("bindings")
                .join(format!("bindings_{target_os}_{target_arch}.rs")),
        )
    };

    if cfg!(feature = "buildtime_bindgen") {
        // Regeneration path — invoked by maintainers when the vendored
        // ReadStat C surface changes. Must be run once per supported
        // (os, arch) target to refresh all five checked-in files
        // (linux/macos × aarch64/x86_64, plus windows x86_64); the verify
        // CI workflow does this for Linux/macOS/Windows. Writes the result
        // to both OUT_DIR (for the current compile) and the target's
        // pre-gen file (so the diff can be committed).
        #[cfg(feature = "buildtime_bindgen")]
        {
            let mut builder = bindgen::Builder::default()
                .header("wrapper.h")
                .allowlist_function("readstat_.*")
                .allowlist_function("xport_.*")
                .allowlist_type("readstat_.*")
                .allowlist_type("xport_.*")
                .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

            if is_emscripten {
                let emsdk = env::var("EMSDK")
                    .or_else(|_| {
                        // emsdk_env.sh on Windows/Git Bash sometimes fails to export EMSDK
                        // even though it adds the emsdk directories to PATH. Scan PATH for
                        // a directory that looks like an emsdk root (contains the sysroot).
                        let sysroot_suffix = std::path::Path::new("upstream")
                            .join("emscripten")
                            .join("cache")
                            .join("sysroot");
                        env::var("PATH")
                            .unwrap_or_default()
                            .split(if cfg!(windows) { ';' } else { ':' })
                            .find_map(|dir| {
                                let candidate = std::path::Path::new(dir);
                                if candidate.join(&sysroot_suffix).is_dir() {
                                    Some(candidate.to_string_lossy().into_owned())
                                } else {
                                    None
                                }
                            })
                            .ok_or(env::VarError::NotPresent)
                    })
                    .expect("EMSDK must be set for Emscripten builds, or emsdk must be on PATH");
                builder = builder
                    .clang_arg(format!(
                        "--sysroot={emsdk}/upstream/emscripten/cache/sysroot"
                    ))
                    .clang_arg("-target")
                    .clang_arg("wasm32-unknown-emscripten")
                    // The wasm32 backend defaults to hidden visibility, which causes
                    // bindgen/libclang to silently omit all function declarations.
                    // See: https://github.com/rust-lang/rust-bindgen/issues/1941
                    .clang_arg("-fvisibility=default");
            }

            let bindings = builder.generate().expect("Unable to generate bindings");

            bindings
                .write_to_file(out_path.join("bindings.rs"))
                .expect("Couldn't write bindings to OUT_DIR!");
            if let Some(path) = &pregenerated_bindings {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .expect("Couldn't create src/bindings directory");
                }
                bindings
                    .write_to_file(path)
                    .expect("Couldn't refresh per-target pre-generated bindings file");
            }
        }
    } else {
        // Default path — copy the target-appropriate pre-generated bindings
        // into OUT_DIR so `src/lib.rs` can `include!` them via
        // `env!("OUT_DIR")` exactly as before. No bindgen, no libclang at
        // consumer build time.

        // The pre-gen files are keyed by (os, arch) only. The lone Windows file
        // is generated for the MSVC ABI, where C enums are `signed int`; the GNU
        // ABI emits them as `unsigned int`, so reusing the MSVC bindings on
        // `*-pc-windows-gnu` would silently mis-type the enums. Reject that target
        // on the pre-gen path and steer the user to `buildtime_bindgen`.
        let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
        assert!(
            !(target_os == "windows" && target_env == "gnu"),
            "the `*-pc-windows-gnu` target is not supported with pre-generated bindings \
             (they are generated for the MSVC ABI, whose enum signedness differs); \
             build for `*-pc-windows-msvc`, or enable the `buildtime_bindgen` feature \
             to generate GNU-ABI bindings at build time"
        );

        let path = pregenerated_bindings.as_ref().unwrap_or_else(|| {
            panic!(
                "no pre-generated bindings available for target `{target}`; \
                 enable `buildtime_bindgen` to generate them at build time"
            )
        });
        assert!(
            path.exists(),
            "{} is missing; run `cargo build -p readstat-sys --features buildtime_bindgen` to regenerate it",
            path.display()
        );
        std::fs::copy(path, out_path.join("bindings.rs"))
            .expect("Couldn't copy pre-generated bindings into OUT_DIR");
        println!("cargo:rerun-if-changed={}", path.display());
    }
}
