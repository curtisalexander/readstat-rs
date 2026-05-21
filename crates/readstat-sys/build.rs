use std::env;
use std::path::PathBuf;

// Restores derives that bindgen drops when a struct contains a blocklisted
// external type (here: `off_t` / `time_t`, which we re-export from `libc`).
// Bindgen can't introspect external types, so it conservatively removes
// derives it can't prove. These four structs had derives in the original
// bindgen output; this callback re-emits exactly that set so the public
// `readstat-sys` API stays identical for direct consumers.
//
// `readstat_variable_s` and `readstat_schema_entry_s` intentionally omit
// `Debug` — they contain by-value fields (e.g. `readstat_missingness_t`)
// that themselves don't derive `Debug`, so adding it would fail to compile.
#[cfg(feature = "buildtime_bindgen")]
#[derive(Debug)]
struct ForceDerives;

#[cfg(feature = "buildtime_bindgen")]
impl bindgen::callbacks::ParseCallbacks for ForceDerives {
    fn add_derives(&self, info: &bindgen::callbacks::DeriveInfo<'_>) -> Vec<String> {
        match info.name {
            "readstat_metadata_s" | "readstat_writer_s" => {
                vec!["Copy".into(), "Clone".into(), "Debug".into()]
            }
            "readstat_variable_s" | "readstat_schema_entry_s" => {
                vec!["Copy".into(), "Clone".into()]
            }
            _ => vec![],
        }
    }
}

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
        // Ensure LIBCLANG_PATH is set so bindgen can find libclang.dll
        if env::var_os("LIBCLANG_PATH").is_none() {
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
    println!("cargo:rustc-link-lib=static=readstat");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let pregenerated_bindings = project_dir.join("src").join("bindings.rs");

    if cfg!(feature = "buildtime_bindgen") {
        // Regeneration path — invoked by maintainers when the vendored
        // ReadStat C surface changes. Runs bindgen, writes the result to
        // both OUT_DIR (for the current compile) and `src/bindings.rs` (so
        // the diff can be committed). Default consumer builds skip this
        // entire block and use the checked-in `src/bindings.rs` below.
        #[cfg(feature = "buildtime_bindgen")]
        {
            let mut builder = bindgen::Builder::default()
                .header("wrapper.h")
                .allowlist_function("readstat_.*")
                .allowlist_function("xport_.*")
                .allowlist_type("readstat_.*")
                .allowlist_type("xport_.*")
                // Re-export `off_t` / `time_t` from the `libc` crate instead
                // of emitting the host OS's libc typedef chain. Without this,
                // bindings generated on Linux bake in `__off_t = c_long`,
                // bindings generated on macOS bake in `__darwin_off_t =
                // __int64_t`, etc. — making the checked-in `bindings.rs`
                // host-dependent. With this, the file is identical across
                // hosts and each consumer resolves the types through `libc`
                // (which is itself per-target).
                .blocklist_type("off_t")
                .blocklist_type("time_t")
                .blocklist_type("__off_t")
                .blocklist_type("__off64_t")
                .blocklist_type("__time_t")
                .blocklist_type("__time64_t")
                .blocklist_type("__darwin_off_t")
                .blocklist_type("__darwin_time_t")
                .blocklist_type("__int64_t")
                .raw_line("pub use libc::{off_t, time_t};")
                .parse_callbacks(Box::new(ForceDerives))
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
                    .expect(
                        "EMSDK must be set for Emscripten builds, or emsdk must be on PATH",
                    );
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
            bindings
                .write_to_file(&pregenerated_bindings)
                .expect("Couldn't refresh src/bindings.rs!");
        }
    } else {
        // Default path — copy the pre-generated bindings into OUT_DIR so
        // `src/lib.rs` can `include!` them via `env!("OUT_DIR")` exactly as
        // before. No bindgen, no libclang at consumer build time.
        assert!(
            pregenerated_bindings.exists(),
            "src/bindings.rs is missing; run `cargo build -p readstat-sys --features buildtime_bindgen` once to generate it"
        );
        std::fs::copy(&pregenerated_bindings, out_path.join("bindings.rs"))
            .expect("Couldn't copy pre-generated bindings into OUT_DIR");
        println!("cargo:rerun-if-changed=src/bindings.rs");
    }
}
