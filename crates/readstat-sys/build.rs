extern crate bindgen;

use std::env;
use std::path::PathBuf;

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

    cc.include(&src)
        .warnings(false);

    // AddressSanitizer: instrument ReadStat C code when requested.
    // Uses a targeted env var so third-party sys crates (e.g. zstd-sys)
    // are not affected — global CFLAGS would break their linking.
    if env::var("READSTAT_SANITIZE_ADDRESS").is_ok() {
        if target.contains("windows-msvc") {
            // MSVC cl.exe uses /fsanitize=address (forward-slash syntax)
            cc.flag("/fsanitize=address");
        } else {
            cc.flag("-fsanitize=address");
            cc.flag("-fno-omit-frame-pointer");
        }
    }

    // Include iconv.h — Emscripten provides its own
    if !is_emscripten {
        if let Some(include) = env::var_os("DEP_ICONV_INCLUDE") {
            cc.include(include);
        }
    }

    // Include zlib.h — Emscripten provides its own
    if !is_emscripten {
        if let Some(include) = env::var_os("DEP_Z_INCLUDE") {
            cc.include(include);
        }
    }

    // Linking
    // Note: zlib linking is handled by the libz-sys crate dependency
    if is_emscripten {
        // Emscripten provides iconv and zlib — no extra link directives needed.
        // emcc links them automatically.
    } else if target.contains("windows-msvc") {
        // Ensure LIBCLANG_PATH is set so bindgen can find libclang.dll
        if env::var_os("LIBCLANG_PATH").is_none() {
            let default = PathBuf::from(r"C:\Program Files\LLVM\lib");
            if !default.exists() {
                panic!(
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
            }
            // SAFETY: This runs in a build script which is single-threaded.
            unsafe { env::set_var("LIBCLANG_PATH", &default) };
        }
        println!("cargo:rustc-link-lib=static=iconv");
    } else if target.contains("apple-darwin") {
        println!("cargo:rustc-link-lib=iconv");
    }

    // Compile
    cc.compile("readstat");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // Linking
    println!("cargo:rustc-link-lib=static=readstat");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let mut builder = bindgen::Builder::default()
        // The input header we would like to generate bindings for
        .header("wrapper.h")
        // Expose all ReadStat public API functions and types
        .allowlist_function("readstat_.*")
        .allowlist_function("xport_.*")
        .allowlist_type("readstat_.*")
        .allowlist_type("xport_.*")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    if is_emscripten {
        let emsdk = env::var("EMSDK").or_else(|_| {
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
                    // PATH contains both <emsdk> and <emsdk>/upstream/emscripten
                    let candidate = std::path::Path::new(dir);
                    if candidate.join(&sysroot_suffix).is_dir() {
                        Some(candidate.to_string_lossy().into_owned())
                    } else {
                        None
                    }
                })
                .ok_or(env::VarError::NotPresent)
        }).expect("EMSDK must be set for Emscripten builds, or emsdk must be on PATH");
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

    let bindings = builder
        // Finish the builder and generate the bindings
        .generate()
        // Unwrap the Result and panic on failure
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
