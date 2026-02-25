#[cfg(windows)]
fn main() {
    use std::env;
    use std::fs;
    use std::path::PathBuf;

    // Emscripten provides its own iconv — skip the Windows vendor build
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "emscripten" {
        return;
    }

    let project_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    let root = project_dir.join("vendor").join("libiconv-win-build");
    let include = root.join("include");
    let lib = root.join("lib");
    let libcharset = root.join("libcharset").join("lib");
    let srclib = root.join("srclib");

    cc::Build::new()
        .file(libcharset.join("localcharset.c"))
        .file(lib.join("iconv.c"))
        .include(&include)
        .include(&lib)
        .include(&srclib)
        .warnings(false)
        .compile("iconv");

    println!("cargo:rerun-if-changed=wrapper.h");

    // Copy and communicate headers
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    if env::var_os("LIBCLANG_PATH").is_none() {
        println!("cargo:rustc-env=LIBCLANG_PATH='C:/Program Files/LLVM/lib'");
    }
    println!("cargo:rustc-link-lib=static=iconv");
    println!(
        "cargo:rustc-link-search=native={}",
        out_path.to_str().unwrap()
    );

    fs::create_dir_all(out_path.join("include")).unwrap();
    fs::copy(
        include.join("iconv.h"),
        out_path.join("include").join("iconv.h"),
    )
    .unwrap();

    println!("cargo:include={}/include", out_path.to_str().unwrap());

    // Generate bindings
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

// no-op for not windows as not needed
#[cfg(not(windows))]
fn main() {}
