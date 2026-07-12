use std::env;

fn main() {
    // Only build iconv when *targeting* Windows (checked via
    // CARGO_CFG_TARGET_OS rather than #[cfg(windows)], which would test the
    // host and break cross-compilation). Unix links the system iconv and
    // Emscripten provides its own — both handled by readstat-sys.
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "windows" {
        return;
    }

    use std::fs;
    use std::path::PathBuf;

    let project_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    // win-iconv: a public-domain iconv implementation backed by the Win32
    // conversion APIs (MultiByteToWideChar / WideCharToMultiByte).
    // https://github.com/win-iconv/win-iconv
    let root = project_dir.join("vendor").join("win-iconv");

    cc::Build::new()
        .file(root.join("win_iconv.c"))
        .include(&root)
        .warnings(false)
        .compile("iconv");

    println!("cargo:rerun-if-changed=wrapper.h");
    println!(
        "cargo:rerun-if-changed={}",
        root.join("win_iconv.c").display()
    );

    // Copy and communicate headers
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Only relevant when *building on* Windows: help bindgen find libclang.
    if cfg!(windows) && env::var_os("LIBCLANG_PATH").is_none() {
        println!("cargo:rustc-env=LIBCLANG_PATH='C:/Program Files/LLVM/lib'");
    }
    println!("cargo:rustc-link-lib=static=iconv");
    println!(
        "cargo:rustc-link-search=native={}",
        out_path.to_str().unwrap()
    );

    fs::create_dir_all(out_path.join("include")).unwrap();
    fs::copy(
        root.join("iconv.h"),
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

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
