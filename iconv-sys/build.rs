use cc;
use dunce;
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let project_dir = dunce::canonicalize(PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())).unwrap();

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

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // Linking
    println!("cargo:rustc-link-lib=static=iconv");

    // Copy and communicate headers
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    fs::create_dir_all(out_path.join("include")).unwrap();
    fs::copy(include.join("iconv.h"), out_path.join("include/iconv.h")).unwrap();

    println!("cargo:root={}", out_path.to_str().unwrap());
    println!("cargo:include={}/include", out_path.to_str().unwrap());
}