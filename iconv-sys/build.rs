extern crate bindgen;

use cc;
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
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

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");


    // Copy and communicate headers
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    // Linking
    println!("cargo:rustc-link-lib=static=iconv");
    println!("cargo:rustc-link-search=native={}", out_path.to_str().unwrap());

    fs::create_dir_all(out_path.join("include")).unwrap();
    fs::copy(include.join("iconv.h"), out_path.join("include/iconv.h")).unwrap();

    // println!("cargo:root={}", out_path.to_str().unwrap());
    println!("cargo:include={}/include", out_path.to_str().unwrap());

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate bindings for
        .header("wrapper.h")
        // Select which functions and types to build bindings for
        // Register callbacks
        //.whitelist_function("libiconv_close")
        //.whitelist_function("libiconv_open")
        // Types
        //.whitelist_type("libiconv_t")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
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