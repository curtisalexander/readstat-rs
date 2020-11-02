extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // Linking
    println!("cargo:rustc-link-lib=readstat");
    println!("cargo:rustc-link-search=/usr/local/lib");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
        // Select which functions and types to build bindings for
        .whitelist_function("readstat_get_row_count")
        .whitelist_function("readstat_parse_sas7bdat")
        .whitelist_function("readstat_parser_free")
        .whitelist_function("readstat_parser_init")
        .whitelist_function("readstat_set_metadata_handler")
        .whitelist_type("readstat_error_t")
        .whitelist_type("readstat_metadata_t")
        .whitelist_type("readstat_parser_t")
        .whitelist_type("READSTAT_HANDLER_OK")
        .whitelist_type("READSTAT_OK")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}