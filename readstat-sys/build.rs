extern crate bindgen;

use cc;
use dunce;
use std::env;
use std::fs;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

fn is_c_file(entry: &fs::DirEntry) -> bool {
    let c_extension = OsStr::new("c");
    match entry.path().extension() {
        Some(e) => e == c_extension,
        None => false
    }
}

fn is_file(entry: &fs::DirEntry) -> bool {
    entry
        .file_type()
        .unwrap()
        .is_file()
}

fn get_all_c_files<P: AsRef<Path>>(dir: P) -> Vec<PathBuf> {
    fs::read_dir(&dir)
        .unwrap()
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| is_file(e) && is_c_file(e))
        .map(|e| e.path())
        .collect()
}

fn main() {
    let project_dir = dunce::canonicalize(PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())).unwrap();

    let src = project_dir.join("vendor").join("ReadStat").join("src");
    let sas = src.join("sas");
    let spss = src.join("spss");
    let stata = src.join("stata");
    let txt = src.join("txt");
    
    let src_c_files = get_all_c_files(&src);
    let sas_c_files = get_all_c_files(&sas);
    let spss_c_files = get_all_c_files(&spss);
    let stata_c_files = get_all_c_files(&stata);
    let txt_c_files = get_all_c_files(&txt);

    cc::Build::new()
        .files(src_c_files)
        .files(sas_c_files)
        .files(spss_c_files)
        .files(stata_c_files)
        .files(txt_c_files)
        .include(&src)
        .warnings(false)
        .compile("readstat");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // Linking
    println!("cargo:rustc-link-lib=static=readstat");
    // println!("cargo:rustc-link-search=/home/calex/code/readstat-rs/readstat/target/debug/build/readstat-sys-87fcd7a4da21a534/out");
    // println!("cargo:rustc-link-lib=readstat");
    // println!("cargo:rustc-link-search=/usr/local/lib");

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