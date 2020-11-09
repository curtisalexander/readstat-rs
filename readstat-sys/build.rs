extern crate bindgen;

use cc;
use dunce;
use std::env;
use std::path::PathBuf;

fn main() {
    let project_dir = dunce::canonicalize(PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())).unwrap();

    let src = project_dir.join("vendor").join("ReadStat").join("src");
    let sas = src.join("sas");
    let spss = src.join("spss");
    let stata = src.join("stata");
    let txt = src.join("txt");
  
    cc::Build::new()
        .file(src.join("CKHashTable.c"))
        .file(src.join("readstat_bits.c"))
        .file(src.join("readstat_convert.c"))
        .file(src.join("readstat_error.c"))
        .file(src.join("readstat_io_unistd.c"))
        .file(src.join("readstat_malloc.c"))
        .file(src.join("readstat_metadata.c"))
        .file(src.join("readstat_parser.c"))
        .file(src.join("readstat_value.c"))
        .file(src.join("readstat_variable.c"))
        .file(src.join("readstat_writer.c"))
        .file(sas.join("ieee.c"))
        .file(sas.join("readstat_sas.c"))
        .file(sas.join("readstat_sas7bcat_read.c"))
        .file(sas.join("readstat_sas7bcat_write.c"))
        .file(sas.join("readstat_sas7bdat_read.c"))
        .file(sas.join("readstat_sas7bdat_write.c"))
        .file(sas.join("readstat_sas_rle.c"))
        .file(sas.join("readstat_xport.c"))
        .file(sas.join("readstat_xport_read.c"))
        .file(sas.join("readstat_xport_write.c"))
        .file(spss.join("readstat_por.c"))
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
        .file(txt.join("readstat_txt_read.c"))
        .include(&src)
        .warnings(false)
        .compile("readstat");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // Linking
    println!("cargo:rustc-link-lib=static=readstat");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate bindings for
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