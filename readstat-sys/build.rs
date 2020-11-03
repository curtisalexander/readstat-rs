use cc::Build;
use std::env;
use std::path::PathBuf;

fn main() {
    let project_fir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .canonicalize()
        .unwrap();
    let root_dir = project_dir.parent().unwrap();
    let src = root_dir.join("vendor").join("ReadStat").join("src");


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
        .file(src.join("sas").join("ieee.c"))
        .file(src.join("sas").join("readstat_sas.c"))
        .file(src.join("sas").join("readstat_cas7bcat_read.c"))
        .file(src.join("sas").join("readstat_sas7bcat_write.c"))
        .file(src.join("sas").join("readstat_sas7bdat_read.c"))
        .file(src.join("sas").join("readstat_sas7bdat_write.c"))
        .file(src.join("sas").join("readstat_sas_rle.c"))
        .file(src.join("sas").join("readstat_xport.c"))
        .file(src.join("sas").join("readstat_xport_read.c"))
        .include(&src)
        .warnings(false)
        .compile("readstat");
}