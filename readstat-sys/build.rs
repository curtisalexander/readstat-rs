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
    let sas = project_dir.join("vendor").join("ReadStat").join("src").join("sas");
    let spss = project_dir.join("vendor").join("ReadStat").join("src").join("spss");
    let stata = project_dir.join("vendor").join("ReadStat").join("src").join("stata");
    let txt = project_dir.join("vendor").join("ReadStat").join("src").join("txt");
    
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
}