use path_abs::{PathAbs, PathInfo};
use readstat;
use std::env;

#[test]
fn input_file_sas7bdat() {
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.parent().unwrap().join("data");
    let sas_path = data_dir.join("hasmissing.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    assert_eq!(rsp.extension, String::from("sas7bdat"));
}

#[test]
fn input_file_not_sas7bdat() {
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.parent().unwrap().join("data");
    let sas_path = data_dir.join("README.md");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None);

    assert!(rsp.is_err());
}