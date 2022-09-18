mod common;

#[test]
fn input_file_sas7bdat() {
    let rsp = common::setup_path("hasmissing.sas7bdat").unwrap();
    assert_eq!(rsp.extension, String::from("sas7bdat"));
}

#[test]
fn input_file_not_sas7bdat() {
    // setup path
    let rsp = common::setup_path("README.md");
    assert!(rsp.is_err());
}
