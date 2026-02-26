#![allow(clippy::float_cmp)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]

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
