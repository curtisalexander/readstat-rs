use assert_cmd::prelude::*; // Add methods on commands
use predicates::prelude::*; // Used for writing assertions

mod common;

#[test]
fn cars_to_parquet() {
    let (mut cmd, _out_file) = common::cli_data_to_parquet("cars").unwrap();

    cmd.assert().success().stdout(predicate::str::contains(
        "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
    ));
}

/*
#[test]
fn cars_from_parquet() {
    let _ = common::cli_data_to_parquet("cars").unwrap();

    let df = common::cli_data_from_parquet("cars").unwrap();

    let (height, width) = df.shape();

    assert_eq!(height, 1081)
}
*/
