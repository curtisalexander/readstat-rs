use assert_cmd::prelude::*; // Add methods on commands
use predicates::prelude::*; // Used for writing assertions
use std::process::Command; // Run programs

#[test]
fn cli_data_subcommand_to_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("readstat")?;

    cmd.arg("data")
        .arg("tests/data/cars.sas7bdat")
        .args(["--format", "parquet"])
        .args(["--output", r#"tests\data\cars.parquet"#])
        .arg("--overwrite");

    cmd.assert().success().stdout(predicate::str::contains(
        "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
    ));

    Ok(())
}
