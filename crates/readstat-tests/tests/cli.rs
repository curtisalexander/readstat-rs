use assert_cmd::Command; // Add methods on commands
use predicates::prelude::*; // Used for writing assertions
                            // use std::process::Command; // Run programs

#[test]
fn cli_file_does_not_exist() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(mut cmd) = Command::cargo_bin("readstat") {
        cmd.arg("data").arg("tests/data/adataset.sas7bdat");
        cmd.assert().failure().stderr(
            predicate::str::is_match(r#"^(Stopping with error: File)\s(.+)\s(does not exist!\n)$"#)
                .unwrap(),
        );
    }
    Ok(())
}
