use assert_cmd::Command;
use assert_fs::NamedTempFile;
use predicates::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::OnceLock;

/// Cache the built binary path to avoid rebuilding for each test.
static READSTAT_BIN: OnceLock<PathBuf> = OnceLock::new();

/// Helper function to get the readstat binary command.
/// Uses escargot to build and locate the binary in the workspace (once).
fn readstat_cmd() -> Command {
    let bin_path = READSTAT_BIN.get_or_init(|| {
        let bin = escargot::CargoBuild::new()
            .bin("readstat")
            .current_release()
            .current_target()
            .manifest_path("../readstat-cli/Cargo.toml")
            .run()
            .expect("Failed to build readstat binary");

        bin.path().to_path_buf()
    });

    Command::new(bin_path)
}

const EXPECTED_COLUMNS: &[&str] = &[
    "Brand", "Model", "Minivan", "Wagon", "Pickup", "Automatic",
    "EngineSize", "Cylinders", "CityMPG", "HwyMPG", "SUV", "AWD", "Hybrid",
];

/// Read a CSV file and return (header_fields, data_row_count).
fn read_csv_info(path: &std::path::Path) -> (Vec<String>, usize) {
    let f = std::fs::File::open(path).expect("failed to open CSV file");
    let reader = BufReader::new(f);
    let mut lines = reader.lines();

    let header_line = lines.next().expect("CSV file is empty").unwrap();
    let header_fields: Vec<String> = header_line.split(',').map(|s| s.to_string()).collect();

    let data_row_count = lines.count();
    (header_fields, data_row_count)
}

#[test]
fn cars_to_csv() {
    let tempfile = NamedTempFile::new("cars.csv").unwrap();
    let mut cmd = readstat_cmd();

    cmd.arg("data")
        .arg("tests/data/cars.sas7bdat")
        .args(["--format", "csv"])
        .args(["--output", tempfile.as_os_str().to_str().unwrap()]);

    cmd.assert().success().stdout(predicate::str::contains(
        "In total, wrote 1,081 rows from file cars.sas7bdat into cars.csv",
    ));

    let (header, data_rows) = read_csv_info(tempfile.path());
    let expected: Vec<String> = EXPECTED_COLUMNS.iter().map(|s| s.to_string()).collect();

    assert_eq!(header, expected, "CSV header row should contain the correct column names");
    assert_eq!(data_rows, 1081, "CSV should contain 1081 data rows");

    tempfile.close().unwrap();
}

#[test]
fn cars_to_csv_with_streaming() {
    let tempfile = NamedTempFile::new("cars_streaming.csv").unwrap();
    let mut cmd = readstat_cmd();

    cmd.arg("data")
        .arg("tests/data/cars.sas7bdat")
        .args(["--format", "csv"])
        .args(["--output", tempfile.as_os_str().to_str().unwrap()])
        .args(["--stream-rows", "500"]);

    cmd.assert().success().stdout(predicate::str::contains(
        "In total, wrote 1,081 rows from file cars.sas7bdat into",
    ));

    let (header, data_rows) = read_csv_info(tempfile.path());
    let expected: Vec<String> = EXPECTED_COLUMNS.iter().map(|s| s.to_string()).collect();

    assert_eq!(header, expected, "CSV header row should contain the correct column names");
    assert_eq!(data_rows, 1081, "CSV should contain 1081 data rows");

    tempfile.close().unwrap();
}

#[test]
fn cars_to_csv_overwrite() {
    let tempfile = NamedTempFile::new("cars_overwrite.csv").unwrap();

    // First write
    let mut cmd = readstat_cmd();
    cmd.arg("data")
        .arg("tests/data/cars.sas7bdat")
        .args(["--format", "csv"])
        .args(["--output", tempfile.as_os_str().to_str().unwrap()]);
    cmd.assert().success();

    // Overwrite
    let mut cmd = readstat_cmd();
    cmd.arg("data")
        .arg("tests/data/cars.sas7bdat")
        .args(["--format", "csv"])
        .args(["--output", tempfile.as_os_str().to_str().unwrap()])
        .arg("--overwrite");
    cmd.assert().success();

    let (header, data_rows) = read_csv_info(tempfile.path());
    let expected: Vec<String> = EXPECTED_COLUMNS.iter().map(|s| s.to_string()).collect();

    assert_eq!(header, expected, "CSV header row should be present after overwrite");
    assert_eq!(data_rows, 1081, "CSV should contain 1081 data rows after overwrite");

    tempfile.close().unwrap();
}
