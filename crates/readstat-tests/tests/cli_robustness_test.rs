//! CLI robustness tests: zero-row outputs must be valid files, and parse
//! errors must produce a nonzero exit code rather than silent partial output.

use assert_cmd::Command;
use assert_fs::NamedTempFile;
use std::path::PathBuf;
use std::sync::OnceLock;

/// Cache the built binary path to avoid rebuilding for each test.
static READSTAT_BIN: OnceLock<PathBuf> = OnceLock::new();

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

/// `--rows 0` must still create a header-only CSV file.
#[test]
fn zero_rows_csv_creates_header_only_file() {
    let tempfile = NamedTempFile::new("zero.csv").unwrap();

    readstat_cmd()
        .arg("data")
        .arg("tests/data/cars.sas7bdat")
        .arg("--rows")
        .arg("0")
        .arg("--output")
        .arg(tempfile.path())
        .arg("--format")
        .arg("csv")
        .arg("--overwrite")
        .assert()
        .success();

    let contents = std::fs::read_to_string(tempfile.path()).unwrap();
    let mut lines = contents.lines();
    let header = lines.next().expect("expected a header row");
    assert!(header.starts_with("Brand,Model,"), "header was: {header}");
    assert_eq!(lines.next(), None, "expected no data rows");
}

/// `--rows 0` must still create a structurally valid (empty) Parquet file.
#[test]
fn zero_rows_parquet_creates_valid_empty_file() {
    let tempfile = NamedTempFile::new("zero.parquet").unwrap();

    readstat_cmd()
        .arg("data")
        .arg("tests/data/cars.sas7bdat")
        .arg("--rows")
        .arg("0")
        .arg("--output")
        .arg(tempfile.path())
        .arg("--format")
        .arg("parquet")
        .arg("--overwrite")
        .assert()
        .success();

    let bytes = std::fs::read(tempfile.path()).unwrap();
    assert!(bytes.len() > 8, "file too small to be valid parquet");
    assert_eq!(&bytes[..4], b"PAR1", "missing parquet header magic");
    assert_eq!(
        &bytes[bytes.len() - 4..],
        b"PAR1",
        "missing parquet footer magic — file was not finalized"
    );
}

/// `--rows 0` must still create a structurally valid (empty) Feather file.
#[test]
fn zero_rows_feather_creates_valid_empty_file() {
    let tempfile = NamedTempFile::new("zero.feather").unwrap();

    readstat_cmd()
        .arg("data")
        .arg("tests/data/cars.sas7bdat")
        .arg("--rows")
        .arg("0")
        .arg("--output")
        .arg(tempfile.path())
        .arg("--format")
        .arg("feather")
        .arg("--overwrite")
        .assert()
        .success();

    let bytes = std::fs::read(tempfile.path()).unwrap();
    assert!(
        bytes.starts_with(b"ARROW1"),
        "missing Arrow IPC file magic"
    );
}

/// A file that fails mid-parse must exit nonzero — never report success over
/// missing data.
#[test]
fn truncated_input_exits_nonzero() {
    let data = std::fs::read("tests/data/rand_ds_largepage_ok.sas7bdat").unwrap();
    let truncated = NamedTempFile::new("truncated.sas7bdat").unwrap();
    std::fs::write(truncated.path(), &data[..data.len() / 2]).unwrap();

    let out = NamedTempFile::new("truncated_out.parquet").unwrap();

    readstat_cmd()
        .arg("data")
        .arg(truncated.path())
        .arg("--output")
        .arg(out.path())
        .arg("--format")
        .arg("parquet")
        .arg("--overwrite")
        .assert()
        .failure();
}
