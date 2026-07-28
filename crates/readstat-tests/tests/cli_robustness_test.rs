//! CLI robustness tests: zero-row outputs must be valid files, and parse
//! errors must produce a nonzero exit code rather than silent partial output.

use assert_cmd::Command;
use assert_fs::NamedTempFile;
use predicates::prelude::*;
use std::path::PathBuf;
use std::sync::OnceLock;

/// Cache the built binary path to avoid rebuilding for each test.
static READSTAT_BIN: OnceLock<PathBuf> = OnceLock::new();

fn readstat_bin() -> &'static std::path::Path {
    READSTAT_BIN
        .get_or_init(|| {
            let bin = escargot::CargoBuild::new()
                .bin("readstat")
                .current_release()
                .current_target()
                .manifest_path("../readstat-cli/Cargo.toml")
                .run()
                .expect("Failed to build readstat binary");

            bin.path().to_path_buf()
        })
        .as_path()
}

fn readstat_cmd() -> Command {
    Command::new(readstat_bin())
}

#[cfg(unix)]
fn run_with_closed_stdout(args: &[&str]) -> std::process::Output {
    let (reader, writer) = std::io::pipe().expect("failed to create stdout pipe");
    drop(reader);

    std::process::Command::new(readstat_bin())
        .args(args)
        .stdout(writer)
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("failed to run readstat")
}

#[cfg(unix)]
fn assert_closed_stdout_success(args: &[&str]) {
    let output = run_with_closed_stdout(args);
    assert!(
        output.status.success(),
        "status: {}; stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `--rows 0` must still create a header-only CSV file.
#[test]
fn zero_rows_csv_creates_header_only_file() {
    let tempfile = NamedTempFile::new("zero.csv").unwrap();

    readstat_cmd()
        .arg("convert")
        .arg("tests/data/cars.sas7bdat")
        .arg("--rows")
        .arg("0")
        .arg("--output")
        .arg(tempfile.path())
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
        .arg("convert")
        .arg("tests/data/cars.sas7bdat")
        .arg("--rows")
        .arg("0")
        .arg("--output")
        .arg(tempfile.path())
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
        .arg("convert")
        .arg("tests/data/cars.sas7bdat")
        .arg("--rows")
        .arg("0")
        .arg("--output")
        .arg(tempfile.path())
        .arg("--overwrite")
        .assert()
        .success();

    let bytes = std::fs::read(tempfile.path()).unwrap();
    assert!(bytes.starts_with(b"ARROW1"), "missing Arrow IPC file magic");
}

/// Extension inference also covers NDJSON; an empty result is an empty file.
#[test]
fn zero_rows_ndjson_creates_valid_empty_file() {
    let tempfile = NamedTempFile::new("zero.ndjson").unwrap();

    readstat_cmd()
        .arg("convert")
        .arg("tests/data/cars.sas7bdat")
        .arg("--rows")
        .arg("0")
        .arg("--output")
        .arg(tempfile.path())
        .arg("--overwrite")
        .assert()
        .success();

    assert!(std::fs::read(tempfile.path()).unwrap().is_empty());
}

/// A file that fails mid-parse must exit nonzero — never report success over
/// missing data.
#[test]
fn truncated_input_exits_nonzero() {
    let data = std::fs::read("tests/data/rand_ds_largepage_ok.sas7bdat").unwrap();
    let truncated = NamedTempFile::new("truncated.sas7bdat").unwrap();
    std::fs::write(truncated.path(), &data[..data.len() / 2]).unwrap();

    let out = NamedTempFile::new("truncated_out.parquet").unwrap();
    std::fs::write(out.path(), b"existing destination").unwrap();

    readstat_cmd()
        .arg("convert")
        .arg(truncated.path())
        .arg("--output")
        .arg(out.path())
        .arg("--format")
        .arg("parquet")
        .arg("--overwrite")
        .assert()
        .failure();

    assert_eq!(
        std::fs::read(out.path()).unwrap(),
        b"existing destination",
        "a failed conversion must not damage the previous destination"
    );
}

#[test]
fn ineffective_option_combinations_are_rejected() {
    let input = "tests/data/cars.sas7bdat";

    readstat_cmd()
        .args(["convert", input, "--reader", "mem", "--parallel"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument '--parallel'"));

    readstat_cmd()
        .args(["convert", input, "--parallel-write"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "unexpected argument '--parallel-write'",
        ));

    readstat_cmd()
        .args(["convert", input, "--format", "parquet"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "only CSV may be written to stdout",
        ));
}

#[cfg(unix)]
#[test]
fn stdout_commands_accept_an_early_closed_pipe() {
    let input = "tests/data/cars.sas7bdat";

    assert_closed_stdout_success(&["metadata", input, "--as-json"]);
    assert_closed_stdout_success(&["preview", input, "--rows", "428", "--no-progress"]);
    assert_closed_stdout_success(&["convert", input, "--rows", "1", "--no-progress"]);
}

#[cfg(unix)]
#[test]
fn closed_stderr_does_not_fail_a_valid_conversion() {
    let output = NamedTempFile::new("closed-stderr.csv").unwrap();
    let (reader, writer) = std::io::pipe().expect("failed to create stderr pipe");
    drop(reader);

    let status = std::process::Command::new(readstat_bin())
        .args([
            "convert",
            "tests/data/cars.sas7bdat",
            "--rows",
            "1",
            "--output",
        ])
        .arg(output.path())
        .args(["--overwrite", "--no-progress"])
        .stdout(std::process::Stdio::null())
        .stderr(writer)
        .status()
        .expect("failed to run readstat");

    assert!(status.success(), "status was {status}");
    assert!(
        std::fs::metadata(output.path()).unwrap().len() > 0,
        "conversion did not produce output"
    );
}

#[cfg(unix)]
#[test]
fn closed_stderr_preserves_a_runtime_error_exit_code() {
    let (reader, writer) = std::io::pipe().expect("failed to create stderr pipe");
    drop(reader);

    let status = std::process::Command::new(readstat_bin())
        .args(["metadata", "tests/data/definitely-missing.sas7bdat"])
        .stdout(std::process::Stdio::null())
        .stderr(writer)
        .status()
        .expect("failed to run readstat");

    assert_eq!(status.code(), Some(1), "status was {status}");
}
