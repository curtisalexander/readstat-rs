#![allow(clippy::float_cmp)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]

use assert_cmd::Command;
use assert_fs::prelude::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
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

fn read_parquet(path: &std::path::Path) -> arrow_array::RecordBatch {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let batches = builder
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

#[test]
fn test_parallel_write_cli_option() {
    let temp = assert_fs::TempDir::new().unwrap();
    let parallel_output = temp.child("parallel.parquet");
    let serial_output = temp.child("serial.parquet");

    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("cars.sas7bdat");

    let mut cmd = readstat_cmd();
    cmd.arg("convert")
        .arg(&test_data_path)
        .arg("--output")
        .arg(parallel_output.path())
        .args(["--rows", "100", "--stream-rows", "17"])
        .args(["--columns", "Brand,Model,EngineSize"])
        .arg("--parallel-write")
        .arg("--overwrite");
    cmd.assert().success();

    readstat_cmd()
        .arg("convert")
        .arg(&test_data_path)
        .arg("--output")
        .arg(serial_output.path())
        .args(["--rows", "100", "--stream-rows", "17"])
        .args(["--columns", "Brand,Model,EngineSize"])
        .arg("--overwrite")
        .assert()
        .success();

    assert_eq!(
        read_parquet(parallel_output.path()),
        read_parquet(serial_output.path())
    );

    temp.close().unwrap();
}

#[test]
fn test_parallel_reader_and_writer_remain_compatible() {
    // Create a temp directory for output
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.parquet");

    // Get path to test data
    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    let mut cmd = readstat_cmd();
    cmd.arg("convert")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("parquet")
        // The two flags remain compatible while parallel reading is retained
        // for benchmark comparison.
        .arg("--parallel")
        .arg("--parallel-write")
        .arg("--overwrite");

    let assert = cmd.assert();
    assert.success();

    // Verify the output file was created
    output_file.assert(predicates::path::exists());

    temp.close().unwrap();
}

#[test]
fn test_parallel_write_rejects_non_parquet_output() {
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.csv");

    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    let mut cmd = readstat_cmd();
    cmd.arg("convert")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("csv")
        .arg("--parallel-write")
        .arg("--overwrite");

    cmd.assert()
        .failure()
        .stderr(predicates::str::contains("only supported for Parquet"));

    temp.close().unwrap();
}

#[test]
fn test_parallel_write_rejects_whole_file_reader() {
    // A whole-file batch cannot provide useful write parallelism and defeats
    // the bounded-memory contract.
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.parquet");

    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    let mut cmd = readstat_cmd();
    cmd.arg("convert")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("parquet")
        .arg("--reader")
        .arg("mem")
        .arg("--parallel-write")
        .arg("--overwrite");

    cmd.assert()
        .failure()
        .stderr(predicates::str::contains("--reader mem"));

    temp.close().unwrap();
}

#[test]
fn whole_file_reader_allows_zero_selected_rows() {
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("empty.parquet");
    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    readstat_cmd()
        .arg("convert")
        .arg(test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .args(["--reader", "mem", "--rows", "0", "--overwrite"])
        .assert()
        .success();

    output_file.assert(predicates::path::exists());
    temp.close().unwrap();
}
