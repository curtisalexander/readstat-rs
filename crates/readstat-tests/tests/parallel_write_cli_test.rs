use assert_cmd::Command;
use assert_fs::prelude::*;
use std::path::PathBuf;

#[test]
fn test_parallel_write_cli_option() {
    // Create a temp directory for output
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.parquet");

    // Get path to test data
    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    // Run the CLI with parallel write enabled
    let mut cmd = Command::cargo_bin("readstat").unwrap();
    cmd.arg("data")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("parquet")
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
fn test_parallel_write_buffer_size_cli_option() {
    // Create a temp directory for output
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.parquet");

    // Get path to test data
    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    // Run the CLI with custom buffer size
    let mut cmd = Command::cargo_bin("readstat").unwrap();
    cmd.arg("data")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("parquet")
        .arg("--parallel")
        .arg("--parallel-write")
        .arg("--parallel-write-buffer-mb")
        .arg("50")
        .arg("--overwrite");

    let assert = cmd.assert();
    assert.success();

    // Verify the output file was created
    output_file.assert(predicates::path::exists());

    temp.close().unwrap();
}

#[test]
fn test_parallel_write_buffer_size_default() {
    // Create a temp directory for output
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.parquet");

    // Get path to test data
    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    // Run the CLI without specifying buffer size (should use default 100 MB)
    let mut cmd = Command::cargo_bin("readstat").unwrap();
    cmd.arg("data")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("parquet")
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
fn test_parallel_write_buffer_size_small() {
    // Test with very small buffer (1 MB) to ensure spilling works
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.parquet");

    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    let mut cmd = Command::cargo_bin("readstat").unwrap();
    cmd.arg("data")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("parquet")
        .arg("--parallel")
        .arg("--parallel-write")
        .arg("--parallel-write-buffer-mb")
        .arg("1")
        .arg("--overwrite");

    let assert = cmd.assert();
    assert.success();

    output_file.assert(predicates::path::exists());

    temp.close().unwrap();
}

#[test]
fn test_parallel_write_buffer_size_large() {
    // Test with large buffer (500 MB)
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.parquet");

    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    let mut cmd = Command::cargo_bin("readstat").unwrap();
    cmd.arg("data")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("parquet")
        .arg("--parallel")
        .arg("--parallel-write")
        .arg("--parallel-write-buffer-mb")
        .arg("500")
        .arg("--overwrite");

    let assert = cmd.assert();
    assert.success();

    output_file.assert(predicates::path::exists());

    temp.close().unwrap();
}

#[test]
fn test_parallel_write_without_parallel_reads() {
    // Test that parallel-write works even without parallel reads
    let temp = assert_fs::TempDir::new().unwrap();
    let output_file = temp.child("output.parquet");

    let test_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("all_types.sas7bdat");

    let mut cmd = Command::cargo_bin("readstat").unwrap();
    cmd.arg("data")
        .arg(&test_data_path)
        .arg("--output")
        .arg(output_file.path())
        .arg("--format")
        .arg("parquet")
        .arg("--parallel-write")
        .arg("--overwrite");

    let assert = cmd.assert();
    assert.success();

    // Note: parallel-write is only effective with --parallel, so this should use sequential write
    output_file.assert(predicates::path::exists());

    temp.close().unwrap();
}
