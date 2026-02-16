use assert_cmd::Command;
use assert_fs::NamedTempFile;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{fs::File, path::PathBuf, sync::OnceLock};

/// Cache the built binary path to avoid rebuilding for each test.
static READSTAT_BIN: OnceLock<PathBuf> = OnceLock::new();

/// Helper function to get the readstat binary command.
fn readstat_cmd() -> Command {
    let bin_path = READSTAT_BIN.get_or_init(|| {
        let bin = escargot::CargoBuild::new()
            .bin("readstat")
            .current_release()
            .current_target()
            .manifest_path("../readstat/Cargo.toml")
            .run()
            .expect("Failed to build readstat binary");

        bin.path().to_path_buf()
    });

    Command::new(bin_path)
}

#[test]
fn somedata_parquet_has_column_labels() {
    // Convert somedata.sas7bdat to parquet
    let tempfile = NamedTempFile::new("somedata_labels.parquet")
        .expect("Failed to create temp file");

    let mut cmd = readstat_cmd();
    cmd.arg("data")
        .arg("tests/data/somedata.sas7bdat")
        .args(["--format", "parquet"])
        .args(["--output", tempfile.as_os_str().to_str().unwrap()])
        .arg("--no-progress");

    cmd.assert().success();

    // Read the parquet file and check metadata
    let file = File::open(tempfile.path()).expect("Failed to open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Failed to create parquet reader");

    let schema = builder.schema();

    // Check that column labels are present in field metadata
    // somedata.sas7bdat has these labels:
    // ID -> "ID Number"
    // GP -> "Intervention Group"
    // AGE -> "Age on Jan 1, 2000"
    // TIME1 -> "Baseline"
    // TIME2 -> "6 Months"
    // TIME3 -> "12 Months"
    // TIME4 -> "24 Months"
    // STATUS -> "Socioeconomic Status"

    let id_field = schema.field_with_name("ID").expect("ID field not found");
    assert!(id_field.metadata().contains_key("label"), "ID field should have label metadata");
    assert_eq!(id_field.metadata().get("label").unwrap(), "ID Number");

    let gp_field = schema.field_with_name("GP").expect("GP field not found");
    assert!(gp_field.metadata().contains_key("label"), "GP field should have label metadata");
    assert_eq!(gp_field.metadata().get("label").unwrap(), "Intervention Group");

    let age_field = schema.field_with_name("AGE").expect("AGE field not found");
    assert!(age_field.metadata().contains_key("label"), "AGE field should have label metadata");
    assert_eq!(age_field.metadata().get("label").unwrap(), "Age on Jan 1, 2000");

    let time1_field = schema.field_with_name("TIME1").expect("TIME1 field not found");
    assert!(time1_field.metadata().contains_key("label"), "TIME1 field should have label metadata");
    assert_eq!(time1_field.metadata().get("label").unwrap(), "Baseline");

    let time2_field = schema.field_with_name("TIME2").expect("TIME2 field not found");
    assert!(time2_field.metadata().contains_key("label"), "TIME2 field should have label metadata");
    assert_eq!(time2_field.metadata().get("label").unwrap(), "6 Months");

    let time3_field = schema.field_with_name("TIME3").expect("TIME3 field not found");
    assert!(time3_field.metadata().contains_key("label"), "TIME3 field should have label metadata");
    assert_eq!(time3_field.metadata().get("label").unwrap(), "12 Months");

    let time4_field = schema.field_with_name("TIME4").expect("TIME4 field not found");
    assert!(time4_field.metadata().contains_key("label"), "TIME4 field should have label metadata");
    assert_eq!(time4_field.metadata().get("label").unwrap(), "24 Months");

    let status_field = schema.field_with_name("STATUS").expect("STATUS field not found");
    assert!(status_field.metadata().contains_key("label"), "STATUS field should have label metadata");
    assert_eq!(status_field.metadata().get("label").unwrap(), "Socioeconomic Status");

    // SEX and GENDER should not have labels (they're empty in the source)
    let sex_field = schema.field_with_name("SEX").expect("SEX field not found");
    assert!(!sex_field.metadata().contains_key("label"), "SEX field should not have label metadata");

    let gender_field = schema.field_with_name("GENDER").expect("GENDER field not found");
    assert!(!gender_field.metadata().contains_key("label"), "GENDER field should not have label metadata");

    tempfile.close().unwrap();
}

#[test]
fn cars_parquet_has_table_label() {
    // Convert cars.sas7bdat to parquet
    // cars.sas7bdat has file_label = "Written by SAS"
    let tempfile = NamedTempFile::new("cars_labels.parquet")
        .expect("Failed to create temp file");

    let mut cmd = readstat_cmd();
    cmd.arg("data")
        .arg("tests/data/cars.sas7bdat")
        .args(["--format", "parquet"])
        .args(["--output", tempfile.as_os_str().to_str().unwrap()])
        .arg("--no-progress");

    cmd.assert().success();

    // Read the parquet file and check metadata
    let file = File::open(tempfile.path()).expect("Failed to open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Failed to create parquet reader");

    let schema = builder.schema();

    // Check that table label is present in schema metadata
    assert!(schema.metadata().contains_key("table_label"), "Schema should have table_label metadata");
    assert_eq!(schema.metadata().get("table_label").unwrap(), "Written by SAS");

    tempfile.close().unwrap();
}

#[test]
fn hasmissing_parquet_has_column_labels() {
    // Convert hasmissing.sas7bdat to parquet
    let tempfile = NamedTempFile::new("hasmissing_labels.parquet")
        .expect("Failed to create temp file");

    let mut cmd = readstat_cmd();
    cmd.arg("data")
        .arg("tests/data/hasmissing.sas7bdat")
        .args(["--format", "parquet"])
        .args(["--output", tempfile.as_os_str().to_str().unwrap()])
        .arg("--no-progress");

    cmd.assert().success();

    // Read the parquet file and check metadata
    let file = File::open(tempfile.path()).expect("Failed to open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Failed to create parquet reader");

    let schema = builder.schema();

    // Check some of the column labels
    let pre_field = schema.field_with_name("PRE").expect("PRE field not found");
    assert!(pre_field.metadata().contains_key("label"), "PRE field should have label metadata");
    assert_eq!(pre_field.metadata().get("label").unwrap(), "PRE");

    let month6_field = schema.field_with_name("MONTH6").expect("MONTH6 field not found");
    assert!(month6_field.metadata().contains_key("label"), "MONTH6 field should have label metadata");
    assert_eq!(month6_field.metadata().get("label").unwrap(), "MONTH6");

    tempfile.close().unwrap();
}
