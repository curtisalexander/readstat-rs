#![allow(dead_code)]

use arrow::datatypes::DataType;
use arrow_array::{
    Array, Date32Array, Float64Array, Int16Array, Int32Array, RecordBatch, StringArray,
    Time32SecondArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampSecondArray,
};
use std::os::raw::c_int;
use std::path::PathBuf;

// ── Setup helpers ──────────────────────────────────────────────────

/// Creates a `ReadStatPath` pointing at a test data file.
pub fn setup_path<P>(ds: P) -> Result<readstat::ReadStatPath, readstat::ReadStatError>
where
    P: AsRef<std::path::Path>,
{
    let sas_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(ds);
    readstat::ReadStatPath::new(sas_path)
}

/// Reads a dataset fully: path -> metadata -> data -> RecordBatch.
///
/// Returns (path, metadata, data) with the batch already produced.
pub fn setup_and_read(
    dataset: &str,
) -> (
    readstat::ReadStatPath,
    readstat::ReadStatMetadata,
    readstat::ReadStatData,
) {
    let rsp = setup_path(dataset).unwrap();
    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let mut d = readstat::ReadStatData::new().set_no_progress(true).init(
        md.clone(),
        0,
        md.row_count as u32,
    );
    d.read_data(&rsp).unwrap();
    (rsp, md, d)
}

/// Reads a dataset with a custom row range.
pub fn setup_and_read_rows(
    dataset: &str,
    row_start: u32,
    row_end: u32,
) -> (
    readstat::ReadStatPath,
    readstat::ReadStatMetadata,
    readstat::ReadStatData,
) {
    let rsp = setup_path(dataset).unwrap();
    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let mut d =
        readstat::ReadStatData::new()
            .set_no_progress(true)
            .init(md.clone(), row_start, row_end);
    d.read_data(&rsp).unwrap();
    (rsp, md, d)
}

/// Reads a dataset with `skip_row_count = true`.
pub fn setup_and_read_skip_row_count(
    dataset: &str,
) -> (
    readstat::ReadStatPath,
    readstat::ReadStatMetadata,
    readstat::ReadStatData,
) {
    let rsp = setup_path(dataset).unwrap();
    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata(&rsp, true).unwrap();
    let mut d = readstat::ReadStatData::new().set_no_progress(true).init(
        md.clone(),
        0,
        md.row_count as u32,
    );
    d.read_data(&rsp).unwrap();
    (rsp, md, d)
}

// ── Variable attribute helpers ─────────────────────────────────────

pub fn contains_var(d: &readstat::ReadStatData, var_index: i32) -> bool {
    d.vars.contains_key(&var_index)
}

pub fn get_metadata(d: &readstat::ReadStatData, var_index: i32) -> &readstat::ReadStatVarMetadata {
    d.vars.get(&var_index).unwrap()
}

pub fn get_var_attrs(
    d: &readstat::ReadStatData,
    var_index: i32,
) -> (
    readstat::ReadStatVarTypeClass,
    readstat::ReadStatVarType,
    Option<readstat::ReadStatVarFormatClass>,
    String,
    &DataType,
) {
    let m = get_metadata(d, var_index);
    let s = &d.schema;
    (
        m.var_type_class,
        m.var_type,
        m.var_format_class,
        m.var_format.clone(),
        s.fields[var_index as usize].data_type(),
    )
}

// ── Metadata assertion ─────────────────────────────────────────────

/// Expected metadata values for a dataset.
pub struct ExpectedMetadata<'a> {
    pub row_count: c_int,
    pub var_count: c_int,
    pub table_name: &'a str,
    pub file_label: &'a str,
    pub file_encoding: &'a str,
    pub version: c_int,
    pub is64bit: c_int,
    pub creation_time: &'a str,
    pub modified_time: &'a str,
}

/// Asserts all file-level metadata fields match expected values.
pub fn assert_metadata(md: &readstat::ReadStatMetadata, expected: &ExpectedMetadata) {
    assert_eq!(md.row_count, expected.row_count, "row_count");
    assert_eq!(md.var_count, expected.var_count, "var_count");
    assert_eq!(md.table_name, expected.table_name, "table_name");
    assert_eq!(md.file_label, expected.file_label, "file_label");
    assert_eq!(md.file_encoding, expected.file_encoding, "file_encoding");
    assert_eq!(md.version, expected.version, "version");
    assert_eq!(md.is64bit, expected.is64bit, "is64bit");
    assert_eq!(md.creation_time, expected.creation_time, "creation_time");
    assert_eq!(md.modified_time, expected.modified_time, "modified_time");
}

// ── Column downcast helpers ────────────────────────────────────────

/// Extracts a `Float64Array` from a `RecordBatch` by column index.
pub fn get_f64_col(batch: &RecordBatch, col: usize) -> &Float64Array {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
}

/// Extracts a `StringArray` from a `RecordBatch` by column index.
pub fn get_string_col(batch: &RecordBatch, col: usize) -> &StringArray {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

/// Extracts a `Date32Array` from a `RecordBatch` by column index.
pub fn get_date32_col(batch: &RecordBatch, col: usize) -> &Date32Array {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap()
}

/// Extracts a `TimestampSecondArray` from a `RecordBatch` by column index.
pub fn get_ts_sec_col(batch: &RecordBatch, col: usize) -> &TimestampSecondArray {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap()
}

/// Extracts a `TimestampMillisecondArray` from a `RecordBatch` by column index.
pub fn get_ts_ms_col(batch: &RecordBatch, col: usize) -> &TimestampMillisecondArray {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap()
}

/// Extracts a `TimestampMicrosecondArray` from a `RecordBatch` by column index.
pub fn get_ts_us_col(batch: &RecordBatch, col: usize) -> &TimestampMicrosecondArray {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
}

/// Extracts a `Time32SecondArray` from a `RecordBatch` by column index.
pub fn get_time32_col(batch: &RecordBatch, col: usize) -> &Time32SecondArray {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap()
}

/// Extracts a `Time64MicrosecondArray` from a `RecordBatch` by column index.
pub fn get_time64_us_col(batch: &RecordBatch, col: usize) -> &Time64MicrosecondArray {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap()
}

/// Extracts an `Int16Array` from a `RecordBatch` by column index.
pub fn get_i16_col(batch: &RecordBatch, col: usize) -> &Int16Array {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int16Array>()
        .unwrap()
}

/// Extracts an `Int32Array` from a `RecordBatch` by column index.
pub fn get_i32_col(batch: &RecordBatch, col: usize) -> &Int32Array {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
}

/// Checks if a column value is null at the given row.
pub fn is_null(batch: &RecordBatch, col: usize, row: usize) -> bool {
    batch.column(col).is_null(row)
}
