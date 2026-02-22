/// Tests to verify the arrow2 -> arrow migration correctness.
/// These tests ensure that the migration from arrow2 to the arrow crate
/// does not introduce any data integrity issues.
use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    Array, Date32Array, Float64Array, StringArray, Time32SecondArray, TimestampSecondArray,
};
use chrono::{Datelike, NaiveDate};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};
use std::sync::Arc;

mod common;

/// Helper to initialize test data
fn init_all_types() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path("all_types.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    let d = readstat::ReadStatData::new().set_no_progress(true).init(
        md.clone(),
        0,
        md.row_count as u32,
    );

    (rsp, md, d)
}

/// Test: Verify RecordBatch structure is correctly created
#[test]
fn migration_record_batch_structure() {
    let (rsp, md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok(), "read_data should succeed");

    // Verify batch exists
    assert!(d.batch.is_some(), "RecordBatch should be created");

    let batch = d.batch.as_ref().unwrap();

    // Verify column count matches schema
    assert_eq!(
        batch.num_columns(),
        md.var_count as usize,
        "Column count should match variable count"
    );

    // Verify row count matches
    assert_eq!(
        batch.num_rows(),
        md.row_count as usize,
        "Row count should match metadata"
    );

    // Verify schema is properly attached
    assert_eq!(
        batch.schema().fields().len(),
        md.var_count as usize,
        "Schema field count should match variable count"
    );
}

/// Test: Verify schema data types match expected arrow types
#[test]
fn migration_schema_data_types() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let schema = &d.schema;

    // Verify each field's data type
    // Field 0: _int -> Float64 (SAS stores all numbers as doubles)
    assert!(
        matches!(schema.fields[0].data_type(), DataType::Float64),
        "_int should be Float64"
    );

    // Field 1: _float -> Float64
    assert!(
        matches!(schema.fields[1].data_type(), DataType::Float64),
        "_float should be Float64"
    );

    // Field 2: _char -> Utf8
    assert!(
        matches!(schema.fields[2].data_type(), DataType::Utf8),
        "_char should be Utf8"
    );

    // Field 3: _string -> Utf8
    assert!(
        matches!(schema.fields[3].data_type(), DataType::Utf8),
        "_string should be Utf8"
    );

    // Field 4: _date -> Date32
    assert!(
        matches!(schema.fields[4].data_type(), DataType::Date32),
        "_date should be Date32"
    );

    // Field 5: _datetime -> Timestamp(Second, None)
    assert!(
        matches!(
            schema.fields[5].data_type(),
            DataType::Timestamp(TimeUnit::Second, None)
        ),
        "_datetime should be Timestamp(Second)"
    );

    // Field 6: _datetime_with_ms -> Timestamp(Millisecond, None)
    assert!(
        matches!(
            schema.fields[6].data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        ),
        "_datetime_with_ms should be Timestamp(Millisecond)"
    );

    // Field 7: _datetime_with_us -> Timestamp(Microsecond, None)
    assert!(
        matches!(
            schema.fields[7].data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        ),
        "_datetime_with_us should be Timestamp(Microsecond)"
    );

    // Field 8: _time -> Time32(Second)
    assert!(
        matches!(
            schema.fields[8].data_type(),
            DataType::Time32(TimeUnit::Second)
        ),
        "_time should be Time32(Second)"
    );

    // Field 9: _time_with_us -> Time64(Microsecond)
    assert!(
        matches!(
            schema.fields[9].data_type(),
            DataType::Time64(TimeUnit::Microsecond)
        ),
        "_time_with_us should be Time64(Microsecond)"
    );
}

/// Test: Verify all fields are marked as nullable
#[test]
fn migration_schema_nullable() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let schema = &d.schema;

    for field in schema.fields() {
        assert!(
            field.is_nullable(),
            "Field '{}' should be nullable",
            field.name()
        );
    }
}

/// Test: Verify array downcasting works for all types
#[test]
fn migration_array_downcasting() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Test Float64Array downcast (columns 0, 1)
    let col_float = columns[0].as_any().downcast_ref::<Float64Array>();
    assert!(
        col_float.is_some(),
        "Column 0 should downcast to Float64Array"
    );

    // Test StringArray downcast (columns 2, 3)
    let col_string = columns[3].as_any().downcast_ref::<StringArray>();
    assert!(
        col_string.is_some(),
        "Column 3 should downcast to StringArray"
    );

    // Test Date32Array downcast (column 4)
    let col_date = columns[4].as_any().downcast_ref::<Date32Array>();
    assert!(
        col_date.is_some(),
        "Column 4 should downcast to Date32Array"
    );

    // Test TimestampSecondArray downcast (columns 5, 6)
    let col_timestamp = columns[5].as_any().downcast_ref::<TimestampSecondArray>();
    assert!(
        col_timestamp.is_some(),
        "Column 5 should downcast to TimestampSecondArray"
    );

    // Test Time32SecondArray downcast (column 8)
    let col_time = columns[8].as_any().downcast_ref::<Time32SecondArray>();
    assert!(
        col_time.is_some(),
        "Column 8 should downcast to Time32SecondArray"
    );
}

/// Test: Verify null handling in arrays
#[test]
fn migration_null_handling() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Column 0 (_int) has a null in row 2
    let col = columns[0].as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(col.is_null(2), "Row 2 in _int column should be null");
    assert!(!col.is_null(0), "Row 0 in _int column should not be null");
    assert!(!col.is_null(1), "Row 1 in _int column should not be null");

    // Verify null_count works
    assert!(
        col.null_count() >= 1,
        "Should have at least 1 null in _int column"
    );
}

/// Test: Verify numeric data values after migration
#[test]
fn migration_numeric_values() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Test _int column (index 0) - value from parse_all_types_test.rs
    let col_int = columns[0].as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(col_int.value(0), 1234f64, "_int row 0 should be 1234");

    // Test _float column (index 1) - verify it's a valid float
    let col_float = columns[1].as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(
        !col_float.is_null(0),
        "_float row 0 should have a value (not null)"
    );
    assert!(
        col_float.value(0).is_finite(),
        "_float row 0 should be a finite number"
    );
}

/// Test: Verify string data values after migration
#[test]
fn migration_string_values() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Test _string column (index 3) - values from parse_all_types_test.rs
    let col_string = columns[3].as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        col_string.value(0),
        "string",
        "_string row 0 should be 'string'"
    );
    assert_eq!(
        col_string.value(2),
        "stringy string",
        "_string row 2 should be 'stringy string'"
    );

    // Test _char column (index 2) - verify it returns a string
    let col_char = columns[2].as_any().downcast_ref::<StringArray>().unwrap();
    assert!(
        !col_char.value(0).is_empty(),
        "_char row 0 should have a value"
    );
}

/// Test: Verify date values after migration
#[test]
fn migration_date_values() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Test _date column (index 4)
    let col_date = columns[4].as_any().downcast_ref::<Date32Array>().unwrap();

    // Verify we can read the date value
    assert!(!col_date.is_null(0), "_date row 0 should have a value");

    // Convert Date32 value to NaiveDate
    // Date32 stores days since Unix epoch (1970-01-01)
    let days_since_epoch = col_date.value(0);
    let date = NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163);
    assert!(
        date.is_some(),
        "Should be able to convert Date32 to NaiveDate"
    );

    // Verify year is reasonable (after 2000, before 2100)
    let date = date.unwrap();
    assert!(
        date.year() >= 2000 && date.year() <= 2100,
        "Date year should be reasonable"
    );
}

/// Test: Verify timestamp values after migration
#[test]
fn migration_timestamp_values() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Test _datetime column (index 5)
    let col_datetime = columns[5]
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();

    // Get timestamp value (seconds since Unix epoch)
    assert!(
        !col_datetime.is_null(1),
        "_datetime row 1 should have a value"
    );
    let timestamp_seconds = col_datetime.value(1);

    // Verify timestamp is reasonable (after 2000, before 2100)
    // 2000-01-01 00:00:00 UTC = 946684800
    // 2100-01-01 00:00:00 UTC = 4102444800
    assert!(
        (946684800..=4102444800).contains(&timestamp_seconds),
        "Timestamp should be in reasonable range"
    );
}

/// Test: Verify Arc<Schema> is properly shared in RecordBatch
#[test]
fn migration_schema_arc_sharing() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.as_ref().unwrap();

    // Get schema from batch
    let schema_from_batch = batch.schema();

    // Verify it's a valid Arc reference
    assert_eq!(
        schema_from_batch.fields().len(),
        10,
        "Schema should have 10 fields"
    );

    // Verify we can clone the Arc
    let schema_clone = Arc::clone(&schema_from_batch);
    assert_eq!(
        schema_clone.fields().len(),
        schema_from_batch.fields().len(),
        "Cloned schema should have same field count"
    );
}

/// Test: Verify streaming/chunked data processing works
#[test]
fn migration_chunked_processing() {
    let rsp = common::setup_path("all_types.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    // Process in two chunks: rows 0-1, then row 2
    // First chunk
    let mut d1 = readstat::ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, 2);

    let error = d1.read_data(&rsp);
    assert!(error.is_ok(), "First chunk should read successfully");

    let batch1 = d1.batch.as_ref().unwrap();
    assert_eq!(batch1.num_rows(), 2, "First chunk should have 2 rows");

    // Second chunk
    let mut d2 = readstat::ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 2, 3);

    let error = d2.read_data(&rsp);
    assert!(error.is_ok(), "Second chunk should read successfully");

    let batch2 = d2.batch.as_ref().unwrap();
    assert_eq!(batch2.num_rows(), 1, "Second chunk should have 1 row");

    // Verify data in each chunk
    let col1 = batch1.columns()[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(col1.value(0), 1234f64, "First chunk row 0 should be 1234");

    let col2 = batch2.columns()[3]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        col2.value(0),
        "stringy string",
        "Second chunk row 0 (original row 2) should be 'stringy string'"
    );
}

/// Test: Verify cars.sas7bdat larger dataset handling
#[test]
fn migration_larger_dataset() {
    let rsp = common::setup_path("cars.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    let mut d = readstat::ReadStatData::new().set_no_progress(true).init(
        md.clone(),
        0,
        md.row_count as u32,
    );

    let error = d.read_data(&rsp);
    assert!(error.is_ok(), "Should read cars.sas7bdat successfully");

    let batch = d.batch.as_ref().unwrap();

    // Cars dataset has 1081 rows and 13 variables (from parse_cars_md_test.rs)
    assert_eq!(batch.num_rows(), 1081, "Should have 1081 rows");
    assert_eq!(batch.num_columns(), 13, "Should have 13 columns");

    // Verify schema
    assert_eq!(d.schema.fields().len(), 13, "Schema should have 13 fields");

    // Verify we can access data from all columns
    for (i, col) in batch.columns().iter().enumerate() {
        assert_eq!(col.len(), 1081, "Column {} should have 1081 elements", i);
    }
}

/// Test: Verify metadata skip_row_count functionality
#[test]
fn migration_metadata_skip_row_count() {
    let rsp = common::setup_path("all_types.sas7bdat").unwrap();

    // Read metadata without skipping row count
    let mut md_full = ReadStatMetadata::new();
    md_full.read_metadata(&rsp, false).unwrap();

    // Read metadata with skipping row count
    let mut md_skip = ReadStatMetadata::new();
    md_skip.read_metadata(&rsp, true).unwrap();

    // Both should have same variable count
    assert_eq!(
        md_full.var_count, md_skip.var_count,
        "Variable count should be same"
    );

    // Schema should be same
    assert_eq!(
        md_full.schema.fields().len(),
        md_skip.schema.fields().len(),
        "Schema field count should be same"
    );

    // Full read should have accurate row count
    assert_eq!(md_full.row_count, 3, "Full metadata should report 3 rows");

    // Skip read returns 1 (it reads only 1 row to get metadata)
    assert_eq!(md_skip.row_count, 1, "Skip metadata should report 1 row");
}

/// Test: Verify RecordBatch can be cloned (important for data sharing)
#[test]
fn migration_record_batch_clone() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.as_ref().unwrap();

    // Clone the batch
    let batch_clone = batch.clone();

    // Verify clone has same structure
    assert_eq!(batch.num_rows(), batch_clone.num_rows());
    assert_eq!(batch.num_columns(), batch_clone.num_columns());

    // Verify data is accessible from clone
    let col_original = batch.columns()[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let col_clone = batch_clone.columns()[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(col_original.value(0), col_clone.value(0));
}

/// Test: Verify array slicing works (important for streaming)
#[test]
fn migration_array_slicing() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.as_ref().unwrap();

    // Slice the batch to get first 2 rows
    let sliced = batch.slice(0, 2);

    assert_eq!(sliced.num_rows(), 2, "Sliced batch should have 2 rows");
    assert_eq!(
        sliced.num_columns(),
        batch.num_columns(),
        "Sliced batch should have same columns"
    );

    // Verify data in sliced batch
    let col = sliced.columns()[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(col.value(0), 1234f64);
}

/// Test: Verify time column handling
#[test]
fn migration_time_values() {
    let (rsp, _md, mut d) = init_all_types();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Test _time column (index 8)
    let col_time = columns[8]
        .as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap();

    // Verify we can read the time value
    assert!(!col_time.is_null(0), "_time row 0 should have a value");

    // Time32Second stores seconds since midnight
    let time_seconds = col_time.value(0);
    // Should be between 0 and 86400 (seconds in a day)
    assert!(
        (0..86400).contains(&time_seconds),
        "Time should be valid (0-86399 seconds)"
    );
}
