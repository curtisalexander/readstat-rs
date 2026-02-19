use arrow_array::{Float64Array, StringArray, Array};
use readstat::{ReadStatData, ReadStatMetadata};
use std::sync::Arc;

mod common;

/// Helper: read all cars.sas7bdat data into record batches.
fn read_cars_batches() -> (ReadStatMetadata, Vec<arrow_array::RecordBatch>) {
    let rsp = common::setup_path("cars.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    d.read_data(&rsp).unwrap();

    let batch = d.batch.unwrap();
    (md, vec![batch])
}

#[test]
fn sql_select_all() {
    let (md, batches) = read_cars_batches();
    let schema = Arc::new(md.schema.clone());

    let results = readstat::execute_sql(
        batches,
        schema,
        "cars",
        "SELECT * FROM cars",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_columns(), 13);
    assert_eq!(batch.num_rows(), 1081);
}

#[test]
fn sql_select_columns() {
    let (md, batches) = read_cars_batches();
    let schema = Arc::new(md.schema.clone());

    let results = readstat::execute_sql(
        batches,
        schema,
        "cars",
        "SELECT \"Brand\", \"Model\" FROM cars",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "Brand");
    assert_eq!(batch.schema().field(1).name(), "Model");
    assert_eq!(batch.num_rows(), 1081);
}

#[test]
fn sql_where_filter() {
    let (md, batches) = read_cars_batches();
    let schema = Arc::new(md.schema.clone());

    let results = readstat::execute_sql(
        batches,
        schema,
        "cars",
        "SELECT \"Brand\", \"Model\", \"Horsepower\" FROM cars WHERE \"Horsepower\" > 300",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);
    // All rows should have Horsepower > 300
    let hp_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        if !hp_col.is_null(i) {
            assert!(hp_col.value(i) > 300.0, "Row {i} has Horsepower <= 300");
        }
    }
    // Should be fewer rows than the full dataset
    assert!(batch.num_rows() < 1081);
    assert!(batch.num_rows() > 0);
}

#[test]
fn sql_count_aggregation() {
    let (md, batches) = read_cars_batches();
    let schema = Arc::new(md.schema.clone());

    let results = readstat::execute_sql(
        batches,
        schema,
        "cars",
        "SELECT COUNT(*) as cnt FROM cars",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "cnt");
}

#[test]
fn sql_group_by() {
    let (md, batches) = read_cars_batches();
    let schema = Arc::new(md.schema.clone());

    let results = readstat::execute_sql(
        batches,
        schema,
        "cars",
        "SELECT \"Brand\", COUNT(*) as cnt FROM cars GROUP BY \"Brand\" ORDER BY cnt DESC",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_columns(), 2);
    // Should have multiple brands
    assert!(batch.num_rows() > 1);
    // First column should be Brand
    assert_eq!(batch.schema().field(0).name(), "Brand");
}

#[test]
fn sql_order_by_limit() {
    let (md, batches) = read_cars_batches();
    let schema = Arc::new(md.schema.clone());

    let results = readstat::execute_sql(
        batches,
        schema,
        "cars",
        "SELECT \"Brand\", \"Model\", \"Horsepower\" FROM cars ORDER BY \"Horsepower\" DESC LIMIT 5",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 5);

    // Verify descending order
    let hp_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 1..batch.num_rows() {
        if !hp_col.is_null(i) && !hp_col.is_null(i - 1) {
            assert!(hp_col.value(i - 1) >= hp_col.value(i));
        }
    }
}

#[test]
fn sql_invalid_query_returns_error() {
    let (md, batches) = read_cars_batches();
    let schema = Arc::new(md.schema.clone());

    let result = readstat::execute_sql(
        batches,
        schema,
        "cars",
        "SELECT nonexistent_column FROM cars",
    );

    assert!(result.is_err());
}

#[test]
fn sql_read_sql_file() {
    let temp_dir = std::env::temp_dir();
    let sql_file = temp_dir.join("test_query.sql");

    std::fs::write(&sql_file, "SELECT \"Brand\", \"Model\" FROM cars LIMIT 3").unwrap();

    let sql = readstat::read_sql_file(&sql_file).unwrap();
    assert_eq!(sql, "SELECT \"Brand\", \"Model\" FROM cars LIMIT 3");

    // Clean up
    let _ = std::fs::remove_file(&sql_file);
}

#[test]
fn sql_read_empty_sql_file_returns_error() {
    let temp_dir = std::env::temp_dir();
    let sql_file = temp_dir.join("test_empty_query.sql");

    std::fs::write(&sql_file, "   \n  \n  ").unwrap();

    let result = readstat::read_sql_file(&sql_file);
    assert!(result.is_err());

    // Clean up
    let _ = std::fs::remove_file(&sql_file);
}
