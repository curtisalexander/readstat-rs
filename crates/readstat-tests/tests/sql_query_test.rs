#![cfg(feature = "sql")]

use arrow_array::{Array, Float64Array};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};
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

    let results = readstat::execute_sql(batches, schema, "cars", "SELECT * FROM cars").unwrap();

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
        "SELECT \"Brand\", \"Model\", \"EngineSize\" FROM cars WHERE \"EngineSize\" > 5.0",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);
    // All rows should have EngineSize > 5.0
    let engine_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        if !engine_col.is_null(i) {
            assert!(engine_col.value(i) > 5.0, "Row {i} has EngineSize <= 5.0");
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

    let results =
        readstat::execute_sql(batches, schema, "cars", "SELECT COUNT(*) as cnt FROM cars").unwrap();

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
        "SELECT \"Brand\", \"Model\", \"EngineSize\" FROM cars ORDER BY \"EngineSize\" DESC LIMIT 5",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 5);

    // Verify descending order
    let engine_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 1..batch.num_rows() {
        if !engine_col.is_null(i) && !engine_col.is_null(i - 1) {
            assert!(engine_col.value(i - 1) >= engine_col.value(i));
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

// ── Streaming SQL tests ─────────────────────────────────────────────

/// Helper: send cars data through a crossbeam channel, returns (receiver, schema).
fn send_cars_via_channel() -> (
    crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>,
    arrow_schema::SchemaRef,
) {
    let rsp = common::setup_path("cars.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    let schema = Arc::new(md.schema.clone());

    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    d.read_data(&rsp).unwrap();

    let (s, r) = crossbeam::channel::bounded(10);
    s.send((d, rsp, 1)).unwrap();
    drop(s); // close the channel

    (r, schema)
}

#[test]
fn sql_stream_select_where() {
    let (receiver, schema) = send_cars_via_channel();

    let stream_results = readstat::execute_sql_stream(
        receiver,
        schema.clone(),
        "cars",
        "SELECT \"Brand\", \"Model\", \"EngineSize\" FROM cars WHERE \"EngineSize\" > 5.0",
    )
    .unwrap();

    // Verify results match the non-streaming path
    let (md, batches) = read_cars_batches();
    let mem_results = readstat::execute_sql(
        batches,
        Arc::new(md.schema.clone()),
        "cars",
        "SELECT \"Brand\", \"Model\", \"EngineSize\" FROM cars WHERE \"EngineSize\" > 5.0",
    )
    .unwrap();

    let stream_total: usize = stream_results.iter().map(|b| b.num_rows()).sum();
    let mem_total: usize = mem_results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(stream_total, mem_total);
    assert!(stream_total > 0);
    assert!(stream_total < 1081);

    // All rows should have EngineSize > 5.0
    for batch in &stream_results {
        let engine_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if !engine_col.is_null(i) {
                assert!(engine_col.value(i) > 5.0, "Row {i} has EngineSize <= 5.0");
            }
        }
    }
}

#[test]
fn sql_stream_aggregation() {
    let (receiver, schema) = send_cars_via_channel();

    let results = readstat::execute_sql_stream(
        receiver,
        schema,
        "cars",
        "SELECT \"Brand\", COUNT(*) as cnt FROM cars GROUP BY \"Brand\" ORDER BY cnt DESC",
    )
    .unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 1);

    // Verify against non-streaming path
    let (md, batches) = read_cars_batches();
    let mem_results = readstat::execute_sql(
        batches,
        Arc::new(md.schema.clone()),
        "cars",
        "SELECT \"Brand\", COUNT(*) as cnt FROM cars GROUP BY \"Brand\" ORDER BY cnt DESC",
    )
    .unwrap();

    let mem_total: usize = mem_results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, mem_total);
}

#[test]
fn sql_stream_and_write() {
    let (receiver, schema) = send_cars_via_channel();

    let temp_dir = std::env::temp_dir();
    let out_path = temp_dir.join("sql_stream_test_output.parquet");

    readstat::execute_sql_and_write_stream(
        receiver,
        schema,
        "cars",
        "SELECT \"Brand\", \"Model\", \"EngineSize\" FROM cars WHERE \"EngineSize\" > 5.0",
        &out_path,
        readstat::OutFormat::parquet,
        None,
        None,
    )
    .unwrap();

    // Verify the written file is valid and contains data
    let file = std::fs::File::open(&out_path).unwrap();
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, 1024).unwrap();
    let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0);
    assert!(total_rows < 1081);

    // Clean up
    let _ = std::fs::remove_file(&out_path);
}
