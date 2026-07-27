use std::{fs::File, sync::Arc};

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use readstat::{
    OutFormat, ParallelParquetWriter, ParquetCompression, ReadStatData, ReadStatError,
    ReadStatMetadata, WriteConfig,
};

mod common;

fn read_fixture(name: &str) -> RecordBatch {
    let input = common::setup_path(name).unwrap();
    let mut metadata = ReadStatMetadata::new();
    metadata.read_metadata(&input, false).unwrap();
    let mut data =
        ReadStatData::new().init(metadata.clone(), 0, metadata.row_count.unwrap() as u32);
    data.read_data(&input).unwrap();
    data.batch.unwrap()
}

fn read_parquet(path: &std::path::Path) -> (RecordBatch, usize) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let row_groups = builder.metadata().num_row_groups();
    let schema = builder.schema().clone();
    let batches = builder
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    (concat_batches(&schema, &batches).unwrap(), row_groups)
}

fn config(path: &std::path::Path) -> WriteConfig {
    WriteConfig::new(OutFormat::Parquet).output(path).unwrap()
}

#[test]
fn native_parallel_writer_preserves_values_order_and_row_groups() {
    let fixture = read_fixture("all_types.sas7bdat");
    let batch = concat_batches(
        &fixture.schema(),
        &[fixture.clone(), fixture.clone(), fixture.clone(), fixture],
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("parallel.parquet");

    // Deliberately cross both input-batch and row-group boundaries.
    let split = batch.num_rows() / 2;
    let mut writer = ParallelParquetWriter::new(config(&output), batch.schema(), 3).unwrap();
    writer.write(&batch.slice(0, split)).unwrap();
    writer
        .write(&batch.slice(split, batch.num_rows() - split))
        .unwrap();
    assert_eq!(writer.finish().unwrap(), batch.num_rows());

    let (actual, row_groups) = read_parquet(&output);
    assert_eq!(actual, batch);
    assert_eq!(row_groups, batch.num_rows().div_ceil(3));
}

#[test]
fn native_parallel_writer_supports_compression_and_empty_output() {
    let batch = read_fixture("all_types.sas7bdat");
    let dir = tempfile::tempdir().unwrap();
    let compressed = dir.path().join("compressed.parquet");
    let empty = dir.path().join("empty.parquet");

    let compressed_config = config(&compressed)
        .compression(ParquetCompression::Snappy, None)
        .unwrap();
    let mut writer = ParallelParquetWriter::new(compressed_config, batch.schema(), 2).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    assert_eq!(read_parquet(&compressed).0, batch);

    let schema = Arc::new(Schema::empty());
    let writer = ParallelParquetWriter::new(config(&empty), schema.clone(), 2).unwrap();
    assert_eq!(writer.finish().unwrap(), 0);
    let (actual, row_groups) = read_parquet(&empty);
    assert_eq!(actual, RecordBatch::new_empty(schema.clone()));
    assert_eq!(row_groups, 0);

    let rows_without_columns = RecordBatch::try_new_with_options(
        schema.clone(),
        Vec::new(),
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let no_columns = dir.path().join("no-columns.parquet");
    let mut writer = ParallelParquetWriter::new(config(&no_columns), schema, 2).unwrap();
    assert!(writer.write(&rows_without_columns).is_err());
    drop(writer);
    assert!(!no_columns.exists());
}

#[test]
fn native_parallel_writer_is_transactional_and_checks_schema() {
    let batch = read_fixture("cars.sas7bdat");
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("existing.parquet");
    std::fs::write(&output, b"sentinel").unwrap();

    let mut writer = ParallelParquetWriter::new(config(&output), batch.schema(), 2).unwrap();
    writer.write(&batch).unwrap();
    let error = match writer.finish() {
        Ok(_) => panic!("existing output must be rejected when publishing"),
        Err(error) => error,
    };
    assert!(matches!(error, ReadStatError::OutputFileExists(_)));
    assert_eq!(std::fs::read(&output).unwrap(), b"sentinel");

    let mut writer = ParallelParquetWriter::new(
        config(&output).overwrite(true),
        Arc::new(Schema::empty()),
        2,
    )
    .unwrap();
    assert!(matches!(
        writer.write(&batch),
        Err(ReadStatError::SchemaMismatch)
    ));
    drop(writer);
    assert_eq!(std::fs::read(&output).unwrap(), b"sentinel");
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        1,
        "failed write must remove its staging file"
    );
}
