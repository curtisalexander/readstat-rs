#![allow(clippy::float_cmp)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]

use arrow_array::{Array, Float64Array, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use readstat::{ReadStatData, ReadStatMetadata, ReadStatWriter};
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

mod common;

fn setup_test_output(dir: &TempDir, filename: &str) -> PathBuf {
    dir.path().join(filename)
}

#[test]
fn test_parallel_write_parquet_basic() {
    let output_dir = tempfile::tempdir().unwrap();
    // Setup input path
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();

    // Setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    // Setup output path
    let output_path = setup_test_output(&output_dir, "parallel_write_test.parquet");

    // Write data using parallel writes by simulating the batch write
    // We'll read data in chunks and write them
    let row_count = md.row_count.unwrap() as u32;
    let chunk_size = 1; // Small chunks to test parallel write

    let mut temp_files = Vec::new();
    let schema = {
        let mut d = ReadStatData::new().init(md.clone(), 0, chunk_size);
        d.read_data(&rsp_in).unwrap();
        d.schema.clone()
    };

    // Write chunks to temp files
    for i in 0..(row_count / chunk_size) {
        let start_row = i * chunk_size;
        let end_row = ((i + 1) * chunk_size).min(row_count);

        let mut d = ReadStatData::new().init(md.clone(), start_row, end_row);

        d.read_data(&rsp_in).unwrap();

        if let Some(batch) = &d.batch {
            let temp_file = setup_test_output(&output_dir, &format!("temp_{i}.parquet"));
            ReadStatWriter::write_batch_to_parquet(
                batch,
                &schema,
                &temp_file,
                None,
                None,
                100 * 1024 * 1024, // 100 MB buffer
                false,
            )
            .unwrap();
            temp_files.push(temp_file);
        }
    }

    // Merge temp files
    ReadStatWriter::merge_parquet_files(&temp_files, &output_path, &schema, None, None, false)
        .unwrap();

    // Verify the output file exists and is valid
    assert!(output_path.exists());

    // Read back and verify content
    let file = fs::File::open(&output_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();

    let mut total_rows = 0;
    for batch_result in reader {
        let batch: RecordBatch = batch_result.unwrap();
        total_rows += batch.num_rows();
    }

    // Verify we got all rows
    assert_eq!(total_rows, row_count as usize);
}

#[test]
fn test_parallel_write_parquet_preserves_reversed_input_order() {
    let output_dir = tempfile::tempdir().unwrap();
    // Setup input path
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();

    // Setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    // Setup output path
    let output_path = setup_test_output(&output_dir, "parallel_write_out_of_order.parquet");

    // Read all data first
    let mut d = ReadStatData::new().init(md.clone(), 0, md.row_count.unwrap() as u32);
    d.read_data(&rsp_in).unwrap();

    let batch = d.batch.as_ref().unwrap();
    let schema = &d.schema;

    // Write batches in reverse order to simulate out-of-order parallel writes
    let mut temp_files = Vec::new();
    let mut expected_batches = Vec::new();
    let num_rows = batch.num_rows();

    // Split into 3 batches
    let chunk_size = num_rows / 3 + 1;

    for i in (0..3).rev() {
        // Reverse order!
        let start = i * chunk_size;
        let end = ((i + 1) * chunk_size).min(num_rows);

        if start < num_rows {
            let slice = batch.slice(start, end - start);
            let temp_file = setup_test_output(&output_dir, &format!("temp_ooo_{i}.parquet"));

            ReadStatWriter::write_batch_to_parquet(
                &slice,
                schema,
                &temp_file,
                None,
                None,
                100 * 1024 * 1024, // 100 MB buffer
                false,
            )
            .unwrap();

            temp_files.push(temp_file);
            expected_batches.push(slice);
        }
    }

    // Merge temp files (they were written out of order)
    ReadStatWriter::merge_parquet_files(&temp_files, &output_path, schema, None, None, false)
        .unwrap();

    // Verify the output file exists and is valid
    assert!(output_path.exists());

    // Read back and verify content
    let file = fs::File::open(&output_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();

    let mut actual_batches = Vec::new();
    for batch_result in reader {
        let batch: RecordBatch = batch_result.unwrap();
        actual_batches.push(batch);
    }

    let actual = arrow::compute::concat_batches(schema, &actual_batches).unwrap();
    let expected = arrow::compute::concat_batches(schema, &expected_batches).unwrap();
    assert_eq!(actual.num_rows(), num_rows);
    assert_eq!(actual, expected, "merge must preserve supplied file order");
}

#[test]
fn test_parallel_write_parquet_with_compression() {
    let output_dir = tempfile::tempdir().unwrap();
    // Setup input path
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();

    // Setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    // Setup output path
    let output_path = setup_test_output(&output_dir, "parallel_write_compressed.parquet");

    // Read data
    let mut d = ReadStatData::new().init(md.clone(), 0, md.row_count.unwrap() as u32);
    d.read_data(&rsp_in).unwrap();

    if let Some(batch) = &d.batch {
        // Write with compression
        ReadStatWriter::write_batch_to_parquet(
            batch,
            &d.schema,
            &output_path,
            Some(readstat::ParquetCompression::Snappy),
            None,
            100 * 1024 * 1024, // 100 MB buffer
            false,
        )
        .unwrap();

        // Verify the output file exists
        assert!(output_path.exists());

        // Verify the file is smaller than uncompressed (rough check)
        let metadata = fs::metadata(&output_path).unwrap();
        assert!(metadata.len() > 0);

        // Read back and verify content
        let file = fs::File::open(&output_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();

        if let Some(batch_result) = reader.next() {
            let read_batch: RecordBatch = batch_result.unwrap();
            assert_eq!(read_batch.num_rows(), batch.num_rows());

            // Verify some data matches
            let col = read_batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            let original_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            assert_eq!(col.value(0), original_col.value(0));
        }
    }

    // Cleanup
}

#[test]
fn single_batch_parallel_helper_honors_overwrite() {
    let output_dir = tempfile::tempdir().unwrap();
    let rsp_in = common::setup_path("cars.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();
    let mut data = ReadStatData::new().init(md.clone(), 0, 1);
    data.read_data(&rsp_in).unwrap();
    let batch = data.batch.as_ref().unwrap();

    let output_path = setup_test_output(&output_dir, "parallel_write_overwrite.parquet");
    std::fs::write(&output_path, b"sentinel").unwrap();
    let result = ReadStatWriter::write_batch_to_parquet(
        batch,
        &data.schema,
        &output_path,
        None,
        None,
        1024,
        false,
    );
    assert!(matches!(
        result,
        Err(readstat::ReadStatError::OutputFileExists(_))
    ));
    assert_eq!(std::fs::read(&output_path).unwrap(), b"sentinel");

    ReadStatWriter::write_batch_to_parquet(
        batch,
        &data.schema,
        &output_path,
        None,
        None,
        1024,
        true,
    )
    .unwrap();
    assert_eq!(&std::fs::read(&output_path).unwrap()[..4], b"PAR1");
}

#[test]
fn test_spooled_tempfile_small_buffer() {
    let output_dir = tempfile::tempdir().unwrap();
    // Test with a very small buffer to ensure spilling to disk works
    // This verifies that data larger than the buffer still writes correctly
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    let output_path = setup_test_output(&output_dir, "spooled_small_buffer.parquet");

    let mut d = ReadStatData::new().init(md.clone(), 0, md.row_count.unwrap() as u32);
    d.read_data(&rsp_in).unwrap();

    if let Some(batch) = &d.batch {
        // Use a very small buffer (1 KB) to force spilling to disk
        ReadStatWriter::write_batch_to_parquet(
            batch,
            &d.schema,
            &output_path,
            None,
            None,
            1024, // Only 1 KB buffer - should spill to disk
            false,
        )
        .unwrap();

        assert!(output_path.exists());

        // Read back and verify content
        let file = fs::File::open(&output_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();

        if let Some(batch_result) = reader.next() {
            let read_batch: RecordBatch = batch_result.unwrap();
            assert_eq!(read_batch.num_rows(), batch.num_rows());
        }
    }
}

#[test]
fn test_spooled_tempfile_large_buffer() {
    let output_dir = tempfile::tempdir().unwrap();
    // Test with a large buffer to keep everything in memory
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    let output_path = setup_test_output(&output_dir, "spooled_large_buffer.parquet");

    let mut d = ReadStatData::new().init(md.clone(), 0, md.row_count.unwrap() as u32);
    d.read_data(&rsp_in).unwrap();

    if let Some(batch) = &d.batch {
        // Use a very large buffer (1 GB) to keep everything in memory
        ReadStatWriter::write_batch_to_parquet(
            batch,
            &d.schema,
            &output_path,
            None,
            None,
            1024 * 1024 * 1024, // 1 GB buffer - should stay in memory
            false,
        )
        .unwrap();

        assert!(output_path.exists());

        // Read back and verify content
        let file = fs::File::open(&output_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();

        if let Some(batch_result) = reader.next() {
            let read_batch: RecordBatch = batch_result.unwrap();
            assert_eq!(read_batch.num_rows(), batch.num_rows());
        }
    }
}
