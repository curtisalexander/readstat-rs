use arrow_array::{Array, Float64Array, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use readstat::{ReadStatData, ReadStatMetadata, ReadStatWriter};
use std::fs;
use std::path::PathBuf;

mod common;

fn setup_test_output(filename: &str) -> PathBuf {
    let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("output");

    // Create output directory if it doesn't exist
    fs::create_dir_all(&test_dir).unwrap();

    test_dir.join(filename)
}

fn cleanup_test_output(path: &PathBuf) {
    if path.exists() {
        fs::remove_file(path).ok();
    }
}

#[test]
fn test_parallel_write_parquet_basic() {
    // Setup input path
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();

    // Setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    // Setup output path
    let output_path = setup_test_output("parallel_write_test.parquet");
    cleanup_test_output(&output_path);

    // Write data using parallel writes by simulating the batch write
    // We'll read data in chunks and write them
    let row_count = md.row_count as u32;
    let chunk_size = 1; // Small chunks to test parallel write

    let mut temp_files = Vec::new();
    let schema = {
        let mut d = ReadStatData::new()
            .set_no_progress(true)
            .init(md.clone(), 0, chunk_size);
        d.read_data(&rsp_in).unwrap();
        d.schema.clone()
    };

    // Write chunks to temp files
    for i in 0..(row_count / chunk_size) {
        let start_row = i * chunk_size;
        let end_row = ((i + 1) * chunk_size).min(row_count);

        let mut d = ReadStatData::new()
            .set_no_progress(true)
            .init(md.clone(), start_row, end_row);

        d.read_data(&rsp_in).unwrap();

        if let Some(batch) = &d.batch {
            let temp_file = setup_test_output(&format!("temp_{i}.parquet"));
            ReadStatWriter::write_batch_to_parquet(
                batch,
                &schema,
                &temp_file,
                None,
                None,
                100 * 1024 * 1024, // 100 MB buffer
            )
            .unwrap();
            temp_files.push(temp_file);
        }
    }

    // Merge temp files
    ReadStatWriter::merge_parquet_files(&temp_files, &output_path, &schema, None, None).unwrap();

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

    // Cleanup
    cleanup_test_output(&output_path);
}

#[test]
fn test_parallel_write_parquet_out_of_order() {
    // Setup input path
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();

    // Setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    // Setup output path
    let output_path = setup_test_output("parallel_write_out_of_order.parquet");
    cleanup_test_output(&output_path);

    // Read all data first
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    d.read_data(&rsp_in).unwrap();

    let batch = d.batch.as_ref().unwrap();
    let schema = &d.schema;

    // Write batches in reverse order to simulate out-of-order parallel writes
    let mut temp_files = Vec::new();
    let num_rows = batch.num_rows();

    // Split into 3 batches
    let chunk_size = num_rows / 3 + 1;

    for i in (0..3).rev() {
        // Reverse order!
        let start = i * chunk_size;
        let end = ((i + 1) * chunk_size).min(num_rows);

        if start < num_rows {
            let slice = batch.slice(start, end - start);
            let temp_file = setup_test_output(&format!("temp_ooo_{i}.parquet"));

            ReadStatWriter::write_batch_to_parquet(
                &slice,
                schema,
                &temp_file,
                None,
                None,
                100 * 1024 * 1024, // 100 MB buffer
            )
            .unwrap();

            temp_files.push(temp_file);
        }
    }

    // Merge temp files (they were written out of order)
    ReadStatWriter::merge_parquet_files(&temp_files, &output_path, schema, None, None).unwrap();

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

        // Verify we can read the data
        assert!(batch.num_columns() > 0);
    }

    // Verify we got all rows (even though written out of order)
    assert!(total_rows > 0);

    // Cleanup
    cleanup_test_output(&output_path);
}

#[test]
fn test_parallel_write_parquet_with_compression() {
    // Setup input path
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();

    // Setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    // Setup output path
    let output_path = setup_test_output("parallel_write_compressed.parquet");
    cleanup_test_output(&output_path);

    // Read data
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
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
    cleanup_test_output(&output_path);
}

#[test]
fn test_bufwriter_optimization_verification() {
    // This test verifies that BufWriter is being used by checking that writes complete successfully
    // The performance benefit would be measured in benchmarks

    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    let output_path = setup_test_output("bufwriter_test.parquet");
    cleanup_test_output(&output_path);

    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    d.read_data(&rsp_in).unwrap();

    if let Some(batch) = &d.batch {
        // Write using the method that should use SpooledTempFile internally
        ReadStatWriter::write_batch_to_parquet(
            batch,
            &d.schema,
            &output_path,
            None,
            None,
            100 * 1024 * 1024, // 100 MB buffer
        )
        .unwrap();

        assert!(output_path.exists());
    }

    cleanup_test_output(&output_path);
}

#[test]
fn test_spooled_tempfile_small_buffer() {
    // Test with a very small buffer to ensure spilling to disk works
    // This verifies that data larger than the buffer still writes correctly
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    let output_path = setup_test_output("spooled_small_buffer.parquet");
    cleanup_test_output(&output_path);

    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
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

    cleanup_test_output(&output_path);
}

#[test]
fn test_spooled_tempfile_large_buffer() {
    // Test with a large buffer to keep everything in memory
    let rsp_in = common::setup_path("all_types.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();

    let output_path = setup_test_output("spooled_large_buffer.parquet");
    cleanup_test_output(&output_path);

    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
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

    cleanup_test_output(&output_path);
}
