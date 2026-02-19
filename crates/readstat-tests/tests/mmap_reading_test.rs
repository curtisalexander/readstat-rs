use common::ExpectedMetadata;

mod common;

/// Helper: returns the path to a test dataset file.
fn test_data_path(dataset: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(dataset)
}

/// Helper: reads metadata + data from mmap and returns (metadata, data).
fn setup_and_read_from_mmap(
    dataset: &str,
) -> (readstat::ReadStatMetadata, readstat::ReadStatData) {
    let path = test_data_path(dataset);

    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata_from_mmap(&path, false).unwrap();

    let mut d = readstat::ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    d.read_data_from_mmap(&path).unwrap();

    (md, d)
}

// ── Metadata tests ─────────────────────────────────────────────────

#[test]
fn mmap_cars_metadata() {
    let (md, _d) = setup_and_read_from_mmap("cars.sas7bdat");

    common::assert_metadata(&md, &ExpectedMetadata {
        row_count: 1081,
        var_count: 13,
        table_name: "CARS",
        file_label: "Written by SAS",
        file_encoding: "WINDOWS-1252",
        version: 9,
        is64bit: 0,
        creation_time: "2008-09-30 12:55:01",
        modified_time: "2008-09-30 12:55:01",
    });

    assert!(matches!(md.compression, readstat::ReadStatCompress::None));
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));
}

// ── Data tests ─────────────────────────────────────────────────────

#[test]
fn mmap_cars_data() {
    let (_md, d) = setup_and_read_from_mmap("cars.sas7bdat");

    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_rows(), 1081);

    let brand = common::get_string_col(batch, 0);
    let model = common::get_string_col(batch, 1);
    let engine_size = common::get_f64_col(batch, 6);

    assert_eq!(brand.value(0), "TOYOTA");
    assert_eq!(model.value(0), "Prius");
    assert_eq!(engine_size.value(0), 1.5);
}

#[test]
fn mmap_all_types() {
    let (_md, d) = setup_and_read_from_mmap("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();
    assert!(batch.num_rows() > 0);
}

#[test]
fn mmap_all_dates() {
    let (_md, d) = setup_and_read_from_mmap("all_dates.sas7bdat");
    let batch = d.batch.as_ref().unwrap();
    assert!(batch.num_rows() > 0);
}

#[test]
fn mmap_hasmissing() {
    let (_md, d) = setup_and_read_from_mmap("hasmissing.sas7bdat");
    let batch = d.batch.as_ref().unwrap();
    assert!(batch.num_rows() > 0);
}

// ── Metadata-only (skip row count) ─────────────────────────────────

#[test]
fn mmap_metadata_skip_row_count() {
    let path = test_data_path("cars.sas7bdat");
    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata_from_mmap(&path, true).unwrap();

    assert_eq!(md.var_count, 13);
    assert_eq!(md.table_name, "CARS");
}

// ── Streaming chunks from mmap ─────────────────────────────────────

#[test]
fn mmap_streaming_chunks() {
    let path = test_data_path("cars.sas7bdat");

    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata_from_mmap(&path, false).unwrap();

    let total_rows = md.row_count as u32;
    let offsets = readstat::build_offsets(total_rows, 500).unwrap();
    let mut total_read = 0usize;

    for w in offsets.windows(2) {
        let mut d = readstat::ReadStatData::new()
            .set_no_progress(true)
            .init(md.clone(), w[0], w[1]);
        d.read_data_from_mmap(&path).unwrap();
        let batch = d.batch.as_ref().unwrap();
        total_read += batch.num_rows();
    }

    assert_eq!(total_read, total_rows as usize);
}

// ── Comparison: mmap vs file produce identical results ──────────────

#[test]
fn mmap_vs_file_identical_results() {
    // Read via file path (original approach)
    let (_rsp, file_md, file_d) = common::setup_and_read("cars.sas7bdat");

    // Read via mmap
    let (mmap_md, mmap_d) = setup_and_read_from_mmap("cars.sas7bdat");

    // Metadata
    assert_eq!(file_md.row_count, mmap_md.row_count);
    assert_eq!(file_md.var_count, mmap_md.var_count);
    assert_eq!(file_md.table_name, mmap_md.table_name);
    assert_eq!(file_md.file_label, mmap_md.file_label);
    assert_eq!(file_md.file_encoding, mmap_md.file_encoding);

    // RecordBatch structure
    let file_batch = file_d.batch.as_ref().unwrap();
    let mmap_batch = mmap_d.batch.as_ref().unwrap();
    assert_eq!(file_batch.num_rows(), mmap_batch.num_rows());
    assert_eq!(file_batch.num_columns(), mmap_batch.num_columns());
    assert_eq!(file_batch.schema(), mmap_batch.schema());

    // Spot-check data values
    let file_brand = common::get_string_col(file_batch, 0);
    let mmap_brand = common::get_string_col(mmap_batch, 0);
    for i in 0..file_batch.num_rows() {
        assert_eq!(file_brand.value(i), mmap_brand.value(i), "row {i}");
    }
}

// ── Error handling ──────────────────────────────────────────────────

#[test]
fn mmap_nonexistent_file_returns_error() {
    let path = std::path::Path::new("/nonexistent/file.sas7bdat");
    let mut md = readstat::ReadStatMetadata::new();
    assert!(md.read_metadata_from_mmap(path, false).is_err());
}
