use arrow::datatypes::DataType;
use common::ExpectedMetadata;

mod common;

/// Helper: reads a test dataset file into bytes.
fn read_test_file_bytes(dataset: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(dataset);
    std::fs::read(path).unwrap()
}

/// Helper: reads metadata + data from bytes and returns (metadata, data).
fn setup_and_read_from_bytes(
    dataset: &str,
) -> (readstat::ReadStatMetadata, readstat::ReadStatData) {
    let bytes = read_test_file_bytes(dataset);

    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata_from_bytes(&bytes, false).unwrap();

    let mut d = readstat::ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    d.read_data_from_bytes(&bytes).unwrap();

    (md, d)
}

// ── Metadata tests ─────────────────────────────────────────────────

#[test]
fn bytes_cars_metadata_matches_file() {
    let (md, _d) = setup_and_read_from_bytes("cars.sas7bdat");

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

#[test]
fn bytes_cars_variable_types() {
    let (_md, d) = setup_and_read_from_bytes("cars.sas7bdat");

    // 0 - Brand (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, "");
    assert!(matches!(adt, DataType::Utf8));

    // 2..12 - All numeric Double -> Float64
    for i in 2..=12 {
        let (vtc, vt, _vfc, _vf, adt) = common::get_var_attrs(&d, i);
        assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric), "var {i}");
        assert!(matches!(vt, readstat::ReadStatVarType::Double), "var {i}");
        assert!(matches!(adt, DataType::Float64), "var {i}");
    }
}

// ── Data tests ─────────────────────────────────────────────────────

#[test]
fn bytes_cars_data_matches_file() {
    let (_md, d) = setup_and_read_from_bytes("cars.sas7bdat");

    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_rows(), 1081);

    // Spot-check row 0
    let brand = common::get_string_col(batch, 0);
    let model = common::get_string_col(batch, 1);
    let engine_size = common::get_f64_col(batch, 6);

    assert_eq!(brand.value(0), "TOYOTA");
    assert_eq!(model.value(0), "Prius");
    assert_eq!(engine_size.value(0), 1.5);

    // Row 1
    assert_eq!(brand.value(1), "HONDA");
    assert_eq!(model.value(1), "Civic Hybrid");
}

#[test]
fn bytes_all_types_data() {
    let (_md, d) = setup_and_read_from_bytes("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();
    assert!(batch.num_rows() > 0);
}

#[test]
fn bytes_dates_data() {
    let (_md, d) = setup_and_read_from_bytes("all_dates.sas7bdat");
    let batch = d.batch.as_ref().unwrap();
    assert!(batch.num_rows() > 0);
}

#[test]
fn bytes_datetimes_data() {
    let (_md, d) = setup_and_read_from_bytes("all_datetimes.sas7bdat");
    let batch = d.batch.as_ref().unwrap();
    assert!(batch.num_rows() > 0);
}

#[test]
fn bytes_hasmissing_data() {
    let (_md, d) = setup_and_read_from_bytes("hasmissing.sas7bdat");
    let batch = d.batch.as_ref().unwrap();
    assert!(batch.num_rows() > 0);
}

// ── Metadata-only (skip row count) ─────────────────────────────────

#[test]
fn bytes_metadata_skip_row_count() {
    let bytes = read_test_file_bytes("cars.sas7bdat");
    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata_from_bytes(&bytes, true).unwrap();

    // Variable metadata should still be populated
    assert_eq!(md.var_count, 13);
    assert_eq!(md.table_name, "CARS");
}

// ── Streaming chunks from bytes ────────────────────────────────────

#[test]
fn bytes_streaming_chunks() {
    let bytes = read_test_file_bytes("cars.sas7bdat");

    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata_from_bytes(&bytes, false).unwrap();

    let total_rows = md.row_count as u32;
    let offsets = readstat::build_offsets(total_rows, 500).unwrap();
    let mut total_read = 0usize;

    for w in offsets.windows(2) {
        let mut d = readstat::ReadStatData::new()
            .set_no_progress(true)
            .init(md.clone(), w[0], w[1]);
        d.read_data_from_bytes(&bytes).unwrap();
        let batch = d.batch.as_ref().unwrap();
        total_read += batch.num_rows();
    }

    assert_eq!(total_read, total_rows as usize);
}

// ── Comparison: bytes vs file produce identical results ─────────────

#[test]
fn bytes_vs_file_identical_results() {
    // Read via file
    let (_rsp, file_md, file_d) = common::setup_and_read("cars.sas7bdat");

    // Read via bytes
    let (bytes_md, bytes_d) = setup_and_read_from_bytes("cars.sas7bdat");

    // Metadata should match
    assert_eq!(file_md.row_count, bytes_md.row_count);
    assert_eq!(file_md.var_count, bytes_md.var_count);
    assert_eq!(file_md.table_name, bytes_md.table_name);
    assert_eq!(file_md.file_label, bytes_md.file_label);
    assert_eq!(file_md.file_encoding, bytes_md.file_encoding);

    // RecordBatches should be identical
    let file_batch = file_d.batch.as_ref().unwrap();
    let bytes_batch = bytes_d.batch.as_ref().unwrap();
    assert_eq!(file_batch.num_rows(), bytes_batch.num_rows());
    assert_eq!(file_batch.num_columns(), bytes_batch.num_columns());
    assert_eq!(file_batch.schema(), bytes_batch.schema());

    // Spot-check actual data values
    let file_brand = common::get_string_col(file_batch, 0);
    let bytes_brand = common::get_string_col(bytes_batch, 0);
    for i in 0..file_batch.num_rows() {
        assert_eq!(file_brand.value(i), bytes_brand.value(i), "row {i}");
    }
}
