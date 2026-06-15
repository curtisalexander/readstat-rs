//! Runtime tests for the high-level convenience API: the `read_metadata` /
//! `read_to_batch` free functions and `ReadStatData::init_filtered`.

use std::path::PathBuf;

fn data_path(ds: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(ds)
}

#[test]
fn read_metadata_matches_low_level() {
    let path = data_path("cars.sas7bdat");

    // High-level convenience function
    let md = readstat::read_metadata(&path).unwrap();

    // Low-level equivalent
    let rsp = readstat::ReadStatPath::new(&path).unwrap();
    let mut md_ll = readstat::ReadStatMetadata::new();
    md_ll.read_metadata(&rsp, false).unwrap();

    assert_eq!(md.row_count, md_ll.row_count);
    assert_eq!(md.var_count, md_ll.var_count);
    assert!(md.var_count > 0);
}

#[test]
fn read_to_batch_returns_all_rows() {
    let path = data_path("cars.sas7bdat");

    let md = readstat::read_metadata(&path).unwrap();
    let batch = readstat::read_to_batch(&path).unwrap();

    assert_eq!(batch.num_rows() as i32, md.row_count);
    assert_eq!(batch.num_columns() as i32, md.var_count);
}

#[test]
fn read_to_batch_with_options_projects_columns_and_rows() {
    let path = data_path("cars.sas7bdat");
    let md = readstat::read_metadata(&path).unwrap();
    let first_col = md.vars.values().next().unwrap().var_name.clone();

    let batch = readstat::read_to_batch_with_options(
        &path,
        readstat::ReadOptions::new()
            .columns([first_col.clone()])
            .row_start(1)
            .row_count(3),
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), &first_col);
}

#[test]
fn read_to_batch_from_bytes_with_options_can_return_zero_rows() {
    let path = data_path("cars.sas7bdat");
    let md = readstat::read_metadata(&path).unwrap();
    let first_col = md.vars.values().next().unwrap().var_name.clone();
    let bytes = std::fs::read(&path).unwrap();

    let batch = readstat::read_to_batch_from_bytes_with_options(
        &bytes,
        readstat::ReadOptions::new()
            .columns([first_col.clone()])
            .row_count(0),
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), &first_col);
}

#[test]
fn read_metadata_accepts_str_and_pathbuf() {
    // Exercises the `impl AsRef<Path>` signature with multiple argument types.
    let path = data_path("cars.sas7bdat");
    let as_str = path.to_str().unwrap();

    assert!(readstat::read_metadata(as_str).is_ok());
    assert!(readstat::read_metadata(path).is_ok());
}

#[test]
fn init_filtered_selects_only_requested_columns() {
    let path = data_path("cars.sas7bdat");
    let rsp = readstat::ReadStatPath::new(&path).unwrap();

    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    // Pick the first column by name from the (unfiltered) metadata.
    let first_col = md.vars.values().next().unwrap().var_name.clone();
    let row_count = u32::try_from(md.row_count).unwrap();

    let mapping = md
        .resolve_selected_columns(Some(vec![first_col.clone()]))
        .unwrap()
        .unwrap();

    let mut d = readstat::ReadStatData::new().init_filtered(md, &mapping, 0, row_count);
    d.read_data(&rsp).unwrap();

    let batch = d.batch.unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows() as u32, row_count);
    assert_eq!(batch.schema().field(0).name(), &first_col);
}
