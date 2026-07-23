//! Runtime tests for the high-level convenience API: the `read_metadata` /
//! `read_to_batch` free functions and `ReadStatData::init_filtered`.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use readstat::{ProgressCallback, ReadStatError, ReadStatReader};

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

#[test]
fn reader_sources_metadata_filter_ranges_and_chunks() {
    let path = data_path("cars.sas7bdat");
    let bytes: Arc<[u8]> = std::fs::read(&path).unwrap().into();
    let path_reader = ReadStatReader::from_path(&path).unwrap();
    let metadata = path_reader.metadata().unwrap();
    let all = path_reader.read().unwrap();
    assert_eq!(all.num_rows(), metadata.row_count as usize);

    let from_bytes = ReadStatReader::from_bytes(bytes).read().unwrap();
    let from_mmap = ReadStatReader::from_mmap(&path).unwrap().read().unwrap();
    assert_eq!(from_bytes, all);
    assert_eq!(from_mmap, all);

    let selected = ReadStatReader::from_path(&path)
        .unwrap()
        .columns(["Brand", "CityMPG"])
        .rows(2, Some(7))
        .chunk_rows(3);
    let chunks = selected.chunks().unwrap();
    assert_eq!(
        chunks.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        [3, 3, 1]
    );
    let batch = selected.read().unwrap();
    assert_eq!(batch.num_rows(), 7);
    assert_eq!(batch.schema().field(0).name(), "Brand");
    assert_eq!(batch.schema().field(1).name(), "CityMPG");
}

#[derive(Default)]
struct Progress {
    increments: Mutex<Vec<u64>>,
    starts: Mutex<Vec<String>>,
}

impl ProgressCallback for Progress {
    fn inc(&self, n: u64) {
        self.increments.lock().unwrap().push(n);
    }

    fn parsing_started(&self, source: &str) {
        self.starts.lock().unwrap().push(source.to_owned());
    }
}

#[test]
fn reader_visit_is_bounded_ordered_and_reports_actual_rows() {
    let progress = Arc::new(Progress::default());
    let mut seen = Vec::new();
    ReadStatReader::from_path(data_path("cars.sas7bdat"))
        .unwrap()
        .rows(1, Some(5))
        .chunk_rows(2)
        .progress(progress.clone())
        .visit(|batch| {
            assert!(batch.num_rows() <= 2);
            seen.push(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        seen.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        [2, 2, 1]
    );
    assert_eq!(*progress.increments.lock().unwrap(), [2, 2, 1]);
    assert_eq!(progress.starts.lock().unwrap().len(), 1);
}

#[test]
fn reader_rejects_invalid_configuration_and_handles_zero_rows_and_columns() {
    let path = data_path("cars.sas7bdat");
    assert!(matches!(
        ReadStatReader::from_path(&path)
            .unwrap()
            .chunk_rows(0)
            .read(),
        Err(ReadStatError::InvalidChunkSize)
    ));
    assert!(matches!(
        ReadStatReader::from_path(&path)
            .unwrap()
            .rows(u32::MAX, None)
            .read(),
        Err(ReadStatError::InvalidRowRange { .. })
    ));
    assert!(matches!(
        ReadStatReader::from_path(&path)
            .unwrap()
            .rows(0, Some(u32::MAX))
            .read(),
        Err(ReadStatError::InvalidRowRange { .. })
    ));
    let empty = ReadStatReader::from_path(&path)
        .unwrap()
        .rows(0, Some(0))
        .read()
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    let no_columns = ReadStatReader::from_path(path)
        .unwrap()
        .columns(Vec::<String>::new())
        .rows(0, Some(3))
        .read()
        .unwrap();
    assert_eq!((no_columns.num_rows(), no_columns.num_columns()), (3, 0));
}

#[test]
fn metadata_reuse_is_transactional_on_failure() {
    let path = data_path("cars.sas7bdat");
    let rsp = readstat::ReadStatPath::new(path).unwrap();
    let mut metadata = readstat::ReadStatMetadata::new();
    metadata.read_metadata(&rsp, false).unwrap();
    let before = (
        metadata.row_count,
        metadata.var_count,
        metadata.schema.clone(),
    );
    assert!(
        metadata
            .read_metadata_from_bytes(b"not a SAS file", false)
            .is_err()
    );
    assert_eq!(
        (
            metadata.row_count,
            metadata.var_count,
            metadata.schema.clone()
        ),
        before
    );
    metadata.read_metadata(&rsp, false).unwrap();
    assert_eq!(
        (
            metadata.row_count,
            metadata.var_count,
            metadata.schema.clone()
        ),
        before
    );
}
