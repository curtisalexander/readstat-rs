use arrow_array::{Array, Date32Array};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};
use std::path::PathBuf;

fn deleted_rows_fixture() -> ReadStatPath {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../readstat-sys/vendor/ReadStat/resources/datetime.sas7bdat");
    ReadStatPath::new(path).unwrap()
}

#[test]
fn sas_deleted_rows_are_excluded_from_counts_and_data() {
    let path = deleted_rows_fixture();
    let mut metadata = ReadStatMetadata::new();
    metadata.read_metadata(&path, false).unwrap();

    assert_eq!(metadata.row_count, Some(3));

    let mut data = ReadStatData::new().init(metadata, 0, 3);
    data.read_data(&path).unwrap();
    let batch = data.batch.unwrap();
    assert_eq!(batch.num_rows(), 3);

    let dates = batch
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(dates.len(), 3);
    assert_eq!(dates.values(), &[-3_648, -3_651, -3_648]);
}

#[test]
fn sas_row_offsets_count_only_live_rows() {
    let path = deleted_rows_fixture();
    let mut metadata = ReadStatMetadata::new();
    metadata.read_metadata(&path, false).unwrap();

    let mut data = ReadStatData::new().init(metadata, 1, 3);
    data.read_data(&path).unwrap();
    let batch = data.batch.unwrap();

    let dates = batch
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(dates.values(), &[-3_651, -3_648]);
}
