use arrow::datatypes::{DataType, TimeUnit};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath, ReadStatVarFormatClass};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path("all_datetimes.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    (rsp, md, d)
}

#[test]
fn all_datetime_value_columns_have_datetime_format_class() {
    let (rsp, _md, mut d) = init();
    d.read_data(&rsp).unwrap();

    let var_count = d.vars.len() as i32;
    let mut checked = 0;

    for idx in (3..var_count).step_by(2) {
        let m = common::get_metadata(&d, idx);
        let col_name = d.schema.fields[idx as usize].name().clone();

        assert!(
            col_name.ends_with("_value"),
            "Column at index {idx} should be a _value column, got: {col_name}"
        );

        assert_eq!(
            m.var_format_class,
            Some(ReadStatVarFormatClass::DateTime),
            "Column {col_name} (format={}) should have DateTime format class",
            m.var_format
        );

        assert!(
            matches!(
                d.schema.fields[idx as usize].data_type(),
                DataType::Timestamp(TimeUnit::Second, None)
            ),
            "Column {col_name} (format={}) should have Timestamp(Second, None) arrow type, got {:?}",
            m.var_format,
            d.schema.fields[idx as usize].data_type()
        );

        checked += 1;
    }

    // 37 datetime formats
    assert_eq!(checked, 37, "Expected 37 datetime format columns");
}

#[test]
fn parse_all_datetimes_metadata() {
    let rsp = common::setup_path("all_datetimes.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    // row count
    assert_eq!(md.row_count, 1);

    // variable count
    assert_eq!(md.var_count, 76);

    // table name
    assert_eq!(md.table_name, String::from(""));

    // table label
    assert_eq!(md.file_label, String::from(""));

    // file encoding
    assert_eq!(md.file_encoding, String::from("UTF-8"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert_eq!(md.is64bit, 1);

    // creation time
    assert_eq!(md.creation_time, "2026-02-16 19:55:11");

    // modified time
    assert_eq!(md.modified_time, "2026-02-16 19:55:11");

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));
}
