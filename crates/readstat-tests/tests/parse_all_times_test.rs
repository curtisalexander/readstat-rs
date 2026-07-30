#![allow(clippy::float_cmp)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::Array;
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath, ReadStatVarFormatClass};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path("all_times.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new().init(md.clone(), 0, md.row_count.unwrap() as u32);
    (rsp, md, d)
}

#[test]
fn all_time_value_columns_have_expected_value_and_format_class() {
    let (rsp, _md, mut d) = init();
    d.read_data(&rsp).unwrap();

    let batch = d.batch.as_ref().unwrap();
    let source = common::get_string_col(batch, 0);
    let raw = common::get_f64_col(batch, 1);
    assert!(!source.is_null(0), "Time source string should not be null");
    assert!(!raw.is_null(0), "Raw SAS time should not be null");
    assert_eq!(source.value(0), "18:43:54.123456789");
    assert_eq!(raw.value(0), 67_434.123_456_789);

    let var_count = d.vars.len() as i32;
    let mut checked = 0;

    for idx in (3..var_count).step_by(2) {
        let m = common::get_metadata(&d, idx);
        let col_name = d.schema.fields[idx as usize].name().clone();

        assert!(
            col_name.ends_with("_value"),
            "Column at index {idx} should be a _value column, got: {col_name}"
        );

        let field = &d.schema.fields[idx as usize];
        match m.var_format_class {
            Some(ReadStatVarFormatClass::Time) => {
                assert_eq!(field.data_type(), &DataType::Time32(TimeUnit::Second));
                let col = common::get_time32_col(batch, idx as usize);
                assert!(!col.is_null(0), "Column {col_name} should not be null");
                assert_eq!(col.value(0), 67_434);
            }
            Some(ReadStatVarFormatClass::TimeWithMilliseconds) => {
                assert_eq!(field.data_type(), &DataType::Time32(TimeUnit::Millisecond));
                let col = common::get_time32_ms_col(batch, idx as usize);
                assert!(!col.is_null(0), "Column {col_name} should not be null");
                assert_eq!(col.value(0), 67_434_123);
            }
            Some(ReadStatVarFormatClass::TimeWithMicroseconds) => {
                assert_eq!(field.data_type(), &DataType::Time64(TimeUnit::Microsecond));
                let col = common::get_time64_us_col(batch, idx as usize);
                assert!(!col.is_null(0), "Column {col_name} should not be null");
                assert_eq!(col.value(0), 67_434_123_457);
            }
            Some(ReadStatVarFormatClass::TimeWithNanoseconds) => {
                assert_eq!(field.data_type(), &DataType::Time64(TimeUnit::Nanosecond));
                let col = common::get_time64_ns_col(batch, idx as usize);
                assert!(!col.is_null(0), "Column {col_name} should not be null");
                assert_eq!(col.value(0), 67_434_123_456_789);
            }
            other => panic!(
                "Column {col_name} (format={}) has unexpected format class {other:?}",
                m.var_format
            ),
        }

        checked += 1;
    }

    // 18 general time formats plus millisecond, microsecond, and nanosecond formats.
    assert_eq!(checked, 21, "Expected 21 time format columns");
}

#[test]
fn parse_all_times_metadata() {
    let rsp = common::setup_path("all_times.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    // row count
    assert_eq!(md.row_count, Some(1));

    // variable count
    assert_eq!(md.var_count, 44);

    // table name
    assert_eq!(md.table_name, String::new());

    // table label
    assert_eq!(md.file_label, String::new());

    // file encoding
    assert_eq!(md.file_encoding, String::from("UTF-8"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert!(md.is_64bit);

    // creation time
    assert!(!md.creation_time.is_empty());

    // A newly generated fixture should not be modified after creation.
    assert_eq!(md.modified_time, md.creation_time);

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));
}
