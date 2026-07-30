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
    let rsp = common::setup_path("all_datetimes.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new().init(md.clone(), 0, md.row_count.unwrap() as u32);
    (rsp, md, d)
}

#[test]
fn all_datetime_value_columns_have_expected_value_and_format_class() {
    let (rsp, _md, mut d) = init();
    d.read_data(&rsp).unwrap();

    let batch = d.batch.as_ref().unwrap();
    let source = common::get_string_col(batch, 0);
    let raw = common::get_f64_col(batch, 1);
    assert!(
        !source.is_null(0),
        "Datetime source string should not be null"
    );
    assert!(!raw.is_null(0), "Raw SAS datetime should not be null");
    assert_eq!(source.value(0), "20JAN2021:18:43:54.123456");
    assert_eq!(raw.value(0), 1_926_787_434.123_456);

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
            Some(ReadStatVarFormatClass::DateTime) => {
                assert_eq!(
                    field.data_type(),
                    &DataType::Timestamp(TimeUnit::Second, None)
                );
                let col = common::get_ts_sec_col(batch, idx as usize);
                assert!(!col.is_null(0), "Column {col_name} should not be null");
                assert_eq!(col.value(0), 1_611_168_234);
            }
            Some(ReadStatVarFormatClass::DateTimeWithMilliseconds) => {
                assert_eq!(
                    field.data_type(),
                    &DataType::Timestamp(TimeUnit::Millisecond, None)
                );
                let col = common::get_ts_ms_col(batch, idx as usize);
                assert!(!col.is_null(0), "Column {col_name} should not be null");
                assert_eq!(col.value(0), 1_611_168_234_123);
            }
            Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => {
                assert_eq!(
                    field.data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, None)
                );
                let col = common::get_ts_us_col(batch, idx as usize);
                assert!(!col.is_null(0), "Column {col_name} should not be null");
                assert_eq!(col.value(0), 1_611_168_234_123_456);
            }
            other => panic!(
                "Column {col_name} (format={}) has unexpected format class {other:?}",
                m.var_format
            ),
        }

        checked += 1;
    }

    // 37 general datetime formats plus millisecond and microsecond formats.
    assert_eq!(checked, 39, "Expected 39 datetime format columns");
}

#[test]
fn parse_all_datetimes_metadata() {
    let rsp = common::setup_path("all_datetimes.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    // row count
    assert_eq!(md.row_count, Some(1));

    // variable count
    assert_eq!(md.var_count, 80);

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
