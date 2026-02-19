use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::Array;
use chrono::{NaiveDate, TimeZone, Utc};
use common::ExpectedMetadata;

mod common;

#[test]
fn parse_all_types_metadata() {
    let (_rsp, md, d) = common::setup_and_read_skip_row_count("all_types.sas7bdat");

    // skip_row_count=true sets row_count to 1
    common::assert_metadata(&md, &ExpectedMetadata {
        row_count: 1,
        var_count: 10,
        table_name: "",
        file_label: "",
        file_encoding: "UTF-8",
        version: 9,
        is64bit: 1,
        creation_time: "2026-02-18 02:32:45",
        modified_time: "2026-02-18 02:32:45",
    });

    assert!(matches!(md.compression, readstat::ReadStatCompress::None));
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    assert!(common::contains_var(&d, 0));
    assert!(!common::contains_var(&d, 100));

    // Verify key variable types
    let (_, _, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(vfc.is_none());
    assert_eq!(vf, "BEST12");
    assert!(matches!(adt, DataType::Float64));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 4);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Date));
    assert!(matches!(adt, DataType::Date32));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 5);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::DateTime));
    assert!(matches!(adt, DataType::Timestamp(TimeUnit::Second, None)));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 6);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::DateTimeWithMilliseconds));
    assert!(matches!(adt, DataType::Timestamp(TimeUnit::Millisecond, None)));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 7);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::DateTimeWithMicroseconds));
    assert!(matches!(adt, DataType::Timestamp(TimeUnit::Microsecond, None)));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 8);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Time));
    assert!(matches!(adt, DataType::Time32(TimeUnit::Second)));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 9);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::TimeWithMicroseconds));
    assert!(matches!(adt, DataType::Time64(TimeUnit::Microsecond)));
}

/// Data tests use normal (non-skip) read to verify values are correct
#[test]
fn skip_row_count_int() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_f64_col(batch, 0);
    assert_eq!(col.value(0), 1234f64);
    assert!(col.is_null(2));
}

#[test]
fn skip_row_count_string() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_string_col(batch, 3);
    assert_eq!(col.value(0), "string");
    assert_eq!(col.value(2), "stringy string");
}

#[test]
fn skip_row_count_datetime() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_ts_sec_col(batch, 5);
    let dt = Utc.timestamp_opt(col.value(1), 0).unwrap().naive_utc();
    let expected = NaiveDate::from_ymd_opt(2021, 6, 1)
        .unwrap()
        .and_hms_milli_opt(13, 42, 25, 0)
        .unwrap();
    assert_eq!(dt, expected);
}
