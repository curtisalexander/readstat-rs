use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::Array;
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use common::ExpectedMetadata;

mod common;

#[test]
fn parse_all_types_metadata() {
    let (_rsp, md, d) = common::setup_and_read("all_types.sas7bdat");

    common::assert_metadata(
        &md,
        &ExpectedMetadata {
            row_count: 3,
            var_count: 10,
            table_name: "",
            file_label: "",
            file_encoding: "UTF-8",
            version: 9,
            is64bit: 1,
            creation_time: "2026-02-18 02:32:45",
            modified_time: "2026-02-18 02:32:45",
        },
    );

    assert!(matches!(md.compression, readstat::ReadStatCompress::None));
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    assert!(common::contains_var(&d, 0));
    assert!(!common::contains_var(&d, 100));

    // 0 - _int (Double -> Float64)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, "BEST12");
    assert!(matches!(adt, DataType::Float64));

    // 1 - _float (Double -> Float64)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, "BEST12");
    assert!(matches!(adt, DataType::Float64));

    // 2 - _char (String -> Utf8)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, "$1");
    assert!(matches!(adt, DataType::Utf8));

    // 3 - _string (String -> Utf8)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 3);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, "$30");
    assert!(matches!(adt, DataType::Utf8));

    // 4 - _date (Date)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Date));
    assert_eq!(vf, "YYMMDD10");
    assert!(matches!(adt, DataType::Date32));

    // 5 - _datetime (DateTime -> Timestamp Second)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::DateTime));
    assert_eq!(vf, "DATETIME22");
    assert!(matches!(adt, DataType::Timestamp(TimeUnit::Second, None)));

    // 6 - _datetime_with_ms (Timestamp Millisecond)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(
        vfc,
        Some(readstat::ReadStatVarFormatClass::DateTimeWithMilliseconds)
    );
    assert_eq!(vf, "DATETIME22.3");
    assert!(matches!(
        adt,
        DataType::Timestamp(TimeUnit::Millisecond, None)
    ));

    // 7 - _datetime_with_us (Timestamp Microsecond)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(
        vfc,
        Some(readstat::ReadStatVarFormatClass::DateTimeWithMicroseconds)
    );
    assert_eq!(vf, "DATETIME26.6");
    assert!(matches!(
        adt,
        DataType::Timestamp(TimeUnit::Microsecond, None)
    ));

    // 8 - _time (Time32 Second)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 8);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Time));
    assert_eq!(vf, "TIME");
    assert!(matches!(adt, DataType::Time32(TimeUnit::Second)));

    // 9 - _time_with_us (Time64 Microsecond)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 9);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(
        vfc,
        Some(readstat::ReadStatVarFormatClass::TimeWithMicroseconds)
    );
    assert_eq!(vf, "TIME15.6");
    assert!(matches!(adt, DataType::Time64(TimeUnit::Microsecond)));
}

#[test]
fn parse_all_types_int() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_f64_col(batch, 0);
    assert_eq!(col.value(0), 1234f64);
    assert!(col.is_null(2), "Row 2 should be missing");
}

#[test]
fn parse_all_types_string() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_string_col(batch, 3);
    assert_eq!(col.value(0), "string");
    assert_eq!(col.value(2), "stringy string");
}

#[test]
fn parse_all_types_datetime() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_ts_sec_col(batch, 5);

    // Row 1: 2021-06-01 13:42:25
    let dt = Utc.timestamp_opt(col.value(1), 0).unwrap().naive_utc();
    let expected = NaiveDate::from_ymd_opt(2021, 6, 1)
        .unwrap()
        .and_hms_milli_opt(13, 42, 25, 0)
        .unwrap();
    assert_eq!(dt, expected);
}

#[test]
fn parse_all_types_datetime_with_milliseconds() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_ts_ms_col(batch, 6);

    // Row 0: 2021-01-01 10:49:39.333
    let dt = Utc.timestamp_millis_opt(col.value(0)).unwrap().naive_utc();
    let expected = NaiveDate::from_ymd_opt(2021, 1, 1)
        .unwrap()
        .and_hms_milli_opt(10, 49, 39, 333)
        .unwrap();
    assert_eq!(dt, expected, "Row 0: Expected 2021-01-01 10:49:39.333");

    // Row 1: 2021-06-01 13:42:25.943
    let dt = Utc.timestamp_millis_opt(col.value(1)).unwrap().naive_utc();
    let expected = NaiveDate::from_ymd_opt(2021, 6, 1)
        .unwrap()
        .and_hms_milli_opt(13, 42, 25, 943)
        .unwrap();
    assert_eq!(dt, expected, "Row 1: Expected 2021-06-01 13:42:25.943");

    // Row 2: missing
    assert!(col.is_null(2));
}

#[test]
fn parse_all_types_datetime_with_microseconds() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_ts_us_col(batch, 7);

    // Row 0: 2021-01-01 10:49:39.123456
    let dt = Utc.timestamp_micros(col.value(0)).unwrap().naive_utc();
    let expected = NaiveDate::from_ymd_opt(2021, 1, 1)
        .unwrap()
        .and_hms_micro_opt(10, 49, 39, 123456)
        .unwrap();
    assert_eq!(dt, expected, "Row 0: Expected 2021-01-01 10:49:39.123456");

    // Row 1: 2021-06-01 13:42:25.987654
    let dt = Utc.timestamp_micros(col.value(1)).unwrap().naive_utc();
    let expected = NaiveDate::from_ymd_opt(2021, 6, 1)
        .unwrap()
        .and_hms_micro_opt(13, 42, 25, 987654)
        .unwrap();
    assert_eq!(dt, expected, "Row 1: Expected 2021-06-01 13:42:25.987654");

    // Row 2: missing
    assert!(col.is_null(2));
}

#[test]
fn parse_all_types_time_with_microseconds() {
    let (_rsp, _md, d) = common::setup_and_read("all_types.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_time64_us_col(batch, 9);

    // Row 0: 02:14:13.654321
    let expected_micros = NaiveTime::from_hms_micro_opt(2, 14, 13, 654321)
        .unwrap()
        .signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        .num_microseconds()
        .unwrap();
    assert_eq!(
        col.value(0),
        expected_micros,
        "Row 0: Expected 02:14:13.654321"
    );

    // Row 1: 19:54:42.123456
    let expected_micros = NaiveTime::from_hms_micro_opt(19, 54, 42, 123456)
        .unwrap()
        .signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        .num_microseconds()
        .unwrap();
    assert_eq!(
        col.value(1),
        expected_micros,
        "Row 1: Expected 19:54:42.123456"
    );

    // Row 2: missing
    assert!(col.is_null(2));
}
