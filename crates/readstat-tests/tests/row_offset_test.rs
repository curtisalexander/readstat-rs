use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::Array;
use chrono::NaiveDate;
use common::ExpectedMetadata;

mod common;

#[test]
fn row_offset_metadata() {
    let (_rsp, md, d) = common::setup_and_read_rows("all_types.sas7bdat", 2, 3);

    // row count = 1 due to offset (rows 2..3, only 1 row)
    assert_eq!(d.chunk_rows_to_process, 1);

    common::assert_metadata(&md, &ExpectedMetadata {
        row_count: 3,
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

    // Verify all variable types match the all_types dataset
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, "BEST12");
    assert!(matches!(adt, DataType::Float64));

    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Date));
    assert_eq!(vf, "YYMMDD10");
    assert!(matches!(adt, DataType::Date32));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 5);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::DateTime));
    assert!(matches!(adt, DataType::Timestamp(TimeUnit::Second, None)));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 8);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Time));
    assert!(matches!(adt, DataType::Time32(TimeUnit::Second)));
}

#[test]
fn row_offset_int() {
    let (_rsp, _md, d) = common::setup_and_read_rows("all_types.sas7bdat", 2, 3);
    let batch = d.batch.as_ref().unwrap();

    // Row 2 of all_types has missing int
    let col = common::get_f64_col(batch, 0);
    assert!(col.is_null(0));
}

#[test]
fn row_offset_string() {
    let (_rsp, _md, d) = common::setup_and_read_rows("all_types.sas7bdat", 2, 3);
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_string_col(batch, 3);
    assert_eq!(col.value(0), "stringy string");
}

#[test]
fn row_offset_date() {
    let (_rsp, _md, d) = common::setup_and_read_rows("all_types.sas7bdat", 2, 3);
    let batch = d.batch.as_ref().unwrap();

    let col = common::get_date32_col(batch, 4);
    // Date32 stores days since Unix epoch (1970-01-01)
    let days_since_epoch = col.value(0);
    let date = NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163).unwrap();
    assert_eq!(date, NaiveDate::from_ymd_opt(2014, 5, 22).unwrap());
}
