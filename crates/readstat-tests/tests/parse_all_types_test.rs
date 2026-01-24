use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{Array, Float64Array, StringArray, TimestampSecondArray};
use chrono::{NaiveDate, TimeZone, Utc};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    // setup path
    let rsp = common::setup_path("all_types.sas7bdat").unwrap();

    // setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    // parse sas7bdat
    // read the entire dataset
    let d = readstat::ReadStatData::new().set_no_progress(true).init(
        md.clone(),
        0,
        md.row_count as u32,
    );

    (rsp, md, d)
}

#[test]
fn parse_all_types_int() {
    let (rsp, _md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // variable index and name
    let var_index = 0;

    // contains variable
    let contains_var = common::contains_var(&d, var_index);
    assert!(contains_var);

    // metadata
    let m = common::get_metadata(&d, var_index);

    // variable type class
    assert!(matches!(
        m.var_type_class,
        readstat::ReadStatVarTypeClass::Numeric
    ));

    // variable type
    assert!(matches!(m.var_type, readstat::ReadStatVarType::Double));

    // variable format class
    assert!(m.var_format_class.is_none());

    // variable format
    assert_eq!(m.var_format, String::from("BEST12"));

    // arrow data type
    assert!(matches!(
        d.schema.fields[var_index as usize].data_type(),
        DataType::Float64
    ));

    // get batch and columns
    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // int column
    let col = columns
        .get(var_index as usize)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // non-missing value
    assert_eq!(col.value(0), 1234f64);

    // missing value
    // is_null == true   ==>  a missing value
    // is_null == false  ==>  a value exists
    assert!(col.is_null(2));
}

#[test]
fn parse_all_types_string() {
    let (rsp, _md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // variable index and name
    let var_index = 3;

    // contains variable
    let contains_var = common::contains_var(&d, var_index);
    assert!(contains_var);

    // metadata
    let m = common::get_metadata(&d, var_index);

    // variable type class
    assert!(matches!(
        m.var_type_class,
        readstat::ReadStatVarTypeClass::String
    ));

    // variable type
    assert!(matches!(m.var_type, readstat::ReadStatVarType::String));

    // variable format class
    assert!(m.var_format_class.is_none());

    // variable format
    assert_eq!(m.var_format, String::from("$30"));

    // arrow data type
    assert!(matches!(
        d.schema.fields[var_index as usize].data_type(),
        DataType::Utf8
    ));

    // get batch and columns
    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // string column
    let col = columns
        .get(var_index as usize)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // non-missing value
    assert_eq!(col.value(0), "string");

    // non-missing value
    assert_eq!(col.value(2), "stringy string");
}

#[test]
fn parse_all_types_datetime() {
    let (rsp, _md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // variable index and name
    let var_index = 5;

    // contains variable
    let contains_var = common::contains_var(&d, var_index);
    assert!(contains_var);

    // metadata
    let m = common::get_metadata(&d, var_index);

    // variable type class
    assert!(matches!(
        m.var_type_class,
        readstat::ReadStatVarTypeClass::Numeric
    ));

    // variable type
    assert!(matches!(m.var_type, readstat::ReadStatVarType::Double));

    // variable format class
    assert!(matches!(
        m.var_format_class,
        Some(readstat::ReadStatVarFormatClass::DateTime)
    ));

    // variable format
    assert_eq!(m.var_format, String::from("DATETIME22"));

    // arrow data type
    assert!(matches!(
        d.schema.fields[var_index as usize].data_type(),
        DataType::Timestamp(TimeUnit::Second, None)
    ));

    // get batch and columns
    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // datetime column - access as TimestampSecondArray
    let col = columns
        .get(var_index as usize)
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();

    // non-missing value - convert timestamp seconds to datetime
    let timestamp_seconds = col.value(1);
    let dt = Utc.timestamp_opt(timestamp_seconds, 0).unwrap().naive_utc();
    let dt_literal = NaiveDate::from_ymd_opt(2021, 6, 1)
        .unwrap()
        .and_hms_milli_opt(13, 42, 25, 0)
        .unwrap();

    assert_eq!(dt, dt_literal);
}

#[test]
fn parse_all_types_metadata() {
    let (rsp, md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // row count
    assert_eq!(md.row_count, 3);

    // variable count
    assert_eq!(md.var_count, 8);

    // table name
    assert_eq!(md.table_name, String::from("ALL_TYPES"));

    // table label
    assert_eq!(md.file_label, String::from(""));

    // file encoding
    assert_eq!(md.file_encoding, String::from("UTF-8"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert_eq!(md.is64bit, 1);

    // creation time
    assert_eq!(md.creation_time, "2022-01-08 19:40:48");

    // modified time
    assert_eq!(md.modified_time, "2022-01-08 19:40:48");

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    // variables - contains variable
    assert!(common::contains_var(&d, 0));

    // variables - does not contain variable
    assert!(!common::contains_var(&d, 100));

    // variables

    // 0 - _int
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST12"));
    assert!(matches!(adt, DataType::Float64));

    // 1 - _float
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST12"));
    assert!(matches!(adt, DataType::Float64));

    // 2 - _char
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$1"));
    assert!(matches!(adt, DataType::Utf8));

    // 3 - _string
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 3);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$30"));
    assert!(matches!(adt, DataType::Utf8));

    // 4 - _date
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Date));
    assert_eq!(vf, String::from("YYMMDD10"));
    assert!(matches!(adt, DataType::Date32));

    // 5 - _datetime
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::DateTime));
    assert_eq!(vf, String::from("DATETIME22"));
    assert!(matches!(
        adt,
        DataType::Timestamp(TimeUnit::Second, None)
    ));

    // 6 - _datetime_with_ms
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::DateTime));
    assert_eq!(vf, String::from("DATETIME22"));
    assert!(matches!(
        adt,
        DataType::Timestamp(TimeUnit::Second, None)
    ));

    // 7 - _time
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Time));
    assert_eq!(vf, String::from("TIME"));
    assert!(matches!(
        adt,
        DataType::Time32(TimeUnit::Second)
    ));
}
