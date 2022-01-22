use arrow::{
    array::{Float64Array, StringArray, TimestampSecondArray},
    datatypes::DataType,
};
use chrono::NaiveDate;
use readstat::ReadStatFormatClass;

mod common;

fn init() -> readstat::ReadStatData {
    // setup path
    let rsp = common::setup_path("all_types.sas7bdat").unwrap();

    // parse sas7bdat
    readstat::ReadStatData::new(rsp)
        .set_reader(Some(readstat::Reader::mem))
        .set_no_progress(true)
        .set_is_test(true)
}

#[test]
fn parse_all_types_int() {
    let mut d = init();

    let error = d.get_data(None, None).unwrap();
    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

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
        d.schema.field(var_index as usize).data_type(),
        DataType::Float64
    ));

    // int column
    let col = d
        .batch
        .column(var_index as usize)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // non-missing value
    assert_eq!(col.value(0), 1234f64);

    // missing value
    assert!(d.batch.column(0).data().is_null(2));
}

#[test]
fn parse_all_types_string() {
    let mut d = init();

    let error = d.get_data(None, None).unwrap();
    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

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
        d.schema.field(var_index as usize).data_type(),
        DataType::Utf8
    ));

    // string column
    let col = d
        .batch
        .column(var_index as usize)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // non-missing value
    assert_eq!(col.value(0), String::from("string"));

    // non-missing value
    assert_eq!(col.value(2), String::from("stringy string"));
}

#[test]
fn parse_all_types_datetime() {
    let mut d = init();

    let error = d.get_data(None, None).unwrap();
    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

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
        Some(readstat::ReadStatFormatClass::DateTime)
    ));

    // variable format
    assert_eq!(m.var_format, String::from("DATETIME22"));

    // non-missing value
    let col = d
        .batch
        .column(var_index as usize)
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();

    let dt = col.value_as_datetime(1).unwrap();
    let dt_literal = NaiveDate::from_ymd(2021, 6, 1).and_hms_milli(13, 42, 25, 0);

    assert_eq!(dt, dt_literal);
}

#[test]
fn parse_all_types_metadata() {
    let mut d = init();

    let error = d.get_metadata(true).unwrap();
    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // row count
    assert_eq!(d.metadata.row_count, 1);

    // variable count
    assert_eq!(d.metadata.var_count, 8);

    // table name
    assert_eq!(d.metadata.table_name, String::from("ALL_TYPES"));

    // table label
    assert_eq!(d.metadata.file_label, String::from(""));

    // file encoding
    assert_eq!(d.metadata.file_encoding, String::from("UTF-8"));

    // format version
    assert_eq!(d.metadata.version, 9);

    // bitness
    assert_eq!(d.metadata.is64bit, 1);

    // creation time
    assert_eq!(d.metadata.creation_time, "2022-01-08 19:40:48");

    // modified time
    assert_eq!(d.metadata.modified_time, "2022-01-08 19:40:48");

    // compression
    assert!(matches!(d.metadata.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(d.metadata.endianness, readstat::ReadStatEndian::Little));

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
    assert_eq!(vfc, Some(ReadStatFormatClass::Date));
    assert_eq!(vf, String::from("YYMMDD10"));
    assert!(matches!(adt, DataType::Date32));

    // 5 - _datetime
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(ReadStatFormatClass::DateTime));
    assert_eq!(vf, String::from("DATETIME22"));
    assert!(matches!(
        adt,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
    ));

    // 6 - _datetime_with_ms
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(ReadStatFormatClass::DateTime));
    assert_eq!(vf, String::from("DATETIME22"));
    assert!(matches!(
        adt,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
    ));

    // 7 - _time
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert_eq!(vfc, Some(ReadStatFormatClass::Time));
    assert_eq!(vf, String::from("TIME"));
    assert!(matches!(
        adt,
        DataType::Time32(arrow::datatypes::TimeUnit::Second)
    ));
}
