use arrow::{
    array::{Float64Array, StringArray, TimestampSecondArray},
    datatypes::DataType,
};
use chrono::NaiveDate;

mod common;

fn init() -> readstat::ReadStatData {
    // setup path
    let rsp = common::setup_path("all_types.sas7bdat").unwrap();

    // parse sas7bdat
    readstat::ReadStatData::new(rsp)
        .set_reader(readstat::Reader::mem)
        .set_is_test(true)
}

#[test]
fn parse_all_types_int() {
    let mut d = init();

    let error = d.get_data(None).unwrap();
    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // variable index and name
    let var_name = String::from("_int");
    let var_index = 0;

    // contains variable
    let contains_var = common::contains_var(&d, var_name.clone(), var_index);
    assert!(contains_var);

    // metadata
    let m = common::get_metadata(&d, var_name.clone(), var_index);

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

    let error = d.get_data(None).unwrap();
    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // variable index and name
    let var_name = String::from("_string");
    let var_index = 3;

    // contains variable
    let contains_var = common::contains_var(&d, var_name.clone(), var_index);
    assert!(contains_var);

    // metadata
    let m = common::get_metadata(&d, var_name.clone(), var_index);

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

    let error = d.get_data(None).unwrap();
    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // variable index and name
    let var_name = String::from("_datetime");
    let var_index = 5;

    // contains variable
    let contains_var = common::contains_var(&d, var_name.clone(), var_index);
    assert!(contains_var);

    // metadata
    let m = common::get_metadata(&d, var_name.clone(), var_index);

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