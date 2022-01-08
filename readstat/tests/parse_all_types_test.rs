use arrow::{
    array::{Float64Array, StringArray, TimestampSecondArray},
    datatypes::DataType,
};
use chrono::NaiveDate;
use path_abs::PathAbs;
use std::env;

/*
_int,_float,_char,_string,_date,_datetime,_time
1234.0,1234.5,s,string,2021-01-01,2021-01-01T10:49:39.000000000,02:14:13
4567.0,4567.8,c,another string,2021-06-01,2021-06-01T13:42:25.000000000,19:54:42
,910.11,,stringy string,2014-05-22,,11:04:44
*/

#[test]
fn parse_all_types_int() {
    // setup path
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.as_path().join("tests").join("data");
    let sas_path = data_dir.join("all_types.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    // parse sas7bdat
    let mut d = readstat::ReadStatData::new(rsp)
        .set_reader(readstat::Reader::mem)
        .set_is_test(true);
    let error = d.get_data(None).unwrap();

    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // variable index and name
    let var_name = String::from("_int");
    let var_index = 0;

    // contains variable
    let vars = d.vars;
    let contains_key = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(
        var_index,
        var_name.clone(),
    ));

    assert!(contains_key);

    // metadata
    let m = &vars
        .get(&readstat::ReadStatVarIndexAndName::new(
            var_index,
            var_name.clone(),
        ))
        .unwrap();

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
    // setup path
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.as_path().join("tests").join("data");
    let sas_path = data_dir.join("all_types.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    // parse sas7bdat
    let mut d = readstat::ReadStatData::new(rsp)
        .set_reader(readstat::Reader::mem)
        .set_is_test(true);
    let error = d.get_data(None).unwrap();

    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // variable index and name
    let var_name = String::from("_string");
    let var_index = 3;

    // contains variable
    let vars = d.vars;
    let contains_key = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(
        var_index,
        var_name.clone(),
    ));

    assert!(contains_key);

    // metadata
    let m = &vars
        .get(&readstat::ReadStatVarIndexAndName::new(
            var_index,
            var_name.clone(),
        ))
        .unwrap();

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
    // setup path
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.as_path().join("tests").join("data");
    let sas_path = data_dir.join("all_types.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    // parse sas7bdat
    let mut d = readstat::ReadStatData::new(rsp)
        .set_reader(readstat::Reader::mem)
        .set_is_test(true);
    let error = d.get_data(None).unwrap();

    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // variable index and name
    let var_name = String::from("_datetime");
    let var_index = 5;

    // contains variable
    let vars = d.vars;
    let contains_key = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(
        var_index,
        var_name.clone(),
    ));

    assert!(contains_key);

    // metadata
    let m = &vars
        .get(&readstat::ReadStatVarIndexAndName::new(
            var_index,
            var_name.clone(),
        ))
        .unwrap();

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
/*
    let var_count = d.var_count;
    assert_eq!(var_count, 9);

    let row_count = d.row_count;
    assert_eq!(row_count, 5);

    // column = 1 (index 0) -> row = 1 (index 0)
    let string_col_with_non_missing = d
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        string_col_with_non_missing.value(0),
        String::from("00101").as_str()
    );

    // column = 4 (index 3) -> row = 2 (index 1)
    let float_col_with_non_missing = d
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(float_col_with_non_missing.value(1), 33.3);

    // column = 5 (index 4)
    let float_col_with_missing = d.batch.column(4).data();

    let float_col_with_missing_miss_count = float_col_with_missing.null_count();
    assert_eq!(float_col_with_missing_miss_count, 1);

    // column = 5 (index 4) -> row = 1 (index 0)
    let float_col_with_missing_is_not_null = float_col_with_missing.is_null(0);
    assert!(!float_col_with_missing_is_not_null);

    // column = 5 (index 4) -> row = 2 (index 1)
    let float_col_with_missing_is_null = float_col_with_missing.is_null(1);
    assert!(float_col_with_missing_is_null);
}
*/
