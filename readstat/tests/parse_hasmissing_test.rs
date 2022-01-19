use arrow::{
    array::{Float64Array, StringArray},
    datatypes::DataType,
};
use path_abs::PathAbs;
use std::env;

#[test]
fn parse_hasmissing() {
    // setup path
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.as_path().join("tests").join("data");
    let sas_path = data_dir.join("hasmissing.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    // parse sas7bdat
    // set is_test to true to suppress writing to console
    let mut d = readstat::ReadStatData::new(rsp)
        .set_reader(Some(readstat::Reader::mem))
        .set_is_test(true);
    let error = d.get_data(Some(5)).unwrap();

    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // variable count
    let var_count = d.var_count;
    assert_eq!(var_count, 9);

    // row count
    let row_count = d.row_count;
    assert_eq!(row_count, 5);

    // variable
    let vars = d.vars;
    let contains_key = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(
        0,
        String::from("ID"),
    ));
    assert!(contains_key);

    // metadata
    let m = vars
        .get(&readstat::ReadStatVarIndexAndName::new(
            0,
            String::from("ID"),
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
    assert_eq!(m.var_format, String::from("$"));

    // arrow data type
    assert!(matches!(d.schema.field(0).data_type(), DataType::Utf8));

    // non-missing column value from column that has no missing values
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

    // non-missing column value from column that has missing values
    // column = 4 (index 3) -> row = 2 (index 1)
    let float_col_with_non_missing = d
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(float_col_with_non_missing.value(1), 33.3);

    // missing column value from column that has missing values
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
