use arrow::{
    array::{Float64Array, StringArray},
    datatypes::DataType,
};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    // setup path
    let rsp = common::setup_path("hasmissing.sas7bdat").unwrap();

    // setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    // parse sas7bdat
    // read only up to the 5th row
    let d = readstat::ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, 5);

    (rsp, md, d)
}

#[test]
fn parse_hasmissing() {
    let (rsp, _md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // variable count
    let var_count = d.var_count;
    assert_eq!(var_count, 9);

    // row count
    let row_count = d.chunk_rows_to_process;
    assert_eq!(row_count, 5);

    // contains variable
    let contains_var = common::contains_var(&d, 0);
    assert!(contains_var);

    // metadata
    let m = common::get_metadata(&d, 0);

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
