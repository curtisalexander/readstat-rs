use arrow::datatypes::DataType;
use arrow_array::{Array, Float64Array, StringArray};
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
    assert!(matches!(d.schema.fields[0].data_type(), DataType::Utf8));

    // get batch and columns
    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // non-missing column value from column that has no missing values
    // column = 1 (index 0) -> row = 1 (index 0)
    let string_col_with_non_missing = columns
        .get(0)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(string_col_with_non_missing.value(0), "00101");

    // non-missing column value from column that has missing values
    // column = 4 (index 3) -> row = 2 (index 1)
    let float_col_with_non_missing = columns
        .get(3)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(float_col_with_non_missing.value(1), 33.3);

    // missing column value from column that has missing values
    // column = 5 (index 4)
    let float_col_with_missing = columns
        .get(4)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let float_col_with_missing_miss_count = float_col_with_missing.null_count();

    assert_eq!(float_col_with_missing_miss_count, 1);

    // column = 5 (index 4) -> row = 1 (index 0)
    let float_col_with_missing_is_not_null = !float_col_with_missing.is_null(0);

    assert!(float_col_with_missing_is_not_null);

    // column = 5 (index 4) -> row = 2 (index 1)
    let float_col_with_missing_is_null = float_col_with_missing.is_null(1);

    assert!(float_col_with_missing_is_null);
}

#[test]
fn parse_hasmissing_metadata() {
    let rsp = common::setup_path("hasmissing.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // row count
    assert_eq!(md.row_count, 50);

    // variable count
    assert_eq!(md.var_count, 9);

    // table name
    assert_eq!(md.table_name, String::from("HASMISSING"));

    // table label
    assert_eq!(md.file_label, String::from(""));

    // file encoding
    assert_eq!(md.file_encoding, String::from("WINDOWS-1252"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert_eq!(md.is64bit, 0);

    // creation time
    assert_eq!(md.creation_time, "2014-11-18 14:44:33");

    // modified time
    assert_eq!(md.modified_time, "2014-11-18 14:44:33");

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    // variables

    // 0 - ID (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 1 - PRE (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 2 - MONTH6 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 3 - MONTH12 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 3);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 4 - MONTH24 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 5 - TEMP1 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 6 - TEMP2 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 7 - TEMP3 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 8 - TEMP4 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 8);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));
}
