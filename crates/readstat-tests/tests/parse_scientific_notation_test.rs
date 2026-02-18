use arrow::datatypes::DataType;
use arrow_array::Float64Array;
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    // setup path
    let rsp = common::setup_path("scientific_notation.sas7bdat").unwrap();

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
fn parse_scientific_notation() {
    let (rsp, _md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // variable index and name
    let var_index = 1;

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
    assert_eq!(m.var_format, String::from("BEST32"));

    // arrow data type
    assert!(matches!(
        d.schema.fields[var_index as usize].data_type(),
        DataType::Float64
    ));

    // get batch and columns
    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // float column
    let float_col = columns
        .get(var_index as usize)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // values
    // Due to the way previously utilized lexical to parse floats, was having an issue when
    //   large floats were being read correctly from ReadStat but then were being converted to
    //   strings via lexical and the string conversion resulted in scientific notation; after
    //   trying to parse back from a string to a float with lexical, it would throw errors
    // Fixed by d301a9f9ff8c5e3c34a604a16c095e99d205f624
    assert_eq!(float_col.value(0), 333039375527f64);

    // values
    assert_eq!(float_col.value(1), 1234f64);
}

#[test]
fn parse_scientific_notation_metadata() {
    let (rsp, md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // row count
    assert_eq!(md.row_count, 2);

    // variable count
    assert_eq!(md.var_count, 2);

    // table name
    assert_eq!(md.table_name, String::from("SCIENTIFIC_NOTATION"));

    // table label
    assert_eq!(md.file_label, String::from(""));

    // file encoding
    assert_eq!(md.file_encoding, String::from("UTF-8"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert_eq!(md.is64bit, 1);

    // creation time
    assert_eq!(md.creation_time, "2022-01-08 22:09:34");

    // modified time
    assert_eq!(md.modified_time, "2022-01-08 22:09:34");

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    // variables

    // 0 - note (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$100"));
    assert!(matches!(adt, DataType::Utf8));

    // 1 - f (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST32"));
    assert!(matches!(adt, DataType::Float64));
}
