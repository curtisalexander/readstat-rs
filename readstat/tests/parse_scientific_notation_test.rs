use arrow::{array::Float64Array, datatypes::DataType};
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
        d.schema.field(var_index as usize).data_type(),
        DataType::Float64
    ));

    // float column
    let float_col = d
        .batch
        .column(var_index as usize)
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
