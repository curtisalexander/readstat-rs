use arrow::{array::Float64Array, datatypes::DataType};
use path_abs::PathAbs;
use std::env;

#[test]
fn parse_scientific_notation() {
    // setup path
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.as_path().join("tests").join("data");
    let sas_path = data_dir.join("scientific_notation.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None, false).unwrap();

    // parse sas7bdat
    let mut d = readstat::ReadStatData::new(rsp)
        .set_reader(Some(readstat::Reader::mem))
        .set_no_progress(true)
        .set_no_write(true);
    let error = d.get_data(None, None).unwrap();

    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // variable index and name
    let var_index = 1;

    // contains variable
    let vars = d.metadata.vars;
    let contains_key = vars.contains_key(&var_index);

    assert!(contains_key);

    // metadata
    let m = vars.get(&var_index).unwrap();

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
    let col = d
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
    assert_eq!(col.value(0), 333039375527f64);

    // values
    assert_eq!(col.value(1), 1234f64);
}
