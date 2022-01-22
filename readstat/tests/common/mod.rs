use std::error::Error;

use path_abs::PathAbs;

// used in tests
pub fn setup_path<P>(ds: P) -> Result<readstat::ReadStatPath, Box<dyn Error>>
where
    P: AsRef<std::path::Path>,
{
    // setup path
    let sas_path = PathAbs::new(env!("CARGO_MANIFEST_DIR"))
        .unwrap()
        .as_path()
        .join("tests")
        .join("data")
        .join(ds);
    readstat::ReadStatPath::new(sas_path, None, None, false)
}

// used in tests
#[allow(dead_code)]
pub fn contains_var(d: &readstat::ReadStatData, var_index: i32) -> bool {
    // contains variable
    d.metadata.vars.contains_key(&var_index)
}

// used in tests
#[allow(dead_code)]
pub fn get_metadata<'a>(
    d: &'a readstat::ReadStatData,
    var_index: i32,
) -> &'a readstat::ReadStatVarMetadata {
    // contains variable
    d.metadata.vars
        .get(&var_index)
        .unwrap()
}

// used in tests
#[allow(dead_code)]
pub fn get_var_attrs<'a>(
    d: &'a readstat::ReadStatData,
    var_index: i32,
) -> (
    readstat::ReadStatVarTypeClass,
    readstat::ReadStatVarType,
    Option<readstat::ReadStatFormatClass>,
    String,
    &'a arrow::datatypes::DataType,
) {
    let m = get_metadata(&d, var_index);
    let s = &d.schema;
    (
        m.var_type_class,
        m.var_type,
        m.var_format_class,
        m.var_format.clone(),
        s.field(var_index as usize).data_type(),
    )
}