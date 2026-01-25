use arrow::datatypes::DataType;
use path_abs::PathAbs;
use std::{error::Error, result::Result};

#[allow(dead_code)]
pub fn contains_var(d: &readstat::ReadStatData, var_index: i32) -> bool {
    // contains variable
    d.vars.contains_key(&var_index)
}

#[allow(dead_code)]
pub fn get_metadata(d: &readstat::ReadStatData, var_index: i32) -> &readstat::ReadStatVarMetadata {
    // contains variable
    d.vars.get(&var_index).unwrap()
}

#[allow(dead_code)]
pub fn get_var_attrs(
    d: &readstat::ReadStatData,
    var_index: i32,
) -> (
    readstat::ReadStatVarTypeClass,
    readstat::ReadStatVarType,
    Option<readstat::ReadStatVarFormatClass>,
    String,
    &DataType,
) {
    let m = get_metadata(d, var_index);
    let s = &d.schema;
    (
        m.var_type_class,
        m.var_type,
        m.var_format_class,
        m.var_format.clone(),
        s.fields[var_index as usize].data_type(),
    )
}

#[allow(dead_code)]
pub fn setup_path<P>(ds: P) -> Result<readstat::ReadStatPath, Box<dyn Error + Send + Sync>>
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
    readstat::ReadStatPath::new(sas_path, None, None, false, false, None, None)
}
