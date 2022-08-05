use path_abs::PathAbs;
use std::error::Error;

#[allow(dead_code)]
pub fn contains_var(d: &readstat::ReadStatData, var_index: i32) -> bool {
    // contains variable
    d.vars.contains_key(&var_index)
}

#[allow(dead_code)]
pub fn get_metadata<'a>(
    d: &'a readstat::ReadStatData,
    var_index: i32,
) -> &'a readstat::ReadStatVarMetadata {
    // contains variable
    d.vars.get(&var_index).unwrap()
}

#[allow(dead_code)]
pub fn get_var_attrs<'a>(
    d: &'a readstat::ReadStatData,
    var_index: i32,
) -> (
    readstat::ReadStatVarTypeClass,
    readstat::ReadStatVarType,
    Option<readstat::ReadStatVarFormatClass>,
    String,
    &'a arrow2::datatypes::DataType,
) {
    let m = get_metadata(&d, var_index);
    let s = &d.schema;
    (
        m.var_type_class,
        m.var_type,
        m.var_format_class,
        m.var_format.clone(),
        s.fields[var_index as usize].data_type(),
    )
}

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
    readstat::ReadStatPath::new(sas_path, None, None, false, false)
}
