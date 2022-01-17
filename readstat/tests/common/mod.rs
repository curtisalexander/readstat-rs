use std::error::Error;

use path_abs::PathAbs;

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
    readstat::ReadStatPath::new(sas_path, None, None)
}

pub fn contains_var(d: &readstat::ReadStatData, var_name: String, var_index: i32) -> bool {
    // contains variable
    d.vars.contains_key(&readstat::ReadStatVarIndexAndName::new(
        var_index,
        var_name.clone(),
    ))
}

pub fn get_metadata<'a>(
    d: &'a readstat::ReadStatData,
    var_name: String,
    var_index: i32,
) -> &'a readstat::ReadStatVarMetadata {
    // contains variable
    d.vars
        .get(&readstat::ReadStatVarIndexAndName::new(
            var_index,
            var_name.clone(),
        ))
        .unwrap()
}
