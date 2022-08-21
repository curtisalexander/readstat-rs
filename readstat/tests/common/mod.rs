use assert_cmd::cargo::CommandCargoExt;
use assert_fs::TempDir;
use path_abs::PathAbs;
use polars::prelude::*;
use std::{
    error::Error,
    fs::File,
    ops::Deref,
    path::{Path, PathBuf},
    process::Command,
    result::Result,
};

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
    &arrow2::datatypes::DataType,
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
    readstat::ReadStatPath::new(sas_path, None, None, false, false)
}

#[allow(dead_code)]
pub fn cli_data_to_parquet(
    base_file_name: &str,
) -> Result<(Command, OutFile), Box<dyn std::error::Error>> {
    let out_file = OutFile::new(base_file_name);

    let mut cmd = Command::cargo_bin("readstat")?;

    cmd.arg("data")
        .arg(format!("tests/data/{}.sas7bdat", base_file_name))
        .args(["--format", "parquet"])
        .args(["--output", out_file.out_path.as_os_str().to_str().unwrap()])
        // .args(["--stream-rows", "500"])
        .arg("--overwrite");

    Ok((cmd, out_file))
}

#[allow(dead_code)]
pub fn cli_data_from_parquet(
    base_file_name: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    cli_data_to_parquet(base_file_name)?;

    let path = File::open(format!("tests/data/{}.parquet", base_file_name))?;
    let df = ParquetReader::new(path).finish()?;

    Ok(df)
}

pub struct OutFile {
    out_path: PathBuf,
    _tempdir: TempDir,
}

impl OutFile {
    pub fn new(filename: &str) -> Self {
        let _tempdir = TempDir::new().unwrap();
        let mut out_path = PathBuf::from(_tempdir.path());
        out_path.push(format!("{}.parquet", filename));

        Self { out_path, _tempdir }
    }
}

impl Deref for OutFile {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        self.out_path.deref()
    }
}
