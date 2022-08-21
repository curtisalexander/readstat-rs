use ::predicates::prelude::*; // Used for writing assertions
use assert_cmd::prelude::*; // Add methods on commands
use assert_fs::TempDir;
use polars::prelude::*;
use std::{
    fs::File,
    ops::Deref,
    path::{Path, PathBuf},
    process::Command,
    result::Result,
};

fn cli_data_to_parquet(
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

fn parquet_to_df(path: PathBuf) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let pq_file = File::open(path).unwrap();

    let df = ParquetReader::new(pq_file).finish()?;

    Ok(df)
}

pub struct OutFile {
    pub out_path: PathBuf,
    pub _tempdir: TempDir,
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
#[test]
fn cars_to_parquet() {
    let (mut cmd, out_file) = cli_data_to_parquet("cars").unwrap();

    cmd.assert().success().stdout(predicate::str::contains(
        "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
    ));

    let df = parquet_to_df(out_file.out_path).unwrap();

    let (height, width) = df.shape();

    assert_eq!(height, 1081);
    assert_eq!(width, 13);

    out_file._tempdir.close().unwrap();
}
