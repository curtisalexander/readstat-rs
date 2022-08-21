use ::predicates::prelude::*; // Used for writing assertions
use assert_cmd::prelude::*; // Add methods on commands
use assert_fs::NamedTempFile;
use polars::prelude::*;
use std::{fs::File, path::PathBuf, process::Command, result::Result};

fn cli_data_to_parquet(
    base_file_name: &str,
    rows_to_stream: Option<u32>,
) -> Result<(Command, NamedTempFile), Box<dyn std::error::Error>> {
    let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

    let mut cmd = Command::cargo_bin("readstat")?;

    if let Some(rows) = rows_to_stream {
        cmd.arg("data")
            .arg(format!("tests/data/{}.sas7bdat", base_file_name))
            .args(["--format", "parquet"])
            .args(["--output", tempfile.as_os_str().to_str().unwrap()])
            .args(["--stream-rows", rows.to_string().as_str()])
            .arg("--overwrite");
    } else {
        cmd.arg("data")
            .arg(format!("tests/data/{}.sas7bdat", base_file_name))
            .args(["--format", "parquet"])
            .args(["--output", tempfile.as_os_str().to_str().unwrap()])
            .arg("--overwrite");
    }

    Ok((cmd, tempfile))
}

fn parquet_to_df(path: PathBuf) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let pq_file = File::open(path).unwrap();

    let df = ParquetReader::new(pq_file).finish()?;

    Ok(df)
}

#[test]
fn cars_to_parquet() {
    let (mut cmd, tempfile) = cli_data_to_parquet("cars", None).unwrap();

    cmd.assert().success().stdout(predicate::str::contains(
        "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
    ));

    let df = parquet_to_df(tempfile.to_path_buf()).unwrap();

    let (height, width) = df.shape();

    assert_eq!(height, 1081);
    assert_eq!(width, 13);

    tempfile.close().unwrap();
}

#[test]
fn cars_to_parquet_with_streaming() {
    let (mut cmd, tempfile) = cli_data_to_parquet("cars", Some(500)).unwrap();

    cmd.assert().success().stdout(predicate::str::contains(
        "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
    ));

    let df = parquet_to_df(tempfile.to_path_buf()).unwrap();

    let (height, width) = df.shape();

    assert_eq!(height, 1081);
    assert_eq!(width, 13);

    tempfile.close().unwrap();
}
