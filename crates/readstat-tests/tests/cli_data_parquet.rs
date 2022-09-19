use ::predicates::prelude::*; // Used for writing assertions
use assert_cmd::Command; // Add methods on commands
use assert_fs::NamedTempFile;
use polars::prelude::*;
use std::{fs::File, path::PathBuf, result::Result};

enum OverwriteOption {
    Overwrite(NamedTempFile),
    DoNotOverwrite,
}

fn cli_data_to_parquet(
    base_file_name: &str,
    overwrite: OverwriteOption,
    rows_to_stream: Option<u32>,
) -> Result<(Command, NamedTempFile), Box<dyn std::error::Error>> {
    if let Ok(mut cmd) = Command::cargo_bin("readstat") {
        let tempfile = match (overwrite, rows_to_stream) {
            (OverwriteOption::Overwrite(tempfile), Some(rows)) => {
                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()])
                    .arg("--overwrite");

                tempfile
            }
            (OverwriteOption::DoNotOverwrite, Some(rows)) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()]);

                tempfile
            }
            (OverwriteOption::Overwrite(tempfile), None) => {
                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .arg("--overwrite");

                tempfile
            }
            (OverwriteOption::DoNotOverwrite, None) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()]);

                tempfile
            }
        };

        Ok((cmd, tempfile))
    } else {
        Err(From::from("readstat binary does not exist"))
    }
}

fn parquet_to_df(path: PathBuf) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let pq_file = File::open(path).unwrap();

    let df = ParquetReader::new(pq_file).finish()?;

    Ok(df)
}

#[test]
fn cars_to_parquet() {
    if let Ok((mut cmd, tempfile)) =
        cli_data_to_parquet("cars", OverwriteOption::DoNotOverwrite, None)
    {
        cmd.assert().success().stdout(predicate::str::contains(
            "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
        ));

        let df = parquet_to_df(tempfile.to_path_buf()).unwrap();

        let (height, width) = df.shape();

        assert_eq!(height, 1081);
        assert_eq!(width, 13);

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_with_streaming() {
    if let Ok((mut cmd, tempfile)) =
        cli_data_to_parquet("cars", OverwriteOption::DoNotOverwrite, Some(500))
    {
        cmd.assert().success().stdout(predicate::str::contains(
            "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
        ));

        let df = parquet_to_df(tempfile.to_path_buf()).unwrap();

        let (height, width) = df.shape();

        assert_eq!(height, 1081);
        assert_eq!(width, 13);

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_overwrite() {
    // first stream
    if let Ok((mut cmd, tempfile)) =
        cli_data_to_parquet("cars", OverwriteOption::DoNotOverwrite, Some(500))
    {
        cmd.assert().success().stdout(predicate::str::contains(
            "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
        ));

        // next do not stream
        let (mut cmd, tempfile) =
            cli_data_to_parquet("cars", OverwriteOption::Overwrite(tempfile), None).unwrap();

        cmd.assert().success().stdout(predicate::str::contains(
            "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
        ));

        let df = parquet_to_df(tempfile.to_path_buf()).unwrap();

        let (height, width) = df.shape();

        assert_eq!(height, 1081);
        assert_eq!(width, 13);

        tempfile.close().unwrap();
    }
}
