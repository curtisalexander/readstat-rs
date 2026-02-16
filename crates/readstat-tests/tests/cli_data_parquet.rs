use ::predicates::prelude::*;
use assert_cmd::Command;
use assert_fs::NamedTempFile;
use polars::prelude::*;
use readstat::ParquetCompression;
use std::{fs::File, path::PathBuf, result::Result, sync::OnceLock};

/// Cache the built binary path to avoid rebuilding for each test.
static READSTAT_BIN: OnceLock<PathBuf> = OnceLock::new();

/// Helper function to get the readstat binary command.
/// Uses escargot to build and locate the binary in the workspace (once).
fn readstat_cmd() -> Command {
    let bin_path = READSTAT_BIN.get_or_init(|| {
        let bin = escargot::CargoBuild::new()
            .bin("readstat")
            .current_release()
            .current_target()
            .manifest_path("../readstat/Cargo.toml")
            .run()
            .expect("Failed to build readstat binary");

        bin.path().to_path_buf()
    });

    Command::new(bin_path)
}

enum OverwriteOption {
    Overwrite(NamedTempFile),
    DoNotOverwrite,
}

fn cli_data_to_parquet(
    base_file_name: &str,
    overwrite: OverwriteOption,
    rows_to_stream: Option<u32>,
    compression: Option<ParquetCompression>,
    compression_level: Option<u32>,
) -> Result<(Command, NamedTempFile), Box<dyn std::error::Error>> {
    let mut cmd = readstat_cmd();
    let tempfile = match (overwrite, rows_to_stream, compression, compression_level) {
            // Overwrite | Streaming | No Compression | No Compression Level
            (OverwriteOption::Overwrite(tempfile), Some(rows), None, None) => {
                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()])
                    .arg("--overwrite");

                tempfile
            }
            // Do Not Overwrite | Streaming | No Compression | No Compression Level
            (OverwriteOption::DoNotOverwrite, Some(rows), None, None) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()]);

                tempfile
            }
            // Overwrite | No Streaming | No Compression | No Compression Level
            (OverwriteOption::Overwrite(tempfile), None, None, None) => {
                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .arg("--overwrite");

                tempfile
            }
            // Do Not Overwrite | No Streaming | No Compression | No Compression Level
            (OverwriteOption::DoNotOverwrite, None, None, None) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Uncompressed | No Compression Level
            (
                OverwriteOption::DoNotOverwrite,
                None,
                Some(ParquetCompression::Uncompressed),
                None,
            ) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "uncompressed"]);

                tempfile
            }
            // Do Not Overwrite | Streaming | Uncompressed | No Compression Level
            (
                OverwriteOption::DoNotOverwrite,
                Some(rows),
                Some(ParquetCompression::Uncompressed),
                None,
            ) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()])
                    .args(["--compression", "uncompressed"]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Uncompressed | Compression Level
            (
                OverwriteOption::DoNotOverwrite,
                None,
                Some(ParquetCompression::Uncompressed),
                Some(cl),
            ) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "uncompressed"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Snappy | No Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Snappy), None) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "snappy"]);

                tempfile
            }
            // Do Not Overwrite | Streaming | Snappy | No Compression Level
            (
                OverwriteOption::DoNotOverwrite,
                Some(rows),
                Some(ParquetCompression::Snappy),
                None,
            ) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()])
                    .args(["--compression", "snappy"]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Snappy | Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Snappy), Some(cl)) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "snappy"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Lz4Raw | No Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Lz4Raw), None) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "lz4-raw"]);

                tempfile
            }
            // Do Not Overwrite | Streaming | Lz4Raw | No Compression Level
            (
                OverwriteOption::DoNotOverwrite,
                Some(rows),
                Some(ParquetCompression::Lz4Raw),
                None,
            ) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()])
                    .args(["--compression", "lz4-raw"]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Lz4Raw | Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Lz4Raw), Some(cl)) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "lz4-raw"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Gzip | Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Gzip), Some(cl)) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "gzip"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | Streaming | Gzip | Compression Level
            (
                OverwriteOption::DoNotOverwrite,
                Some(rows),
                Some(ParquetCompression::Gzip),
                Some(cl),
            ) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()])
                    .args(["--compression", "gzip"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Gzip | No Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Gzip), None) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "gzip"]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Brotli | Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Brotli), Some(cl)) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "brotli"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | Streaming | Brotli | Compression Level
            (
                OverwriteOption::DoNotOverwrite,
                Some(rows),
                Some(ParquetCompression::Brotli),
                Some(cl),
            ) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()])
                    .args(["--compression", "brotli"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Brotli | No Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Brotli), None) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "brotli"]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Zstd | Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Zstd), Some(cl)) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "zstd"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | Streaming | Zstd | Compression Level
            (
                OverwriteOption::DoNotOverwrite,
                Some(rows),
                Some(ParquetCompression::Zstd),
                Some(cl),
            ) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--stream-rows", rows.to_string().as_str()])
                    .args(["--compression", "zstd"])
                    .args(["--compression-level", cl.to_string().as_str()]);

                tempfile
            }
            // Do Not Overwrite | No Streaming | Zstd | No Compression Level
            (OverwriteOption::DoNotOverwrite, None, Some(ParquetCompression::Zstd), None) => {
                let tempfile = NamedTempFile::new(format!("{}.parquet", base_file_name))?;

                cmd.arg("data")
                    .arg(format!("tests/data/{}.sas7bdat", base_file_name))
                    .args(["--format", "parquet"])
                    .args(["--output", tempfile.as_os_str().to_str().unwrap()])
                    .args(["--compression", "zstd"]);

                tempfile
            }
            _ => unreachable!(),
        };

    Ok((cmd, tempfile))
}

fn parquet_to_df(path: PathBuf) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let pq_file = File::open(path).unwrap();

    let df = ParquetReader::new(pq_file).finish()?;

    Ok(df)
}

#[test]
fn cars_to_parquet() {
    if let Ok((mut cmd, tempfile)) =
        cli_data_to_parquet("cars", OverwriteOption::DoNotOverwrite, None, None, None)
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
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        Some(500),
        None,
        None,
    ) {
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
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        Some(500),
        None,
        None,
    ) {
        cmd.assert().success().stdout(predicate::str::contains(
            "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
        ));

        // next do not stream
        let (mut cmd, tempfile) = cli_data_to_parquet(
            "cars",
            OverwriteOption::Overwrite(tempfile),
            None,
            None,
            None,
        )
        .unwrap();

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
fn cars_to_parquet_with_compression_uncompressed() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Uncompressed),
        None,
    ) {
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
fn cars_to_parquet_with_compression_uncompressed_with_compression_level() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Uncompressed),
        Some(5),
    ) {
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Compression level is not required for compression=uncompressed, ignoring value of --compression-level"));

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_with_streaming_with_compression_uncompressed() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        Some(500),
        Some(ParquetCompression::Uncompressed),
        None,
    ) {
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
fn cars_to_parquet_with_compression_snappy() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Snappy),
        None,
    ) {
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
fn cars_to_parquet_with_streaming_with_compression_snappy() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        Some(500),
        Some(ParquetCompression::Snappy),
        None,
    ) {
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
fn cars_to_parquet_with_compression_snappy_with_compression_level() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Snappy),
        Some(5),
    ) {
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Compression level is not required for compression=snappy, ignoring value of --compression-level"));

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_with_compression_lz4raw() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Lz4Raw),
        None,
    ) {
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
fn cars_to_parquet_with_streaming_with_compression_lz4raw() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        Some(500),
        Some(ParquetCompression::Lz4Raw),
        None,
    ) {
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
fn cars_to_parquet_with_compression_lz4raw_with_compression_level() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Lz4Raw),
        Some(5),
    ) {
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Compression level is not required for compression=lz4-raw, ignoring value of --compression-level"));

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_with_compression_gzip_level_5() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Gzip),
        Some(5),
    ) {
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
fn cars_to_parquet_with_streaming_with_compression_gzip_level_5() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        Some(500),
        Some(ParquetCompression::Gzip),
        Some(5),
    ) {
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
fn cars_to_parquet_with_compression_gzip_level_10() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Gzip),
        Some(10),
    ) {
        cmd.assert().failure().stderr(
            predicate::str::is_match(r#"^Stopping with error: The compression level of \d+ is not a valid level for gzip compression. Instead, please use values between 0-9.\n?"#)
                .unwrap(),
        );

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_with_compression_gzip_level_55() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Gzip),
        Some(55),
    ) {
        cmd.assert().failure().stderr(
            predicate::str::is_match(r#"^error: invalid value '\d+' for '--compression-level <COMPRESSION_LEVEL>': \d+ is not in 0..=22\n?"#)
                .unwrap(),
        );

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_with_compression_brotli_level_5() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Brotli),
        Some(5),
    ) {
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
fn cars_to_parquet_with_streaming_with_compression_brotli_level_5() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        Some(500),
        Some(ParquetCompression::Brotli),
        Some(5),
    ) {
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
fn cars_to_parquet_with_compression_brotli_level_12() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Brotli),
        Some(12),
    ) {
        cmd.assert().failure().stderr(
            predicate::str::is_match(r#"^Stopping with error: The compression level of \d+ is not a valid level for brotli compression. Instead, please use values between 0-11.\n?"#)
                .unwrap(),
        );

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_with_compression_brotli_level_55() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Brotli),
        Some(55),
    ) {
        cmd.assert().failure().stderr(
            predicate::str::is_match(r#"^error: invalid value '\d+' for '--compression-level <COMPRESSION_LEVEL>': \d+ is not in 0..=22\n?"#)
                .unwrap(),
        );

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_with_compression_zstd_level_5() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Zstd),
        Some(5),
    ) {
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
fn cars_to_parquet_with_sreaming_with_compression_zstd_level_5() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        Some(500),
        Some(ParquetCompression::Zstd),
        Some(5),
    ) {
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
fn cars_to_parquet_with_compression_zstd_level_12() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Zstd),
        Some(12),
    ) {
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
fn cars_to_parquet_with_compression_zstd_level_55() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Zstd),
        Some(55),
    ) {
        cmd.assert().failure().stderr(
            predicate::str::is_match(r#"^error: invalid value '\d+' for '--compression-level <COMPRESSION_LEVEL>': \d+ is not in 0..=22\n?"#)
                .unwrap(),
        );

        tempfile.close().unwrap();
    }
}
