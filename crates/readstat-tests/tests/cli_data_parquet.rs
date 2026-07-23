#![allow(clippy::float_cmp)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]

use ::predicates::prelude::*;
use assert_cmd::Command;
use assert_fs::NamedTempFile;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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
            .manifest_path("../readstat-cli/Cargo.toml")
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
    let (tempfile, overwrite) = match overwrite {
        OverwriteOption::Overwrite(tempfile) => (tempfile, true),
        OverwriteOption::DoNotOverwrite => (
            NamedTempFile::new(format!("{base_file_name}.parquet"))?,
            false,
        ),
    };

    cmd.arg("convert")
        .arg(format!("tests/data/{base_file_name}.sas7bdat"))
        .args(["--output", tempfile.as_os_str().to_str().unwrap()]);

    if let Some(rows) = rows_to_stream {
        cmd.args(["--stream-rows", &rows.to_string()]);
    }
    if let Some(compression) = compression {
        let compression = match compression {
            ParquetCompression::Uncompressed => "uncompressed",
            ParquetCompression::Snappy => "snappy",
            ParquetCompression::Gzip => "gzip",
            ParquetCompression::Brotli => "brotli",
            ParquetCompression::Lz4Raw => "lz4-raw",
            ParquetCompression::Zstd => "zstd",
            _ => unreachable!("unsupported Parquet compression in CLI test"),
        };
        cmd.args(["--compression", compression]);
    }
    if let Some(level) = compression_level {
        cmd.args(["--compression-level", &level.to_string()]);
    }
    if overwrite {
        cmd.arg("--overwrite");
    }

    Ok((cmd, tempfile))
}

fn parquet_shape(path: PathBuf) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let num_rows = builder.metadata().file_metadata().num_rows() as usize;
    let num_cols = builder.schema().fields().len();
    Ok((num_rows, num_cols))
}

fn assert_conversion_success(cmd: &mut Command) {
    cmd.assert()
        .success()
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains(
            "In total, wrote 1,081 rows from file cars.sas7bdat into cars.parquet",
        ));
}

#[test]
fn cars_to_parquet() {
    if let Ok((mut cmd, tempfile)) =
        cli_data_to_parquet("cars", OverwriteOption::DoNotOverwrite, None, None, None)
    {
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        // next do not stream
        let (mut cmd, tempfile) = cli_data_to_parquet(
            "cars",
            OverwriteOption::Overwrite(tempfile),
            None,
            None,
            None,
        )
        .unwrap();

        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

        assert_eq!(height, 1081);
        assert_eq!(width, 13);

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_rejects_compression_level_for_uncompressed() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Uncompressed),
        Some(5),
    ) {
        cmd.assert()
            .failure()
            .stdout(predicate::str::is_empty())
            .stderr(predicate::str::contains(
                "Stopping with error: compression codec uncompressed does not support a level",
            ));

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

        assert_eq!(height, 1081);
        assert_eq!(width, 13);

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_rejects_compression_level_for_snappy() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Snappy),
        Some(5),
    ) {
        cmd.assert()
            .failure()
            .stdout(predicate::str::is_empty())
            .stderr(predicate::str::contains(
                "Stopping with error: compression codec snappy does not support a level",
            ));

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

        assert_eq!(height, 1081);
        assert_eq!(width, 13);

        tempfile.close().unwrap();
    }
}

#[test]
fn cars_to_parquet_rejects_compression_level_for_lz4raw() {
    if let Ok((mut cmd, tempfile)) = cli_data_to_parquet(
        "cars",
        OverwriteOption::DoNotOverwrite,
        None,
        Some(ParquetCompression::Lz4Raw),
        Some(5),
    ) {
        cmd.assert()
            .failure()
            .stdout(predicate::str::is_empty())
            .stderr(predicate::str::contains(
                "Stopping with error: compression codec lz4-raw does not support a level",
            ));

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
            predicate::str::is_match(r"^Stopping with error: The compression level of \d+ is not a valid level for gzip compression. Instead, please use values between 0-9.\n?")
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
            predicate::str::is_match(r"^error: invalid value '\d+' for '--compression-level <COMPRESSION_LEVEL>': \d+ is not in 0..=22\n?")
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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
            predicate::str::is_match(r"^Stopping with error: The compression level of \d+ is not a valid level for brotli compression. Instead, please use values between 0-11.\n?")
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
            predicate::str::is_match(r"^error: invalid value '\d+' for '--compression-level <COMPRESSION_LEVEL>': \d+ is not in 0..=22\n?")
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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
        assert_conversion_success(&mut cmd);

        let (height, width) = parquet_shape(tempfile.to_path_buf()).unwrap();

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
            predicate::str::is_match(r"^error: invalid value '\d+' for '--compression-level <COMPRESSION_LEVEL>': \d+ is not in 0..=22\n?")
                .unwrap(),
        );

        tempfile.close().unwrap();
    }
}
