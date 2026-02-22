//! Output configuration for writing Arrow data to various formats.
//!
//! [`WriteConfig`] captures the output file path, format, compression settings,
//! and overwrite behavior, decoupled from input path validation ([`ReadStatPath`]).

use std::path::{Path, PathBuf};

#[cfg(feature = "parquet")]
use parquet::basic::{BrotliLevel, Compression as ParquetCompressionCodec, GzipLevel, ZstdLevel};

use crate::err::ReadStatError;

/// Output file format for data conversion.
///
/// All variants are always present regardless of enabled features.
/// Attempting to use a format whose feature is not enabled will
/// result in a compile-time error in the writer code.
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub enum OutFormat {
    /// Comma-separated values.
    csv,
    /// Feather (Arrow IPC) format.
    feather,
    /// Newline-delimited JSON.
    ndjson,
    /// Apache Parquet columnar format.
    parquet,
}

impl std::fmt::Display for OutFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::csv => f.write_str("csv"),
            Self::feather => f.write_str("feather"),
            Self::ndjson => f.write_str("ndjson"),
            Self::parquet => f.write_str("parquet"),
        }
    }
}

/// Parquet compression algorithm.
#[derive(Debug, Clone, Copy)]
pub enum ParquetCompression {
    /// No compression.
    Uncompressed,
    /// Snappy compression (fast, moderate ratio).
    Snappy,
    /// Gzip compression (levels 0-9).
    Gzip,
    /// LZ4 raw compression.
    Lz4Raw,
    /// Brotli compression (levels 0-11).
    Brotli,
    /// Zstandard compression (levels 0-22).
    Zstd,
}

impl std::fmt::Display for ParquetCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uncompressed => f.write_str("uncompressed"),
            Self::Snappy => f.write_str("snappy"),
            Self::Gzip => f.write_str("gzip"),
            Self::Lz4Raw => f.write_str("lz4-raw"),
            Self::Brotli => f.write_str("brotli"),
            Self::Zstd => f.write_str("zstd"),
        }
    }
}

/// Output configuration for writing Arrow data.
///
/// Captures the output file path, format, compression settings, and overwrite
/// behavior. Created separately from [`ReadStatPath`](crate::ReadStatPath),
/// which handles only input path validation.
#[derive(Debug, Clone)]
pub struct WriteConfig {
    /// Optional output file path.
    pub out_path: Option<PathBuf>,
    /// Output format (defaults to CSV).
    pub format: OutFormat,
    /// Whether to overwrite an existing output file.
    pub overwrite: bool,
    /// Optional Parquet compression algorithm.
    pub compression: Option<ParquetCompression>,
    /// Optional Parquet compression level.
    pub compression_level: Option<u32>,
}

impl WriteConfig {
    /// Creates a new `WriteConfig` after validating the output path, format,
    /// and compression settings.
    pub fn new(
        out_path: Option<PathBuf>,
        format: Option<OutFormat>,
        overwrite: bool,
        compression: Option<ParquetCompression>,
        compression_level: Option<u32>,
    ) -> Result<Self, ReadStatError> {
        let f = Self::validate_format(format)?;
        let op = Self::validate_out_path(out_path, overwrite)?;
        let op = match op {
            None => op,
            Some(op) => Self::validate_out_extension(&op, f)?,
        };
        let cl = match compression {
            None => match compression_level {
                None => None,
                Some(_) => {
                    println!("Ignoring value of --compression-level as --compression was not set");
                    None
                }
            },
            Some(pc) => Self::validate_compression_level(pc, compression_level)?,
        };

        Ok(Self {
            out_path: op,
            format: f,
            overwrite,
            compression,
            compression_level: cl,
        })
    }

    fn validate_format(format: Option<OutFormat>) -> Result<OutFormat, ReadStatError> {
        Ok(format.unwrap_or(OutFormat::csv))
    }

    /// Validates the output file extension matches the format.
    fn validate_out_extension(
        path: &Path,
        format: OutFormat,
    ) -> Result<Option<PathBuf>, ReadStatError> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(ReadStatError::Other(format!(
                    "File {} does not have an extension! Expecting extension {}.",
                    path.to_string_lossy(),
                    format
                ))),
                |e| match format {
                    OutFormat::csv
                    | OutFormat::ndjson
                    | OutFormat::feather
                    | OutFormat::parquet => {
                        if e == format.to_string() {
                            Ok(Some(path.to_owned()))
                        } else {
                            Err(ReadStatError::Other(format!(
                                "Expecting extension {}. Instead, file {} has extension {}.",
                                format,
                                path.to_string_lossy(),
                                e
                            )))
                        }
                    }
                },
            )
    }

    /// Validates the output path exists and handles overwrite logic.
    fn validate_out_path(
        path: Option<PathBuf>,
        overwrite: bool,
    ) -> Result<Option<PathBuf>, ReadStatError> {
        match path {
            None => Ok(None),
            Some(p) => {
                let abs_path = std::path::absolute(&p)
                    .map_err(|e| ReadStatError::Other(format!("Failed to resolve path: {e}")))?;

                match abs_path.parent() {
                    None => Err(ReadStatError::Other(format!(
                        "The parent directory of the value of the parameter --output ({}) does not exist",
                        abs_path.to_string_lossy()
                    ))),
                    Some(parent) => {
                        if parent.exists() {
                            if abs_path.exists() {
                                if overwrite {
                                    println!(
                                        "The file {} will be overwritten!",
                                        abs_path.to_string_lossy()
                                    );
                                    Ok(Some(abs_path))
                                } else {
                                    Err(ReadStatError::Other(format!(
                                        "The output file - {} - already exists! To overwrite the file, utilize the --overwrite parameter",
                                        abs_path.to_string_lossy()
                                    )))
                                }
                            } else {
                                Ok(Some(abs_path))
                            }
                        } else {
                            Err(ReadStatError::Other(format!(
                                "The parent directory of the value of the parameter --output ({}) does not exist",
                                parent.to_string_lossy()
                            )))
                        }
                    }
                }
            }
        }
    }

    /// Validates compression level is valid for the given compression algorithm.
    fn validate_compression_level(
        compression: ParquetCompression,
        compression_level: Option<u32>,
    ) -> Result<Option<u32>, ReadStatError> {
        let (name, max_level): (&str, Option<u32>) = match compression {
            ParquetCompression::Uncompressed => ("uncompressed", None),
            ParquetCompression::Snappy => ("snappy", None),
            ParquetCompression::Lz4Raw => ("lz4-raw", None),
            ParquetCompression::Gzip => ("gzip", Some(9)),
            ParquetCompression::Brotli => ("brotli", Some(11)),
            ParquetCompression::Zstd => ("zstd", Some(22)),
        };

        match (max_level, compression_level) {
            (None, None) => Ok(None),
            (None, Some(_)) => {
                println!(
                    "Compression level is not required for compression={name}, ignoring value of --compression-level"
                );
                Ok(None)
            }
            (Some(_), None) => Ok(None),
            (Some(max), Some(c)) => {
                if c <= max {
                    Ok(Some(c))
                } else {
                    Err(ReadStatError::Other(format!(
                        "The compression level of {c} is not a valid level for {name} compression. \
                         Instead, please use values between 0-{max}."
                    )))
                }
            }
        }
    }
}

/// Resolves [`ParquetCompression`] and an optional level into a Parquet compression codec.
///
/// Defaults to Snappy when no compression is specified.
#[cfg(feature = "parquet")]
pub fn resolve_parquet_compression(
    compression: Option<ParquetCompression>,
    compression_level: Option<u32>,
) -> Result<ParquetCompressionCodec, ReadStatError> {
    let codec = match compression {
        Some(ParquetCompression::Uncompressed) => ParquetCompressionCodec::UNCOMPRESSED,
        Some(ParquetCompression::Snappy) => ParquetCompressionCodec::SNAPPY,
        Some(ParquetCompression::Gzip) => {
            if let Some(level) = compression_level {
                let gzip_level = GzipLevel::try_new(level).map_err(|e| {
                    ReadStatError::Other(format!("Invalid Gzip compression level: {e}"))
                })?;
                ParquetCompressionCodec::GZIP(gzip_level)
            } else {
                ParquetCompressionCodec::GZIP(GzipLevel::default())
            }
        }
        Some(ParquetCompression::Lz4Raw) => ParquetCompressionCodec::LZ4_RAW,
        Some(ParquetCompression::Brotli) => {
            if let Some(level) = compression_level {
                let brotli_level = BrotliLevel::try_new(level).map_err(|e| {
                    ReadStatError::Other(format!("Invalid Brotli compression level: {e}"))
                })?;
                ParquetCompressionCodec::BROTLI(brotli_level)
            } else {
                ParquetCompressionCodec::BROTLI(BrotliLevel::default())
            }
        }
        Some(ParquetCompression::Zstd) => {
            if let Some(level) = compression_level {
                let zstd_level = ZstdLevel::try_new(level as i32).map_err(|e| {
                    ReadStatError::Other(format!("Invalid Zstd compression level: {e}"))
                })?;
                ParquetCompressionCodec::ZSTD(zstd_level)
            } else {
                ParquetCompressionCodec::ZSTD(ZstdLevel::default())
            }
        }
        None => ParquetCompressionCodec::SNAPPY,
    };
    Ok(codec)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- validate_format ---

    #[test]
    fn format_none_defaults_to_csv() {
        let f = WriteConfig::validate_format(None).unwrap();
        assert!(matches!(f, OutFormat::csv));
    }

    #[test]
    fn format_some_passes_through() {
        let f = WriteConfig::validate_format(Some(OutFormat::parquet)).unwrap();
        assert!(matches!(f, OutFormat::parquet));
    }

    // --- validate_out_extension ---

    #[test]
    fn valid_csv_out_extension() {
        let path = Path::new("/some/output.csv");
        let result = WriteConfig::validate_out_extension(path, OutFormat::csv).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_parquet_out_extension() {
        let path = Path::new("/some/output.parquet");
        let result = WriteConfig::validate_out_extension(path, OutFormat::parquet).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_feather_out_extension() {
        let path = Path::new("/some/output.feather");
        let result = WriteConfig::validate_out_extension(path, OutFormat::feather).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_ndjson_out_extension() {
        let path = Path::new("/some/output.ndjson");
        let result = WriteConfig::validate_out_extension(path, OutFormat::ndjson).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn mismatched_out_extension() {
        let path = Path::new("/some/output.csv");
        assert!(WriteConfig::validate_out_extension(path, OutFormat::parquet).is_err());
    }

    #[test]
    fn no_out_extension() {
        let path = Path::new("/some/output");
        assert!(WriteConfig::validate_out_extension(path, OutFormat::csv).is_err());
    }

    // --- validate_compression_level ---

    #[test]
    fn uncompressed_ignores_level() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Uncompressed, Some(5))
                .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn snappy_ignores_level() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Snappy, Some(5)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn lz4raw_ignores_level() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Lz4Raw, Some(5)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn gzip_valid_level() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Gzip, Some(5)).unwrap();
        assert_eq!(result, Some(5));
    }

    #[test]
    fn gzip_max_valid_level() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Gzip, Some(9)).unwrap();
        assert_eq!(result, Some(9));
    }

    #[test]
    fn gzip_invalid_level() {
        assert!(
            WriteConfig::validate_compression_level(ParquetCompression::Gzip, Some(10),).is_err()
        );
    }

    #[test]
    fn brotli_valid_level() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Brotli, Some(11)).unwrap();
        assert_eq!(result, Some(11));
    }

    #[test]
    fn brotli_invalid_level() {
        assert!(
            WriteConfig::validate_compression_level(ParquetCompression::Brotli, Some(12),).is_err()
        );
    }

    #[test]
    fn zstd_valid_level() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Zstd, Some(22)).unwrap();
        assert_eq!(result, Some(22));
    }

    #[test]
    fn zstd_invalid_level() {
        assert!(
            WriteConfig::validate_compression_level(ParquetCompression::Zstd, Some(23),).is_err()
        );
    }

    #[test]
    fn no_level_passes_through() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Gzip, None).unwrap();
        assert_eq!(result, None);
    }

    // --- validate_out_path ---

    #[test]
    fn validate_out_path_none() {
        assert!(
            WriteConfig::validate_out_path(None, false)
                .unwrap()
                .is_none()
        );
    }
}
