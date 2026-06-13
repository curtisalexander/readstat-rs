//! Output configuration for writing Arrow data to various formats.
//!
//! [`WriteConfig`] captures the output file path, format, compression settings,
//! and overwrite behavior, decoupled from input path validation ([`ReadStatPath`]).

use std::path::{Path, PathBuf};

#[cfg(feature = "parquet")]
use parquet::basic::{BrotliLevel, Compression as ParquetCompressionCodec, GzipLevel, ZstdLevel};

use log::warn;

use crate::err::ReadStatError;

/// Output file format for data conversion.
///
/// All variants are always present regardless of which writer features are
/// enabled. Attempting to *write* a format whose feature is disabled returns a
/// runtime [`ReadStatError`](crate::ReadStatError) from the writer rather than
/// failing to compile.
///
/// This enum is `#[non_exhaustive]`: new format variants may be added in
/// minor releases. Match with a wildcard arm to remain forward-compatible.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum OutFormat {
    /// Comma-separated values.
    Csv,
    /// Feather (Arrow IPC) format.
    Feather,
    /// Newline-delimited JSON.
    Ndjson,
    /// Apache Parquet columnar format.
    Parquet,
}

impl std::fmt::Display for OutFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Csv => f.write_str("csv"),
            Self::Feather => f.write_str("feather"),
            Self::Ndjson => f.write_str("ndjson"),
            Self::Parquet => f.write_str("parquet"),
        }
    }
}

impl std::str::FromStr for OutFormat {
    type Err = ReadStatError;

    /// Parses a format name (case-insensitive) into an [`OutFormat`].
    ///
    /// Accepted values: `"csv"`, `"feather"`, `"ndjson"`, `"parquet"`.
    ///
    /// # Errors
    ///
    /// Returns [`ReadStatError::UnknownFormat`] for unrecognized format strings.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "feather" => Ok(Self::Feather),
            "ndjson" => Ok(Self::Ndjson),
            "parquet" => Ok(Self::Parquet),
            _ => Err(ReadStatError::UnknownFormat(s.to_string())),
        }
    }
}

/// Parquet compression algorithm.
///
/// This enum is `#[non_exhaustive]`: new codec variants may be added in
/// minor releases. Match with a wildcard arm to remain forward-compatible.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl std::str::FromStr for ParquetCompression {
    type Err = ReadStatError;

    /// Parses a codec name (case-insensitive) into a [`ParquetCompression`].
    ///
    /// Accepted values: `"uncompressed"`, `"snappy"`, `"gzip"`, `"lz4-raw"`
    /// (or `"lz4raw"`), `"brotli"`, `"zstd"`.
    ///
    /// # Errors
    ///
    /// Returns [`ReadStatError::UnknownFormat`] for unrecognized codec names.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "uncompressed" => Ok(Self::Uncompressed),
            "snappy" => Ok(Self::Snappy),
            "gzip" => Ok(Self::Gzip),
            "lz4-raw" | "lz4raw" => Ok(Self::Lz4Raw),
            "brotli" => Ok(Self::Brotli),
            "zstd" => Ok(Self::Zstd),
            _ => Err(ReadStatError::UnknownFormat(s.to_string())),
        }
    }
}

/// Output configuration for writing Arrow data.
///
/// Captures the output file path, format, compression settings, and overwrite
/// behavior. Created separately from [`ReadStatPath`](crate::ReadStatPath),
/// which handles only input path validation.
///
/// Fields are private and validated by [`new`](WriteConfig::new); read them via
/// the accessor methods. This prevents constructing a config that bypasses path,
/// extension, and compression-level validation.
#[derive(Debug, Clone)]
pub struct WriteConfig {
    /// Optional output file path.
    pub(crate) out_path: Option<PathBuf>,
    /// Output format (defaults to CSV).
    pub(crate) format: OutFormat,
    /// Whether to overwrite an existing output file.
    pub(crate) overwrite: bool,
    /// Optional Parquet compression algorithm.
    pub(crate) compression: Option<ParquetCompression>,
    /// Optional Parquet compression level.
    pub(crate) compression_level: Option<u32>,
}

impl WriteConfig {
    /// Creates a new `WriteConfig` after validating the output path, format,
    /// and compression settings.
    ///
    /// # Errors
    ///
    /// Returns [`ReadStatError`] if the output path, format, or compression settings
    /// are invalid.
    pub fn new(
        out_path: Option<PathBuf>,
        format: Option<OutFormat>,
        overwrite: bool,
        compression: Option<ParquetCompression>,
        compression_level: Option<u32>,
    ) -> Result<Self, ReadStatError> {
        let f = Self::validate_format(format);
        let op = Self::validate_out_path(out_path, overwrite)?;
        let op = if let Some(op) = op {
            Self::validate_out_extension(&op, f)?
        } else {
            None
        };
        let cl = match compression {
            None => {
                if compression_level.is_some() {
                    warn!("Ignoring value of --compression-level as --compression was not set");
                }
                None
            }
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

    /// The validated output path, or `None` to write CSV to stdout.
    #[must_use]
    pub fn out_path(&self) -> Option<&Path> {
        self.out_path.as_deref()
    }

    /// The output format.
    #[must_use]
    pub const fn format(&self) -> OutFormat {
        self.format
    }

    /// Whether an existing output file may be overwritten.
    #[must_use]
    pub const fn overwrite(&self) -> bool {
        self.overwrite
    }

    /// The configured Parquet compression codec, if any.
    #[must_use]
    pub const fn compression(&self) -> Option<ParquetCompression> {
        self.compression
    }

    /// The configured Parquet compression level, if any.
    #[must_use]
    pub const fn compression_level(&self) -> Option<u32> {
        self.compression_level
    }

    fn validate_format(format: Option<OutFormat>) -> OutFormat {
        format.unwrap_or(OutFormat::Csv)
    }

    /// Validates the output file extension matches the format.
    fn validate_out_extension(
        path: &Path,
        format: OutFormat,
    ) -> Result<Option<PathBuf>, ReadStatError> {
        match path.extension().and_then(|e| e.to_str()) {
            Some(e) if e.eq_ignore_ascii_case(&format.to_string()) => Ok(Some(path.to_owned())),
            _ => Err(ReadStatError::OutputExtensionMismatch {
                path: path.to_owned(),
                expected: format.to_string(),
            }),
        }
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
                    None => Err(ReadStatError::OutputParentMissing(abs_path.clone())),
                    Some(parent) => {
                        if parent.exists() {
                            if abs_path.exists() {
                                if overwrite {
                                    warn!(
                                        "The file {} will be overwritten!",
                                        abs_path.to_string_lossy()
                                    );
                                    Ok(Some(abs_path))
                                } else {
                                    Err(ReadStatError::OutputFileExists(abs_path))
                                }
                            } else {
                                Ok(Some(abs_path))
                            }
                        } else {
                            Err(ReadStatError::OutputParentMissing(parent.to_path_buf()))
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
            (None | Some(_), None) => Ok(None),
            (None, Some(_)) => {
                warn!(
                    "Compression level is not required for compression={name}, ignoring value of --compression-level"
                );
                Ok(None)
            }
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
#[allow(clippy::cast_possible_wrap)]
pub fn resolve_parquet_compression(
    compression: Option<ParquetCompression>,
    compression_level: Option<u32>,
) -> Result<ParquetCompressionCodec, ReadStatError> {
    let codec = match compression {
        Some(ParquetCompression::Uncompressed) => ParquetCompressionCodec::UNCOMPRESSED,
        Some(ParquetCompression::Snappy) | None => ParquetCompressionCodec::SNAPPY,
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
    };
    Ok(codec)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- validate_format ---

    #[test]
    fn format_none_defaults_to_csv() {
        let f = WriteConfig::validate_format(None);
        assert!(matches!(f, OutFormat::Csv));
    }

    #[test]
    fn format_some_passes_through() {
        let f = WriteConfig::validate_format(Some(OutFormat::Parquet));
        assert!(matches!(f, OutFormat::Parquet));
    }

    // --- validate_out_extension ---

    #[test]
    fn valid_csv_out_extension() {
        let path = Path::new("/some/output.csv");
        let result = WriteConfig::validate_out_extension(path, OutFormat::Csv).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_parquet_out_extension() {
        let path = Path::new("/some/output.parquet");
        let result = WriteConfig::validate_out_extension(path, OutFormat::Parquet).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_feather_out_extension() {
        let path = Path::new("/some/output.feather");
        let result = WriteConfig::validate_out_extension(path, OutFormat::Feather).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_ndjson_out_extension() {
        let path = Path::new("/some/output.ndjson");
        let result = WriteConfig::validate_out_extension(path, OutFormat::Ndjson).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn mismatched_out_extension() {
        let path = Path::new("/some/output.csv");
        assert!(WriteConfig::validate_out_extension(path, OutFormat::Parquet).is_err());
    }

    #[test]
    fn no_out_extension() {
        let path = Path::new("/some/output");
        assert!(WriteConfig::validate_out_extension(path, OutFormat::Csv).is_err());
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
