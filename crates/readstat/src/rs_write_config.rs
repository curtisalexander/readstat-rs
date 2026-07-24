//! Output configuration for writing Arrow data to various formats.
//!
//! [`WriteConfig`] captures the output file path, format, compression settings,
//! and overwrite behavior, decoupled from input path validation
//! ([`crate::ReadStatPath`]).

use std::path::{Path, PathBuf};

#[cfg(feature = "parquet")]
use parquet::basic::{BrotliLevel, Compression as ParquetCompressionCodec, GzipLevel, ZstdLevel};

use crate::err::ReadStatError;

/// Output file format for data conversion.
///
/// All variants are always present regardless of which writer features are
/// enabled. Attempting to *write* a format whose feature is disabled returns a
/// runtime [`ReadStatError`] from the writer rather than failing to compile.
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
/// Fields are private and validated by the builder methods; read them via
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
    /// Infers the format from an output path and returns a validated config.
    ///
    /// # Errors
    ///
    /// Returns [`ReadStatError`] if the path has an unknown extension or fails
    /// normal output-path validation.
    pub fn from_output(path: impl Into<PathBuf>) -> Result<Self, ReadStatError> {
        let path = path.into();
        let format = match path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("csv") => OutFormat::Csv,
            Some("feather") => OutFormat::Feather,
            Some("ndjson") => OutFormat::Ndjson,
            Some("parquet") => OutFormat::Parquet,
            _ => {
                return Err(ReadStatError::InvalidOutputConfig(format!(
                    "cannot infer output format from '{}'; supported extensions are .csv, .feather, .ndjson, and .parquet",
                    path.display()
                )));
            }
        };
        Self::new(format).output(path)
    }

    /// Starts a validated configuration for `format`. CSV defaults to stdout;
    /// other formats require [`output`](Self::output) before writer creation.
    #[must_use]
    pub const fn new(format: OutFormat) -> Self {
        Self {
            out_path: None,
            format,
            overwrite: false,
            compression: None,
            compression_level: None,
        }
    }

    /// Sets and validates the output path.
    pub fn output(mut self, path: impl Into<PathBuf>) -> Result<Self, ReadStatError> {
        let path = Self::validate_out_path(Some(path.into()))?.expect("path was supplied");
        self.out_path = Self::validate_out_extension(&path, self.format)?;
        Ok(self)
    }

    /// Controls atomic publication when the writer is successfully finished.
    /// When false, publication fails rather than replacing a destination that
    /// appeared while the output was being written.
    #[must_use]
    pub const fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Sets and validates Parquet compression.
    pub fn compression(
        mut self,
        codec: ParquetCompression,
        level: Option<u32>,
    ) -> Result<Self, ReadStatError> {
        if !matches!(self.format, OutFormat::Parquet) {
            return Err(ReadStatError::InvalidCompressionConfig(
                "compression is only supported for Parquet".into(),
            ));
        }
        self.compression_level = Self::validate_compression_level(codec, level)?;
        self.compression = Some(codec);
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<(), ReadStatError> {
        if self.out_path.is_none() && !matches!(self.format, OutFormat::Csv) {
            return Err(ReadStatError::InvalidOutputConfig(
                "only CSV may be written to stdout".into(),
            ));
        }
        Ok(())
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
    pub const fn is_overwrite(&self) -> bool {
        self.overwrite
    }

    /// The configured Parquet compression codec, if any.
    #[must_use]
    pub const fn compression_codec(&self) -> Option<ParquetCompression> {
        self.compression
    }

    /// The configured Parquet compression level, if any.
    #[must_use]
    pub const fn compression_level(&self) -> Option<u32> {
        self.compression_level
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
    fn validate_out_path(path: Option<PathBuf>) -> Result<Option<PathBuf>, ReadStatError> {
        match path {
            None => Ok(None),
            Some(p) => {
                let abs_path = std::path::absolute(&p)
                    .map_err(|e| ReadStatError::Other(format!("Failed to resolve path: {e}")))?;

                match abs_path.parent() {
                    None => Err(ReadStatError::OutputParentMissing(abs_path.clone())),
                    Some(parent) => {
                        if parent.exists() {
                            Ok(Some(abs_path))
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
            (None, Some(_)) => Err(ReadStatError::Other(format!(
                "compression codec {name} does not support a level"
            ))),
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

/// Creates a uniquely named staging file beside the destination. Keeping the
/// staging file in the same directory makes the eventual rename a same-filesystem
/// operation.
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
pub(crate) fn open_output(config: &WriteConfig) -> Result<(std::fs::File, PathBuf), ReadStatError> {
    create_staging_file(
        config
            .out_path
            .as_ref()
            .ok_or_else(|| ReadStatError::Other("stdout has no output file".into()))?,
    )
}

#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
pub(crate) fn create_staging_file(path: &Path) -> Result<(std::fs::File, PathBuf), ReadStatError> {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_STAGING_ID: AtomicU64 = AtomicU64::new(0);
    let parent = path.parent().expect("validated output has a parent");
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("output");
    for _ in 0..100 {
        let id = NEXT_STAGING_ID.fetch_add(1, Ordering::Relaxed);
        let staging = parent.join(format!(".{name}.readstat-{}-{id}.tmp", std::process::id()));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
        {
            Ok(file) => return Ok((file, staging)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(ReadStatError::Other(format!(
        "could not create a unique staging file for {}",
        path.display()
    )))
}

#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
pub(crate) fn publish_staging(
    staging: &Path,
    destination: &Path,
    overwrite: bool,
) -> Result<(), ReadStatError> {
    if overwrite {
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            use windows_sys::Win32::Storage::FileSystem::{
                MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
            };

            let staging = staging
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>();
            let destination = destination
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>();
            // SAFETY: both buffers are NUL-terminated and remain alive for the
            // duration of the call. Same-directory staging keeps this on one volume.
            let moved = unsafe {
                MoveFileExW(
                    staging.as_ptr(),
                    destination.as_ptr(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            };
            if moved == 0 {
                Err(std::io::Error::last_os_error().into())
            } else {
                Ok(())
            }
        }
        #[cfg(not(windows))]
        {
            std::fs::rename(staging, destination).map_err(Into::into)
        }
    } else {
        std::fs::hard_link(staging, destination).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                ReadStatError::OutputFileExists(destination.to_owned())
            } else {
                error.into()
            }
        })?;
        std::fs::remove_file(staging)?;
        Ok(())
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

    #[test]
    fn from_output_infers_format_case_insensitively() {
        let config = WriteConfig::from_output("result.PARQUET").unwrap();
        assert!(matches!(config.format(), OutFormat::Parquet));
    }

    #[test]
    fn from_output_rejects_unknown_extension() {
        assert!(WriteConfig::from_output("result.unknown").is_err());
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
    fn uncompressed_rejects_level() {
        let result =
            WriteConfig::validate_compression_level(ParquetCompression::Uncompressed, Some(5));
        assert!(result.is_err());
    }

    #[test]
    fn snappy_rejects_level() {
        let result = WriteConfig::validate_compression_level(ParquetCompression::Snappy, Some(5));
        assert!(result.is_err());
    }

    #[test]
    fn lz4raw_rejects_level() {
        let result = WriteConfig::validate_compression_level(ParquetCompression::Lz4Raw, Some(5));
        assert!(result.is_err());
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
        assert!(WriteConfig::validate_out_path(None).unwrap().is_none());
    }
}
