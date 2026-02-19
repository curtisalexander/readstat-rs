//! Path validation and I/O configuration for SAS file processing.
//!
//! [`ReadStatPath`] validates the input `.sas7bdat` file path, output path, file format,
//! and Parquet compression settings. It also converts the path to a C-compatible string
//! for the FFI layer.

use colored::Colorize;
use path_abs::{PathAbs, PathInfo};
use std::{
    ffi::CString,
    path::{Path, PathBuf},
};

use crate::err::ReadStatError;
use crate::OutFormat;
use crate::ParquetCompression;

const IN_EXTENSIONS: &[&str] = &["sas7bdat", "sas7bcat"];

/// Validated file path and I/O configuration for SAS file processing.
///
/// Encapsulates the input `.sas7bdat` path (validated to exist with correct extension),
/// optional output path, output format, and Parquet compression settings. The input path
/// is also converted to a [`CString`] for passing to the ReadStat C library.
#[derive(Debug, Clone)]
pub struct ReadStatPath {
    /// Absolute path to the input `.sas7bdat` file.
    pub path: PathBuf,
    /// File extension of the input file (e.g. `"sas7bdat"`).
    pub extension: String,
    /// Input path as a C-compatible string for FFI.
    pub cstring_path: CString,
    /// Optional output file path.
    pub out_path: Option<PathBuf>,
    /// Output format (defaults to CSV).
    pub format: OutFormat,
    /// Whether to overwrite an existing output file.
    pub overwrite: bool,
    /// Whether writing is disabled (metadata-only mode).
    pub no_write: bool,
    /// Optional Parquet compression algorithm.
    pub compression: Option<ParquetCompression>,
    /// Optional Parquet compression level.
    pub compression_level: Option<u32>,
}

impl ReadStatPath {
    /// Creates a new `ReadStatPath` after validating the input path, output path,
    /// format, and compression settings.
    pub fn new(
        path: PathBuf,
        out_path: Option<PathBuf>,
        format: Option<OutFormat>,
        overwrite: bool,
        no_write: bool,
        compression: Option<ParquetCompression>,
        compression_level: Option<u32>,
    ) -> Result<Self, ReadStatError> {
        let p = Self::validate_path(path)?;
        let ext = Self::validate_in_extension(&p)?;
        let csp = Self::path_to_cstring(&p)?;
        let f = Self::validate_format(format)?;
        let op: Option<PathBuf> = Self::validate_out_path(out_path, overwrite)?;
        let op = match op {
            None => op,
            Some(op) => Self::validate_out_extension(&op, f)?,
        };
        let cl: Option<u32> = match compression {
            None => match compression_level {
                None => None,
                Some(_) => {
                    println!(
                        "Ignoring value of {} as {} was not set",
                        String::from("--compression-level").bright_cyan(),
                        String::from("--compression").bright_cyan()
                    );
                    None
                }
            },
            Some(pc) => Self::validate_compression_level(pc, compression_level)?,
        };

        Ok(Self {
            path: p,
            extension: ext,
            cstring_path: csp,
            out_path: op,
            format: f,
            overwrite,
            no_write,
            compression,
            compression_level: cl,
        })
    }

    /// Converts a file path to a [`CString`] for FFI. Uses raw bytes on Unix.
    #[cfg(unix)]
    pub fn path_to_cstring(path: &PathBuf) -> Result<CString, ReadStatError> {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        Ok(CString::new(bytes)?)
    }

    /// Converts a file path to a [`CString`] for FFI. Uses UTF-8 on non-Unix platforms.
    #[cfg(not(unix))]
    pub fn path_to_cstring(path: &Path) -> Result<CString, ReadStatError> {
        let rust_str = path
            .as_os_str()
            .to_str()
            .ok_or_else(|| ReadStatError::Other("Invalid path".to_string()))?;
        Ok(CString::new(rust_str)?)
    }

    fn validate_format(format: Option<OutFormat>) -> Result<OutFormat, ReadStatError> {
        Ok(format.unwrap_or(OutFormat::csv))
    }

    fn validate_in_extension(path: &Path) -> Result<String, ReadStatError> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(ReadStatError::Other(format!(
                    "File {} does not have an extension!",
                    path.to_string_lossy().bright_yellow()
                ))),
                |e|
                    if IN_EXTENSIONS.iter().any(|&ext| ext == e) {
                        Ok(e)
                    } else {
                        Err(ReadStatError::Other(format!("Expecting extension {} or {}.\nFile {} does not have expected extension!", String::from("sas7bdat").bright_green(), String::from("sas7bcat").bright_blue(), path.to_string_lossy().bright_yellow())))
                    }
            )
    }

    fn validate_out_extension(
        path: &Path,
        format: OutFormat,
    ) -> Result<Option<PathBuf>, ReadStatError> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(ReadStatError::Other(format!(
                    "File {} does not have an extension!  Expecting extension {}.",
                    path.to_string_lossy().bright_yellow(),
                    format.to_string().bright_green()
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
                                "Expecting extension {}.  Instead, file {} has extension {}.",
                                format.to_string().bright_green(),
                                path.to_string_lossy().bright_yellow(),
                                e.bright_red()
                            )))
                        }
                    }
                },
            )
    }

    fn validate_out_path(
        path: Option<PathBuf>,
        overwrite: bool,
    ) -> Result<Option<PathBuf>, ReadStatError> {
        match path {
            None => Ok(None),
            Some(p) => {
                let abs_path = PathAbs::new(p)?;

                match abs_path.parent() {
                    Err(_) => Err(ReadStatError::Other(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &abs_path.to_string_lossy().bright_yellow()))),
                    Ok(parent) => {
                        if parent.exists() {
                            // Check to see if file already exists
                            if abs_path.exists() {
                                if overwrite {
                                    println!("The file {} will be {}!", abs_path.to_string_lossy().bright_yellow(), String::from("overwritten").truecolor(255, 105, 180));
                                    Ok(Some(abs_path.as_path().to_path_buf()))
                                } else {
                                    Err(ReadStatError::Other(format!("The output file - {} - already exists!  To overwrite the file, utilize the {} parameter", abs_path.to_string_lossy().bright_yellow(), String::from("--overwrite").bright_cyan())))
                                }
                            } else {
                                Ok(Some(abs_path.as_path().to_path_buf()))
                            }
                        } else {
                            Err(ReadStatError::Other(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &parent.to_string_lossy().bright_yellow())))
                        }
                    }
                }
            }
        }
    }

    fn validate_path(path: PathBuf) -> Result<PathBuf, ReadStatError> {
        let abs_path = PathAbs::new(path)?;

        if abs_path.exists() {
            Ok(abs_path.as_path().to_path_buf())
        } else {
            Err(ReadStatError::Other(format!(
                "File {} does not exist!",
                abs_path.to_string_lossy().bright_yellow()
            )))
        }
    }

    fn validate_compression_level(
        compression: ParquetCompression,
        compression_level: Option<u32>,
    ) -> Result<Option<u32>, ReadStatError> {
        // (CLI display name, max valid level) - None means level is ignored
        let (name, max_level): (&str, Option<u32>) = match compression {
            ParquetCompression::Uncompressed => ("uncompressed", None),
            ParquetCompression::Snappy => ("snappy", None),
            ParquetCompression::Lz4Raw => ("lz4-raw", None),
            ParquetCompression::Gzip => ("gzip", Some(9)),
            ParquetCompression::Brotli => ("brotli", Some(11)),
            ParquetCompression::Zstd => ("zstd", Some(22)),
        };

        match (max_level, compression_level) {
            // Codec ignores levels
            (None, None) => Ok(None),
            (None, Some(_)) => {
                println!(
                    "Compression level is not required for compression={}, ignoring value of {}",
                    name.bright_magenta(),
                    String::from("--compression-level").bright_cyan()
                );
                Ok(None)
            }
            // Codec supports levels
            (Some(_), None) => Ok(None),
            (Some(max), Some(c)) => {
                if c <= max {
                    Ok(Some(c))
                } else {
                    Err(ReadStatError::Other(format!(
                        "The compression level of {} is not a valid level for {} compression. \
                         Instead, please use values between 0-{max}.",
                        c.to_string().bright_yellow(),
                        name.bright_cyan()
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- validate_in_extension ---

    #[test]
    fn valid_sas7bdat_extension() {
        let path = Path::new("/some/file.sas7bdat");
        assert_eq!(ReadStatPath::validate_in_extension(path).unwrap(), "sas7bdat");
    }

    #[test]
    fn valid_sas7bcat_extension() {
        let path = Path::new("/some/file.sas7bcat");
        assert_eq!(ReadStatPath::validate_in_extension(path).unwrap(), "sas7bcat");
    }

    #[test]
    fn invalid_extension() {
        let path = Path::new("/some/file.csv");
        assert!(ReadStatPath::validate_in_extension(path).is_err());
    }

    #[test]
    fn no_extension() {
        let path = Path::new("/some/file");
        assert!(ReadStatPath::validate_in_extension(path).is_err());
    }

    // --- validate_format ---

    #[test]
    fn format_none_defaults_to_csv() {
        let f = ReadStatPath::validate_format(None).unwrap();
        assert!(matches!(f, OutFormat::csv));
    }

    #[test]
    fn format_some_passes_through() {
        let f = ReadStatPath::validate_format(Some(OutFormat::parquet)).unwrap();
        assert!(matches!(f, OutFormat::parquet));
    }

    // --- validate_out_extension ---

    #[test]
    fn valid_csv_out_extension() {
        let path = Path::new("/some/output.csv");
        let result = ReadStatPath::validate_out_extension(path, OutFormat::csv).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_parquet_out_extension() {
        let path = Path::new("/some/output.parquet");
        let result = ReadStatPath::validate_out_extension(path, OutFormat::parquet).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_feather_out_extension() {
        let path = Path::new("/some/output.feather");
        let result = ReadStatPath::validate_out_extension(path, OutFormat::feather).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn valid_ndjson_out_extension() {
        let path = Path::new("/some/output.ndjson");
        let result = ReadStatPath::validate_out_extension(path, OutFormat::ndjson).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn mismatched_out_extension() {
        let path = Path::new("/some/output.csv");
        assert!(ReadStatPath::validate_out_extension(path, OutFormat::parquet).is_err());
    }

    #[test]
    fn no_out_extension() {
        let path = Path::new("/some/output");
        assert!(ReadStatPath::validate_out_extension(path, OutFormat::csv).is_err());
    }

    // --- validate_compression_level ---

    #[test]
    fn uncompressed_ignores_level() {
        let result = ReadStatPath::validate_compression_level(
            ParquetCompression::Uncompressed,
            Some(5),
        ).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn snappy_ignores_level() {
        let result = ReadStatPath::validate_compression_level(
            ParquetCompression::Snappy,
            Some(5),
        ).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn lz4raw_ignores_level() {
        let result = ReadStatPath::validate_compression_level(
            ParquetCompression::Lz4Raw,
            Some(5),
        ).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn gzip_valid_level() {
        let result = ReadStatPath::validate_compression_level(
            ParquetCompression::Gzip,
            Some(5),
        ).unwrap();
        assert_eq!(result, Some(5));
    }

    #[test]
    fn gzip_max_valid_level() {
        let result = ReadStatPath::validate_compression_level(
            ParquetCompression::Gzip,
            Some(9),
        ).unwrap();
        assert_eq!(result, Some(9));
    }

    #[test]
    fn gzip_invalid_level() {
        assert!(ReadStatPath::validate_compression_level(
            ParquetCompression::Gzip,
            Some(10),
        ).is_err());
    }

    #[test]
    fn brotli_valid_level() {
        let result = ReadStatPath::validate_compression_level(
            ParquetCompression::Brotli,
            Some(11),
        ).unwrap();
        assert_eq!(result, Some(11));
    }

    #[test]
    fn brotli_invalid_level() {
        assert!(ReadStatPath::validate_compression_level(
            ParquetCompression::Brotli,
            Some(12),
        ).is_err());
    }

    #[test]
    fn zstd_valid_level() {
        let result = ReadStatPath::validate_compression_level(
            ParquetCompression::Zstd,
            Some(22),
        ).unwrap();
        assert_eq!(result, Some(22));
    }

    #[test]
    fn zstd_invalid_level() {
        assert!(ReadStatPath::validate_compression_level(
            ParquetCompression::Zstd,
            Some(23),
        ).is_err());
    }

    #[test]
    fn no_level_passes_through() {
        let result = ReadStatPath::validate_compression_level(
            ParquetCompression::Gzip,
            None,
        ).unwrap();
        assert_eq!(result, None);
    }

    // --- path_to_cstring ---

    #[test]
    fn path_to_cstring_normal() {
        let path = PathBuf::from("/tmp/test.sas7bdat");
        let cs = ReadStatPath::path_to_cstring(&path).unwrap();
        assert_eq!(cs.to_str().unwrap(), "/tmp/test.sas7bdat");
    }

    // --- validate_out_path ---

    #[test]
    fn validate_out_path_none() {
        assert!(ReadStatPath::validate_out_path(None, false).unwrap().is_none());
    }
}
