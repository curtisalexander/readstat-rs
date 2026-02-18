//! Error types for the readstat crate.
//!
//! [`ReadStatCError`] maps the 39 error codes from the ReadStat C library to Rust
//! enum variants. [`ReadStatError`] is the main error type, wrapping C library errors
//! alongside Arrow, Parquet, I/O, and other failure modes.

use num_derive::FromPrimitive;

/// Error codes returned by the ReadStat C library.
///
/// Each variant maps directly to a `readstat_error_t` value. A value of
/// [`READSTAT_OK`](ReadStatCError::READSTAT_OK) indicates success; all other
/// variants represent specific failure conditions.
#[derive(Debug, FromPrimitive)]
pub enum ReadStatCError {
    /// Operation completed successfully.
    READSTAT_OK = 0,
    /// Failed to open the file.
    READSTAT_ERROR_OPEN = 1,
    /// Failed to read from the file.
    READSTAT_ERROR_READ = 2,
    /// Memory allocation failure.
    READSTAT_ERROR_MALLOC = 3,
    /// User-initiated abort via callback return value.
    READSTAT_ERROR_USER_ABORT = 4,
    /// General parse error in the file structure.
    READSTAT_ERROR_PARSE = 5,
    /// File uses an unsupported compression method.
    READSTAT_ERROR_UNSUPPORTED_COMPRESSION = 6,
    /// File uses an unsupported character set.
    READSTAT_ERROR_UNSUPPORTED_CHARSET = 7,
    /// Column count in header does not match actual columns.
    READSTAT_ERROR_COLUMN_COUNT_MISMATCH = 8,
    /// Row count in header does not match actual rows.
    READSTAT_ERROR_ROW_COUNT_MISMATCH = 9,
    /// Row width in header does not match actual width.
    READSTAT_ERROR_ROW_WIDTH_MISMATCH = 10,
    /// Invalid or unrecognized format string.
    READSTAT_ERROR_BAD_FORMAT_STRING = 11,
    /// Value type does not match expected type.
    READSTAT_ERROR_VALUE_TYPE_MISMATCH = 12,
    /// Failed to write output.
    READSTAT_ERROR_WRITE = 13,
    /// Writer was not properly initialized before use.
    READSTAT_ERROR_WRITER_NOT_INITIALIZED = 14,
    /// Failed to seek within the file.
    READSTAT_ERROR_SEEK = 15,
    /// Character encoding conversion failed.
    READSTAT_ERROR_CONVERT = 16,
    /// Conversion failed due to invalid string data.
    READSTAT_ERROR_CONVERT_BAD_STRING = 17,
    /// String is too short for conversion.
    READSTAT_ERROR_CONVERT_SHORT_STRING = 18,
    /// String is too long for conversion.
    READSTAT_ERROR_CONVERT_LONG_STRING = 19,
    /// Numeric value is outside the representable range.
    READSTAT_ERROR_NUMERIC_VALUE_IS_OUT_OF_RANGE = 20,
    /// Tagged missing value is outside the valid range.
    READSTAT_ERROR_TAGGED_VALUE_IS_OUT_OF_RANGE = 21,
    /// String value exceeds the maximum allowed length.
    READSTAT_ERROR_STRING_VALUE_IS_TOO_LONG = 22,
    /// Tagged missing values are not supported by this format.
    READSTAT_ERROR_TAGGED_VALUES_NOT_SUPPORTED = 23,
    /// File format version is not supported.
    READSTAT_ERROR_UNSUPPORTED_FILE_FORMAT_VERSION = 24,
    /// Variable name begins with an illegal character.
    READSTAT_ERROR_NAME_BEGINS_WITH_ILLEGAL_CHARACTER = 25,
    /// Variable name contains an illegal character.
    READSTAT_ERROR_NAME_CONTAINS_ILLEGAL_CHARACTER = 26,
    /// Variable name is a reserved word.
    READSTAT_ERROR_NAME_IS_RESERVED_WORD = 27,
    /// Variable name exceeds the maximum allowed length.
    READSTAT_ERROR_NAME_IS_TOO_LONG = 28,
    /// Timestamp string could not be parsed.
    READSTAT_ERROR_BAD_TIMESTAMP_STRING = 29,
    /// Invalid frequency weight specification.
    READSTAT_ERROR_BAD_FREQUENCY_WEIGHT = 30,
    /// Too many missing value definitions for a variable.
    READSTAT_ERROR_TOO_MANY_MISSING_VALUE_DEFINITIONS = 31,
    /// Note text exceeds the maximum allowed length.
    READSTAT_ERROR_NOTE_IS_TOO_LONG = 32,
    /// String references are not supported by this format.
    READSTAT_ERROR_STRING_REFS_NOT_SUPPORTED = 33,
    /// A string reference is required but was not provided.
    READSTAT_ERROR_STRING_REF_IS_REQUIRED = 34,
    /// Row is too wide for a single page.
    READSTAT_ERROR_ROW_IS_TOO_WIDE_FOR_PAGE = 35,
    /// File has too few columns.
    READSTAT_ERROR_TOO_FEW_COLUMNS = 36,
    /// File has too many columns.
    READSTAT_ERROR_TOO_MANY_COLUMNS = 37,
    /// Variable name is empty (zero length).
    READSTAT_ERROR_NAME_IS_ZERO_LENGTH = 38,
    /// Timestamp value is invalid.
    READSTAT_ERROR_BAD_TIMESTAMP_VALUE = 39,
    /// Invalid multiple response (MR) set string.
    READSTAT_ERROR_BAD_MR_STRING = 40,
}

/// The main error type for the readstat crate.
///
/// Wraps errors from the ReadStat C library, Arrow/Parquet processing,
/// I/O operations, and other subsystems into a single error enum.
#[derive(Debug, thiserror::Error)]
pub enum ReadStatError {
    /// Error from the ReadStat C library.
    #[error("ReadStat C library error: {0:?}")]
    CLibrary(ReadStatCError),

    /// Unrecognized C error code not mapped to [`ReadStatCError`].
    #[error("Unknown C error code: {0}")]
    UnknownCError(i32),

    /// Variable index not found in the metadata map.
    #[error("Variable index {index} not found")]
    VarIndexNotFound {
        /// The variable index that was not found.
        index: i32,
    },

    /// Failed to parse a floating-point value from its string representation.
    #[error("Failed to parse numeric value: {0}")]
    NumericParse(String),

    /// Arithmetic overflow during SAS-to-Unix epoch date/time conversion.
    #[error("Date arithmetic overflow")]
    DateOverflow,

    /// Integer conversion error (e.g. `u32` to `i32` overflow).
    #[error("Integer conversion failed: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),

    /// Error from the Arrow library.
    #[error("{0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Error from the Parquet library.
    #[error("{0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// I/O error.
    #[error("{0}")]
    Io(#[from] std::io::Error),

    /// Path resolution error.
    #[error("{0}")]
    PathAbs(#[from] path_abs::Error),

    /// JSON serialization/deserialization error.
    #[error("{0}")]
    SerdeJson(#[from] serde_json::Error),

    /// Rayon thread pool build error.
    #[error("{0}")]
    Rayon(#[from] rayon::ThreadPoolBuildError),

    /// Progress bar template error.
    #[error("{0}")]
    IndicatifTemplate(#[from] indicatif::style::TemplateError),

    /// Null byte found in a string intended for C FFI.
    #[error("{0}")]
    NulError(#[from] std::ffi::NulError),

    /// One or more specified column names were not found in the dataset.
    #[error("Column(s) not found: {requested:?}\nAvailable columns: {available:?}")]
    ColumnsNotFound {
        /// The column names that were requested but not found.
        requested: Vec<String>,
        /// All available column names in the dataset.
        available: Vec<String>,
    },

    /// Catch-all error with a custom message.
    #[error("{0}")]
    Other(String),
}

/// Check a readstat C error code, returning Ok(()) for READSTAT_OK
/// or an appropriate error variant otherwise.
pub fn check_c_error(code: i32) -> Result<(), ReadStatError> {
    use num_traits::FromPrimitive;
    match FromPrimitive::from_i32(code) {
        Some(ReadStatCError::READSTAT_OK) => Ok(()),
        Some(e) => Err(ReadStatError::CLibrary(e)),
        None => Err(ReadStatError::UnknownCError(code)),
    }
}
