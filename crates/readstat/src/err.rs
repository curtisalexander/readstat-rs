use num_derive::FromPrimitive;

#[derive(Debug, FromPrimitive)]
pub enum ReadStatCError {
    READSTAT_OK = 0,
    READSTAT_ERROR_OPEN = 1,
    READSTAT_ERROR_READ = 2,
    READSTAT_ERROR_MALLOC = 3,
    READSTAT_ERROR_USER_ABORT = 4,
    READSTAT_ERROR_PARSE = 5,
    READSTAT_ERROR_UNSUPPORTED_COMPRESSION = 6,
    READSTAT_ERROR_UNSUPPORTED_CHARSET = 7,
    READSTAT_ERROR_COLUMN_COUNT_MISMATCH = 8,
    READSTAT_ERROR_ROW_COUNT_MISMATCH = 9,
    READSTAT_ERROR_ROW_WIDTH_MISMATCH = 10,
    READSTAT_ERROR_BAD_FORMAT_STRING = 11,
    READSTAT_ERROR_VALUE_TYPE_MISMATCH = 12,
    READSTAT_ERROR_WRITE = 13,
    READSTAT_ERROR_WRITER_NOT_INITIALIZED = 14,
    READSTAT_ERROR_SEEK = 15,
    READSTAT_ERROR_CONVERT = 16,
    READSTAT_ERROR_CONVERT_BAD_STRING = 17,
    READSTAT_ERROR_CONVERT_SHORT_STRING = 18,
    READSTAT_ERROR_CONVERT_LONG_STRING = 19,
    READSTAT_ERROR_NUMERIC_VALUE_IS_OUT_OF_RANGE = 20,
    READSTAT_ERROR_TAGGED_VALUE_IS_OUT_OF_RANGE = 21,
    READSTAT_ERROR_STRING_VALUE_IS_TOO_LONG = 22,
    READSTAT_ERROR_TAGGED_VALUES_NOT_SUPPORTED = 23,
    READSTAT_ERROR_UNSUPPORTED_FILE_FORMAT_VERSION = 24,
    READSTAT_ERROR_NAME_BEGINS_WITH_ILLEGAL_CHARACTER = 25,
    READSTAT_ERROR_NAME_CONTAINS_ILLEGAL_CHARACTER = 26,
    READSTAT_ERROR_NAME_IS_RESERVED_WORD = 27,
    READSTAT_ERROR_NAME_IS_TOO_LONG = 28,
    READSTAT_ERROR_BAD_TIMESTAMP_STRING = 29,
    READSTAT_ERROR_BAD_FREQUENCY_WEIGHT = 30,
    READSTAT_ERROR_TOO_MANY_MISSING_VALUE_DEFINITIONS = 31,
    READSTAT_ERROR_NOTE_IS_TOO_LONG = 32,
    READSTAT_ERROR_STRING_REFS_NOT_SUPPORTED = 33,
    READSTAT_ERROR_STRING_REF_IS_REQUIRED = 34,
    READSTAT_ERROR_ROW_IS_TOO_WIDE_FOR_PAGE = 35,
    READSTAT_ERROR_TOO_FEW_COLUMNS = 36,
    READSTAT_ERROR_TOO_MANY_COLUMNS = 37,
    READSTAT_ERROR_NAME_IS_ZERO_LENGTH = 38,
    READSTAT_ERROR_BAD_TIMESTAMP_VALUE = 39,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadStatError {
    #[error("ReadStat C library error: {0:?}")]
    CLibrary(ReadStatCError),

    #[error("Unknown C error code: {0}")]
    UnknownCError(i32),

    #[error("Variable index {index} not found")]
    VarIndexNotFound { index: i32 },

    #[error("Failed to parse numeric value: {0}")]
    NumericParse(String),

    #[error("Date arithmetic overflow")]
    DateOverflow,

    #[error("Integer conversion failed: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),

    #[error("{0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("{0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    PathAbs(#[from] path_abs::Error),

    #[error("{0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("{0}")]
    Rayon(#[from] rayon::ThreadPoolBuildError),

    #[error("{0}")]
    IndicatifTemplate(#[from] indicatif::style::TemplateError),

    #[error("{0}")]
    NulError(#[from] std::ffi::NulError),

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
