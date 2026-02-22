//! Path validation for SAS file input.
//!
//! [`ReadStatPath`] validates the input `.sas7bdat` file path and converts it
//! to a C-compatible string for the FFI layer.

use std::{
    ffi::CString,
    path::{Path, PathBuf},
};

use crate::err::ReadStatError;

const IN_EXTENSIONS: &[&str] = &["sas7bdat", "sas7bcat"];

/// Validated file path for SAS file input.
///
/// Encapsulates the input `.sas7bdat` path (validated to exist with correct extension).
/// The input path is also converted to a [`CString`] for passing to the ReadStat C library.
#[derive(Debug, Clone)]
pub struct ReadStatPath {
    /// Absolute path to the input `.sas7bdat` file.
    pub path: PathBuf,
    /// File extension of the input file (e.g. `"sas7bdat"`).
    pub extension: String,
    /// Input path as a C-compatible string for FFI.
    pub cstring_path: CString,
}

impl ReadStatPath {
    /// Creates a new `ReadStatPath` after validating the input path.
    pub fn new(path: PathBuf) -> Result<Self, ReadStatError> {
        let p = Self::validate_path(path)?;
        let ext = Self::validate_in_extension(&p)?;
        let csp = Self::path_to_cstring(&p)?;

        Ok(Self {
            path: p,
            extension: ext,
            cstring_path: csp,
        })
    }

    /// Converts a file path to a [`CString`] for FFI. Uses raw bytes on Unix.
    #[cfg(unix)]
    pub(crate) fn path_to_cstring(path: &Path) -> Result<CString, ReadStatError> {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        Ok(CString::new(bytes)?)
    }

    /// Converts a file path to a [`CString`] for FFI. Uses UTF-8 on non-Unix platforms.
    #[cfg(not(unix))]
    pub(crate) fn path_to_cstring(path: &Path) -> Result<CString, ReadStatError> {
        let rust_str = path
            .as_os_str()
            .to_str()
            .ok_or_else(|| ReadStatError::Other("Invalid path".to_string()))?;
        Ok(CString::new(rust_str)?)
    }

    fn validate_in_extension(path: &Path) -> Result<String, ReadStatError> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(ReadStatError::Other(format!(
                    "File {} does not have an extension!",
                    path.to_string_lossy()
                ))),
                |e|
                    if IN_EXTENSIONS.iter().any(|&ext| ext == e) {
                        Ok(e)
                    } else {
                        Err(ReadStatError::Other(format!(
                            "Expecting extension sas7bdat or sas7bcat.\nFile {} does not have expected extension!",
                            path.to_string_lossy()
                        )))
                    }
            )
    }

    fn validate_path(path: PathBuf) -> Result<PathBuf, ReadStatError> {
        let abs_path = std::path::absolute(&path)
            .map_err(|e| ReadStatError::Other(format!("Failed to resolve path: {e}")))?;

        if abs_path.exists() {
            Ok(abs_path)
        } else {
            Err(ReadStatError::Other(format!(
                "File {} does not exist!",
                abs_path.to_string_lossy()
            )))
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

    // --- path_to_cstring ---

    #[test]
    fn path_to_cstring_normal() {
        let path = PathBuf::from("/tmp/test.sas7bdat");
        let cs = ReadStatPath::path_to_cstring(&path).unwrap();
        assert_eq!(cs.to_str().unwrap(), "/tmp/test.sas7bdat");
    }
}
