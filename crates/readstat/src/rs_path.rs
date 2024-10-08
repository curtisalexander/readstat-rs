use colored::Colorize;
use path_abs::{PathAbs, PathInfo};
use std::{
    error::Error,
    ffi::CString,
    path::{Path, PathBuf},
};

use crate::OutFormat;

const IN_EXTENSIONS: &[&str] = &["sas7bdat", "sas7bcat"];

#[derive(Debug, Clone)]
pub struct ReadStatPath {
    pub path: PathBuf,
    pub extension: String,
    pub cstring_path: CString,
    pub out_path: Option<PathBuf>,
    pub format: OutFormat,
    pub overwrite: bool,
    pub no_write: bool,
}

impl ReadStatPath {
    pub fn new(
        path: PathBuf,
        out_path: Option<PathBuf>,
        format: Option<OutFormat>,
        overwrite: bool,
        no_write: bool,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let p = Self::validate_path(path)?;
        let ext = Self::validate_in_extension(&p)?;
        let csp = Self::path_to_cstring(&p)?;
        let f = Self::validate_format(format)?;
        let op: Option<PathBuf> = Self::validate_out_path(out_path, overwrite)?;
        let op = match op {
            None => op,
            Some(op) => Self::validate_out_extension(&op, f)?,
        };

        Ok(Self {
            path: p,
            extension: ext,
            cstring_path: csp,
            out_path: op,
            format: f,
            overwrite,
            no_write,
        })
    }

    #[cfg(unix)]
    pub fn path_to_cstring(path: &PathBuf) -> Result<CString, Box<dyn Error + Send + Sync>> {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        CString::new(bytes).map_err(|_| From::from("Invalid path"))
    }

    #[cfg(not(unix))]
    pub fn path_to_cstring(path: &Path) -> Result<CString, Box<dyn Error + Send + Sync>> {
        let rust_str = path.as_os_str().to_str().ok_or("Invalid path")?;
        CString::new(rust_str).map_err(|_| From::from("Invalid path"))
    }

    fn validate_format(
        format: Option<OutFormat>,
    ) -> Result<OutFormat, Box<dyn Error + Send + Sync>> {
        match format {
            None => Ok(OutFormat::csv),
            Some(f) => Ok(f),
        }
    }

    fn validate_in_extension(path: &Path) -> Result<String, Box<dyn Error + Send + Sync>> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(From::from(format!(
                    "File {} does not have an extension!",
                    path.to_string_lossy().bright_yellow()
                ))),
                |e|
                    if IN_EXTENSIONS.iter().any(|&ext| ext == e) {
                        Ok(e)
                    } else {
                        Err(From::from(format!("Expecting extension {} or {}.\nFile {} does not have expected extension!", String::from("sas7bdat").bright_green(), String::from("sas7bcat").bright_blue(), path.to_string_lossy().bright_yellow())))
                    }
            )
    }

    fn validate_out_extension(
        path: &Path,
        format: OutFormat,
    ) -> Result<Option<PathBuf>, Box<dyn Error + Send + Sync>> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(From::from(format!(
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
                            Err(From::from(format!(
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
    ) -> Result<Option<PathBuf>, Box<dyn Error + Send + Sync>> {
        match path {
            None => Ok(None),
            Some(p) => {
                let abs_path = PathAbs::new(p)?;

                match abs_path.parent() {
                    Err(_) => Err(From::from(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &abs_path.to_string_lossy().bright_yellow()))),
                    Ok(parent) => {
                        if parent.exists() {
                            // Check to see if file already exists
                            if abs_path.exists() {
                                if overwrite {
                                    println!("The file {} will be {}!", abs_path.to_string_lossy().bright_yellow(), String::from("overwritten").truecolor(255, 105, 180));
                                    Ok(Some(abs_path.as_path().to_path_buf()))
                                } else {
                                    Err(From::from(format!("The output file - {} - already exists!  To overwrite the file, utilize the {} parameter", abs_path.to_string_lossy().bright_yellow(), String::from("--overwrite").bright_cyan())))
                                }
                            } else {
                                Ok(Some(abs_path.as_path().to_path_buf()))
                            }
                        } else {
                            Err(From::from(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &parent.to_string_lossy().bright_yellow())))
                        }
                    }
                }
            }
        }
    }

    fn validate_path(path: PathBuf) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
        let abs_path = PathAbs::new(path)?;

        if abs_path.exists() {
            Ok(abs_path.as_path().to_path_buf())
        } else {
            Err(From::from(format!(
                "File {} does not exist!",
                abs_path.to_string_lossy().bright_yellow()
            )))
        }
    }
}
