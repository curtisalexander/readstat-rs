#![allow(non_camel_case_types)]

use dunce;
use log::debug;
use readstat_sys;
use std::error::Error;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use thiserror::Error;

#[derive(StructOpt, Debug)]
#[structopt(about = "count rows in sas7bdat")]
pub enum ReadStat {
    /// Get row count
    Rows {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        file: PathBuf,
        #[structopt(long, short)]
        raw: bool,
    },
}

#[derive(Error, Debug, Copy, Clone, PartialEq)]
#[error("Invalid path")]
pub struct InvalidPath;

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
#[repr(C)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE,
}

pub unsafe extern "C" fn handle_metadata(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut c_void,
) -> c_int {
    let mut md = &mut *(ctx as *mut ReadStatMetadata);

    let rc: c_int = readstat_sys::readstat_get_row_count(metadata);

    md.row_count = rc;
    debug!("md struct is {:#?}", md);
    debug!("row_count is {:#?}", md.row_count);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

#[cfg(unix)]
pub fn path_to_cstring(path: &Path) -> Result<CString, InvalidPath> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    CString::new(bytes).map_err(|_| InvalidPath)
}

#[cfg(not(unix))]
pub fn path_to_cstring(path: &Path) -> Result<CString, InvalidPath> {
    let rust_str = path.as_os_str().as_str().ok_or(InvalidPath)?;
    let bytes = path.as_os_str().as_bytes();
    CString::new(rust_str).map_err(|_| InvalidPath)
}

#[derive(Debug, Clone, PartialEq)]
struct ReadStatMetadata {
    row_count: c_int,
}

impl ReadStatMetadata {
    fn new() -> Self {
        Self { row_count: 0 }
    }
}

pub struct ReadStatParser {
    parser: *mut readstat_sys::readstat_parser_t,
}

impl ReadStatParser {
    fn new() -> Self {
        let parser: *mut readstat_sys::readstat_parser_t =
            unsafe { readstat_sys::readstat_parser_init() };

        Self { parser }
    }

    fn set_metadata_handler_error(
        &self,
        metadata_handler: readstat_sys::readstat_metadata_handler,
    ) -> Result<(), Box<dyn Error>> {
        let set_metadata_handler_error =
            unsafe { readstat_sys::readstat_set_metadata_handler(self.parser, metadata_handler) };

        debug!(
            "After setting metadata handler, error ==> {}",
            &set_metadata_handler_error
        );

        if set_metadata_handler_error == readstat_sys::readstat_error_e_READSTAT_OK {
            Ok(())
        } else {
            Err(From::from("Unable to set metadata handler"))
        }
    }

    fn parse_sas7bdat(
        &self,
        path: *const c_char,
        user_ctx: *mut c_void,
    ) -> Result<(), Box<dyn Error>> {
        let parse_sas7bdat_error: readstat_sys::readstat_error_t =
            unsafe { readstat_sys::readstat_parse_sas7bdat(self.parser, path, user_ctx) };

        debug!(
            "After calling parse sas7bdat, error ==> {}",
            &parse_sas7bdat_error
        );

        if parse_sas7bdat_error == readstat_sys::readstat_error_e_READSTAT_OK {
            Ok(())
        } else {
            Err(From::from("Unable to parse sas7bdat"))
        }
    }
}

impl Drop for ReadStatParser {
    fn drop(&mut self) {
        debug!("Freeing parser");

        unsafe { readstat_sys::readstat_parser_free(self.parser) };
    }
}

pub fn get_row_count(
    path: &PathBuf,
) -> Result<(readstat_sys::readstat_error_t, i32), Box<dyn Error>> {
    let sas_path_cstring = path_to_cstring(&path)?;
    let psas_path_cstring = sas_path_cstring.as_ptr();

    debug!(
        "Counting the number of records within the file {}",
        path.to_string_lossy()
    );
    debug!("Path as C string is {:?}", sas_path_cstring);

    let mut readstat_md = ReadStatMetadata::new() ;
    let preadstat_md = &mut readstat_md as *mut ReadStatMetadata as *mut c_void;

    let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
    debug!("Initially, error ==> {}", &error);

    let parser = ReadStatParser::new();

    parser.set_metadata_handler_error(Some(handle_metadata))?;

    parser.parse_sas7bdat(psas_path_cstring, preadstat_md)?;

    let row_count = readstat_md.row_count;

    Ok((error, row_count))
}

pub fn run(rs: ReadStat) -> Result<(), Box<dyn Error>> {
    env_logger::init();

    match rs {
        ReadStat::Rows { file, raw } => {
            let sas_path = dunce::canonicalize(&file)?;
            let (error, record_count) = get_row_count(&sas_path)?;
            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                if !raw {
                    println!(
                        "The file {:#?} contains {:#?} records",
                        &sas_path.display(),
                        record_count
                    );
                } else {
                    println!("{}", record_count);
                }
                Ok(())
            }
        }
    }
}
