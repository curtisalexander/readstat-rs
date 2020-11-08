#![allow(non_camel_case_types)]

use dunce;
use readstat_sys;
use std::error::Error;
use std::ffi::CString;
use std::path::{Path, PathBuf};
use structopt::StructOpt; 
use thiserror::Error;

#[derive(StructOpt, Debug)]
#[structopt(about = "count rows in sas7bdat")]
pub enum ReadStat {
    /// Get row count
    Rows {
        #[structopt(long, short, parse(from_os_str))]
        /// Path to sas7bdat
        file: PathBuf,
        /// Verbose
        #[structopt(long, short)]
        verbose: bool
    }
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
    READSTAT_HANDLER_SKIP_VARIABLE
}

pub unsafe extern "C" fn handle_metadata(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut std::os::raw::c_void
) -> std::os::raw::c_int {
    let my_count = ctx as *mut i32;

    let rc: std::os::raw::c_int = readstat_sys::readstat_get_row_count(metadata);

    *my_count = rc ;
    // println!("my_count is {:#?}", my_count);
    // println!("my_count derefed is {:#?}", *my_count);

    ReadStatHandler::READSTAT_HANDLER_OK as std::os::raw::c_int
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

pub fn get_row_count(path: &PathBuf, verbose: bool) -> Result<(readstat_sys::readstat_error_t, i32), Box<dyn Error>> {
    let sas_path_cstring = path_to_cstring(&path)?;
    let psas_path_cstring = sas_path_cstring.as_ptr();

    if verbose {
        println!("Counting the number of records within the file {}", path.to_string_lossy());
        println!("Path is {:?}", sas_path_cstring);
    } 

    let mut my_count = 0;
    let pmy_count = &mut my_count as *mut i32;
    let pmy_count_void = pmy_count as *mut std::os::raw::c_void;

    let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
    if verbose { println!("Initially, error ==> {}", &error); }

    let parser: *mut readstat_sys::readstat_parser_t = unsafe { readstat_sys::readstat_parser_init() };
    let set_metadata_handler_error = unsafe { readstat_sys::readstat_set_metadata_handler(parser, Some(handle_metadata)) };
    if verbose { println!("After setting metadata handler, error ==> {}", &set_metadata_handler_error); }

    let error: readstat_sys::readstat_error_t = unsafe { readstat_sys::readstat_parse_sas7bdat(parser, psas_path_cstring, pmy_count_void) };
    if verbose { println!("After calling parse sas7bdat, error ==> {}", &error); }

    unsafe { readstat_sys::readstat_parser_free(parser) };

    let record_count = unsafe { *pmy_count };

    Ok((error, record_count))
} 

pub fn run(rs: ReadStat) -> Result<(), Box<dyn Error>> {
    match rs {
        ReadStat::Rows { file, verbose } => {
            let sas_path = dunce::canonicalize(&file)?;
            let (error, record_count) = get_row_count(&sas_path, verbose)?;
            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                println!("The file {:?} contains {} records", &sas_path.display(), record_count);
                Ok(())
            }
        },
    }
}
