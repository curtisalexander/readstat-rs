#![allow(non_camel_case_types)]
mod bindings;

use std::error::Error;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(about = "count rows in sas7bdat")]
pub struct Args {
    /// Path to sas7bdat
    #[structopt(long, short, parse(from_os_str))]
    pub sas: PathBuf
}

#[derive(Copy, Clone)]
#[repr(i32)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE
}

unsafe extern "C" fn handle_metadata(
    metadata: *mut bindings::readstat_metadata_t,
    ctx: *mut std::os::raw::c_void
) -> i32 {
    let my_count = ctx as *mut i32;

    *my_count = bindings::readstat_get_row_count(metadata);

    ReadStatHandler::READSTAT_HANDLER_OK as i32
}

pub fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let sas_path = &args.sas;

    let mut buf = Vec::new();
    buf.extend(sas_path.as_os_str().as_bytes());

    let sas_path_cstring = CString::new(buf).unwrap();

    let my_count = std::ptr::null_mut();

    let error: bindings::readstat_error_t = bindings::readstat_error_e_READSTAT_OK;
    let parser: *mut bindings::readstat_parser_t = unsafe { bindings::readstat_parser_init() };
    unsafe { bindings::readstat_set_metadata_handler(parser, Some(handle_metadata)) };

    let error = unsafe { bindings::readstat_parse_sas7bdat(parser, sas_path_cstring.as_ptr(), my_count) };

    let my_count_int = unsafe {*(my_count as *const i32) };

    unsafe { bindings::readstat_parser_free(parser) };

    if error != bindings::readstat_error_e_READSTAT_OK {
        Err(From::from("Error when attempting to parse sas7bdat"))
    } else {
        println!("Found {} records", my_count_int);
        Ok(())
    }

    // println!("Path is {}", sas_path.to_string_lossy());
}