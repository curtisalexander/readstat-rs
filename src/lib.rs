#![allow(dead_code)]
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

#[derive(Copy, Clone, Debug)]
#[repr(C)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE
}

unsafe extern "C" fn handle_metadata(
    metadata: *mut bindings::readstat_metadata_t,
    ctx: *mut std::os::raw::c_void
) -> std::os::raw::c_int {
    let my_count = ctx as *mut i32;

    let rc: std::os::raw::c_int = bindings::readstat_get_row_count(metadata);

    *my_count = rc ;
    println!("my_count is {:#?}", my_count);
    println!("my_count derefed is {:#?}", *my_count);

    ReadStatHandler::READSTAT_HANDLER_OK as std::os::raw::c_int
}

pub fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let sas_path = &args.sas;

    let mut buf = Vec::new();
    buf.extend(sas_path.as_os_str().as_bytes());

    let sas_path_cstring = CString::new(buf).unwrap();
    let psas_path_cstring = sas_path_cstring.as_ptr();

    println!("Counting the number of records within the file {}", sas_path.to_string_lossy());
    println!("Path is {:?}", sas_path_cstring);

    let mut my_count = 0;
    let pmy_count = &mut my_count as *mut i32;
    let pmy_count_void = pmy_count as *mut std::os::raw::c_void;

    let error: bindings::readstat_error_t = bindings::readstat_error_e_READSTAT_OK;
    println!("Initially, error ==> {}", &error);

    let parser: *mut bindings::readstat_parser_t = unsafe { bindings::readstat_parser_init() };
    let set_metadata_handler_error = unsafe { bindings::readstat_set_metadata_handler(parser, Some(handle_metadata)) };
    println!("After setting metadata handler, error ==> {}", &set_metadata_handler_error);

    let error: bindings::readstat_error_t = unsafe { bindings::readstat_parse_sas7bdat(parser, psas_path_cstring, pmy_count_void) };
    println!("After calling parse sas7bdat, error ==> {}", &error);

    unsafe { bindings::readstat_parser_free(parser) };

    let record_count = unsafe { *pmy_count };

    if error != bindings::readstat_error_e_READSTAT_OK {
        Err(From::from("Error when attempting to parse sas7bdat"))
    } else {
        println!("Found {} records", record_count);
        Ok(())
    }
}