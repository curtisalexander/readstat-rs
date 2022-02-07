use std::{
    error::Error,
    sync::{Arc, Mutex}, ffi::c_void,
};

use log::debug;
use num_traits::FromPrimitive;

use crate::{ReadStatData, ReadStatError, Reader, Format, ReadStatPath, rs_parser::ReadStatParser, cb};

pub fn build_offsets(
    reader: &Option<Reader>,
    row_count: u32,
    stream_rows: Option<u32>,
    row_limit: Option<u32>,
) -> Result<Vec<u32>, Box<dyn Error>> {
    // Get other row counts
    let rc = if let Some(r) = row_limit {
        std::cmp::min(r, row_count)
    } else {
        row_count
    };
    let sr = match reader {
        Some(Reader::stream) => match stream_rows {
            Some(s) => s,
            None => rc
        }
        Some(Reader::mem) | None => row_count,
    };

    // Get number of chunks based on row counts above
    let chunks: u32;
    if sr < rc {
        chunks = if rc % sr == 0 { rc / sr } else { (rc / sr) + 1 };
    } else {
        chunks = 1;
    }

    // Allocate and populate a vector for the offsets
    let mut offsets: Vec<u32> = Vec::with_capacity(chunks as usize);

    for c in 0..=chunks {
        if c == 0 {
            offsets.push(0);
        } else if c == chunks {
            offsets.push(rc);
        } else {
            offsets.push(c * sr);
        }
    }

    Ok(offsets)
}

pub fn read_data(
    d: &mut ReadStatData,
    rsp: &ReadStatPath
) -> Result<(), Box<dyn Error>> {
    // path as pointer
    debug!("Path as C string is {:?}", &self.cstring_path);
    let ppath = rsp.cstring_path.as_ptr();

    // spinner
    /*
    if !self.no_progress {
        self.pb = Some(ProgressBar::new(!0));
    }
    if let Some(pb) = &self.pb {
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("[{spinner:.green} {elapsed_precise}] {msg}"),
        );
        let msg = format!(
            "Parsing sas7bdat data from file {}",
            &self.path.to_string_lossy().bright_red()
        );
        pb.set_message(msg);
        pb.enable_steady_tick(120);
    }
    */

    // initialize context
    let ctx = d as *mut ReadStatData as *mut c_void;

    // initialize error
    let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
    debug!("Initially, error ==> {:#?}", &error);

    // setup parser
    // once call parse_sas7bdat, iteration begins
    let error = ReadStatParser::new()
        // do not set metadata handler nor variable handler as already processed
        .set_value_handler(Some(cb::handle_value))?
        .set_row_limit(Some(d.batch_rows_to_process.try_into().unwrap()))?
        .set_row_offset(Some(d.batch_row_start.try_into().unwrap()))?
        .parse_sas7bdat(ppath, ctx);

    match FromPrimitive::from_i32(error as i32) {
        Some(ReadStatError::READSTAT_OK) => { Ok(()) }
        Some(e) => Err(From::from(format!(
            "Error when attempting to parse sas7bdat: {:#?}",
            e
        ))),
        None => Err(From::from(
            "Error when attempting to parse sas7bdat: Unknown return value",
        )),
    }
}

pub fn get_metadata(
    m: &mut ReadStatMetadata,
    skip_row_count: bool,
) -> Result<(), Box<dyn Error>> {
    let error = m.get_metadata(skip_row_count)?;

    match FromPrimitive::from_i32(error as i32) {
        Some(ReadStatError::READSTAT_OK) => Ok(()),
        Some(e) => Err(From::from(format!(
            "Error when attempting to parse sas7bdat: {:#?}",
            e
        ))),
        None => Err(From::from(
            "Error when attempting to parse sas7bdat: Unknown return value",
        )),
    }
}

pub fn write_metadata(m: ReadStatMetadata) {
    match FromPrimitive::from_i32(error as i32) {
        Some(ReadStatError::READSTAT_OK) => {
            if !as_json {
                d.write_metadata_to_stdout()
            } else {
                d.write_metadata_to_json()
            }
        }
        Some(e) => Err(From::from(format!(
            "Error when attempting to parse sas7bdat: {:#?}",
            e
        ))),
        None => Err(From::from(
            "Error when attempting to parse sas7bdat: Unknown return value",
        )),
    }
}

pub fn get_preview(d: &mut ReadStatData, row_limit: u32) -> Result<(), Box<dyn Error>> {
    // how many rows to process?
    d.batch_rows_to_process = row_limit as usize;
    d.batch_row_start = 0;
    d.batch_row_end = row_limit as usize;

    let error = d.get_preview(Some(row_limit), None)?;

    match FromPrimitive::from_i32(error as i32) {
        Some(ReadStatError::READSTAT_OK) => {
            if !d.no_write {
                d.write()?;
                d.wrote_start = true;
            };
            Ok(())
        }
        Some(e) => Err(From::from(format!(
            "Error when attempting to parse sas7bdat: {:#?}",
            e
        ))),
        None => Err(From::from(
            "Error when attempting to parse sas7bdat: Unknown return value",
        )),
    }
}