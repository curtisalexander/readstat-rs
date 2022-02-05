use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use num_traits::FromPrimitive;

use crate::{ReadStatData, ReadStatError, Reader};

pub fn build_offsets(
    reader: Reader,
    row_count: u32,
    stream_rows: u32,
    row_limit: Option<u32>,
) -> Result<Vec<u32>, Box<dyn Error>> {
    // Get other row counts
    let sr = match reader {
        Reader::stream => stream_rows,
        Reader::mem => row_count,
    };
    let rc = if let Some(r) = row_limit {
        std::cmp::min(r, row_count)
    } else {
        row_count
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

pub fn get_data_from_offsets(
    d: &mut ReadStatData,
    start: u32,
    end: u32,
    total_rows_to_process: usize,
    total_rows_processed: Arc<Mutex<usize>>,
) -> Result<(), Box<dyn Error>> {
    // how many rows to process?
    d.batch_rows_to_process = (end - start) as usize;
    d.batch_row_start = start as usize;
    d.batch_row_end = end as usize;

    // process the rows!
    let error = d.get_data(Some(end - start), Some(start))?;
    *total_rows_processed.lock().unwrap() += d.batch_rows_processed;

    if total_rows_to_process == *total_rows_processed.lock().unwrap() {
        d.finish = true;
    }

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

pub fn get_metadata(
    d: &mut ReadStatData,
    skip_row_count: bool,
    as_json: bool,
) -> Result<(), Box<dyn Error>> {
    let error = d.get_metadata(skip_row_count)?;

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
