#![allow(non_camel_case_types)]

use colored::Colorize;
use log::debug;
// use num_cpus;
use path_abs::{PathAbs, PathInfo};
use rayon::prelude::*;
use serde::Serialize;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::clap::arg_enum;
use structopt::StructOpt;

mod cb;
mod err;
mod formats;
mod rs_data;
mod rs_metadata;
mod rs_parser;
mod rs_path;
mod rs_write;

pub use err::ReadStatError;
pub use rs_data::ReadStatData;
pub use rs_metadata::{
    ReadStatCompress, ReadStatEndian, ReadStatFormatClass, ReadStatMetadata, ReadStatVar,
    ReadStatVarMetadata, ReadStatVarType, ReadStatVarTypeClass,
};
pub use rs_path::ReadStatPath;
pub use rs_write::ReadStatWriter;

// Default stream rows is 50000;
const STREAM_ROWS: u32 = 50000;

// StructOpt
#[derive(StructOpt, Debug)]
/// 💾 Command-line tool for working with SAS binary files; 🦀 Rust wrapper of ReadStat C library
/// {n}    Display metadata{n}    Preview data{n}    Convert SAS file to csv, feather (or the Arrow IPC format), ndjson, or parquet format
pub enum ReadStat {
    /// Display sas7bdat metadata
    Metadata {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        input: PathBuf,
        /// Display sas7bdat metadata as json
        #[structopt(long)]
        as_json: bool,
        /// Do not display progress bar
        #[structopt(long)]
        no_progress: bool,
        /// Skip calculating row count{n}Can speed up parsing if only interested in variable metadata
        #[structopt(long)]
        skip_row_count: bool,
    },
    /// Preview sas7bdat data
    Preview {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        input: PathBuf,
        /// Number of rows to write
        #[structopt(long, default_value = "10")]
        rows: u32,
        /// Type of reader{n}    mem = read all data into memory{n}    stream = read at most stream-rows into memory{n}Defaults to stream
        #[structopt(long, possible_values=&Reader::variants(), case_insensitive=true)]
        reader: Option<Reader>,
        /// Number of rows to stream (read into memory) at a time{n}Note: ↑ rows = ↑ memory usage{n}Ignored if reader is set to mem{n}Defaults to 50,000 rows
        #[structopt(long)]
        stream_rows: Option<u32>,
        /// Do not display progress bar
        #[structopt(long)]
        no_progress: bool,
    },
    /// Convert sas7bdat data to csv, feather (or the Arrow IPC format), ndjson, or parquet format
    Data {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        input: PathBuf,
        /// Output file path
        #[structopt(short = "o", long, parse(from_os_str))]
        output: Option<PathBuf>,
        /// Output file format{n}Defaults to csv
        #[structopt(short="f", long, possible_values=&Format::variants(), case_insensitive=true)]
        format: Option<Format>,
        /// Number of rows to write
        #[structopt(long)]
        rows: Option<u32>,
        /// Type of reader{n}    mem = read all data into memory{n}    stream = read at most stream-rows into memory{n}Defaults to stream
        #[structopt(long, possible_values=&Reader::variants(), case_insensitive=true)]
        reader: Option<Reader>,
        /// Number of rows to stream (read into memory) at a time{n}Note: ↑ rows = ↑ memory usage{n}Ignored if reader is set to mem{n}Defaults to 50,000 rows
        #[structopt(long)]
        stream_rows: Option<u32>,
        /// Do not display progress bar
        #[structopt(long)]
        no_progress: bool,
        /// Overwrite output file if it already exists
        #[structopt(long)]
        overwrite: bool,
        /// Convert sas7bdat data in parallel{n}    Number of threads to utilize
        #[structopt(long)]
        parallel: Option<usize>,
    },
}

arg_enum! {
    #[derive(Debug, Clone, Copy, Serialize)]
    #[allow(non_camel_case_types)]
    pub enum Format {
        csv,
        feather,
        ndjson,
        parquet
    }
}

arg_enum! {
    #[derive(Debug, Clone, Copy, Serialize)]
    #[allow(non_camel_case_types)]
    pub enum Reader {
        mem,
        stream,
    }
}

fn build_offsets(row_count: u32, stream_rows: u32) -> Result<Vec<u32>, Box<dyn Error>> {
    // Get number of chunks
    let chunks = if stream_rows < row_count {
        if row_count % stream_rows == 0 {
            row_count / stream_rows
        } else {
            (row_count / stream_rows) + 1
        }
    } else {
        1
    };

    // Allocate and populate a vector for the offsets
    let mut offsets: Vec<u32> = Vec::with_capacity(chunks as usize);

    for c in 0..=chunks {
        if c == 0 {
            offsets.push(0);
        } else if c == chunks {
            offsets.push(row_count);
        } else {
            offsets.push(c * stream_rows);
        }
    }

    Ok(offsets)
}

pub fn run(rs: ReadStat) -> Result<(), Box<dyn Error>> {
    env_logger::init();

    match rs {
        ReadStat::Metadata {
            input: in_path,
            as_json,
            no_progress: _,
            skip_row_count,
        } => {
            // validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(in_path)?.as_path().to_path_buf();
            debug!(
                "Getting metadata from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and format determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, None, false, false)?;

            // instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();
            md.read_metadata(&rsp, skip_row_count)?;

            // Write metadata
            ReadStatWriter::new().write_metadata(&md, &rsp, as_json)?;

            // return
            Ok(())
        }
        ReadStat::Preview {
            input,
            rows,
            reader,
            stream_rows,
            no_progress,
        } => {
            // validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating a data preview from the file {}",
                &sas_path.to_string_lossy()
            );

            // output and format determine the type of writing to be performed
            let rsp = ReadStatPath::new(sas_path, None, Some(Format::csv), false, false)?;

            // instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();
            md.read_metadata(&rsp, false)?;

            // Determine row count
            let total_rows_to_process = std::cmp::min(rows, md.row_count as u32);

            // Determine stream row count
            // 📝 Default stream rows set to 50,000
            let total_rows_to_stream = match reader {
                Some(Reader::stream) | None => match stream_rows {
                    Some(s) => s,
                    None => STREAM_ROWS,
                },
                Some(Reader::mem) => total_rows_to_process,
            };

            // initialize Mutex to contain total rows processed
            let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            // let total_rows_processed = Arc::new(Mutex::new(0 as usize));

            // Build up offsets
            let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;
            let offsets_pairs = offsets.windows(2);
            let pairs_cnt = *(&offsets_pairs.len());

            // Initialize writing
            let mut wtr = ReadStatWriter::new();

            // Process data in batches (i.e. stream chunks of rows)
            // Read data - for each iteration create a new instance of ReadStatData
            for (i, w) in offsets_pairs.enumerate() {
                let row_start = w[0];
                let row_end = w[1];

                let mut d = ReadStatData::new()
                    .set_no_progress(no_progress)
                    .set_total_rows_to_process(total_rows_to_process as usize)
                    .set_total_rows_processed(total_rows_processed.clone())
                    .init(md.clone(), row_start, row_end);

                // read
                d.read_data(&rsp)?;

                // if last write then need to finish file
                if i == pairs_cnt {
                    wtr.set_finish(true);
                }

                // write
                wtr.write(&d, &rsp)?;
            }

            // return
            Ok(())

            // progress bar
            /*
            if let Some(pb) = &d.pb {
                pb.finish_at_current_pos()
            };
            */
        }
        ReadStat::Data {
            input,
            output,
            format,
            rows,
            reader,
            stream_rows,
            no_progress,
            overwrite,
            parallel: _,
        } => {
            // validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating data from the file {}",
                &sas_path.to_string_lossy()
            );

            // output and format determine the type of writing to be performed
            let rsp = ReadStatPath::new(sas_path, output, format, overwrite, false)?;

            // instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();
            md.read_metadata(&rsp, false)?;

            // if no output path then only read metadata; otherwise read data
            match &rsp.out_path {
                None => {
                    println!("{}: a value was not provided for the parameter {}, thus displaying metadata only\n", "Warning".bright_yellow(), "--output".bright_cyan());

                    // Get metadata

                    // instantiate ReadStatMetadata
                    let mut md = ReadStatMetadata::new();
                    md.read_metadata(&rsp, false)?;

                    // Write metadata
                    ReadStatWriter::new().write_metadata(&md, &rsp, false)?;

                    //get_metadata(&mut d, false, false)
                    Ok(())
                }
                Some(p) => {
                    println!(
                        "Writing parsed data to file {}",
                        p.to_string_lossy().bright_yellow()
                    );

                    // Determine row count
                    let total_rows_to_process = if let Some(r) = rows {
                        std::cmp::min(r, md.row_count as u32)
                    } else {
                        md.row_count as u32
                    };

                    // Determine stream row count
                    // 📝 Default stream rows set to 50,000
                    let total_rows_to_stream = match reader {
                        Some(Reader::stream) | None => match stream_rows {
                            Some(s) => s,
                            None => STREAM_ROWS,
                        },
                        Some(Reader::mem) => total_rows_to_process,
                    };

                    // initialize Mutex to contain total rows processed
                    // let total_rows_processed = Arc::new(Mutex::new(0 as usize));
                    let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

                    // Build up offsets
                    let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;

                    // TODO
                    // Create a parallel iterator
                    //let offsets_pairs = offsets.par_windows(2);
                    let offsets_pairs = offsets.windows(2);
                    let pairs_cnt = *(&offsets_pairs.len());

                    // Initialize writing
                    let mut wtr = ReadStatWriter::new();

                    // Process data in batches (i.e. stream chunks of rows)
                    // Read data - for each iteration create a new instance of ReadStatData
                    for (i, w) in offsets_pairs.enumerate() {
                        let row_start = w[0];
                        let row_end = w[1];

                        let mut d = ReadStatData::new()
                            .set_no_progress(no_progress)
                            .set_total_rows_to_process(total_rows_to_process as usize)
                            .set_total_rows_processed(total_rows_processed.clone())
                            .init(md.clone(), row_start, row_end);

                        // read
                        d.read_data(&rsp)?;

                        // if last write then need to finish file
                        if i == (pairs_cnt-1) {
                            wtr.set_finish(true);
                        }

                        // write
                        wtr.write(&d, &rsp)?;
                    }

                    // return
                    Ok(())

                    // progress bar
                    /*
                    if let Some(pb) = &d.pb {
                        pb.finish_at_current_pos()
                    };
                    */

                    // get and optionally write data - single or multi-threaded
                    /*
                    if let Some(_p) = parallel {
                        let cpu_logical_count: usize = num_cpus::get();
                        let cpu_physical_count: usize = num_cpus::get_physical();
                        println!("Logical count {:?}", cpu_logical_count);
                        println!("Physical count {:?}", cpu_physical_count);

                        d.get_row_count()?;
                        println!("Row count {:?}", d.metadata.row_count);
                        Ok(())
                    */
                }
            }
        }
    }
}
