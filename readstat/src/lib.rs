#![allow(non_camel_case_types)]

use colored::Colorize;
use log::debug;
use num_cpus;
use path_abs::{PathAbs, PathInfo};
use serde::Serialize;
use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use structopt::clap::arg_enum;
use structopt::StructOpt;

mod cb;
mod err;
mod formats;
mod drive;
mod rs_data;
mod rs_metadata;
mod rs_parser;
mod rs_path;

pub use err::ReadStatError;
pub use drive::{build_offsets, get_data_from_offsets, get_metadata, get_preview};
pub use rs_data::ReadStatData;
pub use rs_metadata::{
    ReadStatCompress, ReadStatEndian, ReadStatFormatClass, ReadStatMetadata, ReadStatVar,
    ReadStatVarMetadata, ReadStatVarType, ReadStatVarTypeClass,
};
pub use rs_path::ReadStatPath;

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

pub fn run(rs: ReadStat) -> Result<(), Box<dyn Error>> {
    env_logger::init();

    match rs {
        ReadStat::Metadata {
            input: in_path,
            as_json,
            no_progress,
            skip_row_count,
        } => {
            let sas_path = PathAbs::new(in_path)?.as_path().to_path_buf();
            debug!(
                "Getting metadata from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and format determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, None, false)?;

            // instantiate ReadStatData
            let mut d = ReadStatData::new(rsp).set_no_progress(no_progress);

            // Get metadata
            get_metadata(&mut d, skip_row_count, as_json)
        }
        ReadStat::Preview {
            input,
            rows,
            reader,
            stream_rows,
            no_progress,
        } => {
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating a data preview from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and format determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, Some(Format::csv), false)?;

            // instantiate ReadStatData
            let mut d = ReadStatData::new(rsp)
                .set_reader(reader)
                .set_stream_rows(stream_rows)
                .set_row_limit(Some(rows))
                .set_no_progress(no_progress);

            // Get preview
            get_preview(&mut d, rows)

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
            parallel,
        } => {
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating data from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, output, format, overwrite)?;

            // instantiate ReadStatData
            let mut d = ReadStatData::new(rsp)
                .set_reader(reader)
                .set_stream_rows(stream_rows)
                .set_row_limit(rows)
                .set_no_progress(no_progress);

            match &d {
                ReadStatData { out_path: None, .. } => {
                    println!("{}: a value was not provided for the parameter {}, thus displaying metadata only\n", "Warning".bright_yellow(), "--output".bright_cyan());

                    // Get metadata
                    get_metadata(&mut d, false, false)
                }
                ReadStatData {
                    out_path: Some(p), ..
                } => {
                    println!(
                        "Writing parsed data to file {}",
                        p.to_string_lossy().bright_yellow()
                    );

                    // Determine row count
                    d.get_row_count()?;
                    let total_rows_to_process = if let Some(r) = d.row_limit {
                        std::cmp::min(r, d.metadata.row_count as usize)
                    } else {
                        d.metadata.row_count as usize
                    };
                    let total_rows_processed = Arc::new(Mutex::new(0));

                    // Build up offsets
                    let offsets = build_offsets(d.reader, d.metadata.row_count as u32, d.stream_rows, rows)?;
                    let offsets_pairs = offsets.windows(2);

                    // Get data
                    for w in offsets_pairs {
                        get_data_from_offsets(&mut d, w[0], w[1], total_rows_to_process, Arc::clone(&total_rows_processed))?;
                    }

                    // progress bar
                    if let Some(pb) = &d.pb {
                        pb.finish_at_current_pos()
                    };

                    Ok(())

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
