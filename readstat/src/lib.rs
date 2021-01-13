#![allow(non_camel_case_types)]

use colored::Colorize;
use log::debug;
use num_traits::FromPrimitive;
use path_abs::{PathAbs, PathInfo};
use serde::Serialize;
use std::error::Error;
use std::path::PathBuf;
use structopt::clap::arg_enum;
use structopt::StructOpt;

mod cb;
mod err;
mod rs;

pub use rs::{
    ReadStatData, ReadStatPath, ReadStatVar, ReadStatVarIndexAndName, ReadStatVarTrunc,
    ReadStatVarType,
};

pub use err::ReadStatError;

// StructOpt
#[derive(StructOpt, Debug)]
#[structopt(no_version)]
pub enum ReadStat {
    /// Display sas7bdat metadata
    Metadata {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        input: PathBuf,
    },
    /// Write rows to standard out
    Preview {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        input: PathBuf,
        /// Number of rows to write
        #[structopt(long, default_value = "10")]
        rows: u32,
        /// Type of reader
        #[structopt(long, possible_values=&Reader::variants(), case_insensitive=true)]
        reader: Option<Reader>,
    },
    /// Write parsed data to file of specific type
    Data {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        input: PathBuf,
        /// Output file path
        #[structopt(short, long, parse(from_os_str))]
        output: Option<PathBuf>,
        /// Output file type, defaults to csv
        #[structopt(long, possible_values=&OutType::variants(), case_insensitive=true)]
        out_type: Option<OutType>,
        /// Number of rows to write
        #[structopt(long)]
        rows: Option<u32>,
        /// Type of reader
        #[structopt(long, possible_values=&Reader::variants(), case_insensitive=true)]
        reader: Option<Reader>,
    },
}

arg_enum! {
    #[derive(Debug, Clone, Copy, Serialize)]
    #[allow(non_camel_case_types)]
    pub enum OutType {
        csv,
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
        ReadStat::Metadata { input: in_path } => {
            let sas_path = PathAbs::new(in_path)?.as_path().to_path_buf();
            debug!(
                "Getting metadata from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, None)?;
            let mut d = ReadStatData::new(rsp);
            let error = d.get_metadata()?;

            match FromPrimitive::from_i32(error as i32) {
                Some(ReadStatError::READSTAT_OK) => d.write_metadata_to_stdout(),
                Some(e) => Err(From::from(format!(
                    "Error when attempting to parse sas7bdat: {:#?}",
                    e
                ))),
                None => Err(From::from(
                    "Error when attempting to parse sas7bdat: Unknown return value",
                )),
            }
        }
        ReadStat::Preview {
            input,
            rows,
            reader,
        } => {
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating a data preview from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, Some(OutType::csv))?;
            let mut d = match reader {
                None => ReadStatData::new(rsp),
                Some(r) => ReadStatData::new(rsp).set_reader(r),
            };
            let error = d.get_preview(rows)?;

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
        ReadStat::Data {
            input,
            output,
            out_type,
            rows,
            reader,
        } => {
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating data from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, output, out_type)?;
            let mut d = match reader {
                None => ReadStatData::new(rsp),
                Some(r) => ReadStatData::new(rsp).set_reader(r),
            };

            match &d {
                ReadStatData {
                    out_path: None,
                    out_type: OutType::csv,
                    ..
                } => {
                    println!("{}: a value was not provided for the parameter {}, thus displaying metadata only\n", "Warning".bright_yellow(), "--output".bright_cyan());

                    let error = d.get_metadata()?;

                    match FromPrimitive::from_i32(error as i32) {
                        Some(ReadStatError::READSTAT_OK) => d.write_metadata_to_stdout(),
                        Some(e) => Err(From::from(format!(
                            "Error when attempting to parse sas7bdat: {:#?}",
                            e
                        ))),
                        None => Err(From::from(
                            "Error when attempting to parse sas7bdat: Unknown return value",
                        )),
                    }
                }
                ReadStatData {
                    out_path: Some(p),
                    out_type: OutType::csv,
                    ..
                } => {
                    println!(
                        "Writing parsed data to file {}",
                        p.to_string_lossy().yellow()
                    );

                    let error = d.get_data(rows)?;

                    match FromPrimitive::from_i32(error as i32) {
                        Some(ReadStatError::READSTAT_OK) => Ok(()),
                        // Some(ReadStatError::READSTAT_OK) => d.write(),
                        Some(e) => Err(From::from(format!(
                            "Error when attempting to parse sas7bdat: {:#?}",
                            e
                        ))),
                        None => Err(From::from(
                            "Error when attempting to parse sas7bdat: Unknown return value",
                        )),
                    }
                }
            }
        }
    }
}
