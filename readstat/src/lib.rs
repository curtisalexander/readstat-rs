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
    ReadStatData, ReadStatPath, ReadStatVar, ReadStatVarMetadata, ReadStatVarTrunc, ReadStatVarType,
};

pub use err::ReadStatError;

// StructOpt
#[derive(StructOpt, Debug)]
pub enum ReadStat {
    /// Display sas7bdat metadata
    Metadata {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        in_path: PathBuf,
    },
    /// Write rows to standard out
    Preview {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        in_path: PathBuf,
        #[structopt(long, default_value = "10")]
        rows: u32,
    },
    /// Write parsed data to file of specific type
    Data {
        #[structopt(parse(from_os_str))]
        /// Path to sas7bdat file
        in_path: PathBuf,
        /// Output file path
        #[structopt(long, parse(from_os_str))]
        out_path: Option<PathBuf>,
        /// Output file type, defaults to csv
        #[structopt(long, possible_values=&OutType::variants(), case_insensitive=true)]
        out_type: Option<OutType>,
    },
}

arg_enum! {
    #[derive(Debug, Clone, Copy, Serialize)]
    #[allow(non_camel_case_types)]
    pub enum OutType {
        csv,
    }
}

pub fn run(rs: ReadStat) -> Result<(), Box<dyn Error>> {
    env_logger::init();

    match rs {
        ReadStat::Metadata { in_path } => {
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
                Some(e) => Err(From::from(format!("Error when attempting to parse sas7bdat: {:#?}", e))),
                None => Err(From::from("Error when attempting to parse sas7bdat: Unknown return value")),
            }
        }
        // TODO: create a command line flag --raw
        //       when --raw = True, print the preview using println and strings rather than serde
        ReadStat::Preview { in_path, rows } => {
            let sas_path = PathAbs::new(in_path)?.as_path().to_path_buf();
            debug!(
                "Generating a data preview from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, Some(OutType::csv))?;
            let mut d = ReadStatData::new(rsp);
            let error = d.get_preview(rows)?;

            match FromPrimitive::from_i32(error as i32) {
                Some(ReadStatError::READSTAT_OK) => d.write(),
                Some(e) => Err(From::from(format!("Error when attempting to parse sas7bdat: {:#?}", e))),
                None => Err(From::from("Error when attempting to parse sas7bdat: Unknown return value")),
            }
        }
        ReadStat::Data {
            in_path,
            out_path,
            out_type,
        } => {
            let sas_path = PathAbs::new(in_path)?.as_path().to_path_buf();
            debug!(
                "Generating data from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, out_path, out_type)?;
            let mut d = ReadStatData::new(rsp);

            match &d {
                ReadStatData {
                    out_path: None,
                    out_type: OutType::csv,
                    ..
                } => {
                    println!("{}: a value was not provided for the parameter {}, thus displaying metadata only\n", "Warning".bright_yellow(), "--out-path".bright_cyan());

                    let error = d.get_metadata()?;

                    match FromPrimitive::from_i32(error as i32) {
                        Some(ReadStatError::READSTAT_OK) => d.write_metadata_to_stdout(),
                        Some(e) => Err(From::from(format!("Error when attempting to parse sas7bdat: {:#?}", e))),
                        None => Err(From::from("Error when attempting to parse sas7bdat: Unknown return value")),
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

                    let error = d.get_data()?;

                    match FromPrimitive::from_i32(error as i32) {
                        Some(ReadStatError::READSTAT_OK) => d.write(),
                        Some(e) => Err(From::from(format!("Error when attempting to parse sas7bdat: {:#?}", e))),
                        None => Err(From::from("Error when attempting to parse sas7bdat: Unknown return value")),
                    }
                }
            }
        }
    }
}
