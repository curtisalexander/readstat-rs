#![allow(non_camel_case_types)]

mod cb;
mod rs;

use colored::Colorize;
use dunce;
use log::debug;
use readstat_sys;
use serde::Serialize;
use std::error::Error;
use std::path::PathBuf;
use structopt::clap::arg_enum;
use structopt::StructOpt;

pub use rs::{ReadStatData, ReadStatPath, ReadStatVarMetadata, ReadStatVarTrunc, ReadStatVarType};

// StructOpt
#[derive(StructOpt, Debug)]
#[structopt(about = "Utility for sas7bdat files")]
pub struct ReadStat {
    #[structopt(parse(from_os_str))]
    /// Path to sas7bdat file
    in_path: PathBuf,
    #[structopt(subcommand)]
    cmd: Command,
}

// StructOpts subcommands
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Display sas7bdat metadata
    Metadata {},
    /// Write rows to standard out
    Preview {
        #[structopt(long, default_value = "10")]
        rows: u32,
    },
    /// Write parsed data to file of specific type
    Data {
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

    let sas_path = dunce::canonicalize(&rs.in_path)?;

    debug!(
        "Counting the number of variables within the file {}",
        &sas_path.to_string_lossy()
    );

    match rs.cmd {
        Command::Metadata {} => {
            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, None)?;
            let mut d = ReadStatData::new(rsp);
            let error = d.get_metadata()?;

            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                d.write_metadata_to_stdout()
            }
        }
        Command::Preview { rows } => {
            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, Some(OutType::csv))?;
            let mut d = ReadStatData::new(rsp);
            let error = d.get_preview(rows)?;

            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                d.write()
            }
        }
        Command::Data { out_path, out_type } => {
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

                    if error != readstat_sys::readstat_error_e_READSTAT_OK {
                        Err(From::from("Error when attempting to parse sas7bdat"))
                    } else {
                        d.write_metadata_to_stdout()
                    }
                }
                ReadStatData {
                    out_path: Some(p),
                    out_type: OutType::csv,
                    ..
                } => {
                    println!("Writing parsed data to file {}", p.to_string_lossy());

                    let error = d.get_data()?;

                    if error != readstat_sys::readstat_error_e_READSTAT_OK {
                        Err(From::from("Error when attempting to parse sas7bdat"))
                    } else {
                        d.write()
                    }
                }
            }
        }
    }
}
