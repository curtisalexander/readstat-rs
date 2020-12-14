#![allow(non_camel_case_types)]

mod cb;
mod rs;

use colored::Colorize;
use log::debug;
use path_abs::{PathAbs, PathInfo};
use readstat_sys;
use serde::Serialize;
use std::error::Error;
use std::path::PathBuf;
use structopt::clap::arg_enum;
use structopt::StructOpt;

pub use rs::{
    ReadStatData, ReadStatPath, ReadStatVar, ReadStatVarMetadata, ReadStatVarTrunc, ReadStatVarType,
};

// StructOpts subcommands
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

            if error != readstat_sys::readstat_error_e_READSTAT_OK as u32 {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                d.write_metadata_to_stdout()
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

            if error != readstat_sys::readstat_error_e_READSTAT_OK as u32 {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                d.write()
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

                    if error != readstat_sys::readstat_error_e_READSTAT_OK as u32 {
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
                    println!(
                        "Writing parsed data to file {}",
                        p.to_string_lossy().yellow()
                    );

                    let error = d.get_data()?;

                    if error != readstat_sys::readstat_error_e_READSTAT_OK as u32 {
                        Err(From::from("Error when attempting to parse sas7bdat"))
                    } else {
                        d.write()
                    }
                }
            }
        }
    }
}
