#![allow(non_camel_case_types)]

mod cb;
mod rs;

use colored::Colorize;
use dunce;
use log::debug;
use path_clean::PathClean;
use readstat_sys;
use serde::Serialize;
use std::env;
use std::error::Error;
use std::ffi::CString;
use std::path::PathBuf;
use structopt::clap::arg_enum;
use structopt::StructOpt;

pub use rs::{
    ReadStatData, ReadStatVar, ReadStatVarMetadata, ReadStatVarTrunc, ReadStatVarType
};

// StructOpt
#[derive(StructOpt, Debug)]
#[structopt(about = "Utility for sas7bdat files")]
pub struct ReadStat {
    #[structopt(parse(from_os_str))]
    /// Path to sas7bdat file
    in_file: PathBuf,
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
        /// Output type, defaults to csv
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

#[derive(Debug, Clone)]
pub struct ReadStatPath {
    pub path: PathBuf,
    pub extension: String,
    pub cstring_path: CString,
    pub out_path: Option<PathBuf>,
    pub out_type: OutType,
}

impl ReadStatPath {
    pub fn new(
        path: PathBuf,
        out_path: Option<PathBuf>,
        out_type: Option<OutType>,
    ) -> Result<Self, Box<dyn Error>> {
        let p = Self::validate_path(path)?;
        let ext = Self::validate_extension(&p)?;
        let csp = Self::path_to_cstring(&p)?;
        let op: Option<PathBuf> = Self::validate_out_path(out_path)?;
        let ot = Self::validate_out_type(out_type)?;

        Ok(Self {
            path: p,
            extension: ext,
            cstring_path: csp,
            out_path: op,
            out_type: ot,
        })
    }

    #[cfg(unix)]
    pub fn path_to_cstring(path: &PathBuf) -> Result<CString, Box<dyn Error>> {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        CString::new(bytes).map_err(|_| From::from("Invalid path"))
    }

    #[cfg(not(unix))]
    pub fn path_to_cstring(path: &PathBuf) -> Result<CString, Box<dyn Error>> {
        let rust_str = &self
            .path
            .as_os_str()
            .as_str()
            .ok_or(Err(From::from("Invalid path")))?;
        // let bytes = &self.path.as_os_str().as_bytes();
        CString::new(rust_str).map_err(|_| From::from("Invalid path"))
    }

    fn validate_extension(path: &PathBuf) -> Result<String, Box<dyn Error>> {
        path.extension()
            .and_then(|e| e.to_str())
            .and_then(|e| Some(e.to_owned()))
            .map_or(
                Err(From::from(format!(
                    "File {} does not have an extension!",
                    path.to_string_lossy().yellow()
                ))),
                |e| Ok(e),
            )
    }

    fn validate_path(p: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
        let abs_path = if p.is_absolute() {
            p
        } else {
            env::current_dir()?.join(p)
        };
        let abs_path = abs_path.clean();

        if abs_path.exists() {
            Ok(abs_path)
        } else {
            Err(From::from(format!(
                "File {} does not exist!",
                abs_path.to_string_lossy().yellow()
            )))
        }
    }

    fn validate_out_path(p: Option<PathBuf>) -> Result<Option<PathBuf>, Box<dyn Error>> {
        match p {
            None => Ok(None),
            Some(p) => {
                let abs_path = if p.is_absolute() {
                    p
                } else {
                    env::current_dir()?.join(p)
                };
                let abs_path = abs_path.clean();

                match abs_path.parent() {
                    None => Err(From::from(format!("The parent directory of the value of the parameter  --out-file ({}) does not exist", &abs_path.to_string_lossy()))),
                    Some(parent) => {
                        if parent.exists() {
                            Ok(Some(abs_path))
                        } else {
                            Err(From::from(format!("The parent directory of the value of the parameter  --out-file ({}) does not exist", &parent.to_string_lossy())))
                        }
                    }
                }
            }
        }
    }

    fn validate_out_type(t: Option<OutType>) -> Result<OutType, Box<dyn Error>> {
        match t {
            None => Ok(OutType::csv),
            Some(t) => Ok(t)
        }
    }
}

pub fn run(rs: ReadStat) -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let sas_path = dunce::canonicalize(&rs.in_file)?;

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
        Command::Preview { rows: _ } => {
            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, Some(OutType::csv))?;
            let mut d = ReadStatData::new(rsp);
            let error = d.get_metadata()?;

            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                // TODO: create a preview writer
                // Write header
                for (k, _) in d.vars.iter() {
                    if k.var_index == d.var_count - 1 {
                        println!("{}", k.var_name);
                    } else {
                        print!("{}\t", k.var_name);
                    }
                }
                Ok(())
            }
        }
        Command::Data { out_path, out_type } => {
            // out_path and out_type determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, out_path, out_type)?;
            let mut d = ReadStatData::new(rsp);

            match &d {
                ReadStatData { out_path: None, out_type: OutType::csv, .. } => {
                    println!("A value was not provided for the parameter --out-file, thus displaying metadata only");

                    let error = d.get_metadata()?;

                    if error != readstat_sys::readstat_error_e_READSTAT_OK {
                        Err(From::from("Error when attempting to parse sas7bdat"))
                    } else {
                        d.write_metadata_to_stdout()
                    }
                },
                ReadStatData { out_path: Some(p), out_type: OutType::csv, .. } => {
                    println!("Writing parsed data to file {},", p.to_string_lossy());
                    
                    let error = d.get_data()?;

                    if error != readstat_sys::readstat_error_e_READSTAT_OK {
                        Err(From::from("Error when attempting to parse sas7bdat"))
                    } else {
                        d.write()
                    }
                },
            }

            // let error = d.get_data()?;
            /*
            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                // TODO: Replace hard-coded path with dynamic path provided by user
                let out_dir =
                    dunce::canonicalize(PathBuf::from("/home/calex/code/readstat-rs/data"))
                        .unwrap();
                let out_path = out_dir.join("cars_serde.csv");
                println!("out_path is {}", out_path.to_string_lossy());

                // Write to file (using serde)
                d.write()
            }
            */
        }
    }
}
