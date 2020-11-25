#![allow(non_camel_case_types)]

use colored::Colorize;
use dunce;
use log::debug;
use readstat_sys;
use std::error::Error;
use std::path::PathBuf;
use structopt::StructOpt;

mod cb;
mod rs;
mod util;

pub use rs::{ReadStatData, ReadStatMetadata, ReadStatVar, ReadStatVarMetadata, ReadStatVarTrunc};

// StructOpt
#[derive(StructOpt, Debug)]
#[structopt(about = "Utilities for sas7bdat files")]
pub struct ReadStat {
    #[structopt(parse(from_os_str))]
    /// Path to sas7bdat file
    file: PathBuf,
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(StructOpt, Debug)]
pub enum Command {
    /// Get sas7bdat metadata
    Metadata {},
    PrintData {},
    Data {},
}

pub fn run(rs: ReadStat) -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // TODO: validate path exists and has sas7bdat extension
    let sas_path = dunce::canonicalize(&rs.file)?;

    debug!(
        "Counting the number of variables within the file {}",
        sas_path.to_string_lossy()
    );

    match rs.cmd {
        Command::Metadata {} => {
            let mut md = ReadStatMetadata::new().set_path(sas_path);
            let error = md.get_metadata()?;

            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                println!(
                    "Metadata for the file {}\n",
                    md.path.to_string_lossy().yellow()
                );
                println!("{}: {}", "Row count".green(), md.row_count);
                println!("{}: {}", "Variable count".red(), md.var_count);
                println!("{}:", "Variable names".blue());
                for (k, v) in md.vars.iter() {
                    println!(
                        "{}: {} of type {}",
                        k.var_index,
                        k.var_name.bright_purple(),
                        v
                    );
                }
                Ok(())
            }
        }
        Command::PrintData {} => {
            let mut md = ReadStatMetadata::new().set_path(sas_path);
            let error = md.get_metadata()?;

            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                for (k, _) in md.vars.iter() {
                    if k.var_index == md.var_count - 1 {
                        println!("{}", k.var_name);
                    } else {
                        print!("{}\t", k.var_name);
                    }
                }
                // Write data to standard out
                let error = md.print_data()?;

                if error != readstat_sys::readstat_error_e_READSTAT_OK {
                    Err(From::from("Error when attempting to parse sas7bdat"))
                } else {
                    Ok(())
                }
            }
        }
        Command::Data {} => {
            let mut md = ReadStatMetadata::new().set_path(sas_path);
            let error = md.get_metadata()?;

            if error != readstat_sys::readstat_error_e_READSTAT_OK {
                Err(From::from("Error when attempting to parse sas7bdat"))
            } else {
                /*
                for (k, _) in md.vars.iter() {
                    if k.var_index == md.var_count - 1 {
                        println!("{}", k.var_name);
                    } else {
                        print!("{}\t", k.var_name);
                    }
                }
                */
                // Get data
                let mut d = ReadStatData::new(md);
                let error = d.get_data()?;

                if error != readstat_sys::readstat_error_e_READSTAT_OK {
                    Err(From::from("Error when attempting to parse sas7bdat"))
                } else {
                    /*
                    for row in d.rows.iter() {
                        for (i, v) in row.iter().enumerate() {
                            match v {
                                ReadStatVar::ReadStat_String(s) => print!("{}", s),
                                ReadStatVar::ReadStat_i8(i) => print!("{}", i),
                                ReadStatVar::ReadStat_i16(i) => print!("{}", i),
                                ReadStatVar::ReadStat_i32(i) => print!("{}", i),
                                ReadStatVar::ReadStat_f32(f) => print!("{:.6}", f),
                                ReadStatVar::ReadStat_f64(f) => print!("{:.6}", f),
                            }
                            if i == (d.metadata.var_count - 1) as usize {
                                print!("\n");
                            } else {
                                print!("\t");
                            }
                        }
                    }
                    */
                    // Ok(())
                    let out_dir =
                        dunce::canonicalize(PathBuf::from("/home/calex/code/readstat-rs/data"))
                            .unwrap();
                    let out_file = out_dir.join("cars_serde.csv");
                    println!("out_file is {}", out_file.to_string_lossy());
                    d.write(out_file)?;
                    Ok(())
                }
            }
        }
    }
}
