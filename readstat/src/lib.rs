#![allow(non_camel_case_types)]

use colored::Colorize;
use dunce;
use log::debug;
use readstat_sys;
use serde::Serialize;
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::{Path, PathBuf};
use structopt::StructOpt;

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

// C types
#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
#[repr(C)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE,
}

// C callback functions

// TODO: May need a version of handle_metadata that only gets metadata
//       and a version that does very little and instead metadata handling occurs
//       in handle_value function
//       As an example see the below from the readstat binary
//         https://github.com/WizardMac/ReadStat/blob/master/src/bin/readstat.c#L98
pub extern "C" fn handle_metadata(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut c_void,
) -> c_int {
    let mut md = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    let rc: c_int = unsafe { readstat_sys::readstat_get_row_count(metadata) };
    let vc: c_int = unsafe { readstat_sys::readstat_get_var_count(metadata) };

    md.row_count = rc;
    md.var_count = vc;

    debug!("md struct is {:#?}", md);
    debug!("row_count is {:#?}", md.row_count);
    debug!("var_count is {:#?}", md.var_count);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_variable(
    #[allow(unused_variables)] index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    #[allow(unused_variables)] val_labels: *const c_char,
    ctx: *mut c_void,
) -> c_int {
    let md = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };

    let var_name = unsafe {
        CStr::from_ptr(readstat_sys::readstat_variable_get_name(variable))
            .to_str()
            .unwrap()
            .to_owned()
    };

    let var_type: readstat_sys::readstat_type_t =
        unsafe { readstat_sys::readstat_variable_get_type(variable) };

    debug!("md struct is {:#?}", md);
    debug!("var type pushed is {:#?}", var_type);
    debug!("var pushed is {:#?}", &var_name);

    md.vars
        .insert(ReadStatVarMetadata::new(var_index, var_name), var_type);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_value_print(
    #[allow(unused_variables)] obs_index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    value: readstat_sys::readstat_value_t,
    ctx: *mut c_void,
) -> c_int {
    let md = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };

    let val_type: readstat_sys::readstat_type_t =
        unsafe { readstat_sys::readstat_value_type(value) };

    let is_missing: c_int = unsafe { readstat_sys::readstat_value_is_system_missing(value) };

    if is_missing == 0 {
        let value: ReadStatVar = match val_type {
            readstat_sys::readstat_type_e_READSTAT_TYPE_STRING | readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF => {
                ReadStatVar::ReadStat_String(unsafe {
                    CStr::from_ptr(readstat_sys::readstat_string_value(value))
                        .to_str()
                        .unwrap()
                        .to_owned()
                })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => {
                ReadStatVar::ReadStat_i8(unsafe { readstat_sys::readstat_int8_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 => {
                ReadStatVar::ReadStat_i16(unsafe { readstat_sys::readstat_int16_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 => {
                ReadStatVar::ReadStat_i32(unsafe { readstat_sys::readstat_int32_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT => {
                ReadStatVar::ReadStat_f32(unsafe { readstat_sys::readstat_float_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE => {
                ReadStatVar::ReadStat_f64(unsafe { readstat_sys::readstat_double_value(value) })
            }
            // exhaustive
            // _ => ReadStatVarType::ReadStat_String(String::new()),
            _ => unreachable!()
        };

        match value {
            ReadStatVar::ReadStat_String(s) => print!("{}", s),
            ReadStatVar::ReadStat_i8(i) => print!("{}", i),
            ReadStatVar::ReadStat_i16(i) => print!("{}", i),
            ReadStatVar::ReadStat_i32(i) => print!("{}", i),
            ReadStatVar::ReadStat_f32(f) => print!("{:.6}", f),
            ReadStatVar::ReadStat_f64(f) => print!("{:.6}", f),
        }
    }

    if var_index == md.var_count - 1 {
        print!("\n");
    } else {
        print!("\t");
    }

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_value(
    #[allow(unused_variables)] obs_index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    value: readstat_sys::readstat_value_t,
    ctx: *mut c_void,
) -> c_int {
    let d = unsafe { &mut *(ctx as *mut ReadStatData) };
    let md = &mut d.metadata;
    let var_count = md.var_count;

    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };

    let value_type: readstat_sys::readstat_type_t =
        unsafe { readstat_sys::readstat_value_type(value) };

    let is_missing: c_int = unsafe { readstat_sys::readstat_value_is_system_missing(value) };

    if var_index == 0 {
        d.row = Vec::with_capacity(var_count as usize);
    }

    if is_missing == 0 {
        let value: ReadStatVar = match value_type {
            readstat_sys::readstat_type_e_READSTAT_TYPE_STRING | readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF => {
                ReadStatVar::ReadStat_String(unsafe {
                    CStr::from_ptr(readstat_sys::readstat_string_value(value))
                        .to_str()
                        .unwrap()
                        .to_owned()
                })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => {
                ReadStatVar::ReadStat_i8(unsafe { readstat_sys::readstat_int8_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 => {
                ReadStatVar::ReadStat_i16(unsafe { readstat_sys::readstat_int16_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 => {
                ReadStatVar::ReadStat_i32(unsafe { readstat_sys::readstat_int32_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT => {
                ReadStatVar::ReadStat_f32(unsafe { readstat_sys::readstat_float_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE => {
                ReadStatVar::ReadStat_f64(unsafe { readstat_sys::readstat_double_value(value) })
            }
            // exhaustive
            // _ => ReadStatVarType::ReadStat_String(String::new()),
            _ => unreachable!()
        };

        d.row.push(value);
    }

    if var_index == md.var_count - 1 {
        let row = d.row.clone();
        d.rows.push(row);
        d.row.clear(); 
    }

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

// Structs
#[derive(Debug, Serialize)]
pub struct ReadStatData {
    pub metadata: ReadStatMetadata,
    pub row: Vec<ReadStatVar>,
    pub rows: Vec<Vec<ReadStatVar>>,
}

impl ReadStatData {
    pub fn new(md: ReadStatMetadata) -> Self {
        Self {
            metadata: md,
            row: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn get_data(&mut self) -> Result<u32, Box<dyn Error>> {
        let path = &self.metadata.path;
        let cpath = path_to_cstring(&path)?;
        debug!("Path as C string is {:?}", cpath);
        let ppath = cpath.as_ptr();

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let _ = ReadStatParser::new()
            .set_metadata_handler(Some(handle_metadata))?
            .set_variable_handler(Some(handle_variable))?
            .set_value_handler(Some(handle_value))?
            .parse_sas7bdat(ppath, ctx)?;

        Ok(error)
    }

    pub fn write(&self, out: PathBuf) -> Result<(), Box<dyn Error>> {
        let mut wtr = csv::WriterBuilder::new()
            .quote_style(csv::QuoteStyle::Always)
            .from_path(out)?;

        let vars: Vec<String> = self.metadata.vars.iter()
            .map(|(k, _)| k.var_name.clone())
            .collect();

        wtr.serialize(vars)?;

        for r in &self.rows {
            wtr.serialize(r)?;
        }
        wtr.flush()?;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct ReadStatMetadata {
    pub path: PathBuf,
    pub row_count: c_int,
    pub var_count: c_int,
    pub vars: BTreeMap<ReadStatVarMetadata, readstat_sys::readstat_type_t>,
}

impl ReadStatMetadata {
    pub fn new() -> Self {
        Self {
            path: PathBuf::new(),
            row_count: 0,
            var_count: 0,
            vars: BTreeMap::new(),
        }
    }

    pub fn set_path(self, path: PathBuf) -> Self {
        Self { path: path, ..self }
    }

    pub fn get_metadata(&mut self) -> Result<u32, Box<dyn Error>> {
        let path = &self.path;
        let cpath = path_to_cstring(&path)?;
        debug!("Path as C string is {:?}", cpath);
        let ppath = cpath.as_ptr();

        let ctx = self as *mut ReadStatMetadata as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let _ = ReadStatParser::new()
            .set_metadata_handler(Some(handle_metadata))?
            .set_variable_handler(Some(handle_variable))?
            .parse_sas7bdat(ppath, ctx)?;

        Ok(error)
    }

    pub fn print_data(&mut self) -> Result<u32, Box<dyn Error>> {
        let path = &self.path;
        let cpath = path_to_cstring(&path)?;
        debug!("Path as C string is {:?}", cpath);
        let ppath = cpath.as_ptr();

        let ctx = self as *mut ReadStatMetadata as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let _ = ReadStatParser::new()
            .set_metadata_handler(Some(handle_metadata))?
            .set_variable_handler(Some(handle_variable))?
            .set_value_handler(Some(handle_value_print))?
            .parse_sas7bdat(ppath, ctx)?;

        Ok(error)
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Ord, PartialOrd, Serialize)]
pub struct ReadStatVarMetadata {
    pub var_index: c_int,
    pub var_name: String,
}

impl ReadStatVarMetadata {
    pub fn new(var_index: c_int, var_name: String) -> Self {
        Self {
            var_index,
            var_name,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum ReadStatVar {
    ReadStat_String(String),
    ReadStat_i8(i8),
    ReadStat_i16(i16),
    ReadStat_i32(i32),
    ReadStat_f32(f32),
    ReadStat_f64(f64),
}

impl std::fmt::Display for ReadStatVar {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ReadStatVar::ReadStat_String(s) => s.to_string(),
                ReadStatVar::ReadStat_i8(i) => i.to_string(),
                ReadStatVar::ReadStat_i16(i) => i.to_string(),
                ReadStatVar::ReadStat_i32(i) => i.to_string(),
                ReadStatVar::ReadStat_f32(f) => f.to_string(),
                ReadStatVar::ReadStat_f64(f) => f.to_string(),
            }
        )
    }
}

struct ReadStatParser {
    parser: *mut readstat_sys::readstat_parser_t,
}

impl ReadStatParser {
    fn new() -> Self {
        let parser: *mut readstat_sys::readstat_parser_t =
            unsafe { readstat_sys::readstat_parser_init() };

        Self { parser }
    }

    fn set_metadata_handler(
        self,
        metadata_handler: readstat_sys::readstat_metadata_handler,
    ) -> Result<Self, Box<dyn Error>> {
        let set_metadata_handler_error =
            unsafe { readstat_sys::readstat_set_metadata_handler(self.parser, metadata_handler) };

        debug!(
            "After setting metadata handler, error ==> {}",
            &set_metadata_handler_error
        );

        if set_metadata_handler_error == readstat_sys::readstat_error_e_READSTAT_OK {
            Ok(self)
        } else {
            Err(From::from("Unable to set metadata handler"))
        }
    }

    fn set_variable_handler(
        self,
        variable_handler: readstat_sys::readstat_variable_handler,
    ) -> Result<Self, Box<dyn Error>> {
        let set_variable_handler_error =
            unsafe { readstat_sys::readstat_set_variable_handler(self.parser, variable_handler) };

        debug!(
            "After setting variable handler, error ==> {}",
            &set_variable_handler_error
        );

        if set_variable_handler_error == readstat_sys::readstat_error_e_READSTAT_OK {
            Ok(self)
        } else {
            Err(From::from("Unable to set variable handler"))
        }
    }

    fn set_value_handler(
        self,
        value_handler: readstat_sys::readstat_value_handler,
    ) -> Result<Self, Box<dyn Error>> {
        let set_value_handler_error =
            unsafe { readstat_sys::readstat_set_value_handler(self.parser, value_handler) };

        debug!(
            "After setting value handler, error ==> {}",
            &set_value_handler_error
        );

        if set_value_handler_error == readstat_sys::readstat_error_e_READSTAT_OK {
            Ok(self)
        } else {
            Err(From::from("Unable to set value handler"))
        }
    }

    fn parse_sas7bdat(
        self,
        path: *const c_char,
        user_ctx: *mut c_void,
    ) -> Result<Self, Box<dyn Error>> {
        let parse_sas7bdat_error: readstat_sys::readstat_error_t =
            unsafe { readstat_sys::readstat_parse_sas7bdat(self.parser, path, user_ctx) };

        debug!(
            "After calling parse sas7bdat, error ==> {}",
            &parse_sas7bdat_error
        );

        if parse_sas7bdat_error == readstat_sys::readstat_error_e_READSTAT_OK {
            Ok(self)
        } else {
            Err(From::from("Unable to parse sas7bdat"))
        }
    }
}

impl Drop for ReadStatParser {
    fn drop(&mut self) {
        debug!("Freeing parser");

        unsafe { readstat_sys::readstat_parser_free(self.parser) };
    }
}

// Utility functions
#[cfg(unix)]
pub fn path_to_cstring(path: &Path) -> Result<CString, Box<dyn Error>> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    CString::new(bytes).map_err(|_| From::from("Invalid path"))
}

#[cfg(not(unix))]
pub fn path_to_cstring(path: &Path) -> Result<CString, InvalidPath> {
    let rust_str = path.as_os_str().as_str().ok_or(InvalidPath)?;
    let bytes = path.as_os_str().as_bytes();
    CString::new(rust_str).map_err(|_| From::from("Invalid path"))
}

// Run
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
        },
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
                    let out_dir = dunce::canonicalize(PathBuf::from("/home/calex/code/readstat-rs/data")).unwrap();
                    let out_file = out_dir.join("cars_serde.csv");
                    println!("out_file is {}", out_file.to_string_lossy());
                    d.write(out_file)?;
                    Ok(())
                }
            }
        }
    }
}
