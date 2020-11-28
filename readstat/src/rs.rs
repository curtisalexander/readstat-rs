use colored::Colorize;
use std::env;
use log::debug;
use num_derive::FromPrimitive;
use path_clean::PathClean;
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::CString;
use std::io::stdout;
use std::os::raw::{c_char, c_int, c_void};
use std::path::PathBuf;

use crate::cb;
use crate::OutType;

const DIGITS: usize = 14;

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

#[derive(Debug, Clone)]
pub enum ReadStatVar {
    ReadStat_String(String),
    ReadStat_i8(i8),
    ReadStat_i16(i16),
    ReadStat_i32(i32),
    ReadStat_f32(f32),
    ReadStat_f64(f64),
}

impl Serialize for ReadStatVar {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ReadStatVarTrunc::from(self).serialize(s)
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum ReadStatVarTrunc {
    ReadStat_String(String),
    ReadStat_i8(i8),
    ReadStat_i16(i16),
    ReadStat_i32(i32),
    ReadStat_f32(f32),
    ReadStat_f64(f64),
}

impl<'a> From<&'a ReadStatVar> for ReadStatVarTrunc {
    fn from(other: &'a ReadStatVar) -> Self {
        match other {
            ReadStatVar::ReadStat_String(s) => Self::ReadStat_String(s.to_owned()),
            ReadStatVar::ReadStat_i8(i) => Self::ReadStat_i8(*i),
            ReadStatVar::ReadStat_i16(i) => Self::ReadStat_i16(*i),
            ReadStatVar::ReadStat_i32(i) => Self::ReadStat_i32(*i),
            // Format as string to truncate float to only contain 14 decimal digits
            // Parse back into float so that the trailing zeroes are trimmed when serializing
            ReadStatVar::ReadStat_f32(f) => {
                Self::ReadStat_f32(format!("{1:.0$}", DIGITS, f).parse::<f32>().unwrap())
            }
            ReadStatVar::ReadStat_f64(f) => {
                Self::ReadStat_f64(format!("{1:.0$}", DIGITS, f).parse::<f64>().unwrap())
            }
        }
    }
}

#[derive(Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarType {
    String = readstat_sys::readstat_type_e_READSTAT_TYPE_STRING as isize,
    Int8 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 as isize,
    Int16 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 as isize,
    Int32 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 as isize,
    Float = readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT as isize,
    Double = readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE as isize,
    StringRef = readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF as isize,
    Unknown,
}

#[derive(Debug, Serialize)]
pub struct ReadStatData {
    pub path: PathBuf,
    pub cstring_path: CString,
    pub out_path: Option<PathBuf>,
    pub out_type: OutType,
    pub row_count: c_int,
    pub var_count: c_int,
    pub vars: BTreeMap<ReadStatVarMetadata, ReadStatVarType>,
    pub row: Vec<ReadStatVar>,
    pub rows: Vec<Vec<ReadStatVar>>,
}

impl ReadStatData {
    pub fn new(rsp: ReadStatPath) -> Self {
        Self {
            path: rsp.path,
            cstring_path: rsp.cstring_path,
            out_path: rsp.out_path,
            out_type: rsp.out_type,
            row_count: 0,
            var_count: 0,
            vars: BTreeMap::new(),
            row: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn get_data(&mut self) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let _ = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .set_value_handler(Some(cb::handle_value))?
            .parse_sas7bdat(ppath, ctx)?;

        Ok(error)
    }

    pub fn get_metadata(&mut self) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let _ = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .parse_sas7bdat(ppath, ctx)?;

        Ok(error)
    }

    pub fn write(&self) -> Result<(), Box<dyn Error>> {
        match self {
            Self { out_path: None, out_type: OutType::csv, .. } => self.write_data_to_stdout(),
            Self { out_path: Some(_), out_type: OutType::csv, .. } => self.write_data_to_csv(),
        }
    }

    pub fn write_data_to_csv(&self) -> Result<(), Box<dyn Error>> {
        match &self.out_path {
            None => Err(From::from(
                "Error writing csv as output path is the set to None",
            )),
            Some(p) => {
                let mut wtr = csv::WriterBuilder::new()
                    .quote_style(csv::QuoteStyle::Always)
                    .from_path(p)?;

                // write header
                let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();
                wtr.serialize(vars)?;

                // write rows
                for r in &self.rows {
                    wtr.serialize(r)?;
                }
                wtr.flush()?;
                Ok(())
            }
        }
    }

    pub fn write_data_to_stdout(&self) -> Result<(), Box<dyn Error>> {
        let mut wtr = csv::WriterBuilder::new()
            .quote_style(csv::QuoteStyle::Always)
            .from_writer(stdout());

        // write header
        let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();
        wtr.serialize(vars)?;

        // write rows
        for r in &self.rows {
            wtr.serialize(r)?;
        }
        wtr.flush()?;
        Ok(())
    }

    pub fn write_metadata_to_stdout(&self) -> Result<(), Box<dyn Error>> {
        println!(
            "Metadata for the file {}\n",
            self.path.to_string_lossy().yellow()
        );
        println!("{}: {}", "Row count".green(), self.row_count);
        println!("{}: {}", "Variable count".red(), self.var_count);
        println!("{}:", "Variable names".blue());
        for (k, v) in self.vars.iter() {
            println!(
                "{}: {} of type {:#?}",
                k.var_index,
                k.var_name.bright_purple(),
                v
            );
        }
        Ok(())
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
