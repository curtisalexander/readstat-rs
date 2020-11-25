use log::debug;
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;
use std::error::Error;
use std::os::raw::{c_char, c_int, c_void};
use std::path::PathBuf;

use crate::cb;
use crate::util;

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
        let cpath = util::path_to_cstring(&path)?;
        debug!("Path as C string is {:?}", cpath);
        let ppath = cpath.as_ptr();

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

    pub fn write(&self, out: PathBuf) -> Result<(), Box<dyn Error>> {
        let mut wtr = csv::WriterBuilder::new()
            .quote_style(csv::QuoteStyle::Always)
            .from_path(out)?;

        let vars: Vec<String> = self
            .metadata
            .vars
            .iter()
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
        let cpath = util::path_to_cstring(&path)?;
        debug!("Path as C string is {:?}", cpath);
        let ppath = cpath.as_ptr();

        let ctx = self as *mut ReadStatMetadata as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let _ = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .parse_sas7bdat(ppath, ctx)?;

        Ok(error)
    }

    pub fn print_data(&mut self) -> Result<u32, Box<dyn Error>> {
        let path = &self.path;
        let cpath = util::path_to_cstring(&path)?;
        debug!("Path as C string is {:?}", cpath);
        let ppath = cpath.as_ptr();

        let ctx = self as *mut ReadStatMetadata as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let _ = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .set_value_handler(Some(cb::handle_value_print))?
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
            // Format as strings to get only 14 digits
            // Then parse back into f32 or f64 so that the trailing zeroes are trimmed when serializing
            ReadStatVar::ReadStat_f32(f) => {
                Self::ReadStat_f32(format!("{:.14}", f).parse::<f32>().unwrap())
            }
            ReadStatVar::ReadStat_f64(f) => {
                Self::ReadStat_f64(format!("{:.14}", f).parse::<f64>().unwrap())
            }
        }
    }
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
