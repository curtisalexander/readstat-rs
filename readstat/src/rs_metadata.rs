use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use colored::Colorize;
use log::debug;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use serde::Serialize;
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::error::Error;
use std::os::raw::c_int;

use crate::{ReadStatPath, ReadStatError};
use crate::rs_parser::ReadStatParser;
use crate::cb::{handle_metadata, handle_variable};

#[derive(Clone, Debug, Serialize)]
pub struct ReadStatMetadata {
    pub row_count: c_int,
    pub var_count: c_int,
    pub table_name: String,
    pub file_label: String,
    pub file_encoding: String,
    pub version: c_int,
    pub is64bit: c_int,
    pub creation_time: String,
    pub modified_time: String,
    pub compression: ReadStatCompress,
    pub endianness: ReadStatEndian,
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    pub schema: Schema,
}

impl ReadStatMetadata {
    pub fn new() -> Self {
        Self {
            row_count: 0,
            var_count: 0,
            table_name: String::new(),
            file_label: String::new(),
            file_encoding: String::new(),
            version: 0,
            is64bit: 0,
            creation_time: String::new(),
            modified_time: String::new(),
            compression: ReadStatCompress::None,
            endianness: ReadStatEndian::None,
            vars: BTreeMap::new(),
            schema: Schema::empty(),
        }
    }

    fn initialize_schema(self) -> Schema {
        // build up Schema
        let fields: Vec<Field> = self
            .vars
            .iter()
            .map(|(idx, vm)| {
                let var_dt = match &vm.var_type {
                    ReadStatVarType::String
                    | ReadStatVarType::StringRef
                    | ReadStatVarType::Unknown => DataType::Utf8,
                    ReadStatVarType::Int8 | ReadStatVarType::Int16 => DataType::Int16,
                    ReadStatVarType::Int32 => DataType::Int32,
                    ReadStatVarType::Float => DataType::Float32,
                    ReadStatVarType::Double => match &vm.var_format_class {
                        Some(ReadStatFormatClass::Date) => DataType::Date32,
                        Some(ReadStatFormatClass::DateTime) => {
                            DataType::Timestamp(TimeUnit::Second, None)
                        }
                        Some(ReadStatFormatClass::DateTimeWithMilliseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(TimeUnit::Millisecond, None)
                        }
                        Some(ReadStatFormatClass::DateTimeWithMicroseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(TimeUnit::Microsecond, None)
                        }
                        Some(ReadStatFormatClass::DateTimeWithNanoseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(TimeUnit::Nanosecond, None)
                        }
                        Some(ReadStatFormatClass::Time) => {
                            DataType::Time32(TimeUnit::Second)
                        }
                        None => DataType::Float64,
                    },
                };
                Field::new(&vm.var_name, var_dt, true)
            })
            .collect();

        Schema::new(fields)
    }

    pub fn read_metadata(&mut self, rsp: ReadStatPath, skip_row_count: bool) -> Result<(), Box<dyn Error>> {
        debug!("Path as C string is {:?}", &rsp.cstring_path);
        let ppath = rsp.cstring_path.as_ptr();

        // spinner
        /*
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat metadata from file {}",
                &self.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }
        */
        let msg = format!(
            "Parsing sas7bdat metadata from file {}",
            &rsp.path.to_string_lossy().bright_red()
        );
        
        let ctx = self as *mut ReadStatMetadata as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let row_limit = if skip_row_count { Some(1) } else { None };

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(handle_metadata))?
            .set_variable_handler(Some(handle_variable))?
            .set_row_limit(row_limit)?
            .parse_sas7bdat(ppath, ctx);

        /*
        if let Some(pb) = &self.pb {
            pb.finish_and_clear();
        }
        */

        match FromPrimitive::from_i32(error as i32) {
            Some(ReadStatError::READSTAT_OK) => {
                // if successful, initialize schema
                self.schema = self.initialize_schema();
                Ok(())
            },
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

#[derive(Clone, Debug, FromPrimitive, Serialize)]
pub enum ReadStatCompress {
    None = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_NONE as isize,
    Rows = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_ROWS as isize,
    Binary = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_BINARY as isize,
}

#[derive(Clone, Debug, FromPrimitive, Serialize)]
pub enum ReadStatEndian {
    None = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_NONE as isize,
    Little = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_LITTLE as isize,
    Big = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_BIG as isize,
}

/*********************
 * Variable Metadata *
 ********************/

#[derive(Clone, Debug, Serialize)]
pub struct ReadStatVarMetadata {
    pub var_name: String,
    pub var_type: ReadStatVarType,
    pub var_type_class: ReadStatVarTypeClass,
    pub var_label: String,
    pub var_format: String,
    pub var_format_class: Option<ReadStatFormatClass>,
}

impl ReadStatVarMetadata {
    pub fn new(
        var_name: String,
        var_type: ReadStatVarType,
        var_type_class: ReadStatVarTypeClass,
        var_label: String,
        var_format: String,
        var_format_class: Option<ReadStatFormatClass>,
    ) -> Self {
        Self {
            var_name,
            var_type,
            var_type_class,
            var_label,
            var_format,
            var_format_class,
        }
    }
}

#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
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

#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarTypeClass {
    String = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_STRING as isize,
    Numeric = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_NUMERIC as isize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ReadStatFormatClass {
    Date,
    DateTime,
    DateTimeWithMilliseconds,
    DateTimeWithMicroseconds,
    DateTimeWithNanoseconds,
    Time,
}

/************
 * Variable *
 ***********/

#[derive(Debug, Clone)]
pub enum ReadStatVar {
    ReadStat_String(String),
    ReadStat_i8(i8),
    ReadStat_i16(i16),
    ReadStat_i32(i32),
    ReadStat_f32(f32),
    ReadStat_f64(f64),
    ReadStat_Missing(()),
    ReadStat_Date(i32),
    ReadStat_DateTime(i64),
    ReadStat_DateTimeWithMilliseconds(i64),
    ReadStat_DateTimeWithMicroseconds(i64),
    ReadStat_DateTimeWithNanoseconds(i64),
    ReadStat_Time(i32),
}
