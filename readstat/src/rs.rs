use arrow::array::ArrayBuilder;
use arrow::record_batch::RecordBatch;
use arrow::{datatypes, record_batch};
use arrow::csv;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_derive::FromPrimitive;
use num_format::{Locale, ToFormattedString};
use num_traits::FromPrimitive;
use path_abs::{PathAbs, PathInfo};
use serde::{Serialize, Serializer};
use std::any::Any;
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::stdout;
use std::os::raw::{c_char, c_int, c_long, c_void};
use std::path::PathBuf;
use std::sync::Arc;

use crate::cb;
use crate::err::ReadStatError;
use crate::{OutType, Reader};

const DIGITS: usize = 14;
const EXTENSIONS: &'static [&'static str] = &["sas7bdat", "sas7bcat"];

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
        let ext = Self::validate_in_extension(&p)?;
        let csp = Self::path_to_cstring(&p)?;
        let op: Option<PathBuf> = Self::validate_out_path(out_path)?;
        let ot = Self::validate_out_type(out_type)?;
        let op = match op {
            None => op,
            Some(op) => Self::validate_out_extension(&op, ot)?,
        };

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
        let rust_str = path.as_os_str().to_str().ok_or("Invalid path")?;
        CString::new(rust_str).map_err(|_| From::from("Invalid path"))
    }

    fn validate_in_extension(path: &PathBuf) -> Result<String, Box<dyn Error>> {
        path.extension()
            .and_then(|e| e.to_str())
            .and_then(|e| Some(e.to_owned()))
            .map_or(
                Err(From::from(format!(
                    "File {} does not have an extension!",
                    path.to_string_lossy().bright_yellow()
                ))),
                |e|
                    if EXTENSIONS.iter().any(|&ext| ext == e) {
                        Ok(e)
                    } else {
                        Err(From::from(format!("Expecting extension {} or {}.\nFile {} does not have expected extension!", String::from("sas7bdat").bright_green(), String::from("sas7bcat").bright_blue(), path.to_string_lossy().bright_yellow())))
                    }
            )
    }

    fn validate_out_extension(
        path: &PathBuf,
        out_type: OutType,
    ) -> Result<Option<PathBuf>, Box<dyn Error>> {
        path.extension()
            .and_then(|e| e.to_str())
            .and_then(|e| Some(e.to_owned()))
            .map_or(
                Err(From::from(format!(
                    "File {} does not have an extension!  Expecting extension {}.",
                    path.to_string_lossy().bright_yellow(),
                    out_type.to_string().bright_green()
                ))),
                |e| match out_type {
                    OutType::csv => {
                        if e == String::from("csv") {
                            Ok(Some(path.to_owned()))
                        } else {
                            Err(From::from(format!(
                                "Expecting extension `{}`.  Instead, file {} has extension {}.",
                                out_type,
                                path.to_string_lossy().bright_yellow(),
                                e
                            )))
                        }
                    }
                },
            )
    }

    fn validate_path(path: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
        let abs_path = PathAbs::new(path)?;

        if abs_path.exists() {
            Ok(abs_path.as_path().to_path_buf())
        } else {
            Err(From::from(format!(
                "File {} does not exist!",
                abs_path.to_string_lossy().bright_yellow()
            )))
        }
    }

    fn validate_out_path(path: Option<PathBuf>) -> Result<Option<PathBuf>, Box<dyn Error>> {
        match path {
            None => Ok(None),
            Some(p) => {
                let abs_path = PathAbs::new(p)?;

                match abs_path.parent() {
                    Err(_) => Err(From::from(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &abs_path.to_string_lossy().bright_yellow()))),
                    Ok(parent) => {
                        if parent.exists() {
                            Ok(Some(abs_path.as_path().to_path_buf()))
                        } else {
                            Err(From::from(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &parent.to_string_lossy().bright_yellow())))
                        }
                    }
                }
            }
        }
    }

    fn validate_out_type(out_type: Option<OutType>) -> Result<OutType, Box<dyn Error>> {
        match out_type {
            None => Ok(OutType::csv),
            Some(t) => Ok(t),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Ord, PartialOrd, Serialize)]
pub struct ReadStatVarIndexAndName {
    pub var_index: c_int,
    pub var_name: String,
}

impl ReadStatVarIndexAndName {
    pub fn new(var_index: c_int, var_name: String) -> Self {
        Self {
            var_index,
            var_name,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ReadStatVarMetadata {
    pub var_type: ReadStatVarType,
    pub var_type_class: ReadStatVarTypeClass,
    pub var_label: String,
    pub var_format: String,
    pub var_format_class: Option<ReadStatFormatClass>,
}

impl ReadStatVarMetadata {
    pub fn new(
        var_type: ReadStatVarType,
        var_type_class: ReadStatVarTypeClass,
        var_label: String,
        var_format: String,
        var_format_class: Option<ReadStatFormatClass>,
    ) -> Self {
        Self {
            var_type,
            var_type_class,
            var_label,
            var_format,
            var_format_class,
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
    ReadStat_Missing(()),
    ReadStat_Date(NaiveDate),
    ReadStat_DateTime(DateTime<Utc>),
    ReadStat_Time(NaiveTime),
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
    ReadStat_Missing(()),
    ReadStat_Date(NaiveDate),
    ReadStat_DateTime(DateTime<Utc>),
    ReadStat_Time(NaiveTime),
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
            // TODO: Is there an alternative that does not require conversion from and to a float?
            ReadStatVar::ReadStat_f32(f) => {
                Self::ReadStat_f32(format!("{1:.0$}", DIGITS, f).parse::<f32>().unwrap())
            }
            ReadStatVar::ReadStat_f64(f) => {
                Self::ReadStat_f64(format!("{1:.0$}", DIGITS, f).parse::<f64>().unwrap())
            }
            ReadStatVar::ReadStat_Missing(_) => Self::ReadStat_Missing(()),
            ReadStatVar::ReadStat_Date(d) => Self::ReadStat_Date(*d),
            ReadStatVar::ReadStat_DateTime(dt) => Self::ReadStat_DateTime(*dt),
            ReadStatVar::ReadStat_Time(t) => Self::ReadStat_Time(*t),
        }
    }
}

#[derive(Debug, FromPrimitive, Serialize, Clone, Copy)]
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

#[derive(Debug, FromPrimitive, Serialize)]
pub enum ReadStatCompress {
    None = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_NONE as isize,
    Rows = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_ROWS as isize,
    Binary = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_BINARY as isize,
}

#[derive(Debug, FromPrimitive, Serialize)]
pub enum ReadStatEndian {
    None = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_NONE as isize,
    Little = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_LITTLE as isize,
    Big = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_BIG as isize,
}

#[derive(Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarTypeClass {
    String = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_STRING as isize,
    Numeric = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_NUMERIC as isize,
}

#[derive(Debug, Serialize)]
pub enum ReadStatFormatClass {
    Date,
    DateTime,
    Time,
}

// #[derive(Debug, Serialize)]
#[derive(Serialize)]
pub struct ReadStatData {
    pub path: PathBuf,
    pub cstring_path: CString,
    pub out_path: Option<PathBuf>,
    pub out_type: OutType,
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
    pub vars: BTreeMap<ReadStatVarIndexAndName, ReadStatVarMetadata>,
    #[serde(skip)]
    pub cols: Vec<Box<dyn ArrayBuilder>>,
    pub row: Vec<ReadStatVar>,
    pub rows: Vec<Vec<ReadStatVar>>,
    #[serde(skip)]
    pub schema: datatypes::Schema,
    #[serde(skip)]
    pub batch: record_batch::RecordBatch,
    pub wrote_header: bool,
    pub errors: Vec<String>,
    pub reader: Reader,
    #[serde(skip)]
    pub pb: Option<ProgressBar>,
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
            cols: Vec::new(),
            row: Vec::new(),
            rows: Vec::new(),
            schema: datatypes::Schema::empty(),
            batch: RecordBatch::new_empty(Arc::new(datatypes::Schema::empty())),
            wrote_header: false,
            errors: Vec::new(),
            reader: Reader::stream,
            pb: None,
        }
    }

    pub fn get_data(&mut self, row_limit: Option<u32>) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
        self.pb = Some(ProgressBar::new(!0));
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat file {}",
                &self.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {:#?}", &error);

        // TODO: for parsing data, a new metadata handler may be needed that
        //   does not get the row count but just the var count
        // Believe it will save time when working with extremely large files
        let error = match row_limit {
            Some(r) => ReadStatParser::new()
                .set_metadata_handler(Some(cb::handle_metadata))?
                .set_variable_handler(Some(cb::handle_variable))?
                .set_value_handler(Some(cb::handle_value))?
                .set_row_limit(r as c_long)?
                .parse_sas7bdat(ppath, ctx),
            None => ReadStatParser::new()
                .set_metadata_handler(Some(cb::handle_metadata))?
                .set_variable_handler(Some(cb::handle_variable))?
                .set_value_handler(Some(cb::handle_value))?
                .parse_sas7bdat(ppath, ctx),
        };

        Ok(error as u32)
    }

    pub fn get_metadata(&mut self) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
        self.pb = Some(ProgressBar::new(!0));
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

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .parse_sas7bdat(ppath, ctx);

        if let Some(pb) = &self.pb {
            pb.finish_and_clear();
        }

        Ok(error as u32)
    }

    pub fn get_preview(&mut self, row_limit: u32) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
        self.pb = Some(ProgressBar::new(!0));
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat file {}",
                &self.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let error = ReadStatParser::new()
            // TODO: for just a data preview, a new metadata handler may be needed that
            //   does not get the row count but just the var count
            // Believe it will save time when working with extremely large files
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(row_limit as c_long)?
            .parse_sas7bdat(ppath, ctx);

        Ok(error as u32)
    }

    pub fn get_var_types(&self) -> Vec<ReadStatVarType> {
        let var_types = self
            .vars
            .iter()
            .map(|(_, q)| {
                q.var_type.clone()
            })
            .collect();

        var_types
    }

    pub fn set_reader(self, reader: Reader) -> Self {
        Self { reader, ..self }
    }

    pub fn write(&mut self) -> Result<(), Box<dyn Error>> {
        /*
        match self {
            Self {
                out_path: None,
                out_type: OutType::csv,
                ..
            } if self.wrote_header => self.write_data_to_stdout(),
            Self {
                out_path: None,
                out_type: OutType::csv,
                ..
            } => {
                self.write_header_to_stdout()?;
                self.wrote_header = true;
                self.write_data_to_stdout()
            }
            Self {
                out_path: Some(_),
                out_type: OutType::csv,
                ..
            } if self.wrote_header => self.write_data_to_csv(),
            Self {
                out_path: Some(_),
                out_type: OutType::csv,
                ..
            } => {
                self.write_header_to_csv()?;
                self.wrote_header = true;
                self.write_data_to_csv()
            }
        }
        */
        match self {
            Self {
                out_path: None,
                out_type: OutType::csv,
                ..
            } => {
                self.write_data_to_stdout()
            }
            Self {
                out_path: Some(_),
                out_type: OutType::csv,
                ..
            } => {
                self.write_data_to_csv()
            }
        }
    }

    pub fn write_header_to_csv(&mut self) -> Result<(), Box<dyn Error>> {
        match &self.out_path {
            None => Err(From::from(
                "Error writing csv as output path is set to None",
            )),
            Some(p) => {
                // spinner
                if let Some(pb) = &self.pb {
                    pb.finish_at_current_pos();
                }

                // progress bar
                self.pb = Some(ProgressBar::new(self.row_count as u64));
                if let Some(pb) = &self.pb {
                    pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[{spinner:.green} {elapsed_precise}] {bar:30.cyan/blue} {pos:>7}/{len:7} {msg}")
                        .progress_chars("##-"),
                    );
                    pb.set_message("Rows processed");
                    pb.enable_steady_tick(120);
                }

                let file = std::fs::File::create(p).unwrap();
                let mut wtr = csv::WriterBuilder::new().build(file);
                    //.quote_style(csv::QuoteStyle::Always)
                    //.from_path(p)?;

                // write header
                let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();
                wtr.write(&self.batch).unwrap();
                /*
                wtr.serialize(vars)?;
                wtr.flush()?;
                */
                Ok(())
            }
        }
    }

    pub fn write_data_to_csv(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = OpenOptions::new().write(true).append(true).open(p)?;

            //let file = std::fs::File::create(p).unwrap();
            let mut wtr = csv::WriterBuilder::new().build(f);
            /*
            let mut wtr = csv::WriterBuilder::new()
                .quote_style(csv::QuoteStyle::Always)
                .from_writer(f);
            */
            // write rows
            /*
            for r in &self.rows {
                if let Some(pb) = &self.pb {
                    pb.inc(1)
                }
                // Only used to observe progress bar
                // std::thread::sleep(std::time::Duration::from_millis(100));
                wtr.serialize(r)?;
            }
            */
            wtr.write(&self.batch).unwrap();
            //wtr.flush()?;

            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    pub fn write_header_to_stdout(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(pb) = &self.pb {
            pb.finish_and_clear()
        }

        let mut wtr = csv::WriterBuilder::new().build(stdout());
            // .quote_style(csv::QuoteStyle::Always)
            //.from_writer(stdout());

        // write header
        let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();
        wtr.write(&self.batch).unwrap();
        //wtr.serialize(vars)?;
        //wtr.flush()?;

        Ok(())
    }

    pub fn write_data_to_stdout(&mut self) -> Result<(), Box<dyn Error>> {
        let mut wtr = csv::WriterBuilder::new().build(stdout());
            //.quote_style(csv::QuoteStyle::Always)
            //.from_writer(stdout());

        // write rows
        /*
        for r in &self.rows {
            wtr.serialize(r)?;
        }
        wtr.flush()?;
        */
        wtr.write(&self.batch).unwrap();
        Ok(())
    }

    pub fn write_metadata_to_stdout(&mut self) -> Result<(), Box<dyn Error>> {
        println!(
            "Metadata for the file {}\n",
            self.path.to_string_lossy().bright_yellow()
        );
        println!(
            "{}: {}",
            "Row count".green(),
            self.row_count.to_formatted_string(&Locale::en)
        );
        println!(
            "{}: {}",
            "Variable count".red(),
            self.var_count.to_formatted_string(&Locale::en)
        );
        println!("{}: {}", "Table name".blue(), self.table_name);
        println!("{}: {}", "Table label".cyan(), self.file_label);
        println!("{}: {}", "File encoding".yellow(), self.file_encoding);
        println!("{}: {}", "Format version".green(), self.version);
        println!(
            "{}: {}",
            "Bitness".red(),
            if self.is64bit == 0 {
                "32-bit"
            } else {
                "64-bit"
            }
        );
        println!("{}: {}", "Creation time".blue(), self.creation_time);
        println!("{}: {}", "Modified time".cyan(), self.modified_time);
        println!("{}: {:#?}", "Compression".yellow(), self.compression);
        println!("{}: {:#?}", "Byte order".green(), self.endianness);
        println!("{}:", "Variable names".purple());
        for (k, v) in self.vars.iter() {
            println!(
                "{}: {} {{ type class: {}, type: {}, label: {}, format class: {}, format: {} }}",
                k.var_index.to_formatted_string(&Locale::en),
                k.var_name.bright_purple(),
                format!("{:#?}", v.var_type_class).bright_green(),
                format!("{:#?}", v.var_type).bright_red(),
                v.var_label.bright_blue(),
                (match &v.var_format_class {
                    Some(f) => match f {
                        ReadStatFormatClass::Date => "Date",
                        ReadStatFormatClass::DateTime => "DateTime",
                        ReadStatFormatClass::Time => "Time",
                    },
                    None => "",
                })
                .bright_cyan(),
                v.var_format.bright_yellow(),
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

        match FromPrimitive::from_i32(set_metadata_handler_error as i32) {
            Some(ReadStatError::READSTAT_OK) => Ok(self),
            Some(e) => Err(From::from(format!(
                "Unable to set metdata handler: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to set metadata handler: Unknown return value",
            )),
        }
    }

    fn set_row_limit(self, row_limit: c_long) -> Result<Self, Box<dyn Error>> {
        let set_row_limit_error =
            unsafe { readstat_sys::readstat_set_row_limit(self.parser, row_limit) };

        debug!(
            "After setting row limit, error ==> {}",
            &set_row_limit_error
        );

        match FromPrimitive::from_i32(set_row_limit_error as i32) {
            Some(ReadStatError::READSTAT_OK) => Ok(self),
            Some(e) => Err(From::from(format!("Unable to set row limit: {:#?}", e))),
            None => Err(From::from(
                "Error when attempting to set row limit: Unknown return value",
            )),
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

        match FromPrimitive::from_i32(set_variable_handler_error as i32) {
            Some(ReadStatError::READSTAT_OK) => Ok(self),
            Some(e) => Err(From::from(format!(
                "Unable to set variable handler: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to set variable handler: Unknown return value",
            )),
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

        match FromPrimitive::from_i32(set_value_handler_error as i32) {
            Some(ReadStatError::READSTAT_OK) => Ok(self),
            Some(e) => Err(From::from(format!("Unable to set value handler: {:#?}", e))),
            None => Err(From::from(
                "Error when attempting to set value handler: Unknown return value",
            )),
        }
    }

    fn parse_sas7bdat(
        self,
        path: *const c_char,
        user_ctx: *mut c_void,
    ) -> readstat_sys::readstat_error_t {
        let parse_sas7bdat_error: readstat_sys::readstat_error_t =
            unsafe { readstat_sys::readstat_parse_sas7bdat(self.parser, path, user_ctx) };

        debug!(
            "After calling parse sas7bdat, error ==> {}",
            &parse_sas7bdat_error
        );

        parse_sas7bdat_error
    }
}

impl Drop for ReadStatParser {
    fn drop(&mut self) {
        debug!("Freeing parser");

        unsafe { readstat_sys::readstat_parser_free(self.parser) };
    }
}
