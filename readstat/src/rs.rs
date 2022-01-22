use arrow::array::{
    ArrayBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int8Builder, StringBuilder, Time32SecondBuilder, TimestampSecondBuilder,
};
use arrow::csv as csv_arrow;
use arrow::ipc::writer::FileWriter;
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use arrow::{datatypes, record_batch};
// use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use colored::Colorize;
use csv as csv_crate;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_derive::FromPrimitive;
use num_format::{Locale, ToFormattedString};
use num_traits::FromPrimitive;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use path_abs::{PathAbs, PathInfo};
use serde::Serialize;
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::stdout;
use std::os::raw::{c_char, c_int, c_long, c_uint, c_void};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cb;
use crate::err::ReadStatError;
use crate::{Format, Reader};

const IN_EXTENSIONS: &[&str] = &["sas7bdat", "sas7bcat"];

#[derive(Debug, Clone)]
pub struct ReadStatPath {
    pub path: PathBuf,
    pub extension: String,
    pub cstring_path: CString,
    pub out_path: Option<PathBuf>,
    pub format: Format,
    pub overwrite: bool,
}

impl ReadStatPath {
    pub fn new(
        path: PathBuf,
        out_path: Option<PathBuf>,
        format: Option<Format>,
        overwrite: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let p = Self::validate_path(path)?;
        let ext = Self::validate_in_extension(&p)?;
        let csp = Self::path_to_cstring(&p)?;
        let op: Option<PathBuf> = Self::validate_out_path(out_path, overwrite)?;
        let f = Self::validate_format(format)?;
        let op = match op {
            None => op,
            Some(op) => Self::validate_out_extension(&op, f)?,
        };

        Ok(Self {
            path: p,
            extension: ext,
            cstring_path: csp,
            out_path: op,
            format: f,
            overwrite,
        })
    }

    #[cfg(unix)]
    pub fn path_to_cstring(path: &PathBuf) -> Result<CString, Box<dyn Error>> {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        CString::new(bytes).map_err(|_| From::from("Invalid path"))
    }

    #[cfg(not(unix))]
    pub fn path_to_cstring(path: &Path) -> Result<CString, Box<dyn Error>> {
        let rust_str = path.as_os_str().to_str().ok_or("Invalid path")?;
        CString::new(rust_str).map_err(|_| From::from("Invalid path"))
    }

    fn validate_in_extension(path: &Path) -> Result<String, Box<dyn Error>> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(From::from(format!(
                    "File {} does not have an extension!",
                    path.to_string_lossy().bright_yellow()
                ))),
                |e|
                    if IN_EXTENSIONS.iter().any(|&ext| ext == e) {
                        Ok(e)
                    } else {
                        Err(From::from(format!("Expecting extension {} or {}.\nFile {} does not have expected extension!", String::from("sas7bdat").bright_green(), String::from("sas7bcat").bright_blue(), path.to_string_lossy().bright_yellow())))
                    }
            )
    }

    fn validate_out_extension(
        path: &Path,
        format: Format,
    ) -> Result<Option<PathBuf>, Box<dyn Error>> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(From::from(format!(
                    "File {} does not have an extension!  Expecting extension {}.",
                    path.to_string_lossy().bright_yellow(),
                    format.to_string().bright_green()
                ))),
                |e| match format {
                    Format::csv | Format::ndjson | Format::feather | Format::parquet => {
                        if e == format.to_string() {
                            Ok(Some(path.to_owned()))
                        } else {
                            Err(From::from(format!(
                                "Expecting extension {}.  Instead, file {} has extension {}.",
                                format.to_string().bright_green(),
                                path.to_string_lossy().bright_yellow(),
                                e.bright_red()
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

    fn validate_out_path(
        path: Option<PathBuf>,
        overwrite: bool,
    ) -> Result<Option<PathBuf>, Box<dyn Error>> {
        match path {
            None => Ok(None),
            Some(p) => {
                let abs_path = PathAbs::new(p)?;

                match abs_path.parent() {
                    Err(_) => Err(From::from(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &abs_path.to_string_lossy().bright_yellow()))),
                    Ok(parent) => {
                        if parent.exists() {
                            // Check to see if file already exists
                            if abs_path.exists() {
                                if overwrite {
                                    println!("The file {} will be {}!", abs_path.to_string_lossy().bright_yellow(), String::from("overwritten").bright_blue());
                                    Ok(Some(abs_path.as_path().to_path_buf()))
                                } else {
                                    Err(From::from(format!("The output file - {} - already exists!  To overwrite the file, utilize the {} parameter", abs_path.to_string_lossy().bright_yellow(), String::from("--overwrite").bright_blue())))
                                }
                            } else {
                                Ok(Some(abs_path.as_path().to_path_buf()))
                            }
                        } else {
                            Err(From::from(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &parent.to_string_lossy().bright_yellow())))
                        }
                    }
                }
            }
        }
    }

    fn validate_format(format: Option<Format>) -> Result<Format, Box<dyn Error>> {
        match format {
            None => Ok(Format::csv),
            Some(f) => Ok(f),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Ord, PartialOrd)]
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

#[derive(Debug)]
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
    ReadStat_Date(i32),
    ReadStat_DateTime(i64),
    ReadStat_Time(i32),
}

#[derive(Clone, Copy, Debug, FromPrimitive)]
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

#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarTypeClass {
    String = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_STRING as isize,
    Numeric = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_NUMERIC as isize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ReadStatFormatClass {
    Date,
    DateTime,
    Time,
}

pub enum ReadStatWriter {
    // feather
    Feather(arrow::ipc::writer::FileWriter<std::fs::File>),
    // ndjson
    Json(arrow::json::writer::LineDelimitedWriter<std::fs::File>),
    // parquet
    Parquet(parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>),
}

pub struct ReadStatData {
    pub path: PathBuf,
    pub cstring_path: CString,
    pub out_path: Option<PathBuf>,
    pub format: Format,
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
    pub var_types: Vec<ReadStatVarType>,
    pub var_format_classes: Vec<Option<ReadStatFormatClass>>,
    pub cols: Vec<Box<dyn ArrayBuilder>>,
    pub schema: datatypes::Schema,
    pub batch: record_batch::RecordBatch,
    pub wrote_header: bool,
    pub errors: Vec<String>,
    pub reader: Reader,
    pub stream_rows: c_uint,
    pub pb: Option<ProgressBar>,
    pub wrote_start: bool,
    pub finish: bool,
    pub no_progress: bool,
    pub is_test: bool,
    // should probably be declared with a trait but just utilizing enum for the time being
    pub wtr: Option<ReadStatWriter>,
}

impl ReadStatData {
    pub fn new(rsp: ReadStatPath) -> Self {
        Self {
            path: rsp.path,
            cstring_path: rsp.cstring_path,
            out_path: rsp.out_path,
            format: rsp.format,
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
            var_types: Vec::new(),
            var_format_classes: Vec::new(),
            cols: Vec::new(),
            schema: datatypes::Schema::empty(),
            batch: RecordBatch::new_empty(Arc::new(datatypes::Schema::empty())),
            wrote_header: false,
            errors: Vec::new(),
            reader: Reader::stream,
            stream_rows: 50000,
            pb: None,
            wrote_start: false,
            finish: false,
            no_progress: false,
            is_test: false,
            wtr: None,
        }
    }

    pub fn allocate_cols(&mut self, rows: usize) {
        for i in 0..self.var_count {
            // Allocate space for ArrayBuilder
            let array: Box<dyn ArrayBuilder> = match self.var_types[i as usize] {
                ReadStatVarType::String | ReadStatVarType::StringRef | ReadStatVarType::Unknown => {
                    Box::new(StringBuilder::new(rows))
                }
                ReadStatVarType::Int8 => Box::new(Int8Builder::new(rows)),
                ReadStatVarType::Int16 => Box::new(Int16Builder::new(rows)),
                ReadStatVarType::Int32 => Box::new(Int32Builder::new(rows)),
                ReadStatVarType::Float => Box::new(Float32Builder::new(rows)),
                ReadStatVarType::Double => match self.var_format_classes[i as usize] {
                    Some(ReadStatFormatClass::Date) => Box::new(Date32Builder::new(rows)),
                    Some(ReadStatFormatClass::DateTime) => {
                        Box::new(TimestampSecondBuilder::new(rows))
                    }
                    Some(ReadStatFormatClass::Time) => Box::new(Time32SecondBuilder::new(rows)),
                    None => Box::new(Float64Builder::new(rows)),
                },
            };

            self.cols.push(array);
        }
    }

    pub fn get_data(&mut self, row_limit: Option<u32>, row_offset: Option<u32>) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat data from file {}",
                &self.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }
        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {:#?}", &error);

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(row_limit)?
            .set_row_offset(row_offset)?
            .parse_sas7bdat(ppath, ctx);

        Ok(error as u32)
    }

    pub fn get_metadata(&mut self, skip_row_count: bool) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
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
        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let row_limit = if skip_row_count { Some(1) } else { None };

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .set_row_limit(row_limit)?
            .parse_sas7bdat(ppath, ctx);

        if let Some(pb) = &self.pb {
            pb.finish_and_clear();
        }

        Ok(error as u32)
    }

    pub fn get_preview(&mut self, row_limit: Option<u32>, row_offset: Option<u32>) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat data from file {}",
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
            .set_row_limit(row_limit)?
            .set_row_offset(row_offset)?
            .parse_sas7bdat(ppath, ctx);

        Ok(error as u32)
    }

    pub fn set_is_test(self, is_test: bool) -> Self {
        Self { is_test, ..self }
    }

    pub fn set_no_progress(self, no_progress: bool) -> Self {
        Self {
            no_progress,
            ..self
        }
    }

    pub fn set_reader(self, reader: Option<Reader>) -> Self {
        if let Some(r) = reader {
            Self { reader: r, ..self }
        } else {
            self
        }
    }

    pub fn set_stream_rows(self, stream_rows: Option<c_uint>) -> Self {
        match self.reader {
            Reader::stream => match stream_rows {
                Some(stream_rows) => Self {
                    stream_rows,
                    ..self
                },
                None => self,
            },
            Reader::mem => self,
        }
    }

    pub fn set_var_types(&mut self) {
        let var_types = self.vars.iter().map(|(_, q)| q.var_type).collect();

        self.var_types = var_types;
    }

    pub fn set_var_format_classes(&mut self) {
        let var_format_classes = self.vars.iter().map(|(_, q)| q.var_format_class).collect();

        self.var_format_classes = var_format_classes;
    }

    pub fn write(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            // Write data to standard out
            Self {
                out_path: None,
                format: Format::csv,
                ..
            } if self.wrote_header => self.write_data_to_stdout(),
            // Write header to standard out
            Self {
                out_path: None,
                format: Format::csv,
                ..
            } => {
                self.write_header_to_stdout()?;
                self.wrote_header = true;
                self.write_data_to_stdout()
            }
            // Write csv data to file
            Self {
                out_path: Some(_),
                format: Format::csv,
                ..
            } if self.wrote_header => self.write_data_to_csv(),
            // Write csv header to file
            Self {
                out_path: Some(_),
                format: Format::csv,
                ..
            } => {
                self.write_header_to_csv()?;
                self.wrote_header = true;
                self.write_data_to_csv()
            }
            // Write feather data to file
            Self {
                format: Format::feather,
                ..
            } => self.write_data_to_feather(),
            // Write ndjson data to file
            Self {
                format: Format::ndjson,
                ..
            } => self.write_data_to_ndjson(),
            // Write parquet data to file
            Self {
                format: Format::parquet,
                ..
            } => self.write_data_to_parquet(),
        }
    }

    pub fn write_header_to_csv(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            // spinner
            if let Some(pb) = &self.pb {
                pb.finish_at_current_pos();
            }

            // spinner
            if !self.no_progress {
                self.pb = Some(ProgressBar::new(!0));
            }
            if let Some(pb) = &self.pb {
                pb.set_style(
                    ProgressStyle::default_spinner()
                        .template("[{spinner:.green} {elapsed_precise} | {bytes}] {msg}"),
                );

                let in_f = if let Some(f) = &self.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = &self.out_path {
                    if let Some(f) = p.file_name() {
                        f.to_string_lossy().bright_green()
                    } else {
                        String::from("___").bright_green()
                    }
                } else {
                    String::from("___").bright_green()
                };

                let msg = format!("Writing file {} as {}", in_f, out_f);

                pb.set_message(msg);
                pb.enable_steady_tick(120);
            }

            // progress bar
            /*
            if !self.no_progress {
                self.pb = Some(ProgressBar::new(self.row_count as u64));
            }
            if let Some(pb) = &self.pb {
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[{spinner:.green} {elapsed_precise}] {bar:30.cyan/blue} {pos:>7}/{len:7} {msg}")
                        .progress_chars("##-"),
                );
                pb.set_message("Rows processed");
                pb.enable_steady_tick(120);
            }
            */

            let file = std::fs::File::create(p)?;
            let mut wtr = csv_crate::WriterBuilder::new().from_writer(file);

            // write header
            let vars: Vec<String> = self
                .batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect();

            // Alternate way to get variable names
            // let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();

            wtr.write_record(&vars)?;
            wtr.flush()?;

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

        let mut wtr = csv_crate::WriterBuilder::new().from_writer(stdout());

        // write header
        let vars: Vec<String> = self
            .batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();

        // Alternate way to get variable names
        // let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();

        wtr.write_record(&vars)?;
        wtr.flush()?;

        Ok(())
    }

    pub fn write_data_to_csv(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?;
            if let Some(pb) = &self.pb {
                let pb_f = pb.wrap_write(f);
                let mut wtr = csv_arrow::WriterBuilder::new()
                    .has_headers(false)
                    .build(pb_f);
                wtr.write(&self.batch)?;
            } else {
                let mut wtr = csv_arrow::WriterBuilder::new().has_headers(false).build(f);
                wtr.write(&self.batch)?;
            };

            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    pub fn write_data_to_feather(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            if let Some(pb) = &self.pb {
                let in_f = if let Some(f) = &self.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = &self.out_path {
                    if let Some(f) = p.file_name() {
                        f.to_string_lossy().bright_green()
                    } else {
                        String::from("___").bright_green()
                    }
                } else {
                    String::from("___").bright_green()
                };

                let msg = format!("Writing file {} as {}", in_f, out_f);

                pb.set_message(msg);
            }

            if !self.wrote_start {
                self.wtr = Some(ReadStatWriter::Feather(FileWriter::try_new(
                    f,
                    &self.schema,
                )?));
            }
            if let Some(wtr) = &mut self.wtr {
                match wtr {
                    ReadStatWriter::Feather(w) => {
                        w.write(&self.batch)?;
                        if self.finish {
                            w.finish()?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        } else {
            Err(From::from(
                "Error writing feather file as output path is set to None",
            ))
        }
    }

    pub fn write_data_to_ndjson(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            if let Some(pb) = &self.pb {
                let in_f = if let Some(f) = &self.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = &self.out_path {
                    if let Some(f) = p.file_name() {
                        f.to_string_lossy().bright_green()
                    } else {
                        String::from("___").bright_green()
                    }
                } else {
                    String::from("___").bright_green()
                };

                let msg = format!("Writing file {} as {}", in_f, out_f);

                pb.set_message(msg);
            }

            if !self.wrote_start {
                self.wtr = Some(ReadStatWriter::Json(LineDelimitedWriter::new(f)));
            }
            if let Some(wtr) = &mut self.wtr {
                match wtr {
                    ReadStatWriter::Json(w) => {
                        let mut batch = RecordBatch::new_empty(Arc::new(self.schema.clone()));
                        batch.clone_from(&self.batch);
                        w.write_batches(&[batch])?;
                        if self.finish {
                            w.finish()?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        } else {
            Err(From::from(
                "Error writing ndjson file as output path is set to None",
            ))
        }
    }

    pub fn write_data_to_parquet(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            if let Some(pb) = &self.pb {
                let in_f = if let Some(f) = &self.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = &self.out_path {
                    if let Some(f) = p.file_name() {
                        f.to_string_lossy().bright_green()
                    } else {
                        String::from("___").bright_green()
                    }
                } else {
                    String::from("___").bright_green()
                };

                let msg = format!("Writing file {} as {}", in_f, out_f);

                pb.set_message(msg);
            }

            if !self.wrote_start {
                let props = WriterProperties::builder().build();
                self.wtr = Some(ReadStatWriter::Parquet(ArrowWriter::try_new(
                    f,
                    Arc::new(self.schema.clone()),
                    Some(props),
                )?));
            }
            if let Some(wtr) = &mut self.wtr {
                match wtr {
                    ReadStatWriter::Parquet(w) => {
                        w.write(&self.batch)?;
                        if self.finish {
                            w.close()?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        } else {
            Err(From::from(
                "Error writing parquet file as output path is set to None",
            ))
        }
    }

    pub fn write_data_to_stdout(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(pb) = &self.pb {
            pb.finish_and_clear()
        }

        let mut wtr = csv_arrow::WriterBuilder::new()
            .has_headers(false)
            .build(stdout());
        wtr.write(&self.batch)?;

        Ok(())
    }

    pub fn write_metadata_to_json(&mut self) -> Result<(), Box<dyn Error>> {
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
        for (i, (k, v)) in self.vars.iter().enumerate() {
            println!(
                "{}: {} {{ type class: {}, type: {}, label: {}, format class: {}, format: {}, arrow data type: {} }}",
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
                self.schema.field(i).data_type().to_string().bright_green()
            );
        }

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
        for (i, (k, v)) in self.vars.iter().enumerate() {
            println!(
                "{}: {} {{ type class: {}, type: {}, label: {}, format class: {}, format: {}, arrow data type: {} }}",
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
                self.schema.field(i).data_type().to_string().bright_green()
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

    fn set_row_limit(self, row_limit: Option<u32>) -> Result<Self, Box<dyn Error>> {
        match row_limit {
            Some(r) => {
                let set_row_limit_error =
                    unsafe { readstat_sys::readstat_set_row_limit(self.parser, r as c_long) };

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
            None => Ok(self),
        }
    }

    fn set_row_offset(self, row_offset: Option<u32>) -> Result<Self, Box<dyn Error>> {
        match row_offset {
            Some(r) => {
                let set_row_offset_error =
                    unsafe { readstat_sys::readstat_set_row_offset(self.parser, r as c_long) };

                debug!(
                    "After setting row offset, error ==> {}",
                    &set_row_offset_error
                );

                match FromPrimitive::from_i32(set_row_offset_error as i32) {
                    Some(ReadStatError::READSTAT_OK) => Ok(self),
                    Some(e) => Err(From::from(format!("Unable to set row limit: {:#?}", e))),
                    None => Err(From::from(
                        "Error when attempting to set row limit: Unknown return value",
                    )),
                }
            }
            None => Ok(self),
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
