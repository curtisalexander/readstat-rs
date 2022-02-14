use arrow::array::{
    ArrayBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int8Builder, StringBuilder, Time32SecondBuilder, TimestampSecondBuilder, ArrayRef,
};
use arrow::csv as csv_arrow;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use arrow::{datatypes, record_batch};
use colored::Colorize;
use csv as csv_crate;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_format::{Locale, ToFormattedString};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use path_abs::PathInfo;
use serde_json;
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::stdout;
use std::os::raw::{c_uint, c_void};
use std::path::PathBuf;
use std::sync::Arc;

use crate::rs_metadata::{ReadStatFormatClass, ReadStatMetadata, ReadStatVarType};
use crate::rs_parser::ReadStatParser;
use crate::rs_path::ReadStatPath;
use crate::{cb, ReadStatVarMetadata};
use crate::{Format, Reader};


/********
 * Data *
 *******/

pub struct ReadStatData {
    // metadata
    pub var_count: i32,
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    // data
    pub cols: Vec<Box<dyn ArrayBuilder>>,
    pub schema: datatypes::Schema,
    pub batch: record_batch::RecordBatch,
    // batch rows
    pub batch_rows_to_process: usize, // min(stream_rows, row_limit, row_count)
    pub batch_row_start: usize,
    pub batch_row_end: usize,
    pub batch_rows_processed: usize,
    // progress
    pub pb: Option<ProgressBar>,
    pub no_progress: bool,
    // errors
    pub errors: Vec<String>,
}

impl ReadStatData {
    pub fn new() -> Self {
        Self {
            // metadata
            var_count: 0,
            vars: BTreeMap::new(),
            // data
            cols: Vec::new(),
            schema: datatypes::Schema::empty(),
            batch: RecordBatch::new_empty(Arc::new(datatypes::Schema::empty())),
            // batch rows
            batch_rows_to_process: 0,
            batch_rows_processed: 0,
            batch_row_start: 0,
            batch_row_end: 0,
            // progress
            pb: None,
            no_progress: false,
            // errors
            errors: Vec::new(),
        }
    }

    fn allocate_cols(self) -> Self {
        let rows = self.batch_rows_to_process;
        let cols: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(self.var_count as usize);
        for i in 0..self.var_count {
            // Get variable type
            let var_type = self.vars.get(&i).unwrap().var_type;
            // Allocate space for ArrayBuilder
            let array: Box<dyn ArrayBuilder> = match var_type {
                ReadStatVarType::String | ReadStatVarType::StringRef | ReadStatVarType::Unknown => {
                    Box::new(StringBuilder::new(self.batch_rows_to_process))
                }
                ReadStatVarType::Int8 => Box::new(Int8Builder::new(rows)),
                ReadStatVarType::Int16 => Box::new(Int16Builder::new(rows)),
                ReadStatVarType::Int32 => Box::new(Int32Builder::new(rows)),
                ReadStatVarType::Float => Box::new(Float32Builder::new(rows)),
                ReadStatVarType::Double => match self.vars.get(&i).unwrap().var_format_class {
                    None => Box::new(Float64Builder::new(rows)),
                    Some(ReadStatFormatClass::Date) => Box::new(Date32Builder::new(rows)),
                    Some(ReadStatFormatClass::DateTime)
                    | Some(ReadStatFormatClass::DateTimeWithMilliseconds)
                    | Some(ReadStatFormatClass::DateTimeWithMicroseconds)
                    | Some(ReadStatFormatClass::DateTimeWithNanoseconds) => {
                        Box::new(TimestampSecondBuilder::new(rows))
                    }
                    Some(ReadStatFormatClass::Time) => Box::new(Time32SecondBuilder::new(rows)),
                },
            };

            cols.push(array);
        }

        Self { cols, ..self }
    }

    pub fn cols_to_record_batch(&mut self) -> Result<(), Box<dyn Error>> {
        // Build array references and save in batch
        let arrays: Vec<ArrayRef> = self
            .cols
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();
        self.batch = RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)?;

        // reset
        self.cols.clear();

        Ok(())
    }

    pub fn get_preview(
        &mut self,
        row_limit: Option<u32>,
        row_offset: Option<u32>,
    ) -> Result<u32, Box<dyn Error>> {
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

    pub fn get_row_count(&mut self) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata_row_count_only))?
            .parse_sas7bdat(ppath, ctx);

        Ok(error as u32)
    }

    pub fn init(self, m: ReadStatMetadata, row_start: u32, row_end: u32) -> Self {
        self.set_metadata(m)
            .set_batch_counts(row_start, row_end)
            .allocate_cols()
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
                            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                        }
                        Some(ReadStatFormatClass::DateTimeWithMilliseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
                        }
                        Some(ReadStatFormatClass::DateTimeWithMicroseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                        }
                        Some(ReadStatFormatClass::DateTimeWithNanoseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
                        }
                        Some(ReadStatFormatClass::Time) => {
                            DataType::Time32(arrow::datatypes::TimeUnit::Second)
                        }
                        None => DataType::Float64,
                    },
                };
                Field::new(&vm.var_name, var_dt, true)
            })
            .collect();

        Schema::new(fields)
    }

    fn set_batch_counts(self, row_start: u32, row_end: u32) -> Self {
        let batch_rows_to_process = (row_end - row_start) as usize;
        let batch_row_start = row_start as usize;
        let batch_row_end = row_end as usize;
        let batch_rows_processed = 0_usize;

        Self {
            batch_rows_to_process,
            batch_row_start,
            batch_row_end,
            batch_rows_processed,
            ..self
        }
    }

    fn set_metadata(self, m: ReadStatMetadata) -> Self {
        let var_count = m.var_count;
        let vars = m.vars;
        let schema = self.initialize_schema();
        Self {
            var_count,
            vars,
            schema,
            ..self
        }
    }

    pub fn set_no_progress(self, no_progress: bool) -> Self {
        Self {
            no_progress,
            ..self
        }
    }
}
