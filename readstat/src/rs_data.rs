use arrow::array::{
    ArrayBuilder, ArrayRef, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int8Builder, StringBuilder, Time32SecondBuilder, TimestampSecondBuilder,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_traits::FromPrimitive;
use path_abs::PathInfo;
use std::collections::BTreeMap;
use std::error::Error;
use std::os::raw::c_void;
use std::sync::{Arc, atomic::AtomicUsize};

use crate::cb;
use crate::rs_metadata::{ReadStatFormatClass, ReadStatMetadata, ReadStatVarType};
use crate::rs_parser::ReadStatParser;
use crate::rs_path::ReadStatPath;
use crate::{ReadStatError, ReadStatVarMetadata};

pub struct ReadStatData {
    // metadata
    pub var_count: i32,
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    // data
    pub cols: Vec<Box<dyn ArrayBuilder>>,
    pub schema: Schema,
    pub batch: RecordBatch,
    // batch rows
    pub batch_rows_to_process: usize, // min(stream_rows, row_limit, row_count)
    pub batch_row_start: usize,
    pub batch_row_end: usize,
    pub batch_rows_processed: usize,
    // total rows
    pub total_rows_to_process: usize,
    // pub total_rows_processed: Option<Arc<AtomicUsize>>,
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
            schema: Schema::empty(),
            batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
            // batch rows
            batch_rows_to_process: 0,
            batch_rows_processed: 0,
            batch_row_start: 0,
            batch_row_end: 0,
            // total rows
            total_rows_to_process: 0,
            // total_rows_processed: None,
            // progress
            pb: None,
            no_progress: false,
            // errors
            errors: Vec::new(),
        }
    }

    fn allocate_cols(self) -> Self {
        let rows = self.batch_rows_to_process;
        let mut cols: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(self.var_count as usize);
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

    fn cols_to_record_batch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
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

    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // parse data and if successful then convert cols into a record batch
        self.parse_data(&rsp)?;
        self.cols_to_record_batch()?;
        Ok(())
    }

    fn parse_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // path as pointer
        debug!("Path as C string is {:?}", &rsp.cstring_path);
        let ppath = rsp.cstring_path.as_ptr();

        // spinner
        // TODO - uncomment when ready to reimplement progress bar
        /*
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        */

        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat data from file {}",
                &rsp.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }

        // initialize context
        let ctx = self as *mut ReadStatData as *mut c_void;

        // initialize error
        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {:#?}", &error);

        // setup parser
        // once call parse_sas7bdat, iteration begins
        let mut parser = ReadStatParser::new()
            // do not set metadata handler nor variable handler as already processed
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(Some(self.batch_rows_to_process.try_into().unwrap()))?
            .set_row_offset(Some(self.batch_row_start.try_into().unwrap()))?;

        let error = parser.parse_sas7bdat(ppath, ctx);

        // drop parser after finished
        // drop(parser);

        match FromPrimitive::from_i32(error as i32) {
            Some(ReadStatError::READSTAT_OK) => Ok(()),
            Some(e) => Err(From::from(format!(
                "Error when attempting to parse sas7bdat: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to parse sas7bdat: Unknown return value",
            )),
        }
    }

    /*
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
    */

    pub fn init(self, md: ReadStatMetadata, row_start: u32, row_end: u32) -> Self {
        self.set_metadata(md)
            .set_batch_counts(row_start, row_end)
            .allocate_cols()
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

    fn set_metadata(self, md: ReadStatMetadata) -> Self {
        let var_count = md.var_count;
        let vars = md.vars;
        let schema = md.schema.clone();
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

    pub fn set_total_rows_to_process(self, total_rows_to_process: usize) -> Self {
        Self {
            total_rows_to_process,
            ..self
        }
    }

    /*
    pub fn set_total_rows_processed(self, total_rows_processed: Arc<AtomicUsize>) -> Self {
        Self {
            total_rows_processed: Some(total_rows_processed),
            ..self
        }
    }
    */
}
