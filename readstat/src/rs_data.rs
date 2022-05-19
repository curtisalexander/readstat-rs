use arrow2::array::{Array, MutableArray, MutablePrimitiveArray, MutableUtf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Schema, TimeUnit};
/*
use arrow::array::{
    ArrayBuilder, ArrayRef, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int8Builder, StringBuilder, Time32SecondBuilder, TimestampSecondBuilder,
};
*/
// use arrow::datatypes::Schema;
// use arrow::record_batch::RecordBatch;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_traits::FromPrimitive;
use path_abs::PathInfo;
use std::collections::BTreeMap;
use std::error::Error;
use std::os::raw::c_void;
use std::sync::{atomic::AtomicUsize, Arc};

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
    // pub cols: Vec<Box<dyn ArrayBuilder>>,
    pub arrays: Vec<Arc<dyn MutableArray>>,
    pub schema: Schema,
    pub chunk: Option<Chunk<Arc<dyn Array>>>,
    // pub batch: RecordBatch,
    // batch rows
    pub chunk_rows_to_process: usize, // min(stream_rows, row_limit, row_count)
    pub chunk_row_start: usize,
    pub chunk_row_end: usize,
    pub chunk_rows_processed: usize,
    // total rows
    pub total_rows_to_process: usize,
    pub total_rows_processed: Option<Arc<AtomicUsize>>,
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
            arrays: Vec::new(),
            // cols: Vec::new(),
            schema: Schema::default(),
            chunk: None,
            // batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
            // batch rows
            chunk_rows_to_process: 0,
            chunk_rows_processed: 0,
            chunk_row_start: 0,
            chunk_row_end: 0,
            // total rows
            total_rows_to_process: 0,
            total_rows_processed: None,
            // progress
            pb: None,
            no_progress: false,
            // errors
            errors: Vec::new(),
        }
    }

    fn allocate_arrays(self) -> Self {
        let rows = self.chunk_rows_to_process;
        let mut arrays: Vec<Arc<dyn MutableArray>> = Vec::with_capacity(self.var_count as usize);
        for i in 0..self.var_count {
            // Get variable type
            let var_type = self.vars.get(&i).unwrap().var_type;
            // Allocate space
            let array: Arc<dyn MutableArray> = match var_type {
                ReadStatVarType::String | ReadStatVarType::StringRef | ReadStatVarType::Unknown => {
                    Arc::new(MutableUtf8Array::<i32>::with_capacity(rows))
                }
                ReadStatVarType::Int8 => Arc::new(MutablePrimitiveArray::<i8>::with_capacity(rows)),
                ReadStatVarType::Int16 => {
                    Arc::new(MutablePrimitiveArray::<i16>::with_capacity(rows))
                }
                ReadStatVarType::Int32 => {
                    Arc::new(MutablePrimitiveArray::<i32>::with_capacity(rows))
                }
                ReadStatVarType::Float => {
                    Arc::new(MutablePrimitiveArray::<f32>::with_capacity(rows))
                }
                ReadStatVarType::Double => match self.vars.get(&i).unwrap().var_format_class {
                    None => Arc::new(MutablePrimitiveArray::<f64>::with_capacity(rows)),
                    Some(ReadStatFormatClass::Date) => Arc::new(
                        MutablePrimitiveArray::<i32>::with_capacity(rows).to(DataType::Date32),
                    ),
                    Some(ReadStatFormatClass::DateTime) => Arc::new(
                        MutablePrimitiveArray::<i64>::with_capacity(rows)
                            .to(DataType::Timestamp(TimeUnit::Second, None)),
                    ),
                    Some(ReadStatFormatClass::DateTimeWithMilliseconds) => Arc::new(
                        MutablePrimitiveArray::<i64>::with_capacity(rows)
                            .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
                    ),
                    Some(ReadStatFormatClass::DateTimeWithMicroseconds) => Arc::new(
                        MutablePrimitiveArray::<i64>::with_capacity(rows)
                            .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
                    ),
                    Some(ReadStatFormatClass::DateTimeWithNanoseconds) => Arc::new(
                        MutablePrimitiveArray::<i64>::with_capacity(rows)
                            .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                    ),
                    Some(ReadStatFormatClass::Time) => Arc::new(
                        MutablePrimitiveArray::<i32>::with_capacity(rows)
                            .to(DataType::Time32(TimeUnit::Second)),
                    ),
                },
            };

            arrays.push(array);
        }

        Self { arrays, ..self }
    }

    fn arrays_to_chunk(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        
        // TODO - resume here

        // Build array references and save in chunk
        let arrays = self
            .arrays
            .into_iter()
            .map(|array| {
                /*
                match array.data_type() {
                    DataType::Float64 => {
                        let array = array
                            .as_mut_any()
                            .downcast_mut::<Float64Array>()
                            .unwrap() as &dyn Array;
                        Arc::new(array)
                    },
                    _ => unreachable!()
                }
                */
                let array = (*array).into_arc();
                array
            })
            .collect();
        // let arrays = self.arrays.iter_mut().map(|array| array.as_arc()).collect();
        self.chunk = Some(Chunk::try_new(arrays)?);
        // self.batch = RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)?;

        // reset
        self.arrays.clear();

        Ok(())
    }

    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // parse data and if successful then convert cols into a record batch
        self.parse_data(&rsp)?;
        self.arrays_to_chunk()?;
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
        let error = ReadStatParser::new()
            // do not set metadata handler nor variable handler as already processed
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(Some(self.chunk_rows_to_process.try_into().unwrap()))?
            .set_row_offset(Some(self.chunk_row_start.try_into().unwrap()))?
            .parse_sas7bdat(ppath, ctx);

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
            .set_chunk_counts(row_start, row_end)
            .allocate_arrays()
    }

    fn set_chunk_counts(self, row_start: u32, row_end: u32) -> Self {
        let chunk_rows_to_process = (row_end - row_start) as usize;
        let chunk_row_start = row_start as usize;
        let chunk_row_end = row_end as usize;
        let chunk_rows_processed = 0_usize;

        Self {
            chunk_rows_to_process,
            chunk_row_start,
            chunk_row_end,
            chunk_rows_processed,
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

    pub fn set_total_rows_processed(self, total_rows_processed: Arc<AtomicUsize>) -> Self {
        Self {
            total_rows_processed: Some(total_rows_processed),
            ..self
        }
    }
}
