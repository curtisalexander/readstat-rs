use arrow2::{
    array::{
        Array, Float32Array, Float64Array, Int16Array, Int32Array, MutableArray,
        MutablePrimitiveArray, MutableUtf8Array, Utf8Array,
    },
    chunk::Chunk,
    datatypes::{DataType, Schema, TimeUnit},
};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_traits::FromPrimitive;
use path_abs::PathInfo;
use std::{
    collections::BTreeMap,
    error::Error,
    os::raw::c_void,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{
    cb,
    err::ReadStatError,
    rs_metadata::{ReadStatMetadata, ReadStatVarMetadata},
    rs_parser::ReadStatParser,
    rs_path::ReadStatPath,
    rs_var::{ReadStatVarFormatClass, ReadStatVarType},
    ReadStatVar,
};

pub struct ReadStatData {
    // metadata
    pub var_count: i32,
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    // data
    pub cols: Vec<Vec<ReadStatVar>>,
    pub arrays: Vec<Box<dyn MutableArray>>,
    pub schema: Schema,
    pub chunk: Option<Chunk<Box<dyn Array>>>,
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
            cols: Vec::new(),
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

    fn allocate_cols(self) -> Self {
        todo!()
    }

    fn allocate_arrays(self) -> Self {
        let rows = self.chunk_rows_to_process;
        let mut arrays: Vec<Box<dyn MutableArray>> = Vec::with_capacity(self.var_count as usize);
        for i in 0..self.var_count {
            // Get variable type
            let var_type = self.vars.get(&i).unwrap().var_type;
            // Allocate space
            let array: Box<dyn MutableArray> = match var_type {
                ReadStatVarType::String | ReadStatVarType::StringRef | ReadStatVarType::Unknown => {
                    Box::new(MutableUtf8Array::<i32>::with_capacity(rows))
                }
                ReadStatVarType::Int8 => Box::new(MutablePrimitiveArray::<i8>::with_capacity(rows)),
                ReadStatVarType::Int16 => {
                    Box::new(MutablePrimitiveArray::<i16>::with_capacity(rows))
                }
                ReadStatVarType::Int32 => {
                    Box::new(MutablePrimitiveArray::<i32>::with_capacity(rows))
                }
                ReadStatVarType::Float => {
                    Box::new(MutablePrimitiveArray::<f32>::with_capacity(rows))
                }
                ReadStatVarType::Double => match self.vars.get(&i).unwrap().var_format_class {
                    None => Box::new(MutablePrimitiveArray::<f64>::with_capacity(rows)),
                    Some(ReadStatVarFormatClass::Date) => Box::new(
                        MutablePrimitiveArray::<i32>::with_capacity(rows).to(DataType::Date32),
                    ),
                    Some(ReadStatVarFormatClass::DateTime) => Box::new(
                        MutablePrimitiveArray::<i64>::with_capacity(rows)
                            .to(DataType::Timestamp(TimeUnit::Second, None)),
                    ),
                    Some(ReadStatVarFormatClass::DateTimeWithMilliseconds) => Box::new(
                        MutablePrimitiveArray::<i64>::with_capacity(rows)
                            .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
                    ),
                    Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => Box::new(
                        MutablePrimitiveArray::<i64>::with_capacity(rows)
                            .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
                    ),
                    Some(ReadStatVarFormatClass::DateTimeWithNanoseconds) => Box::new(
                        MutablePrimitiveArray::<i64>::with_capacity(rows)
                            .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                    ),
                    Some(ReadStatVarFormatClass::Time) => Box::new(
                        MutablePrimitiveArray::<i32>::with_capacity(rows)
                            .to(DataType::Time32(TimeUnit::Second)),
                    ),
                },
            };

            arrays.push(array);
        }

        Self { arrays, ..self }
    }

    #[allow(unreachable_patterns)]
    fn arrays_to_chunk(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let arrays = self
            .arrays
            .iter()
            .enumerate()
            .map(|(i, array)| {
                let array = match array.data_type() {
                    DataType::Float64 => {
                        match self.vars.get(&(i as i32)).unwrap().var_format_class {
                            None => {
                                let array =
                                    (*array).as_any().downcast_ref::<Float64Array>().unwrap();
                                Box::new(array.clone()) as Box<dyn Array>
                            }
                            Some(ReadStatVarFormatClass::Date) => {
                                let array =
                                    (*array).as_any().downcast_ref::<Float64Array>().unwrap();
                                Box::new(array.clone().to(DataType::Date32)) as Box<dyn Array>
                            }
                            Some(ReadStatVarFormatClass::DateTime) => {
                                let array =
                                    (*array).as_any().downcast_ref::<Float64Array>().unwrap();
                                Box::new(
                                    array
                                        .clone()
                                        .to(DataType::Timestamp(TimeUnit::Second, None)),
                                ) as Box<dyn Array>
                            }
                            Some(ReadStatVarFormatClass::DateTimeWithMilliseconds) => {
                                let array =
                                    (*array).as_any().downcast_ref::<Float64Array>().unwrap();
                                Box::new(
                                    array
                                        .clone()
                                        .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
                                ) as Box<dyn Array>
                            }
                            Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => {
                                let array =
                                    (*array).as_any().downcast_ref::<Float64Array>().unwrap();
                                Box::new(
                                    array
                                        .clone()
                                        .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
                                ) as Box<dyn Array>
                            }
                            Some(ReadStatVarFormatClass::DateTimeWithNanoseconds) => {
                                let array =
                                    (*array).as_any().downcast_ref::<Float64Array>().unwrap();
                                Box::new(
                                    array
                                        .clone()
                                        .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                                ) as Box<dyn Array>
                            }
                            Some(ReadStatVarFormatClass::Time) => {
                                let array =
                                    (*array).as_any().downcast_ref::<Float64Array>().unwrap();
                                Box::new(array.clone().to(DataType::Time32(TimeUnit::Second)))
                                    as Box<dyn Array>
                            }
                            _ => unreachable!(),
                        }
                    }
                    DataType::Float32 => {
                        let array = (*array).as_any().downcast_ref::<Float32Array>().unwrap();
                        Box::new(array.clone()) as Box<dyn Array>
                    }
                    DataType::Int8 | DataType::Int16 => {
                        let array = (*array).as_any().downcast_ref::<Int16Array>().unwrap();
                        Box::new(array.clone()) as Box<dyn Array>
                    }
                    DataType::Int32 => {
                        let array = (*array).as_any().downcast_ref::<Int32Array>().unwrap();
                        Box::new(array.clone()) as Box<dyn Array>
                    }
                    DataType::Utf8 => {
                        let array = (*array).as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                        Box::new(array.clone()) as Box<dyn Array>
                    }
                    // exhaustive
                    _ => unreachable!(),
                };
                array
            })
            .collect();
        // Build array references and save in chunk
        /*
                let arrays = &self
                    .arrays
                    .iter_mut()
                    .collect();
                    //.into_iter()
                    //.map(|array| {
                        //let array = (*array) as &dyn Array;
                       /*
                        let array = match array.data_type() {
                            DataType::Float64 => {
                                let array = array
                                    .as_mut_any()
                                    .downcast_ref::<Float64Array>()
                                    .unwrap();
                                    //.unwrap() as &dyn Array;

        //                            .into_mut()
         //                           .unwrap_left() as &dyn Array;
                                array
                                //Arc::new(array)
                            },
                            _ => unreachable!()
                        };

                        array as &dyn Array
                        //array.as_arc()
                        // array.into()::<dyn MutableArray>().as_arc()
                        //array
                    */
        //                (*array).as_arc()
         //           })
          //          .collect::<Vec<_>>();
                */
        // reset
        {
            self.arrays.clear();
        }
        // let arrays = self.arrays.iter_mut().map(|array| array.as_arc()).collect();
        self.chunk = Some(Chunk::try_new(arrays)?);
        //self.chunk = Some(Chunk::try_new(arrays.into_iter().map(|a| {*a}).collect())?);
        // self.batch = RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)?;

        Ok(())
    }

    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // parse data and if successful then convert cols into a chunk
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
                    .template("[{spinner:.green} {elapsed_precise}] {msg}")?,
            );
            let msg = format!(
                "Parsing sas7bdat data from file {}",
                &rsp.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(std::time::Duration::new(120, 0));
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
