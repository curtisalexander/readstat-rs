//! Data reading and Arrow [`RecordBatch`](arrow_array::RecordBatch) conversion.
//!
//! [`ReadStatData`] coordinates the FFI parsing of row values from a `.sas7bdat` file,
//! accumulating them column-by-column as `Vec<Vec<ReadStatVar>>`, then converting to
//! an Arrow `RecordBatch` for downstream writing. Supports streaming chunks with
//! configurable row offsets and progress tracking.

use arrow::datatypes::Schema;
use arrow_array::{
    ArrayRef, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int8Array, RecordBatch, StringArray, Time32SecondArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use path_abs::PathInfo;
use std::{
    collections::BTreeMap,
    os::raw::c_void,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{
    cb,
    err::{check_c_error, ReadStatError},
    rs_metadata::{ReadStatMetadata, ReadStatVarMetadata},
    rs_parser::ReadStatParser,
    rs_path::ReadStatPath,
    rs_var::ReadStatVar,
};

/// Holds parsed row data from a `.sas7bdat` file and converts it to Arrow format.
///
/// Each instance processes one streaming chunk of rows. Data accumulates column-by-column
/// in [`cols`](ReadStatData::cols) via the `handle_value`
/// callback, then is converted to an Arrow [`RecordBatch`] via `read_data`.
pub struct ReadStatData {
    /// Number of variables (columns) in the dataset.
    pub var_count: i32,
    /// Per-variable metadata, keyed by variable index.
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    /// Column-major data storage: one `Vec<ReadStatVar>` per variable.
    pub cols: Vec<Vec<ReadStatVar>>,
    /// Arrow schema for the dataset.
    pub schema: Schema,
    /// The Arrow RecordBatch produced after parsing, if available.
    pub batch: Option<RecordBatch>,
    /// Number of rows to process in this chunk.
    pub chunk_rows_to_process: usize,
    /// Starting row offset for this chunk.
    pub chunk_row_start: usize,
    /// Ending row offset (exclusive) for this chunk.
    pub chunk_row_end: usize,
    /// Number of rows actually processed so far in this chunk.
    pub chunk_rows_processed: usize,
    /// Total rows to process across all chunks.
    pub total_rows_to_process: usize,
    /// Shared atomic counter of total rows processed across all chunks.
    pub total_rows_processed: Option<Arc<AtomicUsize>>,
    /// Optional progress bar for visual feedback.
    pub pb: Option<ProgressBar>,
    /// Whether progress display is disabled.
    pub no_progress: bool,
    /// Errors collected during value parsing callbacks.
    pub errors: Vec<String>,
    /// Optional mapping: original var index -> filtered column index.
    /// When present, only variables in this map are included in output.
    pub column_filter: Option<BTreeMap<i32, i32>>,
    /// Total variable count in the unfiltered dataset.
    /// Used for row-boundary detection in handle_value when filtering is active.
    /// Defaults to var_count when no filter is set.
    pub total_var_count: i32,
}

impl Default for ReadStatData {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadStatData {
    /// Creates a new `ReadStatData` with default (empty) values.
    pub fn new() -> Self {
        Self {
            // metadata
            var_count: 0,
            vars: BTreeMap::new(),
            // data
            cols: Vec::new(),
            schema: Schema::empty(),
            // record batch
            batch: None,
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
            // column filtering
            column_filter: None,
            total_var_count: 0,
        }
    }

    fn allocate_cols(self) -> Self {
        let mut cols = Vec::with_capacity(self.var_count as usize);
        for _ in 0..self.var_count {
            cols.push(Vec::with_capacity(self.chunk_rows_to_process))
        }
        Self { cols, ..self }
    }

    fn cols_to_batch(&mut self) -> Result<(), ReadStatError> {
        // for each column in cols
        let arrays: Vec<ArrayRef> = self
            .cols
            .iter()
            .map(|col| {
                // what kind of column is this?
                // grab the first element to determine the column type
                let col_type = &col[0];

                // convert from a Vec<ReadStatVar> into an ArrayRef
                let array: ArrayRef = match col_type {
                    ReadStatVar::ReadStat_String(_) => {
                        // get the inner value
                        let vec = col
                            .iter()
                            .map(|s| {
                                if let ReadStatVar::ReadStat_String(v) = s {
                                    v.clone()
                                } else {
                                    // should NEVER fall into this branch
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<String>>>();

                        Arc::new(StringArray::from(vec))
                    }
                    ReadStatVar::ReadStat_i8(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i8(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i8>>>();

                        Arc::new(Int8Array::from(vec))
                    }
                    ReadStatVar::ReadStat_i16(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i16(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i16>>>();

                        Arc::new(Int16Array::from(vec))
                    }
                    ReadStatVar::ReadStat_i32(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i32(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Arc::new(Int32Array::from(vec))
                    }
                    ReadStatVar::ReadStat_f32(_) => {
                        let vec = col
                            .iter()
                            .map(|f| {
                                if let ReadStatVar::ReadStat_f32(v) = f {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<f32>>>();

                        Arc::new(Float32Array::from(vec))
                    }
                    ReadStatVar::ReadStat_f64(_) => {
                        let vec = col
                            .iter()
                            .map(|f| {
                                if let ReadStatVar::ReadStat_f64(v) = f {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<f64>>>();

                        Arc::new(Float64Array::from(vec))
                    }
                    ReadStatVar::ReadStat_Date(_) => {
                        let vec = col
                            .iter()
                            .map(|d| {
                                if let ReadStatVar::ReadStat_Date(v) = d {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Arc::new(Date32Array::from(vec))
                    }
                    ReadStatVar::ReadStat_DateTime(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTime(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Arc::new(TimestampSecondArray::from(vec))
                    }
                    ReadStatVar::ReadStat_DateTimeWithMilliseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithMilliseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Arc::new(TimestampMillisecondArray::from(vec))
                    }
                    ReadStatVar::ReadStat_DateTimeWithMicroseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithMicroseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Arc::new(TimestampMicrosecondArray::from(vec))
                    }
                    ReadStatVar::ReadStat_DateTimeWithNanoseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithNanoseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Arc::new(TimestampNanosecondArray::from(vec))
                    }
                    ReadStatVar::ReadStat_Time(_) => {
                        let vec = col
                            .iter()
                            .map(|t| {
                                if let ReadStatVar::ReadStat_Time(v) = t {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Arc::new(Time32SecondArray::from(vec))
                    }
                    ReadStatVar::ReadStat_TimeWithMicroseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|t| {
                                if let ReadStatVar::ReadStat_TimeWithMicroseconds(v) = t {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Arc::new(Time64MicrosecondArray::from(vec))
                    }
                };

                // return
                array
            })
            .collect();

        // convert into a RecordBatch
        self.batch = Some(RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)?);

        Ok(())
    }

    /// Parses row data from the file and converts it to an Arrow [`RecordBatch`].
    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), ReadStatError> {
        // parse data and if successful then convert cols into a record batch
        self.parse_data(rsp)?;
        self.cols_to_batch()?;
        Ok(())
    }

    fn parse_data(&mut self, rsp: &ReadStatPath) -> Result<(), ReadStatError> {
        // path as pointer
        debug!("Path as C string is {:?}", &rsp.cstring_path);
        let ppath = rsp.cstring_path.as_ptr();

        // Update progress bar with rows processed for this chunk
        if let Some(pb) = &self.pb {
            // Increment by the number of rows we're about to process in this chunk
            pb.inc(self.chunk_rows_to_process as u64);
        }

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
            .set_row_limit(Some(self.chunk_rows_to_process.try_into()?))?
            .set_row_offset(Some(self.chunk_row_start.try_into()?))?
            .parse_sas7bdat(ppath, ctx);

        check_c_error(error as i32)?;
        Ok(())
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

    /// Initializes this instance with metadata and chunk boundaries, allocating column storage.
    pub fn init(self, md: ReadStatMetadata, row_start: u32, row_end: u32) -> Self {
        self.set_metadata(md)
            .set_chunk_counts(row_start, row_end)
            .allocate_cols()
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
        let schema = md.schema;
        // Only set total_var_count from metadata if not already set by set_column_filter
        let total_var_count = if self.total_var_count != 0 {
            self.total_var_count
        } else {
            var_count
        };
        Self {
            var_count,
            vars,
            schema,
            total_var_count,
            ..self
        }
    }

    /// Disables or enables the progress bar display.
    pub fn set_no_progress(self, no_progress: bool) -> Self {
        Self {
            no_progress,
            ..self
        }
    }

    /// Sets the total number of rows to process across all chunks.
    pub fn set_total_rows_to_process(self, total_rows_to_process: usize) -> Self {
        Self {
            total_rows_to_process,
            ..self
        }
    }

    /// Sets the shared atomic counter for tracking rows processed across chunks.
    pub fn set_total_rows_processed(self, total_rows_processed: Arc<AtomicUsize>) -> Self {
        Self {
            total_rows_processed: Some(total_rows_processed),
            ..self
        }
    }

    /// Sets the column filter and original (unfiltered) variable count.
    ///
    /// Must be called **before** [`init`](ReadStatData::init) so that
    /// `total_var_count` is preserved when `set_metadata` runs.
    pub fn set_column_filter(self, filter: Option<BTreeMap<i32, i32>>, total_var_count: i32) -> Self {
        Self {
            column_filter: filter,
            total_var_count,
            ..self
        }
    }

    /// Attaches a progress bar for visual feedback during parsing.
    pub fn set_progress_bar(self, pb: ProgressBar) -> Self {
        Self {
            pb: Some(pb),
            ..self
        }
    }
}
