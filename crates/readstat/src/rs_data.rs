//! Data reading and Arrow [`RecordBatch`](arrow_array::RecordBatch) conversion.
//!
//! [`ReadStatData`] coordinates the FFI parsing of row values from a `.sas7bdat` file,
//! accumulating them directly into typed Arrow builders via the `handle_value`
//! callback, then finishing them into an Arrow `RecordBatch` for downstream writing.
//! Supports streaming chunks with configurable row offsets and progress tracking.

use arrow::datatypes::Schema;
use arrow_array::{
    builder::{
        Date32Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        StringBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder, TimestampSecondBuilder,
    },
    ArrayRef, RecordBatch,
};
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use std::{
    collections::BTreeMap,
    ffi::CString,
    os::raw::c_void,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{
    cb,
    err::{check_c_error, ReadStatError},
    rs_buffer_io::ReadStatBufferCtx,
    rs_metadata::{ReadStatMetadata, ReadStatVarMetadata},
    rs_parser::ReadStatParser,
    rs_path::ReadStatPath,
    rs_var::{ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass},
};

/// A typed Arrow array builder for a single column.
///
/// Each variant wraps the corresponding Arrow builder, pre-sized with capacity
/// hints from the metadata (row count, string `storage_width`). Values are
/// appended directly during FFI callbacks, eliminating intermediate allocations.
pub enum ColumnBuilder {
    /// UTF-8 string column.
    Str(StringBuilder),
    /// 16-bit signed integer column (covers both SAS Int8 and Int16).
    Int16(Int16Builder),
    /// 32-bit signed integer column.
    Int32(Int32Builder),
    /// 32-bit floating point column.
    Float32(Float32Builder),
    /// 64-bit floating point column.
    Float64(Float64Builder),
    /// Date column (days since Unix epoch).
    Date32(Date32Builder),
    /// Timestamp with second precision.
    TimestampSecond(TimestampSecondBuilder),
    /// Timestamp with millisecond precision.
    TimestampMillisecond(TimestampMillisecondBuilder),
    /// Timestamp with microsecond precision.
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    /// Timestamp with nanosecond precision.
    TimestampNanosecond(TimestampNanosecondBuilder),
    /// Time of day with second precision.
    Time32Second(Time32SecondBuilder),
    /// Time of day with microsecond precision.
    Time64Microsecond(Time64MicrosecondBuilder),
}

impl ColumnBuilder {
    /// Returns a mutable reference to the inner [`StringBuilder`].
    ///
    /// # Panics
    /// Panics if `self` is not `ColumnBuilder::Str`.
    pub(crate) fn as_string_mut(&mut self) -> &mut StringBuilder {
        match self {
            ColumnBuilder::Str(b) => b,
            _ => panic!("ColumnBuilder::as_string_mut called on non-string builder"),
        }
    }

    /// Appends a null value, regardless of the underlying builder type.
    pub(crate) fn append_null(&mut self) {
        match self {
            ColumnBuilder::Str(b) => b.append_null(),
            ColumnBuilder::Int16(b) => b.append_null(),
            ColumnBuilder::Int32(b) => b.append_null(),
            ColumnBuilder::Float32(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Date32(b) => b.append_null(),
            ColumnBuilder::TimestampSecond(b) => b.append_null(),
            ColumnBuilder::TimestampMillisecond(b) => b.append_null(),
            ColumnBuilder::TimestampMicrosecond(b) => b.append_null(),
            ColumnBuilder::TimestampNanosecond(b) => b.append_null(),
            ColumnBuilder::Time32Second(b) => b.append_null(),
            ColumnBuilder::Time64Microsecond(b) => b.append_null(),
        }
    }

    /// Finishes the builder and returns the completed Arrow array.
    pub(crate) fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Str(b) => Arc::new(b.finish()),
            ColumnBuilder::Int16(b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Date32(b) => Arc::new(b.finish()),
            ColumnBuilder::TimestampSecond(b) => Arc::new(b.finish()),
            ColumnBuilder::TimestampMillisecond(b) => Arc::new(b.finish()),
            ColumnBuilder::TimestampMicrosecond(b) => Arc::new(b.finish()),
            ColumnBuilder::TimestampNanosecond(b) => Arc::new(b.finish()),
            ColumnBuilder::Time32Second(b) => Arc::new(b.finish()),
            ColumnBuilder::Time64Microsecond(b) => Arc::new(b.finish()),
        }
    }

    /// Creates a typed builder matching the variable's metadata.
    ///
    /// Uses `var_type`, `var_type_class`, and `var_format_class` to select the
    /// correct builder variant, and pre-sizes it with `capacity` rows.
    /// For string columns, `storage_width` provides a byte-level capacity hint.
    fn from_metadata(vm: &ReadStatVarMetadata, capacity: usize) -> Self {
        match vm.var_type_class {
            ReadStatVarTypeClass::String => {
                ColumnBuilder::Str(StringBuilder::with_capacity(
                    capacity,
                    capacity * vm.storage_width,
                ))
            }
            ReadStatVarTypeClass::Numeric => {
                match vm.var_format_class {
                    Some(ReadStatVarFormatClass::Date) => {
                        ColumnBuilder::Date32(Date32Builder::with_capacity(capacity))
                    }
                    Some(ReadStatVarFormatClass::DateTime) => {
                        ColumnBuilder::TimestampSecond(
                            TimestampSecondBuilder::with_capacity(capacity),
                        )
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithMilliseconds) => {
                        ColumnBuilder::TimestampMillisecond(
                            TimestampMillisecondBuilder::with_capacity(capacity),
                        )
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => {
                        ColumnBuilder::TimestampMicrosecond(
                            TimestampMicrosecondBuilder::with_capacity(capacity),
                        )
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithNanoseconds) => {
                        ColumnBuilder::TimestampNanosecond(
                            TimestampNanosecondBuilder::with_capacity(capacity),
                        )
                    }
                    Some(ReadStatVarFormatClass::Time) => {
                        ColumnBuilder::Time32Second(
                            Time32SecondBuilder::with_capacity(capacity),
                        )
                    }
                    Some(ReadStatVarFormatClass::TimeWithMicroseconds) => {
                        ColumnBuilder::Time64Microsecond(
                            Time64MicrosecondBuilder::with_capacity(capacity),
                        )
                    }
                    None => {
                        // Plain numeric — dispatch by storage type
                        match vm.var_type {
                            ReadStatVarType::Int8 | ReadStatVarType::Int16 => {
                                ColumnBuilder::Int16(Int16Builder::with_capacity(capacity))
                            }
                            ReadStatVarType::Int32 => {
                                ColumnBuilder::Int32(Int32Builder::with_capacity(capacity))
                            }
                            ReadStatVarType::Float => {
                                ColumnBuilder::Float32(Float32Builder::with_capacity(capacity))
                            }
                            _ => {
                                ColumnBuilder::Float64(Float64Builder::with_capacity(capacity))
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Holds parsed row data from a `.sas7bdat` file and converts it to Arrow format.
///
/// Each instance processes one streaming chunk of rows. Values are appended
/// directly into typed Arrow `ColumnBuilder`s during the `handle_value`
/// callback, then finished into an Arrow [`RecordBatch`] via `cols_to_batch`.
pub struct ReadStatData {
    /// Number of variables (columns) in the dataset.
    pub var_count: i32,
    /// Per-variable metadata, keyed by variable index.
    /// Wrapped in `Arc` so parallel chunks share the same metadata without deep cloning.
    pub vars: Arc<BTreeMap<i32, ReadStatVarMetadata>>,
    /// Typed Arrow builders — one per variable, pre-sized with capacity hints.
    pub builders: Vec<ColumnBuilder>,
    /// Arrow schema for the dataset.
    /// Wrapped in `Arc` for cheap sharing across parallel chunks.
    pub schema: Arc<Schema>,
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
    /// Wrapped in `Arc` so parallel chunks share the same filter without deep cloning.
    pub column_filter: Option<Arc<BTreeMap<i32, i32>>>,
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
            vars: Arc::new(BTreeMap::new()),
            // data
            builders: Vec::new(),
            schema: Arc::new(Schema::empty()),
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

    /// Allocates typed Arrow builders with capacity for `chunk_rows_to_process`.
    ///
    /// Each builder's type is determined by the variable metadata. String builders
    /// are additionally pre-sized with `storage_width * chunk_rows` bytes.
    pub fn allocate_builders(self) -> Self {
        let capacity = self.chunk_rows_to_process;
        let builders: Vec<ColumnBuilder> = self
            .vars
            .values()
            .map(|vm| ColumnBuilder::from_metadata(vm, capacity))
            .collect();
        Self { builders, ..self }
    }

    /// Finishes all builders and assembles the Arrow [`RecordBatch`].
    ///
    /// Each builder produces its final array via `finish()`, which is an O(1)
    /// operation (no data copying). The heavy work was already done during
    /// `handle_value` when values were appended directly into the builders.
    pub(crate) fn cols_to_batch(&mut self) -> Result<(), ReadStatError> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(|b| b.finish())
            .collect();

        self.batch = Some(RecordBatch::try_new(self.schema.clone(), arrays)?);

        Ok(())
    }

    /// Parses row data from the file and converts it to an Arrow [`RecordBatch`].
    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), ReadStatError> {
        // parse data and if successful then convert cols into a record batch
        self.parse_data(rsp)?;
        self.cols_to_batch()?;
        Ok(())
    }

    /// Parses row data from an in-memory byte slice and converts it to an Arrow [`RecordBatch`].
    ///
    /// Equivalent to [`read_data`](ReadStatData::read_data) but reads from a `&[u8]`
    /// buffer instead of a file path.
    pub fn read_data_from_bytes(&mut self, bytes: &[u8]) -> Result<(), ReadStatError> {
        self.parse_data_from_bytes(bytes)?;
        self.cols_to_batch()?;
        Ok(())
    }

    /// Parses row data from a memory-mapped `.sas7bdat` file and converts it to an Arrow [`RecordBatch`].
    ///
    /// Opens the file at `path` and memory-maps it, avoiding explicit read syscalls.
    /// Especially beneficial for large files and repeated chunk reads against the
    /// same file, as the OS manages page caching automatically.
    ///
    /// # Safety
    ///
    /// Memory mapping is safe as long as the file is not modified or truncated by
    /// another process while the map is active.
    pub fn read_data_from_mmap(&mut self, path: &std::path::Path) -> Result<(), ReadStatError> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        self.read_data_from_bytes(&mmap)
    }

    /// Parses row data from the file via FFI callbacks (without Arrow conversion).
    pub(crate) fn parse_data(&mut self, rsp: &ReadStatPath) -> Result<(), ReadStatError> {
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
                &rsp.path.to_string_lossy()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(std::time::Duration::from_millis(120));
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

    fn parse_data_from_bytes(&mut self, bytes: &[u8]) -> Result<(), ReadStatError> {
        let mut buffer_ctx = ReadStatBufferCtx::new(bytes);

        // initialize context
        let ctx = self as *mut ReadStatData as *mut c_void;

        // initialize error
        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {:#?}", &error);

        // Dummy path — custom I/O handlers ignore it
        let dummy_path = CString::new("").unwrap();

        // setup parser with buffer I/O
        let error = buffer_ctx
            .configure_parser(
                ReadStatParser::new()
                    .set_value_handler(Some(cb::handle_value))?
                    .set_row_limit(Some(self.chunk_rows_to_process.try_into()?))?
                    .set_row_offset(Some(self.chunk_row_start.try_into()?))?
            )?
            .parse_sas7bdat(dummy_path.as_ptr(), ctx);

        check_c_error(error as i32)?;
        Ok(())
    }

    /// Initializes this instance with metadata and chunk boundaries, allocating builders.
    ///
    /// Wraps `vars` and `schema` in `Arc` internally. For the parallel read path,
    /// prefer [`init_shared`](ReadStatData::init_shared) which accepts pre-wrapped
    /// `Arc`s to avoid repeated deep clones.
    pub fn init(self, md: ReadStatMetadata, row_start: u32, row_end: u32) -> Self {
        self.set_metadata(md)
            .set_chunk_counts(row_start, row_end)
            .allocate_builders()
    }

    /// Initializes this instance with pre-shared metadata and chunk boundaries.
    ///
    /// Accepts `Arc`-wrapped `vars` and `schema` for cheap cloning in parallel loops.
    /// Each call only increments reference counts (atomic +1) instead of deep-cloning
    /// the entire metadata tree.
    pub fn init_shared(
        self,
        var_count: i32,
        vars: Arc<BTreeMap<i32, ReadStatVarMetadata>>,
        schema: Arc<Schema>,
        row_start: u32,
        row_end: u32,
    ) -> Self {
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
        .set_chunk_counts(row_start, row_end)
        .allocate_builders()
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
        let vars = Arc::new(md.vars);
        let schema = Arc::new(md.schema);
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
    /// Accepts an `Arc`-wrapped filter for cheap sharing across parallel chunks.
    /// Must be called **before** [`init`](ReadStatData::init) so that
    /// `total_var_count` is preserved when `set_metadata` runs.
    pub fn set_column_filter(self, filter: Option<Arc<BTreeMap<i32, i32>>>, total_var_count: i32) -> Self {
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
