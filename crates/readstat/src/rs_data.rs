//! Data reading and Arrow [`RecordBatch`](arrow_array::RecordBatch) conversion.
//!
//! [`ReadStatData`] coordinates the FFI parsing of row values from a `.sas7bdat` file,
//! accumulating them directly into typed Arrow builders via the `handle_value`
//! callback, then finishing them into an Arrow `RecordBatch` for downstream writing.
//! Supports streaming chunks with configurable row offsets and progress tracking.

use arrow::datatypes::Schema;
use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        Date32Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, StringBuilder,
        Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
        Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder, TimestampSecondBuilder,
    },
};
use log::debug;
use std::{
    collections::BTreeMap,
    ffi::CString,
    os::raw::c_void,
    sync::{Arc, atomic::AtomicUsize},
};

use crate::{
    cb,
    err::{ReadStatError, check_c_error},
    progress::ProgressCallback,
    rs_buffer_io::ReadStatBufferCtx,
    rs_metadata::{ReadStatMetadata, ReadStatVarMetadata},
    rs_parser::ReadStatParser,
    rs_path::ReadStatPath,
    rs_var::{ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass},
};

/// Upper bound on the row capacity pre-allocated for Arrow builders.
///
/// The claimed row count comes from an untrusted file header, so the up-front
/// allocation is capped here; builders grow on demand past this for honest
/// files. 1,000,000 rows is far beyond the default 10k streaming chunk while
/// keeping the worst-case empty-builder reservation bounded.
const MAX_PREALLOC_ROWS: usize = 1_000_000;

/// A typed Arrow array builder for a single column.
///
/// Each variant wraps the corresponding Arrow builder, pre-sized with capacity
/// hints from the metadata (row count, string `storage_width`). Values are
/// appended directly during FFI callbacks, eliminating intermediate allocations.
pub(crate) enum ColumnBuilder {
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
    /// Time of day with millisecond precision.
    Time32Millisecond(Time32MillisecondBuilder),
    /// Time of day with microsecond precision.
    Time64Microsecond(Time64MicrosecondBuilder),
    /// Time of day with nanosecond precision.
    Time64Nanosecond(Time64NanosecondBuilder),
}

impl ColumnBuilder {
    /// Appends a null value, regardless of the underlying builder type.
    pub(crate) fn append_null(&mut self) {
        match self {
            Self::Str(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::TimestampSecond(b) => b.append_null(),
            Self::TimestampMillisecond(b) => b.append_null(),
            Self::TimestampMicrosecond(b) => b.append_null(),
            Self::TimestampNanosecond(b) => b.append_null(),
            Self::Time32Second(b) => b.append_null(),
            Self::Time32Millisecond(b) => b.append_null(),
            Self::Time64Microsecond(b) => b.append_null(),
            Self::Time64Nanosecond(b) => b.append_null(),
        }
    }

    /// Finishes the builder and returns the completed Arrow array.
    pub(crate) fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Str(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::TimestampSecond(b) => Arc::new(b.finish()),
            Self::TimestampMillisecond(b) => Arc::new(b.finish()),
            Self::TimestampMicrosecond(b) => Arc::new(b.finish()),
            Self::TimestampNanosecond(b) => Arc::new(b.finish()),
            Self::Time32Second(b) => Arc::new(b.finish()),
            Self::Time32Millisecond(b) => Arc::new(b.finish()),
            Self::Time64Microsecond(b) => Arc::new(b.finish()),
            Self::Time64Nanosecond(b) => Arc::new(b.finish()),
        }
    }

    /// Creates a typed builder matching the variable's metadata.
    ///
    /// Uses `var_type`, `var_type_class`, and `var_format_class` to select the
    /// correct builder variant, and pre-sizes it with `capacity` rows.
    /// For string columns, `storage_width` provides a byte-level capacity hint.
    fn from_metadata(vm: &ReadStatVarMetadata, capacity: usize) -> Self {
        match vm.var_type_class {
            ReadStatVarTypeClass::String => Self::Str(StringBuilder::with_capacity(
                capacity,
                // saturating_mul: storage_width is an untrusted file-header
                // field, so guard the byte hint against usize overflow.
                capacity.saturating_mul(vm.storage_width),
            )),
            ReadStatVarTypeClass::Numeric => {
                match vm.var_format_class {
                    Some(ReadStatVarFormatClass::Date) => {
                        Self::Date32(Date32Builder::with_capacity(capacity))
                    }
                    Some(ReadStatVarFormatClass::DateTime) => {
                        Self::TimestampSecond(TimestampSecondBuilder::with_capacity(capacity))
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithMilliseconds) => {
                        Self::TimestampMillisecond(TimestampMillisecondBuilder::with_capacity(
                            capacity,
                        ))
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => {
                        Self::TimestampMicrosecond(TimestampMicrosecondBuilder::with_capacity(
                            capacity,
                        ))
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithNanoseconds) => {
                        Self::TimestampNanosecond(TimestampNanosecondBuilder::with_capacity(
                            capacity,
                        ))
                    }
                    Some(ReadStatVarFormatClass::Time) => {
                        Self::Time32Second(Time32SecondBuilder::with_capacity(capacity))
                    }
                    Some(ReadStatVarFormatClass::TimeWithMilliseconds) => {
                        Self::Time32Millisecond(Time32MillisecondBuilder::with_capacity(capacity))
                    }
                    Some(ReadStatVarFormatClass::TimeWithMicroseconds) => {
                        Self::Time64Microsecond(Time64MicrosecondBuilder::with_capacity(capacity))
                    }
                    Some(ReadStatVarFormatClass::TimeWithNanoseconds) => {
                        Self::Time64Nanosecond(Time64NanosecondBuilder::with_capacity(capacity))
                    }
                    None => {
                        // Plain numeric — dispatch by storage type
                        match vm.var_type {
                            ReadStatVarType::Int8 | ReadStatVarType::Int16 => {
                                Self::Int16(Int16Builder::with_capacity(capacity))
                            }
                            ReadStatVarType::Int32 => {
                                Self::Int32(Int32Builder::with_capacity(capacity))
                            }
                            ReadStatVarType::Float => {
                                Self::Float32(Float32Builder::with_capacity(capacity))
                            }
                            _ => Self::Float64(Float64Builder::with_capacity(capacity)),
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
    pub(crate) builders: Vec<ColumnBuilder>,
    /// Arrow schema for the dataset.
    /// Wrapped in `Arc` for cheap sharing across parallel chunks.
    pub schema: Arc<Schema>,
    /// The Arrow `RecordBatch` produced after parsing, if available.
    pub batch: Option<RecordBatch>,
    /// Number of rows to process in this chunk.
    pub chunk_rows_to_process: usize,
    /// Starting row offset for this chunk.
    pub(crate) chunk_row_start: usize,
    /// Ending row offset (exclusive) for this chunk.
    pub(crate) chunk_row_end: usize,
    /// Number of rows actually processed so far in this chunk.
    pub(crate) chunk_rows_processed: usize,
    /// Shared atomic counter of total rows processed across all chunks.
    pub(crate) total_rows_processed: Option<Arc<AtomicUsize>>,
    /// Optional progress callback for visual feedback during parsing.
    pub(crate) progress: Option<Arc<dyn ProgressCallback>>,
    /// A typed error raised by a value callback that aborted parsing.
    ///
    /// Set by `handle_value` (e.g. on date/time overflow or a builder/value
    /// type mismatch) and surfaced by the parse routines in preference to the
    /// generic `USER_ABORT` the C library reports for any callback abort.
    pub(crate) abort_error: Option<ReadStatError>,
    /// Optional mapping: original var index -> filtered column index.
    /// Wrapped in `Arc` so parallel chunks share the same filter without deep cloning.
    pub(crate) column_filter: Option<Arc<BTreeMap<i32, i32>>>,
    /// Total variable count in the unfiltered dataset.
    /// Used for row-boundary detection in `handle_value` when filtering is active.
    /// Defaults to `var_count` when no filter is set.
    pub(crate) total_var_count: i32,
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
            total_rows_processed: None,
            // progress
            progress: None,
            // errors
            abort_error: None,
            // column filtering
            column_filter: None,
            total_var_count: 0,
        }
    }

    /// Allocates typed Arrow builders with capacity for `chunk_rows_to_process`.
    ///
    /// Each builder's type is determined by the variable metadata. String builders
    /// are additionally pre-sized with `storage_width * chunk_rows` bytes.
    ///
    /// The capacity hint is clamped to [`MAX_PREALLOC_ROWS`] because both the row
    /// count and per-string `storage_width` originate from untrusted file headers;
    /// a crafted file claiming billions of rows would otherwise trigger a multi-GB
    /// up-front allocation (or a multiply overflow) before a single row is parsed.
    /// Builders grow on demand, so clamping costs honest files nothing.
    #[must_use]
    pub fn allocate_builders(self) -> Self {
        let capacity = self.chunk_rows_to_process.min(MAX_PREALLOC_ROWS);
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
            .map(ColumnBuilder::finish)
            .collect();

        self.batch = Some(RecordBatch::try_new(self.schema.clone(), arrays)?);

        Ok(())
    }

    /// Parses row data from the file and converts it to an Arrow [`RecordBatch`].
    ///
    /// # Errors
    ///
    /// Returns [`ReadStatError`] if FFI parsing or Arrow conversion fails.
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
    ///
    /// # Errors
    ///
    /// Returns [`ReadStatError`] if FFI parsing or Arrow conversion fails.
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
    ///
    /// # Errors
    ///
    /// Returns [`ReadStatError`] if the file cannot be opened, mapped, or parsed.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn read_data_from_mmap(&mut self, path: &std::path::Path) -> Result<(), ReadStatError> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        self.read_data_from_bytes(&mmap)
    }

    /// Parses row data from the file via FFI callbacks (without Arrow conversion).
    #[allow(clippy::cast_possible_wrap, clippy::ptr_as_ptr)]
    pub(crate) fn parse_data(&mut self, rsp: &ReadStatPath) -> Result<(), ReadStatError> {
        // path as pointer
        debug!("Path as C string is {:?}", rsp.cstring_path);
        let ppath = rsp.cstring_path.as_ptr();

        // initialize context
        let ctx = std::ptr::from_mut::<Self>(self) as *mut c_void;

        // initialize error
        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {error:#?}");

        // setup parser
        // once call parse_sas7bdat, iteration begins
        let error = ReadStatParser::new()?
            // do not set metadata handler nor variable handler as already processed
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(Some(self.chunk_rows_to_process.try_into()?))?
            .set_row_offset(Some(self.chunk_row_start.try_into()?))?
            .parse_sas7bdat(ppath, ctx);

        // A value callback may have aborted with a specific, typed error; prefer
        // it over the generic `USER_ABORT` the C library reports for any abort.
        if let Some(e) = self.abort_error.take() {
            return Err(e);
        }
        check_c_error(error as i32)?;

        // Advance the progress bar by the rows just parsed. Doing this *after*
        // the chunk completes (rather than before) keeps the displayed position
        // in step with work actually done — under `--parallel` a pre-parse
        // increment made the bar jump straight to 100%.
        if let Some(progress) = &self.progress {
            progress.inc(self.chunk_rows_to_process as u64);
        }

        Ok(())
    }

    #[allow(clippy::cast_possible_wrap, clippy::ptr_as_ptr)]
    fn parse_data_from_bytes(&mut self, bytes: &[u8]) -> Result<(), ReadStatError> {
        let mut buffer_ctx = ReadStatBufferCtx::new(bytes);

        // initialize context
        let ctx = std::ptr::from_mut::<Self>(self) as *mut c_void;

        // initialize error
        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {error:#?}");

        // Dummy path — custom I/O handlers ignore it
        let dummy_path = CString::new("").expect("empty string is valid C string");

        // setup parser with buffer I/O
        let error = buffer_ctx
            .configure_parser(
                ReadStatParser::new()?
                    .set_value_handler(Some(cb::handle_value))?
                    .set_row_limit(Some(self.chunk_rows_to_process.try_into()?))?
                    .set_row_offset(Some(self.chunk_row_start.try_into()?))?,
            )?
            .parse_sas7bdat(dummy_path.as_ptr(), ctx);

        // A value callback may have aborted with a specific, typed error; prefer
        // it over the generic `USER_ABORT` the C library reports for any abort.
        if let Some(e) = self.abort_error.take() {
            return Err(e);
        }
        check_c_error(error as i32)?;
        Ok(())
    }

    /// Initializes this instance with metadata and chunk boundaries, allocating builders.
    ///
    /// Wraps `vars` and `schema` in `Arc` internally. For the parallel read path,
    /// prefer [`init_shared`](ReadStatData::init_shared) which accepts pre-wrapped
    /// `Arc`s to avoid repeated deep clones.
    #[must_use]
    pub fn init(self, md: ReadStatMetadata, row_start: u32, row_end: u32) -> Self {
        self.set_metadata(md)
            .set_chunk_counts(row_start, row_end)
            .allocate_builders()
    }

    /// Initializes this instance with a column filter applied, in one step.
    ///
    /// Combines [`set_column_filter`](ReadStatData::set_column_filter) and
    /// [`init`](ReadStatData::init) in the correct order so callers cannot
    /// accidentally invoke them the wrong way around (which would clobber the
    /// original variable count needed for row-boundary detection).
    ///
    /// `md` must be the **original, unfiltered** metadata and `mapping` the
    /// result of [`ReadStatMetadata::resolve_selected_columns`]. The filtered
    /// metadata and the original variable count are derived internally.
    ///
    /// ```no_run
    /// use readstat::{ReadStatPath, ReadStatMetadata, ReadStatData};
    ///
    /// # fn main() -> Result<(), readstat::ReadStatError> {
    /// let rsp = ReadStatPath::new("data.sas7bdat")?;
    /// let mut md = ReadStatMetadata::new();
    /// md.read_metadata(&rsp, false)?;
    ///
    /// if let Some(mapping) = md.resolve_selected_columns(Some(vec!["name".into(), "age".into()]))? {
    ///     let row_count = u32::try_from(md.row_count)?;
    ///     let mut d = ReadStatData::new().init_filtered(md, &mapping, 0, row_count);
    ///     d.read_data(&rsp)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn init_filtered(
        self,
        md: ReadStatMetadata,
        mapping: &BTreeMap<i32, i32>,
        row_start: u32,
        row_end: u32,
    ) -> Self {
        let original_var_count = md.var_count;
        let filtered = md.filter_to_selected_columns(mapping);
        self.set_column_filter(Some(Arc::new(mapping.clone())), original_var_count)
            .init(filtered, row_start, row_end)
    }

    /// Initializes this instance with pre-shared metadata and chunk boundaries.
    ///
    /// Accepts `Arc`-wrapped `vars` and `schema` for cheap cloning in parallel loops.
    /// Each call only increments reference counts (atomic +1) instead of deep-cloning
    /// the entire metadata tree.
    #[must_use]
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

    #[allow(clippy::cast_possible_truncation)]
    fn set_chunk_counts(self, row_start: u32, row_end: u32) -> Self {
        // saturating_sub: guard against a caller passing row_end < row_start,
        // which would underflow-panic in debug and wrap to ~4 billion in
        // release (then feed an enormous builder pre-allocation).
        let chunk_rows_to_process = row_end.saturating_sub(row_start) as usize;
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

    /// Sets the shared atomic counter for tracking rows processed across chunks.
    #[must_use]
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
    #[must_use]
    pub fn set_column_filter(
        self,
        filter: Option<Arc<BTreeMap<i32, i32>>>,
        total_var_count: i32,
    ) -> Self {
        Self {
            column_filter: filter,
            total_var_count,
            ..self
        }
    }

    /// Attaches a progress callback for feedback during parsing.
    ///
    /// The callback receives progress increments and parsing status updates.
    /// See [`ProgressCallback`] for the required interface.
    #[must_use]
    pub fn set_progress(self, progress: Arc<dyn ProgressCallback>) -> Self {
        Self {
            progress: Some(progress),
            ..self
        }
    }
}
