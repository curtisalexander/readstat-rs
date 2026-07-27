//! High-level convenience entry points for the common case.
//!
//! [`ReadStatReader`] is the primary reading API. The free functions are concise
//! equivalents for reading a path with default settings.

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use arrow_array::{RecordBatch, RecordBatchOptions};

use crate::{
    err::ReadStatError, progress::ProgressCallback, rs_data::ReadStatData,
    rs_metadata::ReadStatMetadata, rs_path::ReadStatPath,
};

enum Source {
    Path(ReadStatPath),
    Bytes(Arc<[u8]>),
    #[cfg(not(target_arch = "wasm32"))]
    Mmap(PathBuf),
}

struct ReadPlan {
    metadata: ReadStatMetadata,
    mapping: Option<std::collections::BTreeMap<i32, i32>>,
    count: u32,
}

/// High-level, reusable SAS reader.
///
/// Configure row and column selection once, then use [`read`](Self::read),
/// [`chunks`](Self::chunks), or [`visit`](Self::visit). `visit` holds only the
/// current chunk and is therefore the bounded-memory option.
pub struct ReadStatReader {
    source: Source,
    metadata: Mutex<Option<ReadStatMetadata>>,
    offset: u32,
    limit: Option<u32>,
    columns: Option<Vec<String>>,
    chunk_rows: u32,
    progress: Option<Arc<dyn ProgressCallback>>,
}

impl ReadStatReader {
    /// Creates a reader for a filesystem path.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ReadStatError> {
        Ok(Self::new(Source::Path(ReadStatPath::new(path)?)))
    }

    /// Creates a reader owning an in-memory SAS file.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self::new(Source::Bytes(bytes.into()))
    }

    /// Creates a reader which memory maps `path` for each parse.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_mmap(path: impl Into<PathBuf>) -> Result<Self, ReadStatError> {
        let path = path.into();
        let _ = ReadStatPath::new(&path)?;
        Ok(Self::new(Source::Mmap(path)))
    }

    fn new(source: Source) -> Self {
        Self {
            source,
            metadata: Mutex::new(None),
            offset: 0,
            limit: None,
            columns: None,
            chunk_rows: 10_000,
            progress: None,
        }
    }

    /// Selects the half-open row range beginning at `offset`, optionally limited
    /// to `limit` rows. The range is validated against metadata when reading.
    #[must_use]
    pub fn rows(mut self, offset: u32, limit: Option<u32>) -> Self {
        self.offset = offset;
        self.limit = limit;
        self
    }

    /// Selects columns by name, preserving dataset order.
    ///
    /// An empty selection deliberately produces a zero-column batch with the
    /// requested number of rows.
    #[must_use]
    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Sets rows per yielded chunk. Zero is rejected when reading.
    #[must_use]
    pub fn chunk_rows(mut self, rows: u32) -> Self {
        self.chunk_rows = rows;
        self
    }

    /// Attaches a progress callback.
    #[must_use]
    pub fn progress(mut self, callback: Arc<dyn ProgressCallback>) -> Self {
        self.progress = Some(callback);
        self
    }

    /// Reads and caches metadata transactionally.
    ///
    /// Reusing the same reader for metadata and data uses one cached metadata
    /// snapshot for planning and avoids a second metadata parse. For path-backed
    /// sources, callers must still prevent the file from being replaced between
    /// metadata and data parsing, or between repeated reads with this reader.
    pub fn metadata(&self) -> Result<ReadStatMetadata, ReadStatError> {
        let mut cached = self
            .metadata
            .lock()
            .map_err(|_| ReadStatError::Other("reader metadata cache is poisoned".into()))?;
        if let Some(md) = cached.as_ref() {
            return Ok(md.clone());
        }

        let mut md = ReadStatMetadata::new();
        match &self.source {
            Source::Path(path) => md.read_metadata(path, false)?,
            Source::Bytes(bytes) => md.read_metadata_from_bytes(bytes, false)?,
            #[cfg(not(target_arch = "wasm32"))]
            Source::Mmap(path) => md.read_metadata_from_mmap(path, false)?,
        }
        *cached = Some(md.clone());
        Ok(md)
    }

    fn plan(&self) -> Result<ReadPlan, ReadStatError> {
        if self.chunk_rows == 0 {
            return Err(ReadStatError::InvalidChunkSize);
        }
        let md = self.metadata()?;
        let total = u32::try_from(md.row_count.ok_or(ReadStatError::RowCountUnavailable)?)?;
        if self.offset > total {
            return Err(ReadStatError::InvalidRowRange {
                offset: self.offset,
                limit: self.limit,
                row_count: total,
            });
        }
        let available = total - self.offset;
        let count = self.limit.unwrap_or(available);
        if count > available {
            return Err(ReadStatError::InvalidRowRange {
                offset: self.offset,
                limit: self.limit,
                row_count: total,
            });
        }
        let mapping = md.resolve_selected_columns(self.columns.clone())?;
        Ok(ReadPlan {
            metadata: md,
            mapping,
            count,
        })
    }

    /// Visits each batch without collecting previous chunks.
    pub fn visit(
        &self,
        visitor: impl FnMut(RecordBatch) -> Result<(), ReadStatError>,
    ) -> Result<(), ReadStatError> {
        let plan = self.plan()?;
        self.visit_with_plan(&plan, visitor)
    }

    fn visit_with_plan(
        &self,
        plan: &ReadPlan,
        mut visitor: impl FnMut(RecordBatch) -> Result<(), ReadStatError>,
    ) -> Result<(), ReadStatError> {
        let ReadPlan {
            metadata: md,
            mapping,
            count,
        } = plan;
        if let Some(progress) = &self.progress {
            let label = match &self.source {
                Source::Path(p) => p.path.to_string_lossy().into_owned(),
                Source::Bytes(_) => "<bytes>".into(),
                #[cfg(not(target_arch = "wasm32"))]
                Source::Mmap(p) => p.to_string_lossy().into_owned(),
            };
            progress.parsing_started(&label);
        }
        if *count == 0 {
            return Ok(());
        }
        let end = self
            .offset
            .checked_add(*count)
            .ok_or_else(|| ReadStatError::Other("row offset overflow".into()))?;
        let mut data = ReadStatData::new().init_for_visit(
            md.clone(),
            mapping.as_ref(),
            self.offset,
            end,
            self.chunk_rows as usize,
        );
        if let Some(progress) = &self.progress {
            data = data.set_progress(progress.clone());
        }
        match &self.source {
            Source::Path(path) => data.visit_data(path, self.chunk_rows as usize, &mut visitor),
            Source::Bytes(bytes) => {
                data.visit_data_from_bytes(bytes, self.chunk_rows as usize, &mut visitor)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Source::Mmap(path) => {
                data.visit_data_from_mmap(path, self.chunk_rows as usize, &mut visitor)
            }
        }
    }

    /// Collects all chunks.
    pub fn chunks(&self) -> Result<Vec<RecordBatch>, ReadStatError> {
        let mut batches = Vec::new();
        self.visit(|batch| {
            batches.push(batch);
            Ok(())
        })?;
        Ok(batches)
    }

    /// Reads the selected rows into one batch.
    pub fn read(&self) -> Result<RecordBatch, ReadStatError> {
        let plan = self.plan()?;
        let ReadPlan {
            metadata: md,
            mapping,
            ..
        } = &plan;
        let schema = mapping.as_ref().map_or_else(
            || md.schema.clone(),
            |m| md.filter_to_selected_columns(m).schema,
        );
        let mut batches = Vec::new();
        self.visit_with_plan(&plan, |batch| {
            batches.push(batch);
            Ok(())
        })?;
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(schema)));
        }
        if schema.fields().is_empty() {
            return RecordBatch::try_new_with_options(
                Arc::new(schema),
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(plan.count as usize)),
            )
            .map_err(Into::into);
        }
        arrow::compute::concat_batches(&Arc::new(schema), &batches).map_err(Into::into)
    }
}

/// Reads file-level and variable metadata from a `.sas7bdat` file without
/// loading any row data.
///
/// This delegates to [`ReadStatReader::metadata`].
///
/// ```no_run
/// # fn main() -> Result<(), readstat::ReadStatError> {
/// let md = readstat::read_metadata("data.sas7bdat")?;
/// println!("{:?} rows x {} columns", md.row_count, md.var_count);
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns [`ReadStatError`] if the path is invalid or FFI parsing fails.
pub fn read_metadata<P: AsRef<Path>>(path: P) -> Result<ReadStatMetadata, ReadStatError> {
    ReadStatReader::from_path(path)?.metadata()
}

/// Reads every row of a `.sas7bdat` file into a single Arrow [`RecordBatch`].
///
/// Best for files that fit comfortably in memory. For large files, use
/// [`ReadStatReader::visit`] to process bounded chunks.
///
/// ```no_run
/// # fn main() -> Result<(), readstat::ReadStatError> {
/// let batch = readstat::read_to_batch("data.sas7bdat")?;
/// println!("{} rows x {} columns", batch.num_rows(), batch.num_columns());
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns [`ReadStatError`] if the path is invalid, FFI parsing fails, or the
/// row count cannot be represented (i.e. is negative).
pub fn read_to_batch<P: AsRef<Path>>(path: P) -> Result<RecordBatch, ReadStatError> {
    ReadStatReader::from_path(path)?.read()
}
