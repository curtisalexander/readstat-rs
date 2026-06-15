//! High-level convenience entry points for the common case.
//!
//! These free functions wrap the lower-level [`ReadStatPath`] / [`ReadStatMetadata`]
//! / [`ReadStatData`] pipeline for callers who just want to read a whole file.
//! For streaming, parallelism, or column filtering, use the types directly —
//! see the crate-level documentation for examples.

use std::path::Path;

use arrow_array::RecordBatch;

use crate::{
    err::ReadStatError, rs_data::ReadStatData, rs_metadata::ReadStatMetadata, rs_path::ReadStatPath,
};

/// Options for high-level reads into an Arrow [`RecordBatch`].
///
/// `ReadOptions` is intentionally small and additive: it covers the common
/// knobs callers need before dropping down to [`ReadStatMetadata`] and
/// [`ReadStatData`] directly.
///
/// ```no_run
/// # fn main() -> Result<(), readstat::ReadStatError> {
/// let batch = readstat::read_to_batch_with_options(
///     "data.sas7bdat",
///     readstat::ReadOptions::new()
///         .columns(["make", "model", "msrp"])
///         .row_range(0, Some(100)),
/// )?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReadOptions {
    columns: Option<Vec<String>>,
    row_start: u32,
    row_count: Option<u32>,
}

impl ReadOptions {
    /// Creates options that read all rows and all columns.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects a subset of columns by name.
    ///
    /// Output columns are returned in the dataset's original order. Unknown
    /// names produce [`ReadStatError::ColumnsNotFound`].
    #[must_use]
    pub fn columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Sets the starting row offset and optional number of rows to read.
    ///
    /// `row_start` is zero-based. `row_count = None` reads to the end of the file.
    #[must_use]
    pub fn row_range(mut self, row_start: u32, row_count: Option<u32>) -> Self {
        self.row_start = row_start;
        self.row_count = row_count;
        self
    }

    /// Sets the starting row offset.
    #[must_use]
    pub fn row_start(mut self, row_start: u32) -> Self {
        self.row_start = row_start;
        self
    }

    /// Sets the maximum number of rows to read.
    #[must_use]
    pub fn row_count(mut self, row_count: u32) -> Self {
        self.row_count = Some(row_count);
        self
    }
}

/// Reads file-level and variable metadata from a `.sas7bdat` file without
/// loading any row data.
///
/// This is the one-call equivalent of constructing a [`ReadStatPath`],
/// creating a [`ReadStatMetadata`], and calling
/// [`ReadStatMetadata::read_metadata`].
///
/// ```no_run
/// # fn main() -> Result<(), readstat::ReadStatError> {
/// let md = readstat::read_metadata("data.sas7bdat")?;
/// println!("{} rows x {} columns", md.row_count, md.var_count);
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns [`ReadStatError`] if the path is invalid or FFI parsing fails.
pub fn read_metadata<P: AsRef<Path>>(path: P) -> Result<ReadStatMetadata, ReadStatError> {
    let rsp = ReadStatPath::new(path)?;
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false)?;
    Ok(md)
}

/// Reads every row of a `.sas7bdat` file into a single Arrow [`RecordBatch`].
///
/// Best for files that fit comfortably in memory. For large files, read in
/// streaming chunks with [`ReadStatData::init`] and [`build_offsets`](crate::build_offsets)
/// instead — see the crate-level documentation.
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
    read_to_batch_with_options(path, ReadOptions::new())
}

/// Reads rows from a `.sas7bdat` file into a single Arrow [`RecordBatch`] using
/// high-level options for row ranges and column projection.
///
/// This is the convenience API to reach for before using the lower-level
/// [`ReadStatMetadata`] / [`ReadStatData`] pipeline directly.
///
/// ```no_run
/// # fn main() -> Result<(), readstat::ReadStatError> {
/// let batch = readstat::read_to_batch_with_options(
///     "data.sas7bdat",
///     readstat::ReadOptions::new()
///         .columns(["name", "age"])
///         .row_start(10)
///         .row_count(25),
/// )?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns [`ReadStatError`] if the path is invalid, requested columns are not
/// present, row bounds cannot be represented, FFI parsing fails, or Arrow batch
/// construction fails.
pub fn read_to_batch_with_options<P: AsRef<Path>>(
    path: P,
    options: ReadOptions,
) -> Result<RecordBatch, ReadStatError> {
    let rsp = ReadStatPath::new(path)?;
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false)?;

    let dataset_rows = u32::try_from(md.row_count)?;
    let (row_start, row_end) = resolve_row_range(dataset_rows, &options)?;
    let mapping = md.resolve_selected_columns(options.columns)?;

    let mut d = match mapping {
        Some(mapping) => ReadStatData::new().init_filtered(md, &mapping, row_start, row_end),
        None => ReadStatData::new().init(md, row_start, row_end),
    };
    d.read_data(&rsp)?;

    d.batch
        .ok_or_else(|| ReadStatError::Other("no record batch was produced".to_string()))
}

/// Reads every row of a `.sas7bdat` byte slice into a single Arrow [`RecordBatch`].
///
/// Useful when data comes from object storage, HTTP uploads, or another source
/// where writing a temporary file would be wasteful.
///
/// # Errors
///
/// Returns [`ReadStatError`] if metadata or data parsing fails.
pub fn read_to_batch_from_bytes(bytes: &[u8]) -> Result<RecordBatch, ReadStatError> {
    read_to_batch_from_bytes_with_options(bytes, ReadOptions::new())
}

/// Reads a `.sas7bdat` byte slice into a single Arrow [`RecordBatch`] using
/// high-level options for row ranges and column projection.
///
/// # Errors
///
/// Returns [`ReadStatError`] if requested columns are not present, row bounds
/// cannot be represented, FFI parsing fails, or Arrow batch construction fails.
pub fn read_to_batch_from_bytes_with_options(
    bytes: &[u8],
    options: ReadOptions,
) -> Result<RecordBatch, ReadStatError> {
    let mut md = ReadStatMetadata::new();
    md.read_metadata_from_bytes(bytes, false)?;

    let dataset_rows = u32::try_from(md.row_count)?;
    let (row_start, row_end) = resolve_row_range(dataset_rows, &options)?;
    let mapping = md.resolve_selected_columns(options.columns)?;

    let mut d = match mapping {
        Some(mapping) => ReadStatData::new().init_filtered(md, &mapping, row_start, row_end),
        None => ReadStatData::new().init(md, row_start, row_end),
    };
    d.read_data_from_bytes(bytes)?;

    d.batch
        .ok_or_else(|| ReadStatError::Other("no record batch was produced".to_string()))
}

fn resolve_row_range(
    dataset_rows: u32,
    options: &ReadOptions,
) -> Result<(u32, u32), ReadStatError> {
    if options.row_start > dataset_rows {
        return Err(ReadStatError::Other(format!(
            "row_start ({}) is beyond the dataset row count ({dataset_rows})",
            options.row_start
        )));
    }

    let row_end = match options.row_count {
        Some(row_count) => options.row_start.checked_add(row_count).ok_or_else(|| {
            ReadStatError::Other(format!(
                "row range overflows u32: start={}, count={row_count}",
                options.row_start
            ))
        })?,
        None => dataset_rows,
    }
    .min(dataset_rows);

    Ok((options.row_start, row_end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_options_defaults_to_all_rows_and_columns() {
        let options = ReadOptions::new();
        assert_eq!(resolve_row_range(10, &options).unwrap(), (0, 10));
        assert_eq!(options.columns, None);
    }

    #[test]
    fn read_options_resolves_row_count() {
        let options = ReadOptions::new().row_range(3, Some(4));
        assert_eq!(resolve_row_range(10, &options).unwrap(), (3, 7));
    }

    #[test]
    fn read_options_clamps_row_count_to_dataset_end() {
        let options = ReadOptions::new().row_range(8, Some(10));
        assert_eq!(resolve_row_range(10, &options).unwrap(), (8, 10));
    }

    #[test]
    fn read_options_rejects_start_beyond_dataset_end() {
        let options = ReadOptions::new().row_start(11);
        assert!(matches!(
            resolve_row_range(10, &options),
            Err(ReadStatError::Other(_))
        ));
    }

    #[test]
    fn read_options_rejects_overflowing_row_range() {
        let options = ReadOptions::new().row_range(u32::MAX, Some(1));
        assert!(matches!(
            resolve_row_range(u32::MAX, &options),
            Err(ReadStatError::Other(_))
        ));
    }

    #[test]
    fn read_options_accepts_columns() {
        let options = ReadOptions::new().columns(["a", "b"]);
        assert_eq!(
            options.columns,
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }
}
