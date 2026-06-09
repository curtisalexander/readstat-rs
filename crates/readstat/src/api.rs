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
    let rsp = ReadStatPath::new(path)?;
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false)?;

    let row_count = u32::try_from(md.row_count)?;
    let mut d = ReadStatData::new().init(md, 0, row_count);
    d.read_data(&rsp)?;

    d.batch
        .ok_or_else(|| ReadStatError::Other("no record batch was produced".to_string()))
}
