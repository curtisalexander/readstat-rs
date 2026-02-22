//! Read SAS binary files (`.sas7bdat`) and convert them to other formats.
//!
//! This crate provides a library for parsing SAS binary data files using FFI
//! bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library,
//! then converting the parsed data into Apache Arrow [`RecordBatch`](arrow_array::RecordBatch)
//! format for output as CSV, Feather (Arrow IPC), NDJSON, or Parquet.
//!
//! **Note:** While the underlying [`readstat-sys`](https://docs.rs/readstat-sys) crate
//! exposes bindings for all formats supported by ReadStat (SAS, SPSS, Stata),
//! this crate currently only implements parsing and conversion for **SAS `.sas7bdat` files**.
//!
//! # Data Pipeline
//!
//! ```text
//! .sas7bdat file
//!     → ReadStat C library (FFI parsing via callbacks)
//!         → Typed Arrow builders (StringBuilder, Float64Builder, etc.)
//!             → Arrow RecordBatch
//!                 → Output format (CSV / Feather / NDJSON / Parquet)
//! ```
//!
//! # Examples
//!
//! ## Inspect file metadata
//!
//! Read metadata without loading any row data. Useful for discovering
//! schema, row counts, variable types, and SAS format classifications.
//!
//! ```no_run
//! use readstat::{ReadStatPath, ReadStatMetadata};
//!
//! # fn main() -> Result<(), readstat::ReadStatError> {
//! let rsp = ReadStatPath::new("data.sas7bdat".into())?;
//!
//! let mut md = ReadStatMetadata::new();
//! md.read_metadata(&rsp, false)?;
//!
//! println!("Rows: {}, Variables: {}", md.row_count, md.var_count);
//! println!("Encoding: {}", md.file_encoding);
//! println!("Compression: {:?}", md.compression);
//!
//! // Iterate over variable metadata
//! for (idx, var) in &md.vars {
//!     println!(
//!         "  [{idx}] {} ({:?}, format: {})",
//!         var.var_name, var.var_type_class, var.var_format
//!     );
//! }
//!
//! // The Arrow schema is also available
//! println!("Schema: {:?}", md.schema);
//! # Ok(())
//! # }
//! ```
//!
//! ## Read all data into Arrow RecordBatch
//!
//! Parse the entire file into a single Arrow [`RecordBatch`](arrow_array::RecordBatch).
//! Best for smaller files that fit comfortably in memory.
//!
//! ```no_run
//! use readstat::{ReadStatPath, ReadStatMetadata, ReadStatData};
//!
//! # fn main() -> Result<(), readstat::ReadStatError> {
//! let rsp = ReadStatPath::new("data.sas7bdat".into())?;
//!
//! // Read metadata first
//! let mut md = ReadStatMetadata::new();
//! md.read_metadata(&rsp, false)?;
//!
//! // Read all rows into a single chunk
//! let row_count = md.row_count as u32;
//! let mut d = ReadStatData::new().init(md, 0, row_count);
//! d.read_data(&rsp)?;
//!
//! // Access the Arrow RecordBatch
//! if let Some(batch) = &d.batch {
//!     println!("Got {} rows x {} columns", batch.num_rows(), batch.num_columns());
//!     println!("Schema: {:?}", batch.schema());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Stream data in chunks and write to Parquet
//!
//! For large files, read in streaming chunks to control memory usage.
//! Each chunk is parsed and written incrementally.
//!
//! ```no_run
//! use readstat::{
//!     ReadStatPath, ReadStatMetadata, ReadStatData, ReadStatWriter,
//!     WriteConfig, OutFormat, build_offsets,
//! };
//!
//! # fn main() -> Result<(), readstat::ReadStatError> {
//! let rsp = ReadStatPath::new("data.sas7bdat".into())?;
//! let wc = WriteConfig::new(
//!     Some("output.parquet".into()),
//!     Some(OutFormat::parquet),
//!     false, // overwrite
//!     None,  // compression (defaults to Snappy for Parquet)
//!     None,  // compression_level
//! )?;
//!
//! let mut md = ReadStatMetadata::new();
//! md.read_metadata(&rsp, false)?;
//!
//! // Build chunk offsets: [0, 10000, 20000, ..., row_count]
//! let offsets = build_offsets(md.row_count as u32, 10_000)?;
//! let mut wtr = ReadStatWriter::new();
//! let pairs = offsets.windows(2);
//! let pairs_cnt = pairs.len();
//!
//! for (i, w) in pairs.enumerate() {
//!     let mut d = ReadStatData::new().init(md.clone(), w[0], w[1]);
//!     d.read_data(&rsp)?;
//!     wtr.write(&d, &wc)?;
//!     if i == pairs_cnt - 1 {
//!         wtr.finish(&d, &wc, &rsp.path)?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Read from in-memory bytes
//!
//! Parse a `.sas7bdat` file from a byte slice instead of the filesystem.
//! Useful for cloud storage, HTTP uploads, WASM targets, and testing.
//!
//! ```no_run
//! use readstat::{ReadStatMetadata, ReadStatData};
//!
//! # fn main() -> Result<(), readstat::ReadStatError> {
//! # let sas_bytes: &[u8] = &[];
//! // sas_bytes: &[u8] — obtained from S3, HTTP, etc.
//! let mut md = ReadStatMetadata::new();
//! md.read_metadata_from_bytes(sas_bytes, false)?;
//!
//! let row_count = md.row_count as u32;
//! let mut d = ReadStatData::new().init(md, 0, row_count);
//! d.read_data_from_bytes(sas_bytes)?;
//!
//! if let Some(batch) = &d.batch {
//!     println!("Parsed {} rows from bytes", batch.num_rows());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Filter to specific columns
//!
//! Select only specific columns before reading data. Unselected columns
//! are skipped during parsing, reducing both memory and CPU usage.
//!
//! ```no_run
//! use readstat::{ReadStatPath, ReadStatMetadata, ReadStatData};
//! use std::sync::Arc;
//!
//! # fn main() -> Result<(), readstat::ReadStatError> {
//! let rsp = ReadStatPath::new("data.sas7bdat".into())?;
//!
//! let mut md = ReadStatMetadata::new();
//! md.read_metadata(&rsp, false)?;
//!
//! // Select only these columns
//! let columns = vec!["name".to_string(), "age".to_string()];
//! let filter = md.resolve_selected_columns(Some(columns))?;
//!
//! if let Some(ref mapping) = filter {
//!     // Apply filter to metadata (updates schema and vars)
//!     let original_var_count = md.var_count;
//!     md = md.filter_to_selected_columns(mapping);
//!
//!     let row_count = md.row_count as u32;
//!     let mut d = ReadStatData::new()
//!         .set_column_filter(Some(Arc::new(mapping.clone())), original_var_count)
//!         .init(md, 0, row_count);
//!     d.read_data(&rsp)?;
//!
//!     if let Some(batch) = &d.batch {
//!         // batch only contains "name" and "age" columns
//!         println!(
//!             "Columns: {:?}",
//!             batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>()
//!         );
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Convert RecordBatch to in-memory bytes
//!
//! Serialize a parsed [`RecordBatch`](arrow_array::RecordBatch) directly to
//! in-memory bytes without writing to a file. Useful for HTTP responses,
//! message queues, or piping to other Arrow-aware tools.
//!
//! ```no_run
//! use readstat::{ReadStatPath, ReadStatMetadata, ReadStatData};
//! # #[cfg(feature = "parquet")]
//! use readstat::write_batch_to_parquet_bytes;
//! # #[cfg(feature = "csv")]
//! use readstat::write_batch_to_csv_bytes;
//!
//! # fn main() -> Result<(), readstat::ReadStatError> {
//! let rsp = ReadStatPath::new("data.sas7bdat".into())?;
//!
//! let mut md = ReadStatMetadata::new();
//! md.read_metadata(&rsp, false)?;
//!
//! let row_count = md.row_count as u32;
//! let mut d = ReadStatData::new().init(md, 0, row_count);
//! d.read_data(&rsp)?;
//!
//! if let Some(batch) = &d.batch {
//!     // Get Parquet bytes (e.g. for an HTTP response)
//!     # #[cfg(feature = "parquet")]
//!     let parquet_bytes = write_batch_to_parquet_bytes(batch)?;
//!
//!     // Or CSV bytes
//!     # #[cfg(feature = "csv")]
//!     let csv_bytes = write_batch_to_csv_bytes(batch)?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Key Types
//!
//! - [`ReadStatPath`] — Validated input file path for SAS files
//! - [`WriteConfig`] — Output configuration (path, format, compression)
//! - [`ReadStatMetadata`] — File-level metadata (row/var counts, encoding, Arrow schema)
//! - [`ReadStatData`] — Parsed row data, convertible to Arrow [`RecordBatch`](arrow_array::RecordBatch)
//! - [`ReadStatVarFormatClass`] — SAS format classification (Date, DateTime, Time variants)
//! - [`ReadStatWriter`] — Writes Arrow batches to the configured output format
//!
//! # Features
//!
//! Output format writers are feature-gated (all enabled by default):
//!
//! | Feature | Format | Notes |
//! |---------|--------|-------|
//! | `csv` | CSV | Comma-separated values via `arrow-csv` |
//! | `parquet` | Parquet | Columnar format via `parquet` crate, 5 compression codecs |
//! | `feather` | Feather | Arrow IPC format via `arrow-ipc` |
//! | `ndjson` | NDJSON | Newline-delimited JSON via `arrow-json` |
//! | `sql` | SQL | Query data with SQL via DataFusion (not enabled by default) |

#![warn(missing_docs)]
#![allow(non_camel_case_types)]

pub use common::build_offsets;
pub use err::{ReadStatCError, ReadStatError};
pub use rs_data::ReadStatData;
pub use rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata};
pub use rs_path::ReadStatPath;
pub use rs_var::{ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass};
pub use rs_write::ReadStatWriter;
#[cfg(feature = "csv")]
pub use rs_write::write_batch_to_csv_bytes;
#[cfg(feature = "ndjson")]
pub use rs_write::write_batch_to_ndjson_bytes;
#[cfg(feature = "parquet")]
pub use rs_write::write_batch_to_parquet_bytes;
#[cfg(feature = "feather")]
pub use rs_write::write_batch_to_feather_bytes;
pub use rs_write_config::{OutFormat, ParquetCompression, WriteConfig};
#[cfg(feature = "sql")]
pub use rs_query::{execute_sql, execute_sql_stream, execute_sql_and_write_stream, read_sql_file, write_sql_results};

mod cb;
mod common;
mod err;
mod formats;
mod rs_buffer_io;
mod rs_data;
mod rs_metadata;
mod rs_parser;
mod rs_path;
#[cfg(feature = "sql")]
mod rs_query;
mod rs_var;
mod rs_write;
mod rs_write_config;
