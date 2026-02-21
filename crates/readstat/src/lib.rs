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
//! # Quick Start
//!
//! ```no_run
//! use readstat::{ReadStatPath, ReadStatMetadata, ReadStatData, ReadStatWriter, WriteConfig, OutFormat};
//! use readstat::build_offsets;
//!
//! // Configure input/output paths
//! # fn main() -> Result<(), readstat::ReadStatError> {
//! let rsp = ReadStatPath::new("data.sas7bdat".into())?;
//! let wc = WriteConfig::new(
//!     Some("output.parquet".into()),
//!     Some(OutFormat::parquet),
//!     false,
//!     None,
//!     None,
//! )?;
//!
//! // Read metadata
//! let mut md = ReadStatMetadata::new();
//! md.read_metadata(&rsp, false)?;
//!
//! // Read and write data in streaming chunks
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
//! # Key Types
//!
//! - [`ReadStatPath`] — Validated input file path for SAS files
//! - [`WriteConfig`] — Output configuration (path, format, compression)
//! - [`ReadStatMetadata`] — File-level metadata (row/var counts, encoding, Arrow schema)
//! - [`ReadStatData`] — Parsed row data, convertible to Arrow [`RecordBatch`](arrow_array::RecordBatch)
//! - [`ReadStatVarFormatClass`] — SAS format classification (Date, DateTime, Time variants)
//! - [`ReadStatWriter`] — Writes Arrow batches to the configured output format

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
