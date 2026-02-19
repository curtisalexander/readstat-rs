//! Read SAS binary files (`.sas7bdat`) and convert them to modern columnar formats.
//!
//! This crate provides both a CLI tool and a library for parsing SAS binary data files
//! using FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library,
//! then converting the parsed data into Apache Arrow [`RecordBatch`](arrow_array::RecordBatch)
//! format for output as CSV, Feather (Arrow IPC), NDJSON, or Parquet.
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
//! use readstat::{ReadStatPath, ReadStatMetadata, ReadStatData, ReadStatWriter, OutFormat};
//! use readstat::build_offsets;
//!
//! // Configure input/output paths
//! # fn main() -> Result<(), readstat::ReadStatError> {
//! let rsp = ReadStatPath::new(
//!     "data.sas7bdat".into(),
//!     Some("output.parquet".into()),
//!     Some(OutFormat::parquet),
//!     false,
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
//!     wtr.write(&d, &rsp)?;
//!     if i == pairs_cnt - 1 {
//!         wtr.finish(&d, &rsp)?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Key Types
//!
//! - [`ReadStatPath`] — Validated file path with I/O configuration (format, compression)
//! - [`ReadStatMetadata`] — File-level metadata (row/var counts, encoding, Arrow schema)
//! - [`ReadStatData`] — Parsed row data, convertible to Arrow [`RecordBatch`](arrow_array::RecordBatch)
//! - [`ReadStatVar`] — Typed value enum (strings, integers, floats, dates, times)
//! - [`ReadStatWriter`] — Writes Arrow batches to the configured output format
//!
//! # Streaming and Parallel Processing
//!
//! By default, data is read in streaming chunks of 10,000 rows to limit memory usage.
//! The [`Reader::mem`] variant reads all rows at once for smaller files. Parallel
//! reading (via Rayon) and parallel writing (via Crossbeam channels) are supported
//! for the CLI's `data` subcommand.

#![warn(missing_docs)]
#![allow(non_camel_case_types)]
use clap::{Parser, Subcommand, ValueEnum, ValueHint};
use colored::Colorize;
use crossbeam::channel::bounded;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use path_abs::{PathAbs, PathInfo};
use rayon::prelude::*;
use std::{fmt, path::PathBuf, sync::Arc, thread};

pub use common::build_offsets;
pub use err::{ReadStatCError, ReadStatError};
pub use rs_data::{ColumnBuilder, ReadStatData};
pub use rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata};
pub use rs_path::ReadStatPath;
pub use rs_var::{ReadStatVar, ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass};
pub use rs_write::ReadStatWriter;
#[cfg(feature = "sql")]
pub use rs_query::{execute_sql, execute_sql_stream, execute_sql_and_write_stream, read_sql_file};

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

/// Default number of rows to read per streaming chunk.
const STREAM_ROWS: u32 = 10000;

/// Capacity of the bounded channel between reader and writer threads.
/// Also used as the batch size for bounded-batch parallel writes.
const CHANNEL_CAPACITY: usize = 10;

/// Determine stream row count based on reader type.
fn resolve_stream_rows(reader: Option<Reader>, stream_rows: Option<u32>, total_rows: u32) -> u32 {
    match reader {
        Some(Reader::stream) | None => stream_rows.unwrap_or(STREAM_ROWS),
        Some(Reader::mem) => total_rows,
    }
}

/// Create a progress bar if progress is enabled.
fn create_progress_bar(no_progress: bool, total_rows: u32) -> Result<Option<ProgressBar>, ReadStatError> {
    if no_progress {
        return Ok(None);
    }
    let pb = ProgressBar::new(total_rows as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} rows {msg}")?
            .progress_chars("##-")
    );
    Ok(Some(pb))
}

// CLI
#[derive(Parser, Debug)]
#[command(version)]
#[command(propagate_version = true)]
/// 💾 Command-line tool for working with SAS binary files
///
/// 🦀 Rust wrapper of ReadStat C library
pub struct ReadStatCli {
    #[command(subcommand)]
    command: ReadStatCliCommands,
}

/// CLI subcommands for readstat.
#[derive(Debug, Subcommand)]
pub enum ReadStatCliCommands {
    /// Display sas7bdat metadata
    Metadata {
        /// Path to sas7bdat file
        #[arg(value_hint = ValueHint::FilePath, value_parser)]
        input: PathBuf,
        /// Display sas7bdat metadata as json
        #[arg(action, long)]
        as_json: bool,
        /// Do not display progress bar
        #[arg(action, long)]
        no_progress: bool,
        /// Skip calculating row count{n}If only interested in variable metadata speeds up parsing
        #[arg(action, long)]
        skip_row_count: bool,
    },
    /// Preview sas7bdat data
    Preview {
        /// Path to sas7bdat file
        #[arg(value_parser)]
        input: PathBuf,
        /// Number of rows to write
        #[arg(default_value = "10", long, value_parser)]
        rows: u32,
        /// Type of reader{n}    mem = read all data into memory{n}    stream = read at most stream-rows into memory{n}Defaults to stream
        #[arg(value_enum, ignore_case = true, long, value_parser)]
        reader: Option<Reader>,
        /// Number of rows to stream (read into memory) at a time{n}↑ rows = ↑ memory usage{n}Ignored if reader is set to mem{n}Defaults to 10,000 rows
        #[arg(long, value_parser)]
        stream_rows: Option<u32>,
        /// Do not display progress bar
        #[arg(action, long)]
        no_progress: bool,
        /// Comma-separated list of column names to include in output
        #[arg(long, value_delimiter = ',', num_args = 1..)]
        columns: Option<Vec<String>>,
        /// Path to a file containing column names (one per line, # comments)
        #[arg(long, value_hint = ValueHint::FilePath, conflicts_with = "columns")]
        columns_file: Option<PathBuf>,
        /// SQL query to run against the data (requires sql feature){n}The table name is the input file stem (e.g. "cars" for cars.sas7bdat){n}Mutually exclusive with --columns/--columns-file
        #[cfg(feature = "sql")]
        #[arg(long, conflicts_with_all = ["columns", "columns_file"])]
        sql: Option<String>,
        /// Path to a file containing a SQL query (requires sql feature){n}Mutually exclusive with --sql and --columns/--columns-file
        #[cfg(feature = "sql")]
        #[arg(long, value_hint = ValueHint::FilePath, conflicts_with_all = ["sql", "columns", "columns_file"])]
        sql_file: Option<PathBuf>,
    },
    /// Convert sas7bdat data to csv, feather (or the Arrow IPC format), ndjson, or parquet format
    Data {
        /// Path to sas7bdat file
        #[arg(value_hint = ValueHint::FilePath, value_parser)]
        input: PathBuf,
        /// Output file path
        #[arg(long, short = 'o', value_parser)]
        output: Option<PathBuf>,
        /// Output file format{n}Defaults to csv
        #[arg(ignore_case = true, long, short = 'f', value_enum, value_parser)]
        format: Option<OutFormat>,
        /// Overwrite output file if it already exists
        #[arg(action, long)]
        overwrite: bool,
        /// Number of rows to write
        #[arg(long, value_parser)]
        rows: Option<u32>,
        /// Type of reader{n}    mem = read all data into memory{n}    stream = read at most stream-rows into memory{n}Defaults to stream
        #[arg(ignore_case = true, long, value_enum, value_parser)]
        reader: Option<Reader>,
        /// Number of rows to stream (read into memory) at a time{n}↑ rows = ↑ memory usage{n}Ignored if reader is set to mem{n}Defaults to 10,000 rows
        #[arg(long, value_parser)]
        stream_rows: Option<u32>,
        /// Do not display progress bar
        #[arg(action, long)]
        no_progress: bool,
        /// Convert sas7bdat data in parallel
        #[arg(action, long)]
        parallel: bool,
        /// Write output data in parallel{n}Only effective when parallel is enabled{n}May write batches out of order for Parquet/Feather
        #[arg(action, long)]
        parallel_write: bool,
        /// Memory buffer size in MB before spilling to disk during parallel writes{n}Defaults to 100 MB{n}Only effective when parallel-write is enabled
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..=10240), default_value = "100")]
        parallel_write_buffer_mb: u64,
        /// Parquet compression algorithm
        #[arg(long, value_enum, value_parser)]
        compression: Option<ParquetCompression>,
        /// Parquet compression level (if applicable)
        #[arg(long, value_parser = clap::value_parser!(u32).range(0..=22))]
        compression_level: Option<u32>,
        /// Comma-separated list of column names to include in output
        #[arg(long, value_delimiter = ',', num_args = 1..)]
        columns: Option<Vec<String>>,
        /// Path to a file containing column names (one per line, # comments)
        #[arg(long, value_hint = ValueHint::FilePath, conflicts_with = "columns")]
        columns_file: Option<PathBuf>,
        /// SQL query to run against the data (requires sql feature){n}The table name is the input file stem (e.g. "cars" for cars.sas7bdat){n}Mutually exclusive with --columns/--columns-file
        #[cfg(feature = "sql")]
        #[arg(long, conflicts_with_all = ["columns", "columns_file"])]
        sql: Option<String>,
        /// Path to a file containing a SQL query (requires sql feature){n}Mutually exclusive with --sql and --columns/--columns-file
        #[cfg(feature = "sql")]
        #[arg(long, value_hint = ValueHint::FilePath, conflicts_with_all = ["sql", "columns", "columns_file"])]
        sql_file: Option<PathBuf>,
    },
}

/// Output file format for data conversion.
#[derive(Debug, Clone, Copy, ValueEnum)]
#[allow(non_camel_case_types)]
pub enum OutFormat {
    /// Comma-separated values.
    csv,
    /// Feather (Arrow IPC) format.
    feather,
    /// Newline-delimited JSON.
    ndjson,
    /// Apache Parquet columnar format.
    parquet,
}

impl fmt::Display for OutFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

/// Strategy for reading SAS data into memory.
#[derive(Debug, Clone, Copy, ValueEnum)]
#[allow(non_camel_case_types)]
pub enum Reader {
    /// Read all data into memory at once.
    mem,
    /// Stream data in chunks (default, lower memory usage).
    stream,
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

/// Parquet compression algorithm.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ParquetCompression {
    /// No compression.
    Uncompressed,
    /// Snappy compression (fast, moderate ratio).
    Snappy,
    /// Gzip compression (levels 0-9).
    Gzip,
    /// LZ4 raw compression.
    Lz4Raw,
    /// Brotli compression (levels 0-11).
    Brotli,
    /// Zstandard compression (levels 0-22).
    Zstd,
}

impl fmt::Display for ParquetCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

/// Resolve the SQL query from `--sql` or `--sql-file` CLI options.
#[cfg(feature = "sql")]
fn resolve_sql(
    sql: Option<String>,
    sql_file: Option<PathBuf>,
) -> Result<Option<String>, ReadStatError> {
    if let Some(path) = sql_file {
        Ok(Some(rs_query::read_sql_file(&path)?))
    } else {
        Ok(sql)
    }
}

/// Extract a table name from the input file stem (e.g. "cars" from "cars.sas7bdat").
#[cfg(feature = "sql")]
fn table_name_from_path(path: &std::path::Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data")
        .to_string()
}

/// Resolve column names from `--columns` or `--columns-file` CLI options.
fn resolve_columns(
    columns: Option<Vec<String>>,
    columns_file: Option<PathBuf>,
) -> Result<Option<Vec<String>>, ReadStatError> {
    if let Some(path) = columns_file {
        let names = ReadStatMetadata::parse_columns_file(&path)?;
        if names.is_empty() {
            Ok(None)
        } else {
            Ok(Some(names))
        }
    } else {
        Ok(columns)
    }
}

/// Executes the CLI command specified by the parsed [`ReadStatCli`] arguments.
///
/// This is the main entry point for the CLI binary, dispatching to the
/// `metadata`, `preview`, or `data` subcommand.
pub fn run(rs: ReadStatCli) -> Result<(), ReadStatError> {
    env_logger::init();

    match rs.command {
        ReadStatCliCommands::Metadata {
            input: in_path,
            as_json,
            no_progress: _,
            skip_row_count,
        } => {
            // Validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(in_path)?.as_path().to_path_buf();
            debug!(
                "Retrieving metadata from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and format determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, None, false, false, None, None)?;

            // Instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();

            // Read metadata
            md.read_metadata(&rsp, skip_row_count)?;

            // Write metadata
            ReadStatWriter::new().write_metadata(&md, &rsp, as_json)?;

            // Return
            Ok(())
        }
        ReadStatCliCommands::Preview {
            input,
            rows,
            reader,
            stream_rows,
            no_progress,
            columns,
            columns_file,
            #[cfg(feature = "sql")]
            sql,
            #[cfg(feature = "sql")]
            sql_file,
        } => {
            #[cfg(feature = "sql")]
            let sql_query = resolve_sql(sql, sql_file)?;
            // Validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating data preview from the file {}",
                &sas_path.to_string_lossy()
            );

            // output and format determine the type of writing to be performed
            let rsp = ReadStatPath::new(
                sas_path,
                None,
                Some(OutFormat::csv),
                false,
                false,
                None,
                None,
            )?;

            // instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();

            // Read metadata
            md.read_metadata(&rsp, false)?;

            // Resolve column selection
            let col_names = resolve_columns(columns, columns_file)?;
            let column_filter = md.resolve_selected_columns(col_names)?;
            let original_var_count = md.var_count;
            if let Some(ref mapping) = column_filter {
                md = md.filter_to_selected_columns(mapping);
            }

            // Determine row count
            let total_rows_to_process = std::cmp::min(rows, md.row_count as u32);

            let total_rows_to_stream = resolve_stream_rows(reader, stream_rows, total_rows_to_process);
            let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let pb = create_progress_bar(no_progress, total_rows_to_process)?;

            // Build up offsets
            let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;
            let offsets_pairs = offsets.windows(2);

            // Read all chunks into batches
            let mut all_batches: Vec<arrow_array::RecordBatch> = Vec::new();
            for w in offsets_pairs {
                let row_start = w[0];
                let row_end = w[1];

                let mut d = ReadStatData::new()
                    .set_column_filter(column_filter.clone(), original_var_count)
                    .set_no_progress(no_progress)
                    .set_total_rows_to_process(total_rows_to_process as usize)
                    .set_total_rows_processed(total_rows_processed.clone())
                    .init(md.clone(), row_start, row_end);

                if let Some(ref pb) = pb {
                    d = d.set_progress_bar(pb.clone());
                }

                d.read_data(&rsp)?;

                if let Some(batch) = d.batch {
                    all_batches.push(batch);
                }
            }

            // Finish progress bar
            if let Some(pb) = pb {
                pb.finish_with_message("Done");
            }

            // Apply SQL query if provided, otherwise write directly
            #[cfg(feature = "sql")]
            let all_batches = if let Some(ref query) = sql_query {
                let schema = Arc::new(md.schema.clone());
                let table_name = table_name_from_path(&rsp.path);
                rs_query::execute_sql(all_batches, schema, &table_name, query)?
            } else {
                all_batches
            };

            // Write all batches to stdout as CSV
            let stdout = std::io::stdout();
            let mut csv_writer = arrow_csv::WriterBuilder::new()
                .with_header(true)
                .build(stdout);
            for batch in &all_batches {
                csv_writer.write(batch)?;
            }

            Ok(())
        }
        ReadStatCliCommands::Data {
            input,
            output,
            format,
            rows,
            reader,
            stream_rows,
            no_progress,
            overwrite,
            parallel,
            parallel_write,
            parallel_write_buffer_mb,
            compression,
            compression_level,
            columns,
            columns_file,
            #[cfg(feature = "sql")]
            sql,
            #[cfg(feature = "sql")]
            sql_file,
        } => {
            #[cfg(feature = "sql")]
            let sql_query = resolve_sql(sql, sql_file)?;

            // Validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating data from the file {}",
                &sas_path.to_string_lossy()
            );

            // output and format determine the type of writing to be performed
            let rsp = ReadStatPath::new(
                sas_path,
                output,
                format,
                overwrite,
                false,
                compression,
                compression_level,
            )?;

            // Instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();
            md.read_metadata(&rsp, false)?;

            // Resolve column selection
            let col_names = resolve_columns(columns, columns_file)?;
            let column_filter = md.resolve_selected_columns(col_names)?;
            let original_var_count = md.var_count;
            if let Some(ref mapping) = column_filter {
                md = md.filter_to_selected_columns(mapping);
            }

            // If no output path then only read metadata; otherwise read data
            match &rsp.out_path {
                None => {
                    println!("{}: a value was not provided for the parameter {}, thus displaying metadata only\n", "Warning".bright_yellow(), "--output".bright_cyan());

                    // Instantiate ReadStatMetadata
                    let mut md = ReadStatMetadata::new();
                    md.read_metadata(&rsp, false)?;

                    // Write metadata
                    ReadStatWriter::new().write_metadata(&md, &rsp, false)?;

                    // Return
                    Ok(())
                }
                Some(p) => {
                    println!(
                        "Writing parsed data to file {}",
                        p.to_string_lossy().bright_yellow()
                    );

                    // Determine row count
                    let total_rows_to_process = if let Some(r) = rows {
                        std::cmp::min(r, md.row_count as u32)
                    } else {
                        md.row_count as u32
                    };

                    let total_rows_to_stream = resolve_stream_rows(reader, stream_rows, total_rows_to_process);
                    let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let pb = create_progress_bar(no_progress, total_rows_to_process)?;

                    // Build up offsets
                    let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;

                    // Determine if we should use parallel writes (check before spawning reader thread)
                    let use_parallel_writes = parallel && parallel_write &&
                        matches!(rsp.format, OutFormat::parquet);

                    // Clone rsp parameters for use in parallel write mode if needed
                    let out_path_clone = rsp.out_path.clone();
                    let compression_clone = rsp.compression;
                    let compression_level_clone = rsp.compression_level;
                    let buffer_size_bytes = parallel_write_buffer_mb * 1024 * 1024; // Convert MB to bytes

                    // Save values needed for SQL query execution before thread spawn
                    #[cfg(feature = "sql")]
                    let sql_schema = Arc::new(md.schema.clone());
                    #[cfg(feature = "sql")]
                    let sql_table_name = table_name_from_path(&rsp.path);
                    #[cfg(feature = "sql")]
                    let sql_format = rsp.format;

                    // Create channels with a bounded capacity
                    // Unbounded channels can result in extreme memory usage if files are large and
                    //   the reader significantly outpaces the writer
                    let (s, r) = bounded(CHANNEL_CAPACITY);

                    // Clone progress bar for the spawned thread
                    let pb_thread = pb.clone();

                    // Process data in batches (i.e. stream chunks of rows)
                    let reader_handle = thread::spawn(move || -> Result<(), ReadStatError> {
                        // Create windows
                        let offsets_pairs: Vec<_> = offsets.windows(2).collect();
                        let pairs_cnt = offsets_pairs.len();

                        // Build a local thread pool instead of mutating the global one
                        // (build_global() can only succeed once per process and is fragile)
                        let num_threads = if parallel { 0 } else { 1 }; // 0 = rayon default (num CPUs)
                        let pool = rayon::ThreadPoolBuilder::new()
                            .num_threads(num_threads)
                            .build()
                            .map_err(|e| ReadStatError::Other(format!("Failed to build thread pool: {e}")))?;

                        // Read all chunks (potentially in parallel), collecting results in order
                        // Collecting into a Vec preserves chunk ordering even with parallel execution
                        let results: Vec<Result<(ReadStatData, ReadStatPath, usize), ReadStatError>> = pool.install(|| {
                            offsets_pairs
                                .par_iter()
                                .map(|w| -> Result<(ReadStatData, ReadStatPath, usize), ReadStatError> {
                                    let row_start = w[0];
                                    let row_end = w[1];

                                    // Initialize ReadStatData struct
                                    let mut d = ReadStatData::new()
                                        .set_column_filter(column_filter.clone(), original_var_count)
                                        .set_no_progress(no_progress)
                                        .set_total_rows_to_process(total_rows_to_process as usize)
                                        .set_total_rows_processed(total_rows_processed.clone())
                                        .init(md.clone(), row_start, row_end);

                                    // Set progress bar if available
                                    if let Some(ref pb) = pb_thread {
                                        d = d.set_progress_bar(pb.clone());
                                    }

                                    // Read
                                    d.read_data(&rsp)?;

                                    Ok((d, rsp.clone(), pairs_cnt))
                                })
                                .collect()
                        });

                        // Send results over the channel in order
                        let mut errors = Vec::new();
                        for result in results {
                            match result {
                                Ok(data) => {
                                    if s.send(data).is_err() {
                                        errors.push(ReadStatError::Other(
                                            "Error when attempting to send read data for writing".to_string(),
                                        ));
                                    }
                                }
                                Err(e) => errors.push(e),
                            }
                        }

                        // Drop sender so that receive iterator will eventually exit
                        drop(s);

                        if !errors.is_empty() {
                            println!("The following errors occured when processing data:");
                            for e in &errors {
                                println!("    Error: {:#?}", e);
                            }
                        }

                        // Return
                        Ok(())
                    });

                    // Write

                    // Determine whether the SQL path will handle the receiver
                    #[cfg(feature = "sql")]
                    let has_sql = sql_query.is_some();
                    #[cfg(not(feature = "sql"))]
                    let has_sql = false;

                    if has_sql {
                        // SQL query mode: stream data through DataFusion and write results
                        #[cfg(feature = "sql")]
                        {
                            let query = sql_query.as_ref().unwrap();
                            if let Some(out_path) = &out_path_clone {
                                rs_query::execute_sql_and_write_stream(
                                    r,
                                    sql_schema,
                                    &sql_table_name,
                                    query,
                                    out_path,
                                    sql_format,
                                    compression_clone,
                                    compression_level_clone,
                                )?;
                            } else {
                                // No output path — just consume the stream
                                let _results = rs_query::execute_sql_stream(
                                    r,
                                    sql_schema,
                                    &sql_table_name,
                                    query,
                                )?;
                            }
                        }
                    } else if use_parallel_writes {
                        // Parallel write mode for Parquet using bounded-batch processing:
                        // Pull up to CHANNEL_CAPACITY batches at a time from the channel,
                        // write those in parallel to temp files, then repeat.
                        // This preserves backpressure — at most CHANNEL_CAPACITY batches
                        // are held in memory beyond what's in the channel.
                        let temp_dir = if let Some(out_path) = &out_path_clone {
                            match out_path.parent() {
                                Ok(parent) => parent.to_path_buf(),
                                Err(_) => std::env::current_dir()?,
                            }
                        } else {
                            return Err(ReadStatError::Other("No output path specified for parallel write".to_string()));
                        };

                        let mut all_temp_files: Vec<PathBuf> = Vec::new();
                        let mut schema: Option<arrow_schema::Schema> = None;
                        let mut batch_idx: usize = 0;

                        loop {
                            // Collect up to CHANNEL_CAPACITY batches from the channel
                            let mut batch_group: Vec<(ReadStatData, ReadStatPath, usize)> = Vec::with_capacity(CHANNEL_CAPACITY);
                            for item in r.iter() {
                                batch_group.push(item);
                                if batch_group.len() >= CHANNEL_CAPACITY {
                                    break;
                                }
                            }

                            if batch_group.is_empty() {
                                break;
                            }

                            // Capture schema from the first batch we see
                            if schema.is_none() {
                                schema = Some(batch_group[0].0.schema.clone());
                            }
                            let schema_ref = schema.as_ref().unwrap();

                            // Write this group of batches in parallel to temp files
                            let temp_files: Vec<PathBuf> = batch_group.par_iter().enumerate()
                                .map(|(i, (d, _rsp, _))| -> Result<PathBuf, ReadStatError> {
                                    let temp_file = temp_dir.join(format!(".readstat_temp_{}.parquet", batch_idx + i));

                                    if let Some(batch) = &d.batch {
                                        ReadStatWriter::write_batch_to_parquet(
                                            batch,
                                            schema_ref,
                                            &temp_file,
                                            compression_clone,
                                            compression_level_clone,
                                            buffer_size_bytes as usize,
                                        )?;
                                    }

                                    Ok(temp_file)
                                })
                                .collect::<Result<Vec<_>, _>>()?;

                            batch_idx += batch_group.len();
                            // Explicitly drop the batch group to free memory before next iteration
                            drop(batch_group);
                            all_temp_files.extend(temp_files);
                        }

                        // Merge all temp files into final output
                        if !all_temp_files.is_empty() {
                            if let Some(out_path) = &out_path_clone {
                                ReadStatWriter::merge_parquet_files(
                                    &all_temp_files,
                                    out_path,
                                    schema.as_ref().unwrap(),
                                    compression_clone,
                                    compression_level_clone,
                                )?;
                            }
                        }
                    } else {
                        // Sequential write mode (default) with BufWriter optimizations
                        let mut wtr = ReadStatWriter::new();

                        for (i, (d, rsp, pairs_cnt)) in r.iter().enumerate() {
                            wtr.write(&d, &rsp)?;

                            if i == (pairs_cnt - 1) {
                                wtr.finish(&d, &rsp)?;
                            }

                            // Explicitly drop to save on memory
                            drop(d);
                        }
                    }

                    // Finish progress bar
                    if let Some(pb) = pb {
                        pb.finish_with_message("Done");
                    }

                    // Join the reader thread to surface any panics or errors
                    match reader_handle.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => return Err(e),
                        Err(_) => return Err(ReadStatError::Other(
                            "Reader thread panicked".to_string(),
                        )),
                    }

                    // Return
                    Ok(())
                }
            }
        }
    }
}
