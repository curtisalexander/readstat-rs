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
//!         → Vec<Vec<ReadStatVar>> (column-major typed values)
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
pub use rs_data::ReadStatData;
pub use rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata};
pub use rs_path::ReadStatPath;
pub use rs_var::{ReadStatVar, ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass};
pub use rs_write::ReadStatWriter;

mod cb;
mod common;
mod err;
mod formats;
mod rs_data;
mod rs_metadata;
mod rs_parser;
mod rs_path;
mod rs_var;
mod rs_write;

/// Default number of rows to read per streaming chunk.
const STREAM_ROWS: u32 = 10000;

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
        } => {
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

            // Determine stream row count
            // 📝 Default stream rows set to 10,000
            let total_rows_to_stream = match reader {
                Some(Reader::stream) | None => match stream_rows {
                    Some(s) => s,
                    None => STREAM_ROWS,
                },
                Some(Reader::mem) => total_rows_to_process,
            };

            // Initialize AtomicUsize to contain total rows processed
            let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

            // Create progress bar if not disabled
            let pb = if !no_progress {
                let pb = ProgressBar::new(total_rows_to_process as u64);
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} rows {msg}")?
                        .progress_chars("##-")
                );
                Some(pb)
            } else {
                None
            };

            // Build up offsets
            let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;
            let offsets_pairs = offsets.windows(2);
            let pairs_cnt = offsets_pairs.len();

            // Initialize writing
            let mut wtr = ReadStatWriter::new();

            // Process data in batches (i.e. stream chunks of rows)
            // Read data - for each iteration create a new instance of ReadStatData
            for (i, w) in offsets_pairs.enumerate() {
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
                if let Some(ref pb) = pb {
                    d = d.set_progress_bar(pb.clone());
                }

                // Read
                d.read_data(&rsp)?;

                // Write
                wtr.write(&d, &rsp)?;

                // Finish
                if i == pairs_cnt {
                    wtr.finish(&d, &rsp)?;
                }
            }

            // Finish writer
            //wtr.finish(&d, &rsp)?;

            // Finish progress bar
            if let Some(pb) = pb {
                pb.finish_with_message("Done");
            }

            // Return
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
        } => {
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

                    // Determine stream row count
                    // 📝 Default stream rows set to 10,000
                    let total_rows_to_stream = match reader {
                        Some(Reader::stream) | None => match stream_rows {
                            Some(s) => s,
                            None => STREAM_ROWS,
                        },
                        Some(Reader::mem) => total_rows_to_process,
                    };

                    // Initialize AtomicUsize to contain total rows processed
                    let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

                    // Create progress bar if not disabled
                    let pb = if !no_progress {
                        let pb = ProgressBar::new(total_rows_to_process as u64);
                        pb.set_style(
                            ProgressStyle::default_bar()
                                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} rows {msg}")?
                                .progress_chars("##-")
                        );
                        Some(pb)
                    } else {
                        None
                    };

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

                    // Create channels with a capacity of 10
                    // Unbounded channels can result in extreme memory usage if files are large and
                    //   the reader significantly outpaces the writer
                    let (s, r) = bounded(10);

                    // Clone progress bar for the spawned thread
                    let pb_thread = pb.clone();

                    // Process data in batches (i.e. stream chunks of rows)
                    thread::spawn(move || -> Result<(), ReadStatError> {
                        // Create windows
                        let offsets_pairs = offsets.par_windows(2);
                        let pairs_cnt = offsets_pairs.len();

                        // Run in parallel or not?
                        // Controlled via number of threads in the rayon threadpool
                        if !parallel {
                            rayon::ThreadPoolBuilder::new()
                                .num_threads(1)
                                .build_global()?;
                        };

                        // Iterate over offset pairs, reading data for each iteration and then
                        //   sending the results over a channel to the writer
                        // 📝 For each iteration a new instance of ReadStatData is created
                        // for w in offsets_pairs {
                        let errors: Vec<_> = offsets_pairs
                            .map(|w| -> Result<(), ReadStatError> {
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

                                // Send
                                let sent = s.send((d, rsp.clone(), pairs_cnt));

                                // Early return if an error
                                if sent.is_err() {
                                    Err(ReadStatError::Other(
                                        "Error when attempting to send read data for writing".to_string(),
                                    ))
                                } else {
                                    Ok(())
                                }
                            })
                            .filter_map(|r| r.err())
                            .collect();

                        // Drop sender so that receive iterator will eventually exit
                        drop(s);

                        if !errors.is_empty() {
                            println!("The following errors occured when processing data:");
                            for e in errors {
                                println!("    Error: {:#?}", e);
                            }
                        }

                        // Return
                        Ok(())
                    });

                    // Write

                    if use_parallel_writes {
                        // Parallel write mode for Parquet: write batches to temp files in parallel, then merge
                        let batches: Vec<_> = r.iter().collect();

                        if !batches.is_empty() {
                            let schema = batches[0].0.schema.clone();
                            let temp_dir = if let Some(out_path) = &out_path_clone {
                                match out_path.parent() {
                                    Ok(parent) => parent.to_path_buf(),
                                    Err(_) => std::env::current_dir()?,
                                }
                            } else {
                                return Err(ReadStatError::Other("No output path specified for parallel write".to_string()));
                            };

                            // Write batches in parallel to temporary files using SpooledTempFile
                            let temp_files: Vec<PathBuf> = batches.par_iter().enumerate()
                                .map(|(i, (d, _rsp, _))| -> Result<PathBuf, ReadStatError> {
                                    let temp_file = temp_dir.join(format!(".readstat_temp_{}.parquet", i));

                                    if let Some(batch) = &d.batch {
                                        ReadStatWriter::write_batch_to_parquet(
                                            batch,
                                            &schema,
                                            &temp_file,
                                            compression_clone,
                                            compression_level_clone,
                                            buffer_size_bytes as usize,
                                        )?;
                                    }

                                    Ok(temp_file)
                                })
                                .collect::<Result<Vec<_>, _>>()?;

                            // Merge temp files into final output
                            if let Some(out_path) = &out_path_clone {
                                ReadStatWriter::merge_parquet_files(
                                    &temp_files,
                                    out_path,
                                    &schema,
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

                    // Return
                    Ok(())

                    // get and optionally write data - single or multi-threaded
                    /*
                    if let Some(_p) = parallel {
                        let cpu_logical_count: usize = num_cpus::get();
                        let cpu_physical_count: usize = num_cpus::get_physical();
                        println!("Logical count {:?}", cpu_logical_count);
                        println!("Physical count {:?}", cpu_physical_count);

                        d.get_row_count()?;
                        println!("Row count {:?}", d.metadata.row_count);
                        Ok(())
                    */
                }
            }
        }
    }
}
