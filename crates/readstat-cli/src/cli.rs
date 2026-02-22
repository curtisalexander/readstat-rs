//! CLI argument types for the readstat binary.

use clap::{Parser, Subcommand, ValueEnum, ValueHint};
use readstat::{OutFormat, ParquetCompression};
use std::fmt;
use std::path::PathBuf;

/// 💾 Command-line tool for working with SAS binary files
///
/// 🦀 Rust wrapper of `ReadStat` C library
#[derive(Parser, Debug)]
#[command(version)]
#[command(propagate_version = true)]
pub struct ReadStatCli {
    #[command(subcommand)]
    pub command: ReadStatCliCommands,
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
        #[arg(value_hint = ValueHint::FilePath, value_parser)]
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
        #[arg(long, short = 'o', value_hint = ValueHint::FilePath, value_parser)]
        output: Option<PathBuf>,
        /// Output file format{n}Defaults to csv
        #[arg(ignore_case = true, long, short = 'f', value_enum, value_parser)]
        format: Option<CliOutFormat>,
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
        compression: Option<CliParquetCompression>,
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

/// CLI output file format (with clap `ValueEnum` derive).
///
/// Clap's `ValueEnum` derive converts `PascalCase` variants to lowercase
/// for CLI input (e.g., `Csv` → `csv`).
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliOutFormat {
    /// Comma-separated values.
    Csv,
    /// Feather (Arrow IPC) format.
    Feather,
    /// Newline-delimited JSON.
    Ndjson,
    /// Apache Parquet columnar format.
    Parquet,
}

impl From<CliOutFormat> for OutFormat {
    fn from(f: CliOutFormat) -> Self {
        match f {
            CliOutFormat::Csv => OutFormat::Csv,
            CliOutFormat::Feather => OutFormat::Feather,
            CliOutFormat::Ndjson => OutFormat::Ndjson,
            CliOutFormat::Parquet => OutFormat::Parquet,
        }
    }
}

impl fmt::Display for CliOutFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Csv => f.write_str("csv"),
            Self::Feather => f.write_str("feather"),
            Self::Ndjson => f.write_str("ndjson"),
            Self::Parquet => f.write_str("parquet"),
        }
    }
}

/// Strategy for reading SAS data into memory.
///
/// Clap's `ValueEnum` derive converts `PascalCase` variants to lowercase
/// for CLI input (e.g., `Mem` → `mem`).
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Reader {
    /// Read all data into memory at once.
    Mem,
    /// Stream data in chunks (default, lower memory usage).
    Stream,
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mem => f.write_str("mem"),
            Self::Stream => f.write_str("stream"),
        }
    }
}

/// CLI Parquet compression algorithm (with clap `ValueEnum` derive).
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliParquetCompression {
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

impl From<CliParquetCompression> for ParquetCompression {
    fn from(c: CliParquetCompression) -> Self {
        match c {
            CliParquetCompression::Uncompressed => ParquetCompression::Uncompressed,
            CliParquetCompression::Snappy => ParquetCompression::Snappy,
            CliParquetCompression::Gzip => ParquetCompression::Gzip,
            CliParquetCompression::Lz4Raw => ParquetCompression::Lz4Raw,
            CliParquetCompression::Brotli => ParquetCompression::Brotli,
            CliParquetCompression::Zstd => ParquetCompression::Zstd,
        }
    }
}

impl fmt::Display for CliParquetCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uncompressed => f.write_str("uncompressed"),
            Self::Snappy => f.write_str("snappy"),
            Self::Gzip => f.write_str("gzip"),
            Self::Lz4Raw => f.write_str("lz4-raw"),
            Self::Brotli => f.write_str("brotli"),
            Self::Zstd => f.write_str("zstd"),
        }
    }
}
