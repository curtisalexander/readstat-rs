//! Output writers for converting Arrow [`RecordBatch`](arrow_array::RecordBatch) data
//! to CSV, Feather (Arrow IPC), NDJSON, or Parquet format.
//!
//! [`ReadStatWriter`] manages the lifecycle of format-specific writers, handling
//! streaming writes across multiple batches. It also supports metadata output
//! (pretty-printed or JSON) and parallel Parquet writes via temporary files.

#[cfg(feature = "parquet")]
use arrow_array::RecordBatch;
#[cfg(feature = "csv")]
use arrow_csv::WriterBuilder as CsvWriterBuilder;
#[cfg(feature = "feather")]
use arrow_ipc::writer::FileWriter as IpcFileWriter;
#[cfg(feature = "ndjson")]
use arrow_json::LineDelimitedWriter as JsonLineDelimitedWriter;
#[cfg(feature = "parquet")]
use arrow_schema::Schema;
#[cfg(feature = "parquet")]
use parquet::{
    arrow::ArrowWriter as ParquetArrowWriter, basic::Compression as ParquetCompressionCodec,
    file::properties::WriterProperties,
};
#[cfg(feature = "parquet")]
use std::fs;
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
use std::fs::{File, OpenOptions};
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
use std::io::BufWriter;
#[cfg(feature = "csv")]
use std::io::stdout;
#[cfg(feature = "parquet")]
use std::io::{Seek, SeekFrom};
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
use std::path::{Path, PathBuf};
#[cfg(feature = "parquet")]
use std::sync::Arc;
#[cfg(feature = "parquet")]
use tempfile::SpooledTempFile;

use crate::err::ReadStatError;
use crate::rs_data::ReadStatData;
use crate::rs_metadata::ReadStatMetadata;
use crate::rs_path::ReadStatPath;
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
use crate::rs_write_config::OutFormat;
#[cfg(feature = "parquet")]
use crate::rs_write_config::ParquetCompression;
use crate::rs_write_config::WriteConfig;

/// Internal wrapper around the Parquet Arrow writer, allowing ownership transfer on close.
#[cfg(feature = "parquet")]
pub(crate) struct ReadStatParquetWriter {
    wtr: Option<ParquetArrowWriter<BufWriter<std::fs::File>>>,
}

#[cfg(feature = "parquet")]
impl ReadStatParquetWriter {
    fn new(wtr: ParquetArrowWriter<BufWriter<std::fs::File>>) -> Self {
        Self { wtr: Some(wtr) }
    }
}

/// Format-specific writer variant, created lazily on first write.
pub(crate) enum ReadStatWriterFormat {
    /// CSV writer to a file.
    #[cfg(feature = "csv")]
    Csv(BufWriter<std::fs::File>),
    /// CSV writer to stdout (used for preview mode without an output file).
    #[cfg(feature = "csv")]
    CsvStdout(std::io::Stdout),
    /// Feather (Arrow IPC) writer.
    #[cfg(feature = "feather")]
    Feather(IpcFileWriter<BufWriter<std::fs::File>>),
    /// Newline-delimited JSON writer.
    #[cfg(feature = "ndjson")]
    Ndjson(BufWriter<std::fs::File>),
    /// Parquet writer.
    #[cfg(feature = "parquet")]
    Parquet(ReadStatParquetWriter),
}

/// Manages writing Arrow [`RecordBatch`] data to the configured output format.
///
/// Supports streaming writes across multiple batches. The writer is created lazily
/// on the first call to [`write`](ReadStatWriter::write) and finalized via
/// [`finish`](ReadStatWriter::finish).
#[derive(Default)]
// With no format features enabled the fields are written but never read.
#[cfg_attr(
    not(any(
        feature = "csv",
        feature = "parquet",
        feature = "feather",
        feature = "ndjson"
    )),
    allow(dead_code)
)]
pub struct ReadStatWriter {
    /// The format-specific writer, created on first write.
    pub(crate) wtr: Option<ReadStatWriterFormat>,
    /// Whether the CSV header row has been written.
    pub(crate) wrote_header: bool,
    /// Whether any data has been written (controls file creation vs. append).
    pub(crate) wrote_start: bool,
}

impl ReadStatWriter {
    /// Creates a new `ReadStatWriter` with no active writer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            wtr: None,
            wrote_header: false,
            wrote_start: false,
        }
    }

    /// Opens an output file: creates or truncates on first write, appends on subsequent writes.
    #[cfg(any(
        feature = "csv",
        feature = "feather",
        feature = "ndjson",
        feature = "parquet"
    ))]
    fn open_output(&self, path: &Path) -> Result<File, ReadStatError> {
        let f = if self.wrote_start {
            OpenOptions::new().create(true).append(true).open(path)?
        } else {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?
        };
        Ok(f)
    }

    /// Write a single batch to a Parquet file (for parallel writes).
    /// Uses `SpooledTempFile` to keep data in memory until `buffer_size_bytes` threshold.
    ///
    /// # Errors
    ///
    /// Returns an error if compression configuration is invalid, writing fails,
    /// or the output file cannot be created.
    #[doc(hidden)] // CLI parallel-write orchestration internal; not a stable API.
    #[cfg(feature = "parquet")]
    pub fn write_batch_to_parquet(
        batch: &RecordBatch,
        schema: &Schema,
        output_path: &Path,
        compression: Option<ParquetCompression>,
        compression_level: Option<u32>,
        buffer_size_bytes: usize,
    ) -> Result<(), ReadStatError> {
        // Create a SpooledTempFile that keeps data in memory until buffer_size_bytes
        let mut spooled_file = SpooledTempFile::new(buffer_size_bytes);

        let compression_codec = Self::resolve_compression(compression, compression_level)?;

        let props = WriterProperties::builder()
            .set_compression(compression_codec)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();

        // Write to SpooledTempFile (in memory until threshold, then spills to temp disk file)
        let mut wtr =
            ParquetArrowWriter::try_new(&mut spooled_file, Arc::new(schema.clone()), Some(props))?;

        wtr.write(batch)?;
        wtr.close()?;

        // Now copy from SpooledTempFile to the actual output file
        spooled_file.seek(SeekFrom::Start(0))?;
        let mut output_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_path)?;
        std::io::copy(&mut spooled_file, &mut output_file)?;

        Ok(())
    }

    /// Merge multiple Parquet files into one by reading and rewriting all batches.
    ///
    /// # Errors
    ///
    /// Returns an error if any temp file cannot be read, the output file cannot
    /// be created, or writing fails.
    #[doc(hidden)] // CLI parallel-write orchestration internal; not a stable API.
    #[cfg(feature = "parquet")]
    pub fn merge_parquet_files(
        temp_files: &[PathBuf],
        output_path: &Path,
        schema: &Schema,
        compression: Option<ParquetCompression>,
        compression_level: Option<u32>,
    ) -> Result<(), ReadStatError> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_path)?;

        let compression_codec = Self::resolve_compression(compression, compression_level)?;

        let props = WriterProperties::builder()
            .set_compression(compression_codec)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();

        let mut writer =
            ParquetArrowWriter::try_new(BufWriter::new(f), Arc::new(schema.clone()), Some(props))?;

        // Read each temp file and write its batches to the final file
        for temp_file in temp_files {
            let file = File::open(temp_file)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;

            for batch in reader {
                writer.write(&batch?)?;
            }

            // Clean up temp file
            fs::remove_file(temp_file)?;
        }

        writer.close()?;
        Ok(())
    }

    #[cfg(feature = "parquet")]
    fn resolve_compression(
        compression: Option<ParquetCompression>,
        compression_level: Option<u32>,
    ) -> Result<ParquetCompressionCodec, ReadStatError> {
        crate::rs_write_config::resolve_parquet_compression(compression, compression_level)
    }

    /// Finalizes the writer, flushing and closing the underlying format writer.
    ///
    /// Returns the total number of rows written, as tracked by the shared
    /// counter on `d`. The library does not print anything — the caller (e.g.
    /// the CLI) is responsible for any user-facing summary output.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails to flush or close,
    /// or if the output format is not enabled.
    #[allow(unused_variables)]
    pub fn finish(&mut self, d: &ReadStatData, wc: &WriteConfig) -> Result<usize, ReadStatError> {
        match wc.format {
            #[cfg(feature = "csv")]
            OutFormat::Csv => {
                // Explicitly flush: relying on BufWriter's Drop would silently
                // discard I/O errors (e.g. disk full), reporting success over
                // a truncated file.
                self.flush_buffered()?;
                Ok(rows_written(d))
            }
            #[cfg(feature = "feather")]
            OutFormat::Feather => {
                self.finish_feather()?;
                Ok(rows_written(d))
            }
            #[cfg(feature = "ndjson")]
            OutFormat::Ndjson => {
                self.flush_buffered()?;
                Ok(rows_written(d))
            }
            #[cfg(feature = "parquet")]
            OutFormat::Parquet => {
                self.finish_parquet()?;
                Ok(rows_written(d))
            }
            #[allow(unreachable_patterns)]
            _ => Err(ReadStatError::Other(format!(
                "Output format {:?} is not enabled. Enable the corresponding feature flag.",
                wc.format
            ))),
        }
    }

    /// Flushes the buffered file writer for formats (CSV, NDJSON) whose
    /// underlying [`BufWriter`] would otherwise flush silently in `Drop`.
    #[cfg(any(feature = "csv", feature = "ndjson"))]
    fn flush_buffered(&mut self) -> Result<(), ReadStatError> {
        use std::io::Write;
        match &mut self.wtr {
            #[cfg(feature = "csv")]
            Some(ReadStatWriterFormat::Csv(f)) => f.flush()?,
            #[cfg(feature = "csv")]
            Some(ReadStatWriterFormat::CsvStdout(f)) => f.flush()?,
            #[cfg(feature = "ndjson")]
            Some(ReadStatWriterFormat::Ndjson(f)) => f.flush()?,
            _ => {}
        }
        Ok(())
    }

    #[cfg(feature = "feather")]
    fn finish_feather(&mut self) -> Result<(), ReadStatError> {
        if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.wtr {
            wtr.finish()?;
            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing feather as associated writer is not for the feather format"
                    .to_string(),
            ))
        }
    }

    #[cfg(feature = "parquet")]
    fn finish_parquet(&mut self) -> Result<(), ReadStatError> {
        if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.wtr {
            if let Some(wtr) = pwtr.wtr.take() {
                wtr.close()?;
            }
            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing parquet as associated writer is not for the parquet format"
                    .to_string(),
            ))
        }
    }

    /// Writes a single batch of data in the format determined by `wc`.
    ///
    /// Handles writer initialization on first call and CSV header writing.
    ///
    /// # Errors
    ///
    /// Returns an error if the output file cannot be opened, writing fails,
    /// or the output format is not enabled.
    #[allow(unused_variables)]
    pub fn write(&mut self, d: &ReadStatData, wc: &WriteConfig) -> Result<(), ReadStatError> {
        match wc.format {
            #[cfg(feature = "csv")]
            OutFormat::Csv => {
                if wc.out_path.is_none() {
                    if self.wrote_header {
                        self.write_data_to_stdout(d)
                    } else {
                        self.write_header_to_stdout(d)?;
                        self.write_data_to_stdout(d)
                    }
                } else {
                    self.write_data_to_csv(d, wc)
                }
            }
            #[cfg(feature = "feather")]
            OutFormat::Feather => self.write_data_to_feather(d, wc),
            #[cfg(feature = "ndjson")]
            OutFormat::Ndjson => self.write_data_to_ndjson(d, wc),
            #[cfg(feature = "parquet")]
            OutFormat::Parquet => self.write_data_to_parquet(d, wc),
            #[allow(unreachable_patterns)]
            _ => Err(ReadStatError::Other(format!(
                "Output format {:?} is not enabled. Enable the corresponding feature flag.",
                wc.format
            ))),
        }
    }

    #[cfg(feature = "csv")]
    fn write_data_to_csv(
        &mut self,
        d: &ReadStatData,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &wc.out_path {
            // Open the file only on the first batch; later batches reuse the
            // open writer. Opening (and immediately dropping) the handle on
            // every batch was wasted syscalls.
            if !self.wrote_start {
                let f = self.open_output(p)?;
                self.wtr = Some(ReadStatWriterFormat::Csv(BufWriter::new(f)));
            }

            // write
            if let Some(ReadStatWriterFormat::Csv(f)) = &mut self.wtr {
                if let Some(batch) = &d.batch {
                    let include_header = !self.wrote_header;
                    let mut writer = CsvWriterBuilder::new().with_header(include_header).build(f);
                    writer.write(batch)?;
                    self.wrote_header = true;
                }

                self.wrote_start = true;
                Ok(())
            } else {
                Err(ReadStatError::Other(
                    "Error writing csv as associated writer is not for the csv format".to_string(),
                ))
            }
        } else {
            Err(ReadStatError::Other(
                "Error writing csv as output path is set to None".to_string(),
            ))
        }
    }

    #[cfg(feature = "feather")]
    fn write_data_to_feather(
        &mut self,
        d: &ReadStatData,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &wc.out_path {
            // Open the file only on the first batch (see write_data_to_csv).
            if !self.wrote_start {
                let f = self.open_output(p)?;
                let wtr = IpcFileWriter::try_new(BufWriter::new(f), &d.schema)?;
                self.wtr = Some(ReadStatWriterFormat::Feather(wtr));
            }

            // write
            if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.wtr {
                if let Some(batch) = &d.batch {
                    wtr.write(batch)?;
                }

                self.wrote_start = true;

                Ok(())
            } else {
                Err(ReadStatError::Other(
                    "Error writing feather as associated writer is not for the feather format"
                        .to_string(),
                ))
            }
        } else {
            Err(ReadStatError::Other(
                "Error writing feather file as output path is set to None".to_string(),
            ))
        }
    }

    #[cfg(feature = "ndjson")]
    fn write_data_to_ndjson(
        &mut self,
        d: &ReadStatData,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &wc.out_path {
            // Open the file only on the first batch (see write_data_to_csv).
            if !self.wrote_start {
                let f = self.open_output(p)?;
                self.wtr = Some(ReadStatWriterFormat::Ndjson(BufWriter::new(f)));
            }

            // write
            if let Some(ReadStatWriterFormat::Ndjson(f)) = &mut self.wtr {
                if let Some(batch) = &d.batch {
                    let mut writer = JsonLineDelimitedWriter::new(f);
                    writer.write(batch)?;
                    writer.finish()?;
                }

                self.wrote_start = true;

                Ok(())
            } else {
                Err(ReadStatError::Other(
                    "Error writing ndjson as associated writer is not for the ndjson format"
                        .to_string(),
                ))
            }
        } else {
            Err(ReadStatError::Other(
                "Error writing ndjson file as output path is set to None".to_string(),
            ))
        }
    }

    #[cfg(feature = "parquet")]
    fn write_data_to_parquet(
        &mut self,
        d: &ReadStatData,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &wc.out_path {
            // setup writer — open the file only on the first batch (see
            // write_data_to_csv).
            if !self.wrote_start {
                let f = self.open_output(p)?;
                let compression_codec =
                    Self::resolve_compression(wc.compression, wc.compression_level)?;

                let props = WriterProperties::builder()
                    .set_compression(compression_codec)
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                    .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                    .build();

                let wtr =
                    ParquetArrowWriter::try_new(BufWriter::new(f), d.schema.clone(), Some(props))?;

                self.wtr = Some(ReadStatWriterFormat::Parquet(ReadStatParquetWriter::new(
                    wtr,
                )));
            }

            // write
            if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.wtr {
                if let Some(batch) = &d.batch
                    && let Some(ref mut wtr) = pwtr.wtr
                {
                    wtr.write(batch)?;
                }

                self.wrote_start = true;

                Ok(())
            } else {
                Err(ReadStatError::Other(
                    "Error writing parquet as associated writer is not for the parquet format"
                        .to_string(),
                ))
            }
        } else {
            Err(ReadStatError::Other(
                "Error writing parquet file as output path is set to None".to_string(),
            ))
        }
    }

    #[cfg(feature = "csv")]
    fn write_data_to_stdout(&mut self, d: &ReadStatData) -> Result<(), ReadStatError> {
        // writer setup
        if !self.wrote_start {
            self.wtr = Some(ReadStatWriterFormat::CsvStdout(stdout()));
        }

        // write
        if let Some(ReadStatWriterFormat::CsvStdout(f)) = &mut self.wtr {
            if let Some(batch) = &d.batch {
                let mut writer = CsvWriterBuilder::new().with_header(false).build(f);
                writer.write(batch)?;
            }

            self.wrote_start = true;

            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing to csv as associated writer is not for the csv format".to_string(),
            ))
        }
    }

    #[cfg(feature = "csv")]
    #[allow(clippy::unnecessary_wraps)]
    fn write_header_to_stdout(&mut self, d: &ReadStatData) -> Result<(), ReadStatError> {
        // CSV-escape each name so the header stays well-formed and column-aligned
        // with the (already-escaped) data rows. Variable names may legally contain
        // commas or quotes under SAS `VALIDVARNAME=ANY`.
        let header = d
            .vars
            .values()
            .map(|m| csv_escape_field(&m.var_name))
            .collect::<Vec<_>>()
            .join(",");

        println!("{header}");

        self.wrote_header = true;

        Ok(())
    }

    /// Formats file and variable metadata for display, as either pretty text
    /// (when `as_json` is false) or pretty-printed JSON.
    ///
    /// The library does not print — the caller is responsible for emitting the
    /// returned string.
    ///
    /// # Errors
    ///
    /// Returns an error if JSON serialization fails.
    pub fn metadata_to_string(
        md: &ReadStatMetadata,
        rsp: &ReadStatPath,
        as_json: bool,
    ) -> Result<String, ReadStatError> {
        if as_json {
            Self::metadata_to_json(md)
        } else {
            Ok(Self::format_metadata(md, rsp))
        }
    }

    /// Serializes metadata as pretty-printed JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if JSON serialization fails.
    pub fn metadata_to_json(md: &ReadStatMetadata) -> Result<String, ReadStatError> {
        Ok(serde_json::to_string_pretty(md)?)
    }

    /// Formats metadata as a human-readable, multi-line string.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn format_metadata(md: &ReadStatMetadata, rsp: &ReadStatPath) -> String {
        use crate::rs_var::ReadStatVarFormatClass;
        use std::fmt::Write as _;

        let mut out = String::new();
        // Writing to a String is infallible; the `let _ =` discards the Result.
        let _ = writeln!(out, "Metadata for the file {}\n", rsp.path.to_string_lossy());
        let _ = writeln!(out, "Row count: {}", md.row_count);
        let _ = writeln!(out, "Variable count: {}", md.var_count);
        let _ = writeln!(out, "Table name: {}", md.table_name);
        let _ = writeln!(out, "Table label: {}", md.file_label);
        let _ = writeln!(out, "File encoding: {}", md.file_encoding);
        let _ = writeln!(out, "Format version: {}", md.version);
        let _ = writeln!(
            out,
            "Bitness: {}",
            if md.is_64bit { "64-bit" } else { "32-bit" }
        );
        let _ = writeln!(out, "Creation time: {}", md.creation_time);
        let _ = writeln!(out, "Modified time: {}", md.modified_time);
        let _ = writeln!(out, "Compression: {:#?}", md.compression);
        let _ = writeln!(out, "Byte order: {:#?}", md.endianness);
        let _ = writeln!(out, "Variable names:");
        for (k, v) in &md.vars {
            let format_class = v.var_format_class.as_ref().map_or("", |f| match f {
                ReadStatVarFormatClass::Date => "Date",
                ReadStatVarFormatClass::DateTime
                | ReadStatVarFormatClass::DateTimeWithMilliseconds
                | ReadStatVarFormatClass::DateTimeWithMicroseconds
                | ReadStatVarFormatClass::DateTimeWithNanoseconds => "DateTime",
                ReadStatVarFormatClass::Time
                | ReadStatVarFormatClass::TimeWithMilliseconds
                | ReadStatVarFormatClass::TimeWithMicroseconds
                | ReadStatVarFormatClass::TimeWithNanoseconds => "Time",
            });
            let data_type = md.schema.fields[*k as usize].data_type();
            let _ = writeln!(
                out,
                "{k}: {} {{ type class: {:#?}, type: {:#?}, label: {}, format class: {format_class}, format: {}, arrow data type: {data_type:#?} }}",
                v.var_name, v.var_type_class, v.var_type, v.var_label, v.var_format,
            );
        }

        out
    }
}

/// Total rows written so far, as tracked by the shared row counter on `d`.
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
fn rows_written(d: &ReadStatData) -> usize {
    d.total_rows_processed
        .as_ref()
        .map_or(0, |trp| trp.load(std::sync::atomic::Ordering::SeqCst))
}

/// Escapes a single CSV field per RFC 4180: if it contains a comma, double
/// quote, CR, or LF, wrap it in double quotes and double any interior quotes.
#[cfg(feature = "csv")]
fn csv_escape_field(field: &str) -> String {
    if field.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Serialize a [`RecordBatch`](arrow_array::RecordBatch) to CSV bytes (with header).
///
/// # Errors
///
/// Returns an error if CSV writing fails.
#[cfg(feature = "csv")]
pub fn write_batch_to_csv_bytes(
    batch: &arrow_array::RecordBatch,
) -> Result<Vec<u8>, ReadStatError> {
    let mut buf = Vec::new();
    let mut writer = CsvWriterBuilder::new().with_header(true).build(&mut buf);
    writer.write(batch)?;
    drop(writer);
    Ok(buf)
}

/// Serialize a [`RecordBatch`](arrow_array::RecordBatch) to NDJSON bytes.
///
/// # Errors
///
/// Returns an error if JSON writing fails.
#[cfg(feature = "ndjson")]
pub fn write_batch_to_ndjson_bytes(
    batch: &arrow_array::RecordBatch,
) -> Result<Vec<u8>, ReadStatError> {
    let mut buf = Vec::new();
    let mut writer = JsonLineDelimitedWriter::new(&mut buf);
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Serialize a [`RecordBatch`](arrow_array::RecordBatch) to Parquet bytes with Snappy compression.
///
/// # Errors
///
/// Returns an error if Parquet writing fails.
#[cfg(feature = "parquet")]
pub fn write_batch_to_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>, ReadStatError> {
    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(ParquetCompressionCodec::SNAPPY)
        .build();
    let mut writer = ParquetArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(buf)
}

/// Serialize a [`RecordBatch`](arrow_array::RecordBatch) to Feather (Arrow IPC) bytes.
///
/// # Errors
///
/// Returns an error if Feather/IPC writing fails.
#[cfg(feature = "feather")]
pub fn write_batch_to_feather_bytes(
    batch: &arrow_array::RecordBatch,
) -> Result<Vec<u8>, ReadStatError> {
    let mut buf = Vec::new();
    let mut writer = IpcFileWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- resolve_compression ---

    #[test]
    fn resolve_compression_none_defaults_to_snappy() {
        let codec = ReadStatWriter::resolve_compression(None, None).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::SNAPPY));
    }

    #[test]
    fn resolve_compression_uncompressed() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Uncompressed), None)
                .unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::UNCOMPRESSED));
    }

    #[test]
    fn resolve_compression_snappy() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Snappy), None).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::SNAPPY));
    }

    #[test]
    fn resolve_compression_lz4raw() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Lz4Raw), None).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::LZ4_RAW));
    }

    #[test]
    fn resolve_compression_gzip_default() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Gzip), None).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::GZIP(_)));
    }

    #[test]
    fn resolve_compression_gzip_with_level() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Gzip), Some(5)).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::GZIP(_)));
    }

    #[test]
    fn resolve_compression_brotli_default() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Brotli), None).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::BROTLI(_)));
    }

    #[test]
    fn resolve_compression_brotli_with_level() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Brotli), Some(8)).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::BROTLI(_)));
    }

    #[test]
    fn resolve_compression_zstd_default() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Zstd), None).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::ZSTD(_)));
    }

    #[test]
    fn resolve_compression_zstd_with_level() {
        let codec =
            ReadStatWriter::resolve_compression(Some(ParquetCompression::Zstd), Some(15)).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::ZSTD(_)));
    }

    // --- ReadStatWriter::new ---

    #[test]
    fn new_writer_defaults() {
        let wtr = ReadStatWriter::new();
        assert!(wtr.wtr.is_none());
        assert!(!wtr.wrote_header);
        assert!(!wtr.wrote_start);
    }

    // --- csv_escape_field ---

    #[cfg(feature = "csv")]
    #[test]
    fn csv_escape_field_cases() {
        // Plain names pass through untouched.
        assert_eq!(csv_escape_field("Brand"), "Brand");
        // A comma forces quoting.
        assert_eq!(csv_escape_field("a,b"), "\"a,b\"");
        // Interior quotes are doubled and the field is wrapped.
        assert_eq!(csv_escape_field("a\"b"), "\"a\"\"b\"");
        // Newlines/CR force quoting too.
        assert_eq!(csv_escape_field("a\nb"), "\"a\nb\"");
        assert_eq!(csv_escape_field("a\rb"), "\"a\rb\"");
    }
}
