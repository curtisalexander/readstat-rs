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
    arrow::ArrowWriter as ParquetArrowWriter,
    basic::Compression as ParquetCompressionCodec,
    file::properties::WriterProperties,
};
#[cfg(feature = "parquet")]
use std::sync::Arc;
#[cfg(any(feature = "csv", feature = "feather", feature = "ndjson", feature = "parquet"))]
use std::fs::{OpenOptions, File};
#[cfg(feature = "parquet")]
use std::fs;
#[cfg(any(feature = "csv", feature = "feather", feature = "ndjson", feature = "parquet"))]
use std::io::BufWriter;
#[cfg(feature = "csv")]
use std::io::stdout;
#[cfg(any(feature = "csv", feature = "feather", feature = "ndjson", feature = "parquet"))]
use std::path::PathBuf;
#[cfg(feature = "parquet")]
use std::io::{Seek, SeekFrom};
#[cfg(feature = "parquet")]
use tempfile::SpooledTempFile;

use crate::err::ReadStatError;
use crate::rs_data::ReadStatData;
use crate::rs_metadata::ReadStatMetadata;
use crate::rs_path::ReadStatPath;
#[cfg(any(feature = "csv", feature = "feather", feature = "ndjson", feature = "parquet"))]
use crate::rs_write_config::OutFormat;
use crate::rs_write_config::WriteConfig;
#[cfg(feature = "parquet")]
use crate::rs_write_config::ParquetCompression;

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
    pub fn new() -> Self {
        Self {
            wtr: None,
            wrote_header: false,
            wrote_start: false,
        }
    }

    /// Opens an output file: creates or truncates on first write, appends on subsequent writes.
    #[cfg(any(feature = "csv", feature = "feather", feature = "ndjson", feature = "parquet"))]
    fn open_output(&self, path: &PathBuf) -> Result<File, ReadStatError> {
        let f = if self.wrote_start {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?
        } else {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?
        };
        Ok(f)
    }

    /// Write a single batch to a Parquet file (for parallel writes)
    /// Uses SpooledTempFile to keep data in memory until buffer_size_bytes threshold
    #[cfg(feature = "parquet")]
    pub fn write_batch_to_parquet(
        batch: &RecordBatch,
        schema: &Schema,
        output_path: &PathBuf,
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
        let mut wtr = ParquetArrowWriter::try_new(
            &mut spooled_file,
            Arc::new(schema.clone()),
            Some(props)
        )?;

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

    /// Merge multiple Parquet files into one by reading and rewriting all batches
    #[cfg(feature = "parquet")]
    pub fn merge_parquet_files(
        temp_files: &[PathBuf],
        output_path: &PathBuf,
        schema: &Schema,
        compression: Option<ParquetCompression>,
        compression_level: Option<u32>,
    ) -> Result<(), ReadStatError> {
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

        let mut writer = ParquetArrowWriter::try_new(
            BufWriter::new(f),
            Arc::new(schema.clone()),
            Some(props)
        )?;

        // Read each temp file and write its batches to the final file
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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

    /// Finalizes the writer, flushing any remaining data and printing a summary.
    ///
    /// `in_path` is used for display messages showing the source file name.
    #[allow(unused_variables)]
    pub fn finish(
        &mut self,
        d: &ReadStatData,
        wc: &WriteConfig,
        in_path: &std::path::Path,
    ) -> Result<(), ReadStatError> {
        match wc.format {
            #[cfg(feature = "csv")]
            OutFormat::csv => {
                self.print_finish_message(d, wc, in_path);
                Ok(())
            }
            #[cfg(feature = "feather")]
            OutFormat::feather => {
                self.finish_feather()?;
                self.print_finish_message(d, wc, in_path);
                Ok(())
            }
            #[cfg(feature = "ndjson")]
            OutFormat::ndjson => {
                self.print_finish_message(d, wc, in_path);
                Ok(())
            }
            #[cfg(feature = "parquet")]
            OutFormat::parquet => {
                self.finish_parquet()?;
                self.print_finish_message(d, wc, in_path);
                Ok(())
            }
            #[allow(unreachable_patterns)]
            _ => Err(ReadStatError::Other(format!(
                "Output format {:?} is not enabled. Enable the corresponding feature flag.",
                wc.format
            ))),
        }
    }

    #[cfg(any(feature = "csv", feature = "feather", feature = "ndjson", feature = "parquet"))]
    fn print_finish_message(&self, d: &ReadStatData, wc: &WriteConfig, in_path: &std::path::Path) {
        let rows = if let Some(trp) = &d.total_rows_processed {
            trp.load(std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };

        let in_f = in_path.file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| "___".to_string());

        let out_f = wc.out_path.as_ref()
            .and_then(|p| p.file_name())
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| "___".to_string());

        let rows_formatted = format_with_commas(rows);
        println!("In total, wrote {} rows from file {} into {}", rows_formatted, in_f, out_f);
    }

    #[cfg(feature = "feather")]
    fn finish_feather(&mut self) -> Result<(), ReadStatError> {
        if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.wtr {
            wtr.finish()?;
            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing feather as associated writer is not for the feather format".to_string(),
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
                "Error writing parquet as associated writer is not for the parquet format".to_string(),
            ))
        }
    }

    /// Writes a single batch of data in the format determined by `wc`.
    ///
    /// Handles writer initialization on first call and CSV header writing.
    #[allow(unused_variables)]
    pub fn write(
        &mut self,
        d: &ReadStatData,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        match wc.format {
            #[cfg(feature = "csv")]
            OutFormat::csv => {
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
            OutFormat::feather => self.write_data_to_feather(d, wc),
            #[cfg(feature = "ndjson")]
            OutFormat::ndjson => self.write_data_to_ndjson(d, wc),
            #[cfg(feature = "parquet")]
            OutFormat::parquet => self.write_data_to_parquet(d, wc),
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
            let f = self.open_output(p)?;

            // setup writer with BufWriter for better performance
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Csv(BufWriter::new(f)))
            };

            // write
            if let Some(ReadStatWriterFormat::Csv(f)) = &mut self.wtr {
                if let Some(batch) = &d.batch {
                    let include_header = !self.wrote_header;
                    let mut writer = CsvWriterBuilder::new()
                        .with_header(include_header)
                        .build(f);
                    writer.write(batch)?;
                    self.wrote_header = true;
                };

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
            let f = self.open_output(p)?;

            // setup writer with BufWriter for better performance
            if !self.wrote_start {
                let wtr = IpcFileWriter::try_new(BufWriter::new(f), &d.schema)?;
                self.wtr = Some(ReadStatWriterFormat::Feather(wtr));
            };

            // write
            if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.wtr {
                if let Some(batch) = &d.batch {
                    wtr.write(batch)?;
                };

                self.wrote_start = true;

                Ok(())
            } else {
                Err(ReadStatError::Other(
                    "Error writing feather as associated writer is not for the feather format".to_string(),
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
            let f = self.open_output(p)?;

            // setup writer with BufWriter for better performance
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Ndjson(BufWriter::new(f)));
            };

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
                    "Error writing ndjson as associated writer is not for the ndjson format".to_string(),
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
            let f = self.open_output(p)?;

            // setup writer
            if !self.wrote_start {
                let compression_codec = Self::resolve_compression(wc.compression, wc.compression_level)?;

                let props = WriterProperties::builder()
                    .set_compression(compression_codec)
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                    .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                    .build();

                let wtr = ParquetArrowWriter::try_new(BufWriter::new(f), d.schema.clone(), Some(props))?;

                self.wtr = Some(ReadStatWriterFormat::Parquet(ReadStatParquetWriter::new(wtr)));
            }

            // write
            if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.wtr {
                if let Some(batch) = &d.batch
                    && let Some(ref mut wtr) = pwtr.wtr {
                        wtr.write(batch)?;
                }

                self.wrote_start = true;

                Ok(())
            } else {
                Err(ReadStatError::Other(
                    "Error writing parquet as associated writer is not for the parquet format".to_string(),
                ))
            }
        } else {
            Err(ReadStatError::Other(
                "Error writing parquet file as output path is set to None".to_string(),
            ))
        }
    }

    #[cfg(feature = "csv")]
    fn write_data_to_stdout(
        &mut self,
        d: &ReadStatData,
    ) -> Result<(), ReadStatError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        // writer setup
        if !self.wrote_start {
            self.wtr = Some(ReadStatWriterFormat::CsvStdout(stdout()));
        };

        // write
        if let Some(ReadStatWriterFormat::CsvStdout(f)) = &mut self.wtr {
            if let Some(batch) = &d.batch {
                let mut writer = CsvWriterBuilder::new()
                    .with_header(false)
                    .build(f);
                writer.write(batch)?;
            };

            self.wrote_start = true;

            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing to csv as associated writer is not for the csv format".to_string(),
            ))
        }
    }


    #[cfg(feature = "csv")]
    fn write_header_to_stdout(
        &mut self,
        d: &ReadStatData,
    ) -> Result<(), ReadStatError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        let vars: Vec<String> = d.vars.values().map(|m| m.var_name.clone()).collect();

        println!("{}", vars.join(","));

        self.wrote_header = true;

        Ok(())
    }

    /// Writes metadata to stdout (pretty-printed) or as JSON.
    pub fn write_metadata(
        &self,
        md: &ReadStatMetadata,
        rsp: &ReadStatPath,
        as_json: bool,
    ) -> Result<(), ReadStatError> {
        if as_json {
            self.write_metadata_to_json(md)
        } else {
            self.write_metadata_to_stdout(md, rsp)
        }
    }

    /// Serializes metadata as pretty-printed JSON and writes to stdout.
    pub fn write_metadata_to_json(
        &self,
        md: &ReadStatMetadata,
    ) -> Result<(), ReadStatError> {
        let s = serde_json::to_string_pretty(md)?;
        println!("{}", s);
        Ok(())
    }

    /// Writes metadata to stdout in a human-readable format.
    pub fn write_metadata_to_stdout(
        &self,
        md: &ReadStatMetadata,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        use crate::rs_var::ReadStatVarFormatClass;

        println!(
            "Metadata for the file {}\n",
            rsp.path.to_string_lossy()
        );
        println!("Row count: {}", md.row_count);
        println!("Variable count: {}", md.var_count);
        println!("Table name: {}", md.table_name);
        println!("Table label: {}", md.file_label);
        println!("File encoding: {}", md.file_encoding);
        println!("Format version: {}", md.version);
        println!(
            "Bitness: {}",
            if md.is64bit == 0 { "32-bit" } else { "64-bit" }
        );
        println!("Creation time: {}", md.creation_time);
        println!("Modified time: {}", md.modified_time);
        println!("Compression: {:#?}", md.compression);
        println!("Byte order: {:#?}", md.endianness);
        println!("Variable names:");
        for (k, v) in md.vars.iter() {
            println!(
                "{}: {} {{ type class: {:#?}, type: {:#?}, label: {}, format class: {}, format: {}, arrow logical data type: {:#?}, arrow physical data type: {:#?} }}",
                k,
                v.var_name,
                v.var_type_class,
                v.var_type,
                v.var_label,
                match &v.var_format_class {
                    Some(f) => match f {
                        ReadStatVarFormatClass::Date => "Date",
                        ReadStatVarFormatClass::DateTime |
                        ReadStatVarFormatClass::DateTimeWithMilliseconds |
                        ReadStatVarFormatClass::DateTimeWithMicroseconds |
                        ReadStatVarFormatClass::DateTimeWithNanoseconds => "DateTime",
                        ReadStatVarFormatClass::Time |
                        ReadStatVarFormatClass::TimeWithMicroseconds => "Time",
                    },
                    None => "",
                },
                v.var_format,
                md.schema.fields[*k as usize].data_type(),
                md.schema.fields[*k as usize].data_type(),
            );
        }

        Ok(())
    }
}

/// Serialize a [`RecordBatch`](arrow_array::RecordBatch) to CSV bytes (with header).
#[cfg(feature = "csv")]
pub fn write_batch_to_csv_bytes(batch: &arrow_array::RecordBatch) -> Result<Vec<u8>, ReadStatError> {
    let mut buf = Vec::new();
    let mut writer = CsvWriterBuilder::new().with_header(true).build(&mut buf);
    writer.write(batch)?;
    drop(writer);
    Ok(buf)
}

/// Serialize a [`RecordBatch`](arrow_array::RecordBatch) to NDJSON bytes.
#[cfg(feature = "ndjson")]
pub fn write_batch_to_ndjson_bytes(batch: &arrow_array::RecordBatch) -> Result<Vec<u8>, ReadStatError> {
    let mut buf = Vec::new();
    let mut writer = JsonLineDelimitedWriter::new(&mut buf);
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Serialize a [`RecordBatch`](arrow_array::RecordBatch) to Parquet bytes with Snappy compression.
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
#[cfg(feature = "feather")]
pub fn write_batch_to_feather_bytes(batch: &arrow_array::RecordBatch) -> Result<Vec<u8>, ReadStatError> {
    let mut buf = Vec::new();
    let mut writer = IpcFileWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Formats a number with comma thousands separators (e.g. 1081 -> "1,081").
#[cfg(any(feature = "csv", feature = "feather", feature = "ndjson", feature = "parquet"))]
fn format_with_commas(n: usize) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len <= 3 {
        return s;
    }
    let mut result = String::with_capacity(len + len / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(',');
        }
        result.push(b as char);
    }
    result
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
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Uncompressed),
            None,
        ).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::UNCOMPRESSED));
    }

    #[test]
    fn resolve_compression_snappy() {
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Snappy),
            None,
        ).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::SNAPPY));
    }

    #[test]
    fn resolve_compression_lz4raw() {
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Lz4Raw),
            None,
        ).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::LZ4_RAW));
    }

    #[test]
    fn resolve_compression_gzip_default() {
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Gzip),
            None,
        ).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::GZIP(_)));
    }

    #[test]
    fn resolve_compression_gzip_with_level() {
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Gzip),
            Some(5),
        ).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::GZIP(_)));
    }

    #[test]
    fn resolve_compression_brotli_default() {
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Brotli),
            None,
        ).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::BROTLI(_)));
    }

    #[test]
    fn resolve_compression_brotli_with_level() {
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Brotli),
            Some(8),
        ).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::BROTLI(_)));
    }

    #[test]
    fn resolve_compression_zstd_default() {
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Zstd),
            None,
        ).unwrap();
        assert!(matches!(codec, ParquetCompressionCodec::ZSTD(_)));
    }

    #[test]
    fn resolve_compression_zstd_with_level() {
        let codec = ReadStatWriter::resolve_compression(
            Some(ParquetCompression::Zstd),
            Some(15),
        ).unwrap();
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
}
