use arrow_array::RecordBatch;
use arrow_csv::WriterBuilder as CsvWriterBuilder;
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use arrow_json::LineDelimitedWriter as JsonLineDelimitedWriter;
use arrow_schema::Schema;
use parquet::{
    arrow::ArrowWriter as ParquetArrowWriter,
    basic::{BrotliLevel, Compression as ParquetCompressionCodec, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use std::sync::Arc;
use colored::Colorize;
use num_format::Locale;
use num_format::ToFormattedString;
use std::{fs::{self, OpenOptions, File}, io::{stdout, BufWriter, Seek, SeekFrom}, path::PathBuf};
use tempfile::SpooledTempFile;

use crate::err::ReadStatError;
use crate::rs_data::ReadStatData;
use crate::rs_metadata::ReadStatMetadata;
use crate::rs_path::ReadStatPath;
use crate::rs_var::ReadStatVarFormatClass;
use crate::OutFormat;

pub struct ReadStatParquetWriter {
    wtr: Option<ParquetArrowWriter<BufWriter<std::fs::File>>>,
}

impl ReadStatParquetWriter {
    fn new(wtr: ParquetArrowWriter<BufWriter<std::fs::File>>) -> Self {
        Self { wtr: Some(wtr) }
    }
}

pub enum ReadStatWriterFormat {
    Csv(BufWriter<std::fs::File>),
    CsvStdout(std::io::Stdout),
    Feather(IpcFileWriter<BufWriter<std::fs::File>>),
    Ndjson(BufWriter<std::fs::File>),
    Parquet(ReadStatParquetWriter),
}

#[derive(Default)]
pub struct ReadStatWriter {
    pub wtr: Option<ReadStatWriterFormat>,
    pub wrote_header: bool,
    pub wrote_start: bool,
}

impl ReadStatWriter {
    pub fn new() -> Self {
        Self {
            wtr: None,
            wrote_header: false,
            wrote_start: false,
        }
    }

    /// Write a single batch to a Parquet file (for parallel writes)
    /// Uses SpooledTempFile to keep data in memory until buffer_size_bytes threshold
    pub fn write_batch_to_parquet(
        batch: &RecordBatch,
        schema: &Schema,
        output_path: &PathBuf,
        compression: Option<crate::ParquetCompression>,
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
    pub fn merge_parquet_files(
        temp_files: &[PathBuf],
        output_path: &PathBuf,
        schema: &Schema,
        compression: Option<crate::ParquetCompression>,
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

    fn resolve_compression(
        compression: Option<crate::ParquetCompression>,
        compression_level: Option<u32>,
    ) -> Result<ParquetCompressionCodec, ReadStatError> {
        let codec = match compression {
            Some(crate::ParquetCompression::Uncompressed) => ParquetCompressionCodec::UNCOMPRESSED,
            Some(crate::ParquetCompression::Snappy) => ParquetCompressionCodec::SNAPPY,
            Some(crate::ParquetCompression::Gzip) => {
                if let Some(level) = compression_level {
                    let gzip_level = GzipLevel::try_new(level)
                        .map_err(|e| ReadStatError::Other(format!("Invalid Gzip compression level: {}", e)))?;
                    ParquetCompressionCodec::GZIP(gzip_level)
                } else {
                    ParquetCompressionCodec::GZIP(GzipLevel::default())
                }
            },
            Some(crate::ParquetCompression::Lz4Raw) => ParquetCompressionCodec::LZ4_RAW,
            Some(crate::ParquetCompression::Brotli) => {
                if let Some(level) = compression_level {
                    let brotli_level = BrotliLevel::try_new(level)
                        .map_err(|e| ReadStatError::Other(format!("Invalid Brotli compression level: {}", e)))?;
                    ParquetCompressionCodec::BROTLI(brotli_level)
                } else {
                    ParquetCompressionCodec::BROTLI(BrotliLevel::default())
                }
            },
            Some(crate::ParquetCompression::Zstd) => {
                if let Some(level) = compression_level {
                    let zstd_level = ZstdLevel::try_new(level as i32)
                        .map_err(|e| ReadStatError::Other(format!("Invalid Zstd compression level: {}", e)))?;
                    ParquetCompressionCodec::ZSTD(zstd_level)
                } else {
                    ParquetCompressionCodec::ZSTD(ZstdLevel::default())
                }
            },
            None => ParquetCompressionCodec::SNAPPY,
        };
        Ok(codec)
    }

    pub fn finish(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        match rsp {
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } => self.finish_txt(d, rsp),
            // Write feather data to file
            ReadStatPath {
                format: OutFormat::feather,
                ..
            } => self.finish_feather(d, rsp),
            // Write ndjson data to file
            ReadStatPath {
                format: OutFormat::ndjson,
                ..
            } => self.finish_txt(d, rsp),
            // Write parquet data to file
            ReadStatPath {
                format: OutFormat::parquet,
                ..
            } => self.finish_parquet(d, rsp),
            _ => Ok(()),
        }
    }

    fn _write_message_for_file(&mut self, d: &ReadStatData, rsp: &ReadStatPath) {
        if let Some(pb) = &d.pb {
            let in_f = if let Some(f) = rsp.path.file_name() {
                f.to_string_lossy().bright_red()
            } else {
                String::from("___").bright_red()
            };

            let out_f = if let Some(p) = &rsp.out_path {
                if let Some(f) = p.file_name() {
                    f.to_string_lossy().bright_green()
                } else {
                    String::from("___").bright_green()
                }
            } else {
                String::from("___").bright_green()
            };

            let msg = format!("Writing file {} as {}", in_f, out_f);

            pb.set_message(msg);
        }
    }

    fn write_message_for_rows(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        // Only print messages if there's no progress bar
        // If there's a progress bar, it will handle showing progress
        if d.pb.is_none() {
            let in_f = if let Some(f) = rsp.path.file_name() {
                f.to_string_lossy().bright_red()
            } else {
                String::from("___").bright_red()
            };

            let out_f = if let Some(p) = &rsp.out_path {
                if let Some(f) = p.file_name() {
                    f.to_string_lossy().bright_green()
                } else {
                    String::from("___").bright_green()
                }
            } else {
                String::from("___").bright_green()
            };

            let rows = d
                .chunk_rows_processed
                .to_formatted_string(&Locale::en)
                .truecolor(255, 132, 0);

            let msg = format!("Wrote {} rows from file {} into {}", rows, in_f, out_f);

            println!("{}", msg);
        }
        Ok(())
    }

    fn finish_txt(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        //if let Some(pb) = &d.pb {
        let in_f = if let Some(f) = rsp.path.file_name() {
            f.to_string_lossy().bright_red()
        } else {
            String::from("___").bright_red()
        };

        let out_f = if let Some(p) = &rsp.out_path {
            if let Some(f) = p.file_name() {
                f.to_string_lossy().bright_green()
            } else {
                String::from("___").bright_green()
            }
        } else {
            String::from("___").bright_green()
        };

        let rows = if let Some(trp) = &d.total_rows_processed {
            trp.load(std::sync::atomic::Ordering::SeqCst)
                .to_formatted_string(&Locale::en)
                .truecolor(255, 132, 0)
        } else {
            0.to_formatted_string(&Locale::en).truecolor(255, 132, 0)
        };

        let msg = format!(
            "In total, wrote {} rows from file {} into {}",
            rows, in_f, out_f
        );

        println!("{}", msg);

        //pb.set_message(msg);
        //}
        Ok(())
    }

    pub fn write(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        match rsp {
            // Write data to standard out
            ReadStatPath {
                out_path: None,
                format: OutFormat::csv,
                ..
            } if self.wrote_header => self.write_data_to_stdout(d),
            // Write header and data to standard out
            ReadStatPath {
                out_path: None,
                format: OutFormat::csv,
                ..
            } => {
                self.write_header_to_stdout(d)?;
                self.write_data_to_stdout(d)
            }
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } if self.wrote_header => self.write_data_to_csv(d, rsp),
            // Write csv header to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } => {
                self.write_header_to_csv(d, rsp)?;
                self.write_data_to_csv(d, rsp)
            }
            // Write feather data to file
            ReadStatPath {
                format: OutFormat::feather,
                ..
            } => self.write_data_to_feather(d, rsp),
            // Write ndjson data to file
            ReadStatPath {
                format: OutFormat::ndjson,
                ..
            } => self.write_data_to_ndjson(d, rsp),
            // Write parquet data to file
            ReadStatPath {
                format: OutFormat::parquet,
                ..
            } => self.write_data_to_parquet(d, rsp),
        }
    }

    fn write_data_to_csv(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

            // setup writer with BufWriter for better performance
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Csv(BufWriter::new(f)))
            };

            // write
            if let Some(ReadStatWriterFormat::Csv(f)) = &mut self.wtr {
                if let Some(batch) = &d.batch {
                    // Build writer without header (header written separately)
                    let mut writer = CsvWriterBuilder::new()
                        .with_header(false)
                        .build(f);
                    writer.write(batch)?;
                };

                // update
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

    fn write_data_to_feather(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

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

                // update
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

    fn finish_feather(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.wtr {
            wtr.finish()?;

            // set message for what is being read/written
            self.finish_txt(d, rsp)?;

            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing feather as associated writer is not for the feather format".to_string(),
            ))
        }
    }

    fn write_data_to_ndjson(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

            // setup writer with BufWriter for better performance
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Ndjson(BufWriter::new(f)));
            };

            // write
            if let Some(ReadStatWriterFormat::Ndjson(f)) = &mut self.wtr {
                if let Some(batch) = &d.batch {
                    // Create a line-delimited JSON writer
                    let mut writer = JsonLineDelimitedWriter::new(f);
                    writer.write(batch)?;
                    writer.finish()?;
                }

                // update
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

    fn write_data_to_parquet(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

            // setup writer
            if !self.wrote_start {
                let compression_codec = Self::resolve_compression(rsp.compression, rsp.compression_level)?;

                let props = WriterProperties::builder()
                    .set_compression(compression_codec)
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                    .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                    .build();

                // Use BufWriter for better performance
                let wtr = ParquetArrowWriter::try_new(BufWriter::new(f), Arc::new(d.schema.clone()), Some(props))?;

                self.wtr = Some(ReadStatWriterFormat::Parquet(ReadStatParquetWriter::new(wtr)));
            }

            // write
            if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.wtr {
                if let Some(batch) = &d.batch
                    && let Some(ref mut wtr) = pwtr.wtr {
                        wtr.write(batch)?;
                }

                // update
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

    fn finish_parquet(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.wtr {
            // Take ownership of the writer to close it
            if let Some(wtr) = pwtr.wtr.take() {
                wtr.close()?;
            }

            // set message for what is being read/written
            self.finish_txt(d, rsp)?;

            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing parquet as associated writer is not for the parquet format".to_string(),
            ))
        }
    }

    fn write_data_to_stdout(
        &mut self,
        d: &ReadStatData,
    ) -> Result<(), ReadStatError> {
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
                // Build writer without header (header written separately)
                let mut writer = CsvWriterBuilder::new()
                    .with_header(false)
                    .build(f);
                writer.write(batch)?;
            };

            // update
            self.wrote_start = true;

            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing to csv as associated writer is not for the csv format".to_string(),
            ))
        }
    }

    fn write_header_to_csv(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        if let Some(p) = &rsp.out_path {
            // create file
            let mut f = std::fs::File::create(p)?;

            // Get variable names
            let vars: Vec<String> = d.vars.values().map(|m| m.var_name.clone()).collect();

            // write header manually as CSV line
            use std::io::Write;
            writeln!(f, "{}", vars.join(","))?;

            // wrote header
            self.wrote_header = true;

            // return
            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing csv as output path is set to None".to_string(),
            ))
        }
    }

    fn write_header_to_stdout(
        &mut self,
        d: &ReadStatData,
    ) -> Result<(), ReadStatError> {
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        // Get variable names
        let vars: Vec<String> = d.vars.values().map(|m| m.var_name.clone()).collect();

        // write header manually as CSV line
        println!("{}", vars.join(","));

        // wrote header
        self.wrote_header = true;

        // return
        Ok(())
    }

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

    pub fn write_metadata_to_json(
        &self,
        md: &ReadStatMetadata,
    ) -> Result<(), ReadStatError> {
        let s = serde_json::to_string_pretty(md)?;
        println!("{}", s);
        Ok(())
    }

    pub fn write_metadata_to_stdout(
        &self,
        md: &ReadStatMetadata,
        rsp: &ReadStatPath,
    ) -> Result<(), ReadStatError> {
        println!(
            "Metadata for the file {}\n",
            rsp.path.to_string_lossy().bright_yellow()
        );
        println!(
            "{}: {}",
            "Row count".green(),
            md.row_count.to_formatted_string(&Locale::en)
        );
        println!(
            "{}: {}",
            "Variable count".red(),
            md.var_count.to_formatted_string(&Locale::en)
        );
        println!("{}: {}", "Table name".blue(), md.table_name);
        println!("{}: {}", "Table label".cyan(), md.file_label);
        println!("{}: {}", "File encoding".yellow(), md.file_encoding);
        println!("{}: {}", "Format version".green(), md.version);
        println!(
            "{}: {}",
            "Bitness".red(),
            if md.is64bit == 0 { "32-bit" } else { "64-bit" }
        );
        println!("{}: {}", "Creation time".blue(), md.creation_time);
        println!("{}: {}", "Modified time".cyan(), md.modified_time);
        println!("{}: {:#?}", "Compression".yellow(), md.compression);
        println!("{}: {:#?}", "Byte order".green(), md.endianness);
        println!("{}:", "Variable names".purple());
        for (k, v) in md.vars.iter() {
            println!(
                "{}: {} {{ type class: {}, type: {}, label: {}, format class: {}, format: {}, arrow logical data type: {}, arrow physical data type: {} }}",
                (*k).to_formatted_string(&Locale::en),
                v.var_name.bright_purple(),
                format!("{:#?}", v.var_type_class).bright_green(),
                format!("{:#?}", v.var_type).bright_red(),
                v.var_label.bright_blue(),
                (match &v.var_format_class {
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
                })
                .bright_cyan(),
                v.var_format.bright_yellow(),
                format!("{:#?}", md.schema.fields[*k as usize].data_type()).bright_green(),
                format!("{:#?}", md.schema.fields[*k as usize].data_type()).bright_red(),
            );
        }

        Ok(())
    }
}
